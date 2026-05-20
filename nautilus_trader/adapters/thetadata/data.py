# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.constants import THETADATA
from nautilus_trader.adapters.thetadata.constants import THETADATA_VENUE
from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import RequestInstrument
from nautilus_trader.data.messages import RequestInstruments
from nautilus_trader.data.messages import RequestQuoteTicks
from nautilus_trader.data.messages import RequestTradeTicks
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId


_BAR_AGGREGATION_TO_INTERVAL: dict[tuple[int, int], str] = {
    (BarAggregation.SECOND, 1): "1s",
    (BarAggregation.SECOND, 5): "5s",
    (BarAggregation.SECOND, 10): "10s",
    (BarAggregation.SECOND, 15): "15s",
    (BarAggregation.SECOND, 30): "30s",
    (BarAggregation.MINUTE, 1): "1m",
    (BarAggregation.MINUTE, 5): "5m",
    (BarAggregation.MINUTE, 10): "10m",
    (BarAggregation.MINUTE, 15): "15m",
    (BarAggregation.MINUTE, 30): "30m",
    (BarAggregation.HOUR, 1): "1h",
}


class ThetaDataDataClient(LiveMarketDataClient):
    """
    Provides a data client for the ThetaData market-data feed.

    Wraps the pyo3 ``ThetaDataHttpClient`` (historical REST) and
    ``ThetaDataWsClient`` (live streaming) and routes engine commands through
    them. Inbound WebSocket frames are decoded Rust-side and surfaced as
    Nautilus ticks via the registered handlers; historical requests bypass
    the WebSocket path entirely.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    http_client : nautilus_pyo3.ThetaDataHttpClient
        The pyo3 HTTP client used for historical / instrument-list requests.
    ws_client : nautilus_pyo3.ThetaDataWsClient
        The pyo3 WebSocket client used for live subscriptions.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : ThetaDataInstrumentProvider
        The instrument provider.
    config : ThetaDataDataClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        http_client: nautilus_pyo3.ThetaDataHttpClient,
        ws_client: nautilus_pyo3.ThetaDataWsClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: ThetaDataInstrumentProvider,
        config: ThetaDataDataClientConfig,
        name: str | None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or THETADATA),
            venue=THETADATA_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        self._config = config
        self._http = http_client
        self._ws = ws_client

        self._log.info(f"http_url={config.http_url}", LogColor.BLUE)
        self._log.info(f"ws_url={config.ws_url}", LogColor.BLUE)
        self._log.info(f"tier={config.tier}", LogColor.BLUE)
        self._log.info(f"http_timeout_secs={config.http_timeout_secs}", LogColor.BLUE)

        self._ws.set_quote_handler(self._on_pyo3_quote)
        self._ws.set_trade_handler(self._on_pyo3_trade)

    @property
    def instrument_provider(self) -> ThetaDataInstrumentProvider:
        return self._instrument_provider  # type: ignore[return-value]

    # -- CONNECTION -------------------------------------------------------------------------------

    async def _connect(self) -> None:
        if self._config.instrument_ids:
            await self.instrument_provider.load_ids_async(list(self._config.instrument_ids))
            self._send_all_instruments_to_data_engine()

        await self._ws.connect()
        self._log.info(f"Connected to {self._config.ws_url}", LogColor.BLUE)

    async def _disconnect(self) -> None:
        await self._ws.close()
        self._log.info(f"Disconnected from {self._config.ws_url}", LogColor.BLUE)

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self.instrument_provider.get_all().values():
            self._handle_data(instrument)

    # -- SUBSCRIPTIONS ----------------------------------------------------------------------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        instrument_id = command.instrument_id
        await self._ensure_instrument_loaded(instrument_id)
        pyo3_id, p_prec, s_prec = self._instrument_metadata(instrument_id)
        if pyo3_id is None:
            return
        self._ws.cache_instrument(pyo3_id, p_prec, s_prec)
        await self._ws.subscribe_quotes(pyo3_id)

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        instrument_id = command.instrument_id
        await self._ensure_instrument_loaded(instrument_id)
        pyo3_id, p_prec, s_prec = self._instrument_metadata(instrument_id)
        if pyo3_id is None:
            return
        self._ws.cache_instrument(pyo3_id, p_prec, s_prec)
        await self._ws.subscribe_trades(pyo3_id)

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        pyo3_id = self._pyo3_instrument_id(command.instrument_id)
        await self._ws.unsubscribe_quotes(pyo3_id)

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        pyo3_id = self._pyo3_instrument_id(command.instrument_id)
        await self._ws.unsubscribe_trades(pyo3_id)

    # -- REQUESTS ---------------------------------------------------------------------------------

    async def _request_instruments(self, request: RequestInstruments) -> None:
        instruments = list(self.instrument_provider.get_all().values())
        self._handle_instruments(
            request.venue,
            instruments,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_instrument(self, request: RequestInstrument) -> None:
        instrument_id = request.instrument_id
        await self._ensure_instrument_loaded(instrument_id)
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.warning(f"Instrument {instrument_id} not found")
            return
        self._handle_instrument(
            instrument,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        instrument_id = request.instrument_id
        await self._ensure_instrument_loaded(instrument_id)
        pyo3_id, p_prec, s_prec = self._instrument_metadata(instrument_id)
        if pyo3_id is None:
            return
        start_str, end_str = self._date_range(request.start, request.end)
        try:
            pyo3_ticks = await self._http.hist_quotes(
                pyo3_id,
                start_str,
                end_str,
                "tick",
                p_prec,
                s_prec,
                request.limit or None,
            )
        except Exception as e:  # pragma: no cover - network failures
            self._log.exception(f"Failed to request quotes for {instrument_id}", e)
            return

        ticks = QuoteTick.from_pyo3_list(pyo3_ticks)
        self._handle_quote_ticks(
            instrument_id,
            ticks,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None:
        instrument_id = request.instrument_id
        await self._ensure_instrument_loaded(instrument_id)
        pyo3_id, p_prec, s_prec = self._instrument_metadata(instrument_id)
        if pyo3_id is None:
            return
        start_str, end_str = self._date_range(request.start, request.end)
        try:
            pyo3_ticks = await self._http.hist_trades(
                pyo3_id,
                start_str,
                end_str,
                p_prec,
                s_prec,
                request.limit or None,
            )
        except Exception as e:  # pragma: no cover - network failures
            self._log.exception(f"Failed to request trades for {instrument_id}", e)
            return

        ticks = TradeTick.from_pyo3_list(pyo3_ticks)
        self._handle_trade_ticks(
            instrument_id,
            ticks,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        bar_type = request.bar_type
        if bar_type.aggregation_source != AggregationSource.EXTERNAL:
            self._log.error(
                f"Cannot request {bar_type} bars: ThetaData only provides EXTERNAL aggregation",
            )
            return

        spec = bar_type.spec
        if spec.price_type != PriceType.LAST:
            self._log.error(
                f"Cannot request {bar_type} bars: ThetaData only supports price_type=LAST",
            )
            return

        interval = _BAR_AGGREGATION_TO_INTERVAL.get((spec.aggregation, spec.step))
        if interval is None:
            self._log.error(
                f"Cannot request {bar_type} bars: unsupported aggregation/step "
                f"({spec.aggregation}, {spec.step})",
            )
            return

        instrument_id = bar_type.instrument_id
        await self._ensure_instrument_loaded(instrument_id)
        _, p_prec, s_prec = self._instrument_metadata(instrument_id)

        pyo3_bar_type = nautilus_pyo3.BarType.from_str(str(bar_type))
        start_str, end_str = self._date_range(request.start, request.end)
        try:
            pyo3_bars = await self._http.hist_ohlc(
                pyo3_bar_type,
                start_str,
                end_str,
                interval,
                p_prec,
                s_prec,
                request.limit or None,
            )
        except Exception as e:  # pragma: no cover - network failures
            self._log.exception(f"Failed to request bars for {bar_type}", e)
            return

        bars = Bar.from_pyo3_list(pyo3_bars)
        self._handle_bars(
            bar_type,
            bars,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    # -- WEBSOCKET INBOUND ------------------------------------------------------------------------

    def _on_pyo3_quote(self, pyo3_tick: nautilus_pyo3.QuoteTick) -> None:
        try:
            self._handle_data(QuoteTick.from_pyo3(pyo3_tick))
        except Exception as e:  # pragma: no cover - defensive
            self._log.exception("Error handling inbound quote tick", e)

    def _on_pyo3_trade(self, pyo3_tick: nautilus_pyo3.TradeTick) -> None:
        try:
            self._handle_data(TradeTick.from_pyo3(pyo3_tick))
        except Exception as e:  # pragma: no cover - defensive
            self._log.exception("Error handling inbound trade tick", e)

    # -- HELPERS ----------------------------------------------------------------------------------

    async def _ensure_instrument_loaded(self, instrument_id: InstrumentId) -> None:
        if self._cache.instrument(instrument_id) is not None:
            return
        await self.instrument_provider.load_async(instrument_id)
        instrument = self.instrument_provider.find(instrument_id)
        if instrument is not None:
            self._handle_data(instrument)

    def _instrument_metadata(
        self,
        instrument_id: InstrumentId,
    ) -> tuple[nautilus_pyo3.InstrumentId | None, int, int]:
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.warning(
                f"No cached instrument for {instrument_id}; cannot derive precisions",
            )
            return None, 2, 0
        pyo3_id = self._pyo3_instrument_id(instrument_id)
        return pyo3_id, instrument.price_precision, instrument.size_precision

    @staticmethod
    def _pyo3_instrument_id(instrument_id: InstrumentId) -> nautilus_pyo3.InstrumentId:
        return nautilus_pyo3.InstrumentId.from_str(instrument_id.value)

    def _date_range(self, start, end) -> tuple[str, str]:
        if start is None or end is None:
            today = self._clock.utc_now().date().isoformat()
            return today, today
        return start.date().isoformat(), end.date().isoformat()
