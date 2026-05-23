"""
ThetaData LiveMarketDataClient — wires HTTP, WS, and provider into the
Nautilus data plane.

Subscribe/unsubscribe route to ws_client; quote/trade request paths fan out
to http_client and decode rows into ticks. Bar requests validate
aggregation+price_type, then map to the ThetaData ivl (ms) parameter.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.constants import THETADATA, THETADATA_VENUE
from nautilus_trader.adapters.thetadata.decode import (
    rest_ohlc_row_to_bar,
    rest_quote_row_to_quote_tick,
    rest_trade_row_to_trade_tick,
)
from nautilus_trader.adapters.thetadata.http import ThetaDataHttpClient
from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider
from nautilus_trader.adapters.thetadata.symbology import decode_occ
from nautilus_trader.adapters.thetadata.ws import ThetaDataWsClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.data.messages import (
    RequestBars,
    RequestQuoteTicks,
    RequestTradeTicks,
    SubscribeQuoteTicks,
    SubscribeTradeTicks,
    UnsubscribeQuoteTicks,
    UnsubscribeTradeTicks,
)
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.enums import BarAggregation, PriceType
from nautilus_trader.model.identifiers import ClientId, InstrumentId


# Supported bar aggregations for ThetaData /v2/hist/stock/ohlc (ivl_ms).
_BAR_IVL_MS: dict[BarAggregation, int] = {
    BarAggregation.MINUTE: 60_000,
    BarAggregation.HOUR: 3_600_000,
    BarAggregation.DAY: 86_400_000,
}


def _contract_from_instrument_id(instrument_id: InstrumentId) -> dict:
    """Build the WS contract payload from an OCC-encoded instrument_id."""
    root, expiry, right, strike = decode_occ(instrument_id.symbol.value)
    # Wire strike scale is 1/10¢ (×10000); OCC carries thousandths so multiply by 10.
    strike_wire = int(Decimal(strike) * Decimal(10000))
    return {
        "security_type": "OPTION",
        "root": root,
        "expiration": expiry.year * 10000 + expiry.month * 100 + expiry.day,
        "strike": strike_wire,
        "right": right,
    }


class ThetaDataDataClient(LiveMarketDataClient):
    """LiveMarketDataClient for ThetaData (data-only)."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        http_client: ThetaDataHttpClient,
        ws_client: ThetaDataWsClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: ThetaDataInstrumentProvider,
        config: ThetaDataDataClientConfig | None = None,
        name: str | None = None,
    ) -> None:
        if config is None:
            config = ThetaDataDataClientConfig()
        PyCondition.type(config, ThetaDataDataClientConfig, "config")

        super().__init__(
            loop=loop,
            client_id=ClientId(name or THETADATA),
            venue=THETADATA_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )
        self._http_client = http_client
        self._ws_client = ws_client
        self._config_typed = config

        ws_client.set_quote_handler(self._handle_data)
        ws_client.set_trade_handler(self._handle_data)

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def _connect(self) -> None:
        # Pre-load configured instruments so subscribers find precision in the cache.
        if self._config_typed.instrument_ids:
            await self._instrument_provider.load_ids_async(self._config_typed.instrument_ids)
            for iid in self._config_typed.instrument_ids:
                self._cache_precision(iid)
        await self._ws_client.connect()

    async def _disconnect(self) -> None:
        await self._ws_client.close()
        await self._http_client.close()

    def _cache_precision(self, instrument_id: InstrumentId) -> None:
        inst = self._instrument_provider.find(instrument_id)
        if inst is not None:
            self._ws_client.cache_instrument(
                instrument_id,
                inst.price_precision,
                inst.size_precision,
            )

    async def _ensure_instrument(self, instrument_id: InstrumentId) -> None:
        if self._instrument_provider.find(instrument_id) is None:
            await self._instrument_provider.load_async(instrument_id)
        self._cache_precision(instrument_id)

    # -----------------------------------------------------------------------
    # Subscriptions
    # -----------------------------------------------------------------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        await self._ensure_instrument(command.instrument_id)
        contract = _contract_from_instrument_id(command.instrument_id)
        await self._ws_client.subscribe_quotes(command.instrument_id, contract)

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        await self._ensure_instrument(command.instrument_id)
        contract = _contract_from_instrument_id(command.instrument_id)
        await self._ws_client.subscribe_trades(command.instrument_id, contract)

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        await self._ws_client.unsubscribe_quotes(command.instrument_id)

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        await self._ws_client.unsubscribe_trades(command.instrument_id)

    # -----------------------------------------------------------------------
    # Historical requests
    # -----------------------------------------------------------------------

    @staticmethod
    def _date_int(dt) -> int:
        return dt.year * 10000 + dt.month * 100 + dt.day

    def _precision_for(self, instrument_id: InstrumentId) -> tuple[int, int]:
        inst = self._instrument_provider.find(instrument_id)
        if inst is None:
            return 2, 0  # OPRA defaults
        return inst.price_precision, inst.size_precision

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        await self._ensure_instrument(request.instrument_id)
        root, _, _, _ = decode_occ(request.instrument_id.symbol.value)
        rows = await self._http_client.hist_quotes(
            root, self._date_int(request.start), self._date_int(request.end)
        )
        price_precision, size_precision = self._precision_for(request.instrument_id)
        ts_init = self._clock.timestamp_ns()
        ticks = [
            rest_quote_row_to_quote_tick(
                row, request.instrument_id, price_precision, size_precision, ts_init
            )
            for row in rows
        ]
        self._handle_quote_ticks(
            request.instrument_id, ticks, request.correlation_id, request.start, request.end, request.params,
        )

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None:
        await self._ensure_instrument(request.instrument_id)
        root, _, _, _ = decode_occ(request.instrument_id.symbol.value)
        rows = await self._http_client.hist_trades(
            root, self._date_int(request.start), self._date_int(request.end)
        )
        price_precision, size_precision = self._precision_for(request.instrument_id)
        ts_init = self._clock.timestamp_ns()
        ticks = [
            rest_trade_row_to_trade_tick(
                row, request.instrument_id, price_precision, size_precision, ts_init
            )
            for row in rows
        ]
        self._handle_trade_ticks(
            request.instrument_id, ticks, request.correlation_id, request.start, request.end, request.params,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        bar_type = request.bar_type
        spec = bar_type.spec
        if spec.aggregation not in _BAR_IVL_MS:
            raise ValueError(
                f"Unsupported bar aggregation {spec.aggregation}; "
                f"supported: {list(_BAR_IVL_MS)}"
            )
        if spec.price_type != PriceType.LAST:
            raise ValueError(
                f"Unsupported price_type {spec.price_type}; only LAST is supported"
            )
        await self._ensure_instrument(bar_type.instrument_id)
        root, _, _, _ = decode_occ(bar_type.instrument_id.symbol.value)
        ivl_ms = _BAR_IVL_MS[spec.aggregation] * spec.step
        rows = await self._http_client.hist_ohlc(
            root, self._date_int(request.start), self._date_int(request.end), ivl_ms=ivl_ms
        )
        price_precision, _ = self._precision_for(bar_type.instrument_id)
        ts_init = self._clock.timestamp_ns()
        bars = [
            rest_ohlc_row_to_bar(row, bar_type, price_precision, ts_init)
            for row in rows
        ]
        self._handle_bars(
            bar_type, bars, request.correlation_id, request.start, request.end, request.params,
        )
