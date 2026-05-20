# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  See LICENSE for full text.
# -------------------------------------------------------------------------------------------------
"""
Python `LiveMarketDataClient` subclass for Zerodha.

Delegates subscribe / unsubscribe / connect / disconnect to the Rust `PyZerodhaClient`
façade, which composes the existing `ZerodhaWsClient` + `ZerodhaDataDispatcher` +
forwarder. Tick data flows: Kite WS → Rust decoder → dispatcher mpsc sinks → forwarder
task → `nautilus_common::live::runner::get_data_event_sender()` → framework data engine
→ strategy `on_quote_tick` / `on_trade_tick` / `on_order_book` handlers.
"""

import asyncio

from nautilus_trader.adapters.zerodha.config import ZerodhaDataClientConfig
from nautilus_trader.adapters.zerodha.constants import ZERODHA_VENUE
from nautilus_trader.adapters.zerodha.providers import ZerodhaInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.data.messages import SubscribeBars
from nautilus_trader.data.messages import SubscribeOrderBook
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeOrderBook
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.identifiers import ClientId


class ZerodhaDataClient(LiveMarketDataClient):
    """
    Provides a Zerodha live market data client.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client_id: ClientId,
        rust_client: "nautilus_pyo3.zerodha.PyZerodhaClient",
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: ZerodhaInstrumentProvider,
        config: ZerodhaDataClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or client_id.value),
            venue=ZERODHA_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )
        self._rust = rust_client
        self._config = config

    async def _connect(self) -> None:
        await self._rust.connect()
        # Drain the Rust dispatcher's mpsc sinks via async polls; each tick is published
        # through `_handle_data` (the standard LiveMarketDataClient hook) so it lands in
        # `Strategy.on_quote_tick` / `on_trade_tick` / `on_order_book` like any other adapter.
        self._forwarders = [
            self._loop.create_task(self._forward_quotes()),
            self._loop.create_task(self._forward_trades()),
            self._loop.create_task(self._forward_depths()),
        ]
        self._log.info("ZerodhaDataClient connected to Kite ticker WebSocket")

    async def _disconnect(self) -> None:
        for task in getattr(self, "_forwarders", ()):
            task.cancel()
        await self._rust.close()
        self._log.info("ZerodhaDataClient disconnected")

    async def _forward_quotes(self) -> None:
        try:
            while True:
                tick = await self._rust.next_quote()
                if tick is None:
                    return
                self._handle_data(tick)
        except asyncio.CancelledError:
            return

    async def _forward_trades(self) -> None:
        try:
            while True:
                tick = await self._rust.next_trade()
                if tick is None:
                    return
                self._handle_data(tick)
        except asyncio.CancelledError:
            return

    async def _forward_depths(self) -> None:
        try:
            while True:
                depth = await self._rust.next_depth()
                if depth is None:
                    return
                self._handle_data(depth)
        except asyncio.CancelledError:
            return

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        await self._rust.subscribe_quotes(str(command.instrument_id))
        self._log.info(f"Subscribed quotes: {command.instrument_id}")

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        await self._rust.unsubscribe_quotes(str(command.instrument_id))

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        await self._rust.subscribe_trades(str(command.instrument_id))
        self._log.info(f"Subscribed trades: {command.instrument_id}")

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        await self._rust.unsubscribe_trades(str(command.instrument_id))

    async def _subscribe_order_book_snapshots(self, command: SubscribeOrderBook) -> None:
        await self._rust.subscribe_book(str(command.instrument_id))
        self._log.info(f"Subscribed book: {command.instrument_id}")

    async def _unsubscribe_order_book_snapshots(self, command: UnsubscribeOrderBook) -> None:
        await self._rust.unsubscribe_book(str(command.instrument_id))

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        # Phase 4 historical bars come via request_bars (request/response), not subscribe.
        self._log.warning(
            f"subscribe_bars({command.bar_type}) not supported — use request_bars instead",
        )
