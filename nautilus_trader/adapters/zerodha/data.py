# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  See LICENSE for full text.
# -------------------------------------------------------------------------------------------------
"""
Python `LiveMarketDataClient` subclass for Zerodha.

Phase 8 minimum-viable surface: lets ``node.build()`` succeed and the strategy register
subscriptions. The actual subscribe → ticker WebSocket → strategy data flow requires
wiring the Rust ``ZerodhaDataDispatcher`` primitives through PyO3 — that pass is the
next milestone (the Rust trait impl in ``crates/adapters/zerodha/src/data_client.rs``
already covers the equivalent surface for purely-Rust consumers).
"""

import asyncio

from nautilus_trader.adapters.zerodha.config import ZerodhaDataClientConfig
from nautilus_trader.adapters.zerodha.constants import ZERODHA_VENUE
from nautilus_trader.adapters.zerodha.providers import ZerodhaInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
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
        self._config = config

    async def _connect(self) -> None:
        self._log.info("ZerodhaDataClient connected (Phase 8 stub — no live WS yet)")

    async def _disconnect(self) -> None:
        self._log.info("ZerodhaDataClient disconnected")

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        self._log.info(f"subscribe_quote_ticks({command.instrument_id}) — Phase 8 stub")

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        self._log.info(f"unsubscribe_quote_ticks({command.instrument_id})")

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        self._log.info(f"subscribe_trade_ticks({command.instrument_id}) — Phase 8 stub")

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        self._log.info(f"unsubscribe_trade_ticks({command.instrument_id})")

    async def _subscribe_order_book_snapshots(self, command: SubscribeOrderBook) -> None:
        self._log.info(f"subscribe_order_book_snapshots({command.instrument_id}) — Phase 8 stub")

    async def _unsubscribe_order_book_snapshots(self, command: UnsubscribeOrderBook) -> None:
        self._log.info(f"unsubscribe_order_book_snapshots({command.instrument_id})")

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        self._log.info(f"subscribe_bars({command.bar_type}) — Phase 8 stub")
