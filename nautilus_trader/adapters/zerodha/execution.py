# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  See LICENSE for full text.
# -------------------------------------------------------------------------------------------------
"""
Python `LiveExecutionClient` subclass for Zerodha.

Phase 8 minimum-viable surface mirroring the data client. Lets ``node.build()`` succeed
when an exec client is registered. The actual submit/modify/cancel routing requires
PyO3 exposure of the Rust ``ZerodhaExecClient`` — landing next.
"""

import asyncio

from nautilus_trader.adapters.zerodha.config import ZerodhaExecClientConfig
from nautilus_trader.adapters.zerodha.constants import ZERODHA_VENUE
from nautilus_trader.adapters.zerodha.providers import ZerodhaInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.currencies import INR
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId


class ZerodhaExecutionClient(LiveExecutionClient):
    """
    Provides a Zerodha live execution client.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client_id: ClientId,
        account_id: AccountId,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: ZerodhaInstrumentProvider,
        config: ZerodhaExecClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or client_id.value),
            venue=ZERODHA_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=INR,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        self._account_id = account_id
        self._config = config

    async def _connect(self) -> None:
        self._log.info("ZerodhaExecutionClient connected (Phase 8 stub — no live REST yet)")

    async def _disconnect(self) -> None:
        self._log.info("ZerodhaExecutionClient disconnected")

    async def _submit_order(self, command: SubmitOrder) -> None:
        self._log.info(f"submit_order({command.client_order_id}) — Phase 8 stub")

    async def _modify_order(self, command: ModifyOrder) -> None:
        self._log.info(f"modify_order({command.client_order_id}) — Phase 8 stub")

    async def _cancel_order(self, command: CancelOrder) -> None:
        self._log.info(f"cancel_order({command.client_order_id}) — Phase 8 stub")
