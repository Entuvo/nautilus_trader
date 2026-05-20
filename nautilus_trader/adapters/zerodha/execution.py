# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  See LICENSE for full text.
# -------------------------------------------------------------------------------------------------
"""
Python `LiveExecutionClient` subclass for Zerodha.

Delegates the cancel path to the Rust `PyZerodhaClient`. submit / modify are still
stubs — they need the SubmitOrder → SubmitRequest field mapping (we have it in
Rust ``execution_client.rs``) re-exposed through PyZerodhaClient. Cancel works
because its mapping is trivial (just the client_order_id).
"""

import asyncio

from nautilus_trader.adapters.zerodha.config import ZerodhaExecClientConfig
from nautilus_trader.adapters.zerodha.constants import ZERODHA_VENUE
from nautilus_trader.adapters.zerodha.providers import ZerodhaInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core import nautilus_pyo3
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
        rust_client: "nautilus_pyo3.zerodha.PyZerodhaClient",
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
        self._rust = rust_client
        self._account_id = account_id
        self._config = config

    async def _connect(self) -> None:
        # The shared PyZerodhaClient also drives the data side which calls .connect() — we
        # don't open another WS here. The exec REST surface is already usable through
        # `submit_order` / `cancel_order` without an explicit connect step.
        self._log.info("ZerodhaExecutionClient ready")

    async def _disconnect(self) -> None:
        self._log.info("ZerodhaExecutionClient disconnected")

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order
        instrument = self._cache.instrument(command.instrument_id)
        price_precision = instrument.price_precision if instrument is not None else 2
        try:
            kite_order_id = await self._rust.submit_order(
                client_order_id=str(command.client_order_id),
                instrument_id=str(command.instrument_id),
                order_side=order.side.name,
                order_type=order.order_type.name,
                time_in_force=order.time_in_force.name,
                quantity=float(order.quantity),
                price=float(order.price) if getattr(order, "price", None) is not None else None,
                trigger_price=(
                    float(order.trigger_price)
                    if getattr(order, "trigger_price", None) is not None
                    else None
                ),
                product=self._config.default_product,
                price_precision=price_precision,
            )
            self._log.info(f"Submitted {command.client_order_id} → kite_order_id={kite_order_id}")
        except Exception as e:
            self._log.error(f"submit_order({command.client_order_id}) failed: {e}")

    async def _modify_order(self, command: ModifyOrder) -> None:
        instrument = self._cache.instrument(command.instrument_id)
        price_precision = instrument.price_precision if instrument is not None else 2
        try:
            await self._rust.modify_order(
                client_order_id=str(command.client_order_id),
                quantity=float(command.quantity) if command.quantity is not None else None,
                price=float(command.price) if command.price is not None else None,
                trigger_price=(
                    float(command.trigger_price) if command.trigger_price is not None else None
                ),
                price_precision=price_precision,
            )
            self._log.info(f"Modify sent: {command.client_order_id}")
        except Exception as e:
            self._log.error(f"modify_order({command.client_order_id}) failed: {e}")

    async def _cancel_order(self, command: CancelOrder) -> None:
        await self._rust.cancel_order(str(command.client_order_id))
        self._log.info(f"Cancel sent: {command.client_order_id}")
