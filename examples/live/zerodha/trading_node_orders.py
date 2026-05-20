#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  See LICENSE for full text.
# -------------------------------------------------------------------------------------------------
"""
Zerodha TradingNode orders smoke test: submits one far-from-market LIMIT order,
then cancels it. Use this to verify the execution path before going live.

DO NOT RUN with funded credentials unless you understand the order parameters.
The default price is 1.0 INR for RELIANCE-EQ.NSE — Kite will reject it with
InputException ("price out of circuit limit") which is the intended outcome:
this exercises the submit -> error-mapping path without risking a fill.

Prerequisites identical to ``zerodha_subscribe_quotes.py`` — refresh the daily
token first via ``python examples/live/zerodha/login.py``.
"""

import os
from datetime import timedelta
from decimal import Decimal

from nautilus_trader.adapters.zerodha import ZERODHA
from nautilus_trader.adapters.zerodha import ZerodhaDataClientConfig
from nautilus_trader.adapters.zerodha import ZerodhaExecClientConfig
from nautilus_trader.adapters.zerodha import ZerodhaLiveDataClientFactory
from nautilus_trader.adapters.zerodha import ZerodhaLiveExecClientFactory
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import RoutingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.currencies import INR
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig


DEFAULT_INSTRUMENT = "RELIANCE-EQ.NSE"
DEFAULT_LIMIT_PRICE = Decimal("1.00")  # Far below circuit — expects rejection.


class OrderProberConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    limit_price: Decimal = DEFAULT_LIMIT_PRICE


class OrderProber(Strategy):
    """Submits one LIMIT order then cancels it 5 s later (or after fill rejection)."""

    def __init__(self, config: OrderProberConfig) -> None:
        super().__init__(config=config)
        self._submitted_order_id = None

    def on_start(self) -> None:
        instrument = self.cache.instrument(self.config.instrument_id)
        if instrument is None:
            # Without instrument metadata we don't know the price precision; bail.
            self.log.error(f"Instrument {self.config.instrument_id} not in cache")
            self.stop()
            return

        order = self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            price=Price(self.config.limit_price, instrument.price_precision),
            time_in_force=TimeInForce.DAY,
        )
        self._submitted_order_id = order.client_order_id
        self.log.info(
            f"Submitting LIMIT BUY 1 @ {order.price} for {self.config.instrument_id}",
        )
        self.submit_order(order)
        self.clock.set_time_alert("cancel_probe", self.clock.utc_now() + timedelta(seconds=5))

    def on_event(self, event) -> None:  # type: ignore[no-untyped-def]
        self.log.info(f"Event: {event!r}")

    def on_time_event(self, event) -> None:  # type: ignore[no-untyped-def]
        if event.name == "cancel_probe" and self._submitted_order_id is not None:
            order = self.cache.order(self._submitted_order_id)
            if order is not None and not order.is_closed:
                self.log.info(f"Cancelling {self._submitted_order_id}")
                self.cancel_order(order)
            self.clock.set_time_alert(
                "stop_node",
                self.clock.utc_now() + timedelta(seconds=3),
            )
        elif event.name == "stop_node":
            self.log.info("Probe complete — stopping node")
            self.stop()


instrument_id = InstrumentId.from_str(
    os.getenv("ZERODHA_INSTRUMENT_ID", DEFAULT_INSTRUMENT),
)

config_node = TradingNodeConfig(
    trader_id=TraderId("ORDER-PROBE-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    exec_engine=LiveExecEngineConfig(reconciliation=False),
    data_clients={
        ZERODHA: ZerodhaDataClientConfig(
            routing=RoutingConfig(venues=frozenset({"NSE", "BSE", "NFO", "BFO", "MCX", "CDS"})),
        ),
    },
    exec_clients={
        ZERODHA: ZerodhaExecClientConfig(
            routing=RoutingConfig(venues=frozenset({"NSE", "BSE", "NFO", "BFO", "MCX", "CDS"})),
            default_product="MIS",
            starting_balances=[Money(0, INR)],
        ),
    },
    timeout_connection=20.0,
    timeout_disconnection=5.0,
    timeout_post_stop=2.0,
)

node = TradingNode(config=config_node)
node.trader.add_strategy(OrderProber(OrderProberConfig(instrument_id=instrument_id)))
node.add_data_client_factory(ZERODHA, ZerodhaLiveDataClientFactory)
node.add_exec_client_factory(ZERODHA, ZerodhaLiveExecClientFactory)
node.build()

try:
    node.run()
except KeyboardInterrupt:
    node.stop()
finally:
    node.dispose()
