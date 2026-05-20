#!/usr/bin/env python3
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
"""
ThetaData Python TradingNode smoke test: subscribes to live NBBO quotes for a single SPXW
contract and prints the first 10 ticks that arrive.

Prerequisites:
- ThetaTerminalv3.jar running with valid creds.txt and Standard+ subscription.
- US market hours (9:30 AM – 4:00 PM ET regular, or SPX/SPXW overnight 16:15 ET – 04:00 ET).

Override the target contract via THETADATA_INSTRUMENT_ID, e.g.:
    THETADATA_INSTRUMENT_ID="SPXW260520C07400000.THETADATA" \\
        python examples/live/thetadata/thetadata_subscribe_quotes.py
"""

import os

from nautilus_trader.adapters.thetadata import THETADATA
from nautilus_trader.adapters.thetadata import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata import ThetaDataLiveDataClientFactory
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig


DEFAULT_INSTRUMENT = "SPXW260520C07400000.THETADATA"


class QuotePrinterConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    max_ticks: int = 10


class QuotePrinter(Strategy):
    """Prints the first ``max_ticks`` quote ticks for ``instrument_id`` then stops the node."""

    def __init__(self, config: QuotePrinterConfig) -> None:
        super().__init__(config=config)
        self._count = 0

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.config.instrument_id)
        self.log.info(f"Subscribed to quotes for {self.config.instrument_id}")

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._count += 1
        self.log.info(
            f"[{self._count}/{self.config.max_ticks}] bid={tick.bid_price} x {tick.bid_size} | "
            f"ask={tick.ask_price} x {tick.ask_size} | ts_event={tick.ts_event}",
        )
        if self._count >= self.config.max_ticks:
            self.log.info("Target tick count reached, stopping node")
            self.stop()


instrument_id = InstrumentId.from_str(
    os.getenv("THETADATA_INSTRUMENT_ID", DEFAULT_INSTRUMENT),
)

config_node = TradingNodeConfig(
    trader_id=TraderId("TESTER-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    exec_engine=LiveExecEngineConfig(reconciliation=False),
    data_clients={
        THETADATA: ThetaDataDataClientConfig(
            tier="standard",
            instrument_ids=[instrument_id],
        ),
    },
    timeout_connection=15.0,
    timeout_disconnection=5.0,
    timeout_post_stop=1.0,
)

node = TradingNode(config=config_node)
node.trader.add_strategy(
    QuotePrinter(QuotePrinterConfig(instrument_id=instrument_id, max_ticks=10)),
)
node.add_data_client_factory(THETADATA, ThetaDataLiveDataClientFactory)
node.build()

try:
    node.run()
except KeyboardInterrupt:
    node.stop()
finally:
    node.dispose()
