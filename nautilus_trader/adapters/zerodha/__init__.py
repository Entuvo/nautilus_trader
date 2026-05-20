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
Zerodha (Kite Connect v3) integration adapter.

Live market data + execution against ``api.kite.trade`` / ``ws.kite.trade``. The Rust
implementation lives in ``crates/adapters/zerodha`` and is exposed via the PyO3 module
``nautilus_pyo3.zerodha``; the Python factories in this package are thin facades over the
registered Rust factories.

See ``docs/integrations/zerodha.md`` and ``specs/zerodha-adapter.md`` for the full
architecture, the daily-login workflow, and the v1 scope (regular + AMO varieties; CNC /
MIS / NRML products; MARKET / LIMIT / SL / SL-M order types; DAY / IOC TIF).
"""

from nautilus_trader.adapters.zerodha.config import ZerodhaDataClientConfig
from nautilus_trader.adapters.zerodha.config import ZerodhaExecClientConfig
from nautilus_trader.adapters.zerodha.constants import ZERODHA
from nautilus_trader.adapters.zerodha.constants import ZERODHA_CLIENT_ID
from nautilus_trader.adapters.zerodha.constants import ZERODHA_VENUE
from nautilus_trader.adapters.zerodha.factories import ZerodhaLiveDataClientFactory
from nautilus_trader.adapters.zerodha.factories import ZerodhaLiveExecClientFactory


__all__ = [
    "ZERODHA",
    "ZERODHA_CLIENT_ID",
    "ZERODHA_VENUE",
    "ZerodhaDataClientConfig",
    "ZerodhaExecClientConfig",
    "ZerodhaLiveDataClientFactory",
    "ZerodhaLiveExecClientFactory",
]
