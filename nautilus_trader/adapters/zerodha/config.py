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
Python config DTOs for the Zerodha adapter — msgspec.Struct subclasses of
``LiveDataClientConfig`` / ``LiveExecClientConfig`` so they fit into
``TradingNodeConfig.data_clients`` / ``.exec_clients``.

The Rust-side ``ZerodhaDataClientConfig`` / ``ZerodhaExecClientConfig`` PyO3 types are
constructed by the factories from these Python DTOs (see ``factories.py``). Credentials are
NOT carried here — they resolve from env vars
(``ZERODHA_API_KEY`` / ``ZERODHA_API_SECRET`` / ``ZERODHA_ACCESS_TOKEN``) inside the Rust
``credential.rs``.
"""

from pathlib import Path
from typing import Literal

from nautilus_trader.common.config import PositiveInt
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.model.identifiers import AccountId


# Mirror the defaults baked into the Rust bon::Builder so a strategy that constructs the
# config with no args gets the same shape as `ZerodhaDataClientConfig::builder().build()`.
DEFAULT_HTTP_URL = "https://api.kite.trade"
DEFAULT_WS_URL = "wss://ws.kite.trade"


class ZerodhaDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for the Zerodha live market data client.

    Parameters
    ----------
    http_url : str, default "https://api.kite.trade"
        REST base URL. Override only for tests / mock servers.
    ws_url : str, default "wss://ws.kite.trade"
        Ticker WebSocket base URL.
    http_timeout_secs : PositiveInt, default 30
        Timeout for REST requests in seconds.
    instrument_refresh_enabled : bool, default True
        Whether to spawn the daily 07:45 IST instrument-cache refresh task.

    """

    http_url: str = DEFAULT_HTTP_URL
    ws_url: str = DEFAULT_WS_URL
    http_timeout_secs: PositiveInt = 30
    instrument_refresh_enabled: bool = True


class ZerodhaExecClientConfig(LiveExecClientConfig, frozen=True):
    """
    Configuration for the Zerodha live execution client.

    Parameters
    ----------
    account_id : AccountId or None, default None
        Account identifier used in emitted reports. When ``None`` the factory derives
        ``ZERODHA-DEFAULT``.
    http_url : str, default "https://api.kite.trade"
        REST base URL. Should match the data client's ``http_url`` so the shared-deps
        singleton can reuse the same session + cache + http.
    http_timeout_secs : PositiveInt, default 30
        Timeout for REST requests in seconds.
    order_store_path : Path or None, default None
        Path to the on-disk order-meta JSON file. ``None`` keeps the map purely in-memory.
        Production deployments should set this so a node restart doesn't orphan live orders.
    default_product : {"CNC", "MIS", "NRML"}, default "MIS"
        Default Kite product class for orders that don't override it. ``MIS`` is the
        conservative intraday default (Kite auto-squares at 15:20 IST).
    poll_interval_ms : PositiveInt, default 1000
        ``/orders`` polling cadence hint. The framework's reconciliation manager schedules
        the actual polls.

    """

    account_id: AccountId | None = None
    http_url: str = DEFAULT_HTTP_URL
    http_timeout_secs: PositiveInt = 30
    order_store_path: Path | None = None
    default_product: Literal["CNC", "MIS", "NRML"] = "MIS"
    poll_interval_ms: PositiveInt = 1000
