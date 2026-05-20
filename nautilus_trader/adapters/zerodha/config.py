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
Config DTOs for the Zerodha adapter — re-export of the PyO3-bound Rust ``bon::Builder``
structs so callers can write::

    from nautilus_trader.adapters.zerodha import ZerodhaDataClientConfig
    cfg = ZerodhaDataClientConfig(http_timeout_secs=10)

without importing from ``nautilus_pyo3`` directly.

Credentials are NOT carried on these DTOs — they resolve from env vars
(``ZERODHA_API_KEY`` / ``ZERODHA_API_SECRET`` / ``ZERODHA_ACCESS_TOKEN``) inside the Rust
``credential.rs``.
"""

from nautilus_trader.core import nautilus_pyo3


ZerodhaDataClientConfig = nautilus_pyo3.zerodha.ZerodhaDataClientConfig
ZerodhaExecClientConfig = nautilus_pyo3.zerodha.ZerodhaExecClientConfig

__all__ = [
    "ZerodhaDataClientConfig",
    "ZerodhaExecClientConfig",
]
