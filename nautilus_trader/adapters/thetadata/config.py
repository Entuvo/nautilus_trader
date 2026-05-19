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

from typing import Literal

from nautilus_trader.adapters.thetadata.constants import DEFAULT_HTTP_URL
from nautilus_trader.adapters.thetadata.constants import DEFAULT_WS_URL
from nautilus_trader.common.config import PositiveInt
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.model.identifiers import InstrumentId


class ThetaDataDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for ``ThetaDataDataClient`` instances.

    ThetaData is a data-only integration that connects to a locally running
    ``ThetaTerminalv3.jar`` Java process. Authentication is owned by the
    Terminal (via its ``creds.txt``); the adapter never sends credentials
    over the wire itself.

    Parameters
    ----------
    http_url : str, default "http://127.0.0.1:25503/v3"
        The base URL of the local ThetaTerminal HTTP server.
    ws_url : str, default "ws://127.0.0.1:25520/v1/events"
        The WebSocket URL of the local ThetaTerminal streaming endpoint.
    tier : {"value", "standard", "pro"}, default "standard"
        The ThetaData subscription tier for the connected account. Surfaces
        capability differences (e.g. snapshot vs full stream) — used by the
        client for logging only; the Terminal enforces the actual access.
    http_timeout_secs : PositiveInt, default 30
        The timeout for HTTP requests in seconds.
    max_reconnects : PositiveInt, default 10
        The maximum number of WebSocket reconnect attempts before giving up.
    instrument_ids : list[InstrumentId] or None, default None
        An optional list of OCC-encoded option ``InstrumentId``s to preload
        in the instrument provider on connect. When ``None``, instruments
        are loaded lazily as subscribe/request commands arrive.

    """

    http_url: str = DEFAULT_HTTP_URL
    ws_url: str = DEFAULT_WS_URL
    tier: Literal["value", "standard", "pro"] = "standard"
    http_timeout_secs: PositiveInt = 30
    max_reconnects: PositiveInt = 10
    instrument_ids: list[InstrumentId] | None = None
