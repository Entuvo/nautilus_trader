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

from typing import Final

from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue


THETADATA: Final[str] = "THETADATA"
THETADATA_VENUE: Final[Venue] = Venue(THETADATA)
THETADATA_CLIENT_ID: Final[ClientId] = ClientId(THETADATA)

DEFAULT_HTTP_URL: Final[str] = "http://127.0.0.1:25503/v3"
DEFAULT_WS_URL: Final[str] = "ws://127.0.0.1:25520/v1/events"
