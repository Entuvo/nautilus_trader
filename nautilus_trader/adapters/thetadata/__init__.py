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
ThetaData market data integration adapter.

This subpackage provides a data client factory and configuration for connecting to a local
ThetaTerminal which proxies the ThetaData REST and streaming APIs for US options, stocks, and
indices.

This adapter is data-only — there is no execution client.

"""

from nautilus_trader.adapters.thetadata.constants import THETADATA
from nautilus_trader.adapters.thetadata.constants import THETADATA_CLIENT_ID
from nautilus_trader.adapters.thetadata.constants import THETADATA_VENUE
from nautilus_trader.adapters.thetadata.factories import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.factories import ThetaDataDataClientFactory


__all__ = [
    "THETADATA",
    "THETADATA_CLIENT_ID",
    "THETADATA_VENUE",
    "ThetaDataDataClientConfig",
    "ThetaDataDataClientFactory",
]
