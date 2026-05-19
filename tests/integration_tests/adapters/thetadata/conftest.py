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

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.thetadata.constants import THETADATA_VENUE
from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue


@pytest.fixture
def venue() -> Venue:
    return THETADATA_VENUE


@pytest.fixture
def option_instrument_id() -> InstrumentId:
    """
    Return an OCC-encoded SPXW option InstrumentId: SPXW 2025-03-15 480.000 C.
    """
    return InstrumentId(Symbol("SPXW250315C00480000"), THETADATA_VENUE)


@pytest.fixture
def mock_http_client():
    """
    Create a mock ThetaData HTTP client.
    """
    mock = MagicMock(spec=nautilus_pyo3.ThetaDataHttpClient)
    mock.base_url = "http://127.0.0.1:25503/v3"

    mock.list_expirations = AsyncMock(return_value=["2025-03-15", "2025-04-19"])
    mock.list_strikes = AsyncMock(return_value=[470.0, 480.0, 490.0])
    mock.list_contracts = AsyncMock(return_value=[])
    mock.hist_quotes = AsyncMock(return_value=[])
    mock.hist_trades = AsyncMock(return_value=[])
    mock.hist_ohlc = AsyncMock(return_value=[])
    mock.hist_stock_eod = AsyncMock(return_value=[])
    mock.hist_index_eod = AsyncMock(return_value=[])

    return mock


@pytest.fixture
def mock_ws_client():
    """
    Create a mock ThetaData WebSocket client.
    """
    mock = MagicMock(spec=nautilus_pyo3.ThetaDataWsClient)
    mock.url = "ws://127.0.0.1:25520/v1/events"
    mock.is_connected = MagicMock(return_value=False)

    mock.connect = AsyncMock()
    mock.close = AsyncMock()
    mock.subscribe_quotes = AsyncMock()
    mock.subscribe_trades = AsyncMock()
    mock.unsubscribe_quotes = AsyncMock()
    mock.unsubscribe_trades = AsyncMock()

    mock.cache_instrument = MagicMock()
    mock.set_quote_handler = MagicMock()
    mock.set_trade_handler = MagicMock()

    return mock


@pytest.fixture
def mock_instrument_provider():
    """
    Create a mock ThetaDataInstrumentProvider.
    """
    mock = MagicMock(spec=ThetaDataInstrumentProvider)
    mock.initialize = AsyncMock()
    mock.load_ids_async = AsyncMock()
    mock.load_async = AsyncMock()
    mock.get_all = MagicMock(return_value={})
    mock.find = MagicMock(return_value=None)
    mock.instruments_pyo3 = MagicMock(return_value=[])
    return mock
