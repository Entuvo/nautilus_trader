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

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.data import ThetaDataDataClient
from nautilus_trader.adapters.thetadata.factories import ThetaDataLiveDataClientFactory
from nautilus_trader.adapters.thetadata.factories import get_cached_thetadata_http_client
from nautilus_trader.adapters.thetadata.factories import get_cached_thetadata_instrument_provider
from nautilus_trader.adapters.thetadata.factories import get_cached_thetadata_ws_client
from nautilus_trader.live.factories import LiveDataClientFactory


class TestThetaDataLiveDataClientFactory:
    def test_is_subclass_of_live_data_client_factory(self) -> None:
        # This is the precise gate that previously dropped the pyo3 factory silently
        # inside live/node_builder.py::add_data_client_factory().
        assert issubclass(ThetaDataLiveDataClientFactory, LiveDataClientFactory)

    def test_create_returns_thetadata_data_client(
        self,
        event_loop,
        msgbus,
        cache,
        live_clock,
        monkeypatch,
        mock_http_client,
        mock_ws_client,
        mock_instrument_provider,
    ) -> None:
        monkeypatch.setattr(
            "nautilus_trader.adapters.thetadata.factories.get_cached_thetadata_http_client",
            lambda http_url, timeout_secs: mock_http_client,
        )
        monkeypatch.setattr(
            "nautilus_trader.adapters.thetadata.factories.get_cached_thetadata_ws_client",
            lambda ws_url: mock_ws_client,
        )
        monkeypatch.setattr(
            "nautilus_trader.adapters.thetadata.factories.get_cached_thetadata_instrument_provider",
            lambda client, config: mock_instrument_provider,
        )

        config = ThetaDataDataClientConfig()
        client = ThetaDataLiveDataClientFactory.create(
            loop=event_loop,
            name="THETADATA",
            config=config,
            msgbus=msgbus,
            cache=cache,
            clock=live_clock,
        )

        assert isinstance(client, ThetaDataDataClient)

    def test_get_cached_http_client_returns_same_instance(self) -> None:
        # The lru_cache decorator means same kwargs → same instance.
        c1 = get_cached_thetadata_http_client(
            "http://127.0.0.1:25503/v3",
            timeout_secs=30,
        )
        c2 = get_cached_thetadata_http_client(
            "http://127.0.0.1:25503/v3",
            timeout_secs=30,
        )
        assert c1 is c2

    def test_get_cached_ws_client_returns_same_instance(self) -> None:
        c1 = get_cached_thetadata_ws_client("ws://127.0.0.1:25520/v1/events")
        c2 = get_cached_thetadata_ws_client("ws://127.0.0.1:25520/v1/events")
        assert c1 is c2

    def test_get_cached_instrument_provider_returns_same_instance(
        self,
        mock_http_client,
    ) -> None:
        c1 = get_cached_thetadata_instrument_provider(mock_http_client, None)
        c2 = get_cached_thetadata_instrument_provider(mock_http_client, None)
        assert c1 is c2
