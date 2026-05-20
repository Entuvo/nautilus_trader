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
from nautilus_trader.config import ImportableConfig


class TestThetaDataDataClientConfig:
    def test_defaults(self) -> None:
        config = ThetaDataDataClientConfig()

        assert config.http_url == "http://127.0.0.1:25503/v3"
        assert config.ws_url == "ws://127.0.0.1:25520/v1/events"
        assert config.tier == "standard"
        assert config.http_timeout_secs == 30
        assert config.max_reconnects == 10
        assert config.instrument_ids is None

    def test_custom_overrides(self) -> None:
        config = ThetaDataDataClientConfig(
            http_url="http://127.0.0.1:9000/v3",
            ws_url="ws://127.0.0.1:9001/v1/events",
            tier="pro",
            http_timeout_secs=60,
        )

        assert config.http_url == "http://127.0.0.1:9000/v3"
        assert config.ws_url == "ws://127.0.0.1:9001/v1/events"
        assert config.tier == "pro"
        assert config.http_timeout_secs == 60

    def test_importable_config_round_trip(self) -> None:
        importable = ImportableConfig(
            path="nautilus_trader.adapters.thetadata.config:ThetaDataDataClientConfig",
            config={"tier": "value", "http_timeout_secs": 45},
        )

        config = importable.create()

        assert isinstance(config, ThetaDataDataClientConfig)
        assert config.tier == "value"
        assert config.http_timeout_secs == 45
