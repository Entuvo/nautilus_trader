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

from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.data import ThetaDataDataClient
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeQuoteTicks


def _make_data_client(
    event_loop,
    msgbus,
    cache,
    live_clock,
    mock_http_client,
    mock_ws_client,
    mock_instrument_provider,
) -> ThetaDataDataClient:
    return ThetaDataDataClient(
        loop=event_loop,
        http_client=mock_http_client,
        ws_client=mock_ws_client,
        msgbus=msgbus,
        cache=cache,
        clock=live_clock,
        instrument_provider=mock_instrument_provider,
        config=ThetaDataDataClientConfig(),
        name=None,
    )


class TestThetaDataDataClient:
    def test_constructor_registers_ws_handlers(
        self,
        event_loop,
        msgbus,
        cache,
        live_clock,
        mock_http_client,
        mock_ws_client,
        mock_instrument_provider,
    ) -> None:
        _make_data_client(
            event_loop,
            msgbus,
            cache,
            live_clock,
            mock_http_client,
            mock_ws_client,
            mock_instrument_provider,
        )

        mock_ws_client.set_quote_handler.assert_called_once()
        mock_ws_client.set_trade_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_opens_ws(
        self,
        event_loop,
        msgbus,
        cache,
        live_clock,
        mock_http_client,
        mock_ws_client,
        mock_instrument_provider,
    ) -> None:
        client = _make_data_client(
            event_loop,
            msgbus,
            cache,
            live_clock,
            mock_http_client,
            mock_ws_client,
            mock_instrument_provider,
        )
        await client._connect()

        mock_ws_client.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_quote_ticks_caches_and_subscribes(
        self,
        event_loop,
        msgbus,
        cache,
        live_clock,
        mock_http_client,
        mock_ws_client,
        mock_instrument_provider,
        option_instrument_id,
    ) -> None:
        client = _make_data_client(
            event_loop,
            msgbus,
            cache,
            live_clock,
            mock_http_client,
            mock_ws_client,
            mock_instrument_provider,
        )

        # Stub instrument cache lookup so the subscribe path finds price precisions.
        fake_instrument = MagicMock()
        fake_instrument.price_precision = 2
        fake_instrument.size_precision = 0
        cache.add_instrument = MagicMock()
        cache.instrument = MagicMock(return_value=fake_instrument)

        command = SubscribeQuoteTicks(
            instrument_id=option_instrument_id,
            client_id=client.id,
            venue=option_instrument_id.venue,
            command_id=client._generate_uuid_id() if hasattr(client, "_generate_uuid_id") else None,
            ts_init=0,
            params=None,
        )
        await client._subscribe_quote_ticks(command)

        mock_ws_client.cache_instrument.assert_called_once()
        mock_ws_client.subscribe_quotes.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unsubscribe_quote_ticks_routes(
        self,
        event_loop,
        msgbus,
        cache,
        live_clock,
        mock_http_client,
        mock_ws_client,
        mock_instrument_provider,
        option_instrument_id,
    ) -> None:
        client = _make_data_client(
            event_loop,
            msgbus,
            cache,
            live_clock,
            mock_http_client,
            mock_ws_client,
            mock_instrument_provider,
        )

        command = UnsubscribeQuoteTicks(
            instrument_id=option_instrument_id,
            client_id=client.id,
            venue=option_instrument_id.venue,
            command_id=client._generate_uuid_id() if hasattr(client, "_generate_uuid_id") else None,
            ts_init=0,
            params=None,
        )
        await client._unsubscribe_quote_ticks(command)

        mock_ws_client.unsubscribe_quotes.assert_awaited_once()
