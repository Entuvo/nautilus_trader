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

import asyncio
from functools import lru_cache

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.data import ThetaDataDataClient
from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.live.factories import LiveDataClientFactory


@lru_cache(maxsize=1)
def get_cached_thetadata_http_client(
    http_url: str,
    timeout_secs: int,
) -> nautilus_pyo3.ThetaDataHttpClient:
    """
    Cache and return a ThetaData HTTP client targeting the local Terminal.

    Parameters
    ----------
    http_url : str
        The base URL of the local ThetaTerminal HTTP server.
    timeout_secs : int
        The timeout for HTTP requests in seconds.

    Returns
    -------
    nautilus_pyo3.ThetaDataHttpClient

    """
    return nautilus_pyo3.ThetaDataHttpClient(
        http_url=http_url,
        timeout_secs=int(timeout_secs),
    )


@lru_cache(maxsize=1)
def get_cached_thetadata_ws_client(ws_url: str) -> nautilus_pyo3.ThetaDataWsClient:
    """
    Cache and return a ThetaData WebSocket client targeting the local Terminal.

    Parameters
    ----------
    ws_url : str
        The WebSocket URL of the local ThetaTerminal streaming endpoint.

    Returns
    -------
    nautilus_pyo3.ThetaDataWsClient

    """
    return nautilus_pyo3.ThetaDataWsClient(ws_url=ws_url)


@lru_cache(maxsize=1)
def get_cached_thetadata_instrument_provider(
    client: nautilus_pyo3.ThetaDataHttpClient,
    config: InstrumentProviderConfig | None,
) -> ThetaDataInstrumentProvider:
    """
    Cache and return a ThetaData instrument provider.

    Parameters
    ----------
    client : nautilus_pyo3.ThetaDataHttpClient
        The ThetaData HTTP client.
    config : InstrumentProviderConfig, optional
        The instrument provider configuration.

    Returns
    -------
    ThetaDataInstrumentProvider

    """
    return ThetaDataInstrumentProvider(client=client, config=config)


class ThetaDataLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a ThetaData live data client factory.
    """

    @staticmethod
    def create(  # type: ignore[override]
        loop: asyncio.AbstractEventLoop,
        name: str | None,
        config: ThetaDataDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> ThetaDataDataClient:
        """
        Create a new ThetaData data client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str, optional
            The custom client ID.
        config : ThetaDataDataClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        ThetaDataDataClient

        """
        http_client = get_cached_thetadata_http_client(
            http_url=config.http_url,
            timeout_secs=config.http_timeout_secs,
        )
        ws_client = get_cached_thetadata_ws_client(ws_url=config.ws_url)
        provider = get_cached_thetadata_instrument_provider(
            client=http_client,
            config=config.instrument_provider,
        )

        return ThetaDataDataClient(
            loop=loop,
            http_client=http_client,
            ws_client=ws_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
