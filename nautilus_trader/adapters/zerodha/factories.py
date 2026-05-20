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
Live factory facades for the Zerodha adapter.

Each ``create()`` instantiates the PyO3-bound Rust factory and hands it to the
``LiveNode.builder().add_data_client(...)`` / ``add_exec_client(...)`` pipeline. The
framework's extractor registry (populated when ``nautilus_pyo3.zerodha`` loads) downcasts
the factory and the supplied config back into their Rust trait objects.

This mirrors the canonical ThetaData / Binance / Bybit factory pattern.
"""

from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


class ZerodhaLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a Zerodha live data client factory.
    """

    @staticmethod
    def create(  # type: ignore[override]
        loop,
        name,
        config,
        msgbus,
        cache,
        clock,
    ):
        """
        Create a new Zerodha data client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client identifier (e.g. ``ZERODHA``).
        config : ZerodhaDataClientConfig
            The PyO3-bound data-client config.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        ZerodhaDataClientFactory
            The PyO3-bound Rust factory instance; the live runner uses the registered
            extractor to downcast it back into ``Box<dyn DataClientFactory>``.

        """
        del loop, name, msgbus, cache, clock, config
        return nautilus_pyo3.zerodha.ZerodhaDataClientFactory()


class ZerodhaLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides a Zerodha live execution client factory.
    """

    @staticmethod
    def create(  # type: ignore[override]
        loop,
        name,
        config,
        msgbus,
        cache,
        clock,
    ):
        """
        Create a new Zerodha execution client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client identifier.
        config : ZerodhaExecClientConfig
            The PyO3-bound exec-client config.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        ZerodhaExecutionClientFactory
            The PyO3-bound Rust factory instance.

        """
        del loop, name, msgbus, cache, clock, config
        return nautilus_pyo3.zerodha.ZerodhaExecutionClientFactory()
