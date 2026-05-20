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
Minimal Python ``InstrumentProvider`` for Zerodha.

The actual instrument cache (``ZerodhaInstrumentCache`` in Rust) holds the full
``/instruments`` dump and is populated by the Rust data client on connect. This Python
provider is a placeholder that the framework requires for the
``LiveMarketDataClient`` constructor; instrument lookups for the strategy's
``InstrumentId``s happen via the Rust cache, not through this provider.
"""

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig


class ZerodhaInstrumentProvider(InstrumentProvider):
    """
    Provides Zerodha instruments.

    Phase 8 minimum-viable: the Rust ``ZerodhaInstrumentCache`` is the source of truth.
    This Python provider exists to satisfy the framework's ``LiveMarketDataClient`` /
    ``LiveExecutionClient`` constructor type check.

    """

    def __init__(self, config: InstrumentProviderConfig | None = None) -> None:
        super().__init__(config=config)

    async def load_all_async(self, filters: dict | None = None) -> None:
        # The Rust cache loads on connect — no Python-side work needed here.
        del filters

    async def load_ids_async(
        self,
        instrument_ids: list,
        filters: dict | None = None,
    ) -> None:
        del instrument_ids, filters

    async def load_async(self, instrument_id, filters: dict | None = None) -> None:
        del instrument_id, filters
