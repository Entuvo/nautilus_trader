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
TC-E conformance stubs for the Zerodha adapter (filled in across Phases 5–7).
"""


def test_imports() -> None:
    """
    Phase 0 smoke test — the adapter execution-side modules import cleanly.
    """
    from nautilus_trader.adapters.zerodha import auth as zerodha_auth
    from nautilus_trader.adapters.zerodha import execution as zerodha_execution
    from nautilus_trader.adapters.zerodha import factories as zerodha_factories

    # Modules import even though their public surface is intentionally empty in Phase 0.
    assert zerodha_auth is not None
    assert zerodha_execution is not None
    assert zerodha_factories is not None
