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
ThetaData data client factory.

Re-exports the Rust-backed `ThetaDataDataClientFactory` and config from the compiled PyO3
module. These are registered with the global PyO3 factory/config extractor registry during
module initialization, so passing the Python-facing factory + config straight to
`LiveNode.add_data_client(...)` is sufficient — no separate Python wrapper class is required.

"""

from nautilus_trader.core import nautilus_pyo3


ThetaDataDataClientConfig = nautilus_pyo3.thetadata.ThetaDataDataClientConfig
ThetaDataDataClientFactory = nautilus_pyo3.thetadata.ThetaDataDataClientFactory


__all__ = [
    "ThetaDataDataClientConfig",
    "ThetaDataDataClientFactory",
]
