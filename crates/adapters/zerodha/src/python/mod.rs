// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Python bindings for the Zerodha adapter.
//!
//! Loaded as `nautilus_pyo3.zerodha`.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

use pyo3::prelude::*;

/// Zerodha adapter Python module.
///
/// Phase 0 placeholder — no classes or factories are registered yet. Phase 1 will add
/// `ZerodhaDataClientConfig`, `ZerodhaExecClientConfig`, `ZerodhaDataClientFactory`,
/// `ZerodhaExecutionClientFactory`, and the matching registry extractors.
#[pymodule]
pub fn zerodha(_py: Python<'_>, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
