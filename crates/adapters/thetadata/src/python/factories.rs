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

//! Python bindings for the ThetaData data client config and factory.

use std::path::PathBuf;

use nautilus_model::identifiers::ClientId;
use pyo3::prelude::*;

use crate::{
    common::{DEFAULT_HTTP_URL, DEFAULT_WS_URL},
    config::{ThetaDataDataClientConfig, ThetaDataTier},
    factories::ThetaDataDataClientFactory,
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ThetaDataDataClientConfig {
    /// Constructor exposed to Python.
    ///
    /// Defaults mirror the Rust builder: HTTP at `127.0.0.1:25503/v3`, WS at
    /// `127.0.0.1:25520/v1/events`, tier = `"standard"`, no creds-file override.
    #[new]
    #[pyo3(signature = (
        client_id = None,
        http_url = DEFAULT_HTTP_URL.to_string(),
        ws_url = DEFAULT_WS_URL.to_string(),
        tier = "standard".to_string(),
        creds_file = None,
        max_reconnects = 10,
        http_timeout_secs = 30,
    ))]
    fn py_new(
        client_id: Option<ClientId>,
        http_url: String,
        ws_url: String,
        tier: String,
        creds_file: Option<PathBuf>,
        max_reconnects: u32,
        http_timeout_secs: u64,
    ) -> PyResult<Self> {
        let tier = match tier.to_lowercase().as_str() {
            "value" => ThetaDataTier::Value,
            "standard" => ThetaDataTier::Standard,
            "pro" => ThetaDataTier::Pro,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown ThetaData tier {other:?} (expected value | standard | pro)"
                )));
            }
        };
        Ok(Self {
            client_id,
            http_url,
            ws_url,
            tier,
            creds_file,
            max_reconnects,
            http_timeout_secs,
        })
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ThetaDataDataClientFactory {
    /// Factory constructor exposed to Python.
    #[new]
    fn py_new() -> Self {
        Self
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        crate::common::THETADATA
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
