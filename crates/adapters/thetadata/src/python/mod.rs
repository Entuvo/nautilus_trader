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

//! Python bindings from [PyO3](https://pyo3.rs).

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

#[cfg(feature = "live")]
pub mod factories;

#[cfg(feature = "live")]
use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

#[cfg(feature = "live")]
use crate::{
    common::THETADATA,
    config::ThetaDataDataClientConfig,
    factories::ThetaDataDataClientFactory,
};

#[cfg(feature = "live")]
#[expect(clippy::needless_pass_by_value)]
fn extract_thetadata_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<ThetaDataDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ThetaDataDataClientFactory: {e}"
        ))),
    }
}

#[cfg(feature = "live")]
#[expect(clippy::needless_pass_by_value)]
fn extract_thetadata_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<ThetaDataDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ThetaDataDataClientConfig: {e}"
        ))),
    }
}

/// ThetaData Python module.
///
/// Exposed at `nautilus_trader.core.nautilus_pyo3.thetadata` (and re-exported as
/// `nautilus_trader.thetadata` when the `cython-compat` build feature is active).
#[pymodule]
pub fn thetadata(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[cfg(feature = "live")]
    {
        m.add_class::<ThetaDataDataClientConfig>()?;
        m.add_class::<ThetaDataDataClientFactory>()?;

        let registry = get_global_pyo3_registry();
        if let Err(e) = registry.register_factory_extractor(
            THETADATA.to_string(),
            extract_thetadata_data_factory,
        ) {
            return Err(to_pyruntime_err(format!(
                "Failed to register ThetaData data factory extractor: {e}"
            )));
        }
        if let Err(e) = registry.register_config_extractor(
            "ThetaDataDataClientConfig".to_string(),
            extract_thetadata_data_config,
        ) {
            return Err(to_pyruntime_err(format!(
                "Failed to register ThetaData config extractor: {e}"
            )));
        }
    }
    Ok(())
}
