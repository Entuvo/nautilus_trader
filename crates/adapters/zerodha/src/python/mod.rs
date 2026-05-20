// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Python bindings for the Zerodha adapter.
//!
//! Loaded as `nautilus_pyo3.zerodha`. Mirrors thetadata's `python/mod.rs` shape exactly:
//! exposes the config + factory classes plus registers extractor closures with the global
//! pyo3 registry so the framework can downcast Python `Py<PyAny>` factories / configs back
//! into their Rust trait objects.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::ZERODHA,
    config::{ZerodhaDataClientConfig, ZerodhaExecClientConfig},
    execution::{KiteProduct, KiteVariety},
    factories::{ZerodhaDataClientFactory, ZerodhaExecutionClientFactory},
};

#[expect(clippy::needless_pass_by_value)]
fn extract_zerodha_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<ZerodhaDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ZerodhaDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_zerodha_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<ZerodhaExecutionClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ZerodhaExecutionClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_zerodha_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<ZerodhaDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ZerodhaDataClientConfig: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_zerodha_exec_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<ZerodhaExecClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ZerodhaExecClientConfig: {e}"
        ))),
    }
}

/// Zerodha adapter Python module.
///
/// Loaded as `nautilus_pyo3.zerodha`.
#[pymodule]
pub fn zerodha(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<KiteVariety>()?;
    m.add_class::<KiteProduct>()?;
    m.add_class::<ZerodhaDataClientConfig>()?;
    m.add_class::<ZerodhaExecClientConfig>()?;
    m.add_class::<ZerodhaDataClientFactory>()?;
    m.add_class::<ZerodhaExecutionClientFactory>()?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor(ZERODHA.to_string(), extract_zerodha_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Zerodha data factory extractor: {e}"
        )));
    }

    if let Err(e) = registry
        .register_exec_factory_extractor(ZERODHA.to_string(), extract_zerodha_exec_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Zerodha exec factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "ZerodhaDataClientConfig".to_string(),
        extract_zerodha_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Zerodha data config extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "ZerodhaExecClientConfig".to_string(),
        extract_zerodha_exec_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Zerodha exec config extractor: {e}"
        )));
    }

    Ok(())
}
