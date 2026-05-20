// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Python `#[pymethods]` blocks for the config + factory `#[pyclass]` types.
//!
//! Per the workspace convention (mirrors thetadata's `python/factories.rs`), the
//! `#[pyclass]` derives in `src/config.rs` and `src/factories.rs` give us the types
//! Python sees, and the impls here give Python the `__init__` constructors + a
//! reasonable `__repr__`.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pymethods;

use crate::{
    common::{REST_BASE, WS_BASE, ZERODHA},
    config::{ZerodhaDataClientConfig, ZerodhaExecClientConfig},
    execution::KiteProduct,
    factories::{ZerodhaDataClientFactory, ZerodhaExecutionClientFactory},
};

#[pymethods]
#[gen_stub_pymethods]
impl ZerodhaDataClientConfig {
    /// Python constructor mirroring the Rust `bon::Builder` defaults.
    #[new]
    #[pyo3(signature = (
        client_id = None,
        http_url = REST_BASE.to_string(),
        ws_url = WS_BASE.to_string(),
        http_timeout_secs = 30,
        instrument_refresh_enabled = true,
    ))]
    fn py_new(
        client_id: Option<nautilus_model::identifiers::ClientId>,
        http_url: String,
        ws_url: String,
        http_timeout_secs: u64,
        instrument_refresh_enabled: bool,
    ) -> Self {
        Self {
            client_id,
            http_url,
            ws_url,
            http_timeout_secs,
            instrument_refresh_enabled,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[gen_stub_pymethods]
impl ZerodhaExecClientConfig {
    /// Python constructor mirroring the Rust `bon::Builder` defaults.
    #[new]
    #[pyo3(signature = (
        account_id = None,
        http_url = REST_BASE.to_string(),
        http_timeout_secs = 30,
        order_store_path = None,
        default_product = KiteProduct::Mis,
        poll_interval_ms = 1000,
    ))]
    fn py_new(
        account_id: Option<nautilus_model::identifiers::AccountId>,
        http_url: String,
        http_timeout_secs: u64,
        order_store_path: Option<PathBuf>,
        default_product: KiteProduct,
        poll_interval_ms: u64,
    ) -> Self {
        Self {
            account_id,
            http_url,
            http_timeout_secs,
            order_store_path,
            default_product,
            poll_interval_ms,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[gen_stub_pymethods]
impl ZerodhaDataClientFactory {
    #[new]
    fn py_new() -> Self {
        Self
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        ZERODHA
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[gen_stub_pymethods]
impl ZerodhaExecutionClientFactory {
    #[new]
    fn py_new() -> Self {
        Self
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        ZERODHA
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
