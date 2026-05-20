// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Configuration DTOs for the Zerodha adapter (Phase 8 §2.4).
//!
//! Two configs — one for the data side, one for the execution side. Both go through the same
//! shared-deps singleton (Phase 8 §2.2) when constructed via the Python factories, so
//! credentials live nowhere in the DTOs themselves: they're resolved from env vars by
//! [`crate::credential::ZerodhaCredentials`].
//!
//! Both configs derive `bon::Builder` so callers can opt into individual fields without
//! filling every default. The PyO3 layer (Phase 8 step 14) wraps these as `#[pyclass]` with
//! `pyo3-stub-gen` annotations.

use std::{any::Any, path::PathBuf};

use bon::Builder;
use nautilus_common::factories::ClientConfig;
use nautilus_model::identifiers::{AccountId, ClientId};
use serde::{Deserialize, Serialize};

use crate::{
    common::{REST_BASE, WS_BASE},
    execution::KiteProduct,
};

/// Configuration for the Zerodha live market data client.
///
/// Credentials are **not** carried here — they resolve from `ZERODHA_API_KEY` /
/// `ZERODHA_API_SECRET` / `ZERODHA_ACCESS_TOKEN` via [`crate::credential::ZerodhaCredentials`].
#[derive(Clone, Debug, Builder, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.zerodha",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.zerodha")
)]
pub struct ZerodhaDataClientConfig {
    /// Optional explicit client id. Defaults to `ZERODHA` when omitted.
    pub client_id: Option<ClientId>,

    /// REST base URL (override only for tests / mock servers).
    #[builder(default = REST_BASE.to_owned())]
    pub http_url: String,

    /// Ticker WebSocket base URL (override only for tests / mock servers).
    #[builder(default = WS_BASE.to_owned())]
    pub ws_url: String,

    /// HTTP request timeout in seconds.
    #[builder(default = 30)]
    pub http_timeout_secs: u64,

    /// Whether to spawn the daily 07:45 IST instrument-cache refresh task on startup.
    /// Default `true` — the refresh task is a no-op until the IST window fires, so leaving
    /// it on costs nothing.
    #[builder(default = true)]
    pub instrument_refresh_enabled: bool,
}

impl ClientConfig for ZerodhaDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Configuration for the Zerodha live execution client.
#[derive(Clone, Debug, Builder, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.zerodha",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.zerodha")
)]
pub struct ZerodhaExecClientConfig {
    /// Account identifier used in emitted reports. Kite is single-account-per-key — typical
    /// shape is `ZERODHA-{kite_user_id}`. When `None`, the factory derives `ZERODHA-DEFAULT`.
    pub account_id: Option<AccountId>,

    /// REST base URL (shared with the data config when both are wired through the same
    /// shared-deps registry — see Phase 8 §2.2).
    #[builder(default = REST_BASE.to_owned())]
    pub http_url: String,

    /// HTTP request timeout in seconds.
    #[builder(default = 30)]
    pub http_timeout_secs: u64,

    /// Optional path to the on-disk [`crate::persistence::OrderStore`] JSON file.
    /// `None` leaves the order-meta map purely in-memory — fine for tests and short-lived
    /// scripts; production deployments want a path so a node restart doesn't orphan live
    /// orders.
    pub order_store_path: Option<PathBuf>,

    /// Default Kite product class when the strategy doesn't override it on the order.
    /// `Mis` is the conservative intraday default (auto-squared at 15:20 IST by Kite's risk
    /// engine). Set to `Cnc` for delivery or `Nrml` for derivatives/positional intraday.
    #[builder(default = KiteProduct::Mis)]
    pub default_product: KiteProduct,

    /// `/orders` polling cadence in milliseconds. The framework's reconciliation manager
    /// schedules the actual polls; this is a hint for the default cadence (spec §3.4 default
    /// 1 s, adaptive 1→5 s under 429).
    #[builder(default = 1000)]
    pub poll_interval_ms: u64,
}

impl ClientConfig for ZerodhaExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// `KiteProduct` needs serde + pyo3 derives to round-trip through the Python config and the
// JSON-serializable Rust config. Adding them here keeps the changes local to this module.

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn data_config_defaults_match_spec() {
        let cfg = ZerodhaDataClientConfig::builder().build();
        assert_eq!(cfg.http_url, REST_BASE);
        assert_eq!(cfg.ws_url, WS_BASE);
        assert_eq!(cfg.http_timeout_secs, 30);
        assert!(cfg.instrument_refresh_enabled);
        assert!(cfg.client_id.is_none());
    }

    #[rstest]
    fn exec_config_defaults_match_spec() {
        let cfg = ZerodhaExecClientConfig::builder().build();
        assert_eq!(cfg.http_url, REST_BASE);
        assert_eq!(cfg.http_timeout_secs, 30);
        assert_eq!(cfg.default_product, KiteProduct::Mis);
        assert_eq!(cfg.poll_interval_ms, 1000);
        assert!(cfg.order_store_path.is_none());
        assert!(cfg.account_id.is_none());
    }

    #[rstest]
    fn data_config_builder_overrides_url() {
        let cfg = ZerodhaDataClientConfig::builder()
            .http_url("http://127.0.0.1:1234".into())
            .build();
        assert_eq!(cfg.http_url, "http://127.0.0.1:1234");
    }

    #[rstest]
    fn exec_config_builder_takes_store_path() {
        let cfg = ZerodhaExecClientConfig::builder()
            .order_store_path(PathBuf::from("/tmp/zerodha_orders.json"))
            .default_product(KiteProduct::Nrml)
            .build();
        assert_eq!(
            cfg.order_store_path.as_ref().unwrap(),
            std::path::Path::new("/tmp/zerodha_orders.json")
        );
        assert_eq!(cfg.default_product, KiteProduct::Nrml);
    }

    #[rstest]
    fn data_config_serializes_json() {
        let cfg = ZerodhaDataClientConfig::builder().build();
        let body = serde_json::to_string(&cfg).expect("serialize");
        let round: ZerodhaDataClientConfig = serde_json::from_str(&body).expect("deserialize");
        assert_eq!(round.http_url, cfg.http_url);
    }

    #[rstest]
    fn exec_config_serializes_json() {
        let cfg = ZerodhaExecClientConfig::builder()
            .default_product(KiteProduct::Cnc)
            .build();
        let body = serde_json::to_string(&cfg).expect("serialize");
        let round: ZerodhaExecClientConfig = serde_json::from_str(&body).expect("deserialize");
        assert_eq!(round.default_product, KiteProduct::Cnc);
    }
}
