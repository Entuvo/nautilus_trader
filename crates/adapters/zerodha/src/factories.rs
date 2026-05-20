// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Factory entry points (Phase 8 step 14).
//!
//! Two factories — [`ZerodhaDataClientFactory`] and [`ZerodhaExecutionClientFactory`] — both
//! implementing the framework's [`DataClientFactory`] / [`ExecutionClientFactory`] traits so
//! they plug straight into `nautilus_common::factories::*` machinery and, by extension, into
//! the live-system Python bridge.
//!
//! # Shared-deps singleton (spec §2.2 in `specs/zerodha-adapter-phase8.md`)
//!
//! Both factories pull from a process-global `(api_key, http_url) → Arc<SharedDeps>` registry.
//! First factory call constructs `ZerodhaSessionManager` + `ZerodhaHttpClient` +
//! `ZerodhaInstrumentCache`; the second factory call (whether data-then-exec or vice versa)
//! finds the existing tuple and reuses it. This avoids two parallel token-rotation loops, two
//! daily-refresh tasks, and two independent caches.
//!
//! Strategies wiring **both** data and exec must use the **same** `api_key` (and matching
//! `http_url`) across both configs. We document this in the README; mismatch is caught at
//! factory time by the lookup miss returning a fresh `SharedDeps` (no error — strategies just
//! end up with two cohorts of shared state and the operator pays for it).

use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::{Arc, Mutex, OnceLock},
};

use anyhow::{Result, anyhow};
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_model::identifiers::{AccountId, ClientId, Venue};

use crate::{
    common::ZERODHA,
    config::{ZerodhaDataClientConfig, ZerodhaExecClientConfig},
    credential::ZerodhaCredentials,
    data_client::ZerodhaDataClient,
    execution_client::ZerodhaExecutionClient,
    http::ZerodhaHttpClient,
    instruments::ZerodhaInstrumentCache,
    session::ZerodhaSessionManager,
};

/// Bundle of long-lived dependencies shared between the data and execution clients for a
/// single `(api_key, http_url)` pair.
#[derive(Debug)]
pub struct SharedDeps {
    /// Process-singleton access-token holder.
    pub session: Arc<ZerodhaSessionManager>,
    /// Rate-limited Kite REST client.
    pub http: Arc<ZerodhaHttpClient>,
    /// Instrument cache populated by the data client's startup `load_all`.
    pub cache: Arc<ZerodhaInstrumentCache>,
}

static SHARED_DEPS: OnceLock<Mutex<HashMap<String, Arc<SharedDeps>>>> = OnceLock::new();

fn shared_key(api_key: &str, http_url: &str) -> String {
    format!("{api_key}|{http_url}")
}

/// Look up — or construct — the [`SharedDeps`] cohort for `(api_key, http_url)`.
///
/// Idempotent: repeat calls with the same key return the same `Arc`.
///
/// # Errors
///
/// - [`ZerodhaError::MissingCredential`](crate::error::ZerodhaError::MissingCredential) if the
///   env vars aren't set.
/// - `reqwest` build error on first construction (extremely unlikely outside resource exhaustion).
pub fn get_or_build_shared(http_url: &str) -> Result<Arc<SharedDeps>> {
    let creds = ZerodhaCredentials::from_env()?;
    let key = shared_key(&creds.api_key, http_url);

    let map = SHARED_DEPS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map
        .lock()
        .map_err(|e| anyhow!("SHARED_DEPS mutex poisoned: {e}"))?;
    if let Some(existing) = guard.get(&key) {
        return Ok(existing.clone());
    }

    let session = Arc::new(ZerodhaSessionManager::new(
        creds.api_key.clone(),
        creds.access_token.clone(),
        None,
    ));
    let http = Arc::new(ZerodhaHttpClient::new(
        session.clone(),
        Some(http_url.to_string()),
    )?);
    let cache = Arc::new(ZerodhaInstrumentCache::new());

    let deps = Arc::new(SharedDeps {
        session,
        http,
        cache,
    });
    guard.insert(key, deps.clone());
    Ok(deps)
}

/// Clear the shared-deps registry (test helper). NEVER call in production code — invalidates
/// every live client's session manager reference.
#[doc(hidden)]
pub fn _clear_shared_deps_for_tests() {
    if let Some(map) = SHARED_DEPS.get()
        && let Ok(mut guard) = map.lock()
    {
        guard.clear();
    }
}

// -------------------------------------------------------------------------------------------------
// ZerodhaDataClientFactory
// -------------------------------------------------------------------------------------------------

/// Constructs [`ZerodhaDataClient`] instances from a [`ZerodhaDataClientConfig`].
#[derive(Debug)]
pub struct ZerodhaDataClientFactory;

impl DataClientFactory for ZerodhaDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> Result<Box<dyn DataClient>> {
        let cfg = config
            .as_any()
            .downcast_ref::<ZerodhaDataClientConfig>()
            .ok_or_else(|| {
                anyhow!(
                    "ZerodhaDataClientFactory: expected ZerodhaDataClientConfig, got {config:?}"
                )
            })?;
        let shared = get_or_build_shared(&cfg.http_url)?;
        let client_id = cfg.client_id.unwrap_or_else(|| ClientId::from(name));
        let venue = Venue::from(ZERODHA);

        let client = ZerodhaDataClient::new(client_id, venue, &shared, cfg.clone())?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ZERODHA
    }

    fn config_type(&self) -> &'static str {
        "ZerodhaDataClientConfig"
    }
}

// -------------------------------------------------------------------------------------------------
// ZerodhaExecutionClientFactory
// -------------------------------------------------------------------------------------------------

/// Constructs [`ZerodhaExecutionClient`] instances from a [`ZerodhaExecClientConfig`].
#[derive(Debug)]
pub struct ZerodhaExecutionClientFactory;

impl ExecutionClientFactory for ZerodhaExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
    ) -> Result<Box<dyn ExecutionClient>> {
        let cfg = config
            .as_any()
            .downcast_ref::<ZerodhaExecClientConfig>()
            .ok_or_else(|| {
                anyhow!(
                    "ZerodhaExecutionClientFactory: expected ZerodhaExecClientConfig, got {config:?}"
                )
            })?;
        let shared = get_or_build_shared(&cfg.http_url)?;
        let client_id = ClientId::from(name);
        let account_id = cfg
            .account_id
            .unwrap_or_else(|| AccountId::from("ZERODHA-DEFAULT"));
        let venue = Venue::from(ZERODHA);

        let client =
            ZerodhaExecutionClient::new(client_id, account_id, venue, &shared, cfg.clone());
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ZERODHA
    }

    fn config_type(&self) -> &'static str {
        "ZerodhaExecClientConfig"
    }
}

#[cfg(test)]
#[allow(unsafe_code)] // env::set_var is unsafe in Rust 2024 (thread-shared).
mod tests {
    use super::*;

    fn set_env_vars() {
        // SAFETY: each test calls _clear_shared_deps_for_tests + sets the same vars;
        // these tests are not run in parallel with other env-mutating tests.
        unsafe { std::env::set_var("ZERODHA_API_KEY", "k") };
        unsafe { std::env::set_var("ZERODHA_API_SECRET", "s") };
        unsafe { std::env::set_var("ZERODHA_ACCESS_TOKEN", "t") };
    }

    #[test]
    fn shared_deps_lookup_returns_same_arc() {
        set_env_vars();
        _clear_shared_deps_for_tests();
        let a = get_or_build_shared("https://api.kite.trade").unwrap();
        let b = get_or_build_shared("https://api.kite.trade").unwrap();
        assert!(Arc::ptr_eq(&a.session, &b.session));
        assert!(Arc::ptr_eq(&a.http, &b.http));
        assert!(Arc::ptr_eq(&a.cache, &b.cache));
        _clear_shared_deps_for_tests();
    }

    #[test]
    fn shared_deps_different_url_returns_different_arc() {
        set_env_vars();
        _clear_shared_deps_for_tests();
        let a = get_or_build_shared("https://api.kite.trade").unwrap();
        let b = get_or_build_shared("http://127.0.0.1:9999").unwrap();
        assert!(!Arc::ptr_eq(&a.session, &b.session));
        _clear_shared_deps_for_tests();
    }
}
