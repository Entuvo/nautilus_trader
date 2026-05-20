// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Live smoke for Phases 4 / 6 / 7 + Phase 5 error path.
//!
//! Designed to be safe to run against an **unfunded** Kite account:
//!
//! - `/user/profile` — confirms auth still works.
//! - `/instruments` + `historical/{token}/5minute` — Phase 4 round-trip for RELIANCE.NSE.
//! - `/orders` (GET) — Phase 5 status-report poll (empty for an idle account).
//! - `/portfolio/positions` — Phase 6 position fetch (empty).
//! - `/user/margins` — Phase 6 account state (zero balance proves the empty-case path).
//! - `submit_order` against an unfunded account — Phase 5 **error path**. The Kite
//!   `MarginException` (or `NetworkException` outside market hours) must classify cleanly
//!   into [`ZerodhaError`] rather than landing in the generic bucket.
//! - `reconcile_startup` with no persisted store — Phase 7 cold-start happy path.
//!
//! Run:
//!
//! ```text
//! set -a && source examples/live/zerodha/.env && set +a
//! cargo run --example zerodha-exec-smoke --package nautilus-zerodha
//! ```

use std::{path::PathBuf, sync::Arc, time::SystemTime};

use anyhow::{Context, Result, anyhow};
use chrono::{Duration, Utc};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::{AccountId, ClientOrderId, InstrumentId, Symbol, Venue},
    types::{Price, Quantity},
};
use nautilus_zerodha::{
    credential::ZerodhaCredentials,
    error::ZerodhaError,
    execution::{KiteProduct, KiteVariety, SubmitRequest, ZerodhaExecClient},
    historical::{KiteResolution, fetch_candles_raw},
    http::ZerodhaHttpClient,
    instruments::{ValidationLimits, ZerodhaInstrumentCache, load_all},
    persistence::OrderStore,
    session::ZerodhaSessionManager,
};
use tempfile::TempDir;

const RELIANCE_INSTRUMENT_ID: &str = "RELIANCE-EQ.NSE";

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let ts_now = wall_clock_ns();
    let mut failures: Vec<String> = Vec::new();

    // -------- Setup --------
    println!("\n[setup] resolving credentials and constructing shared deps");
    let creds = ZerodhaCredentials::from_env().context("ZERODHA_* env vars missing")?;
    if !creds.has_access_token() {
        return Err(anyhow!(
            "ZERODHA_ACCESS_TOKEN is empty — run the daily login first"
        ));
    }
    let session = Arc::new(ZerodhaSessionManager::new(
        creds.api_key.clone(),
        creds.access_token.clone(),
        None,
    ));
    let http = Arc::new(ZerodhaHttpClient::new(session.clone(), None)?);

    // -------- 1. /user/profile (auth) --------
    println!("\n[1/7] /user/profile");
    match http.user_profile().await {
        Ok(profile) => {
            let user_name = profile
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("<unknown>");
            println!("  ✓ auth OK — user_name={user_name}");
        }
        Err(e) => {
            failures.push(format!("user_profile failed: {e}"));
            println!("  ✗ user_profile failed: {e}");
        }
    }

    // -------- 2. Populate the instrument cache --------
    println!("\n[2/7] /instruments → cache.load_all");
    let cache = Arc::new(ZerodhaInstrumentCache::new());
    match load_all(&cache, &session, ValidationLimits::default(), ts_now).await {
        Ok(outcome) => println!("  ✓ {outcome:?}"),
        Err(e) => {
            failures.push(format!("load_all failed: {e}"));
            println!("  ✗ load_all failed: {e}");
        }
    }

    let reliance_id = InstrumentId::new(Symbol::from("RELIANCE-EQ"), Venue::from("NSE"));
    let reliance_token = cache.lookup_by_id(&reliance_id).map(|kt| kt.instrument_token);
    match reliance_token {
        Some(t) => println!("  ✓ {RELIANCE_INSTRUMENT_ID} → instrument_token={t}"),
        None => {
            failures.push(format!("{RELIANCE_INSTRUMENT_ID} not in instrument cache"));
            println!("  ✗ {RELIANCE_INSTRUMENT_ID} not in instrument cache");
        }
    }

    // -------- 3. /instruments/historical (Phase 4) --------
    println!("\n[3/7] /instruments/historical for {RELIANCE_INSTRUMENT_ID} 5min last 5 days");
    if let Some(token) = reliance_token {
        let to = Utc::now();
        let from = to - Duration::days(5);
        match fetch_candles_raw(&session, token, KiteResolution::FiveMinute, from, to, false).await
        {
            Ok(candles) => {
                println!("  ✓ {} candles fetched", candles.len());
                if let (Some(first), Some(last)) = (candles.first(), candles.last()) {
                    println!(
                        "  first: ts={} open={} high={} low={} close={} vol={}",
                        u64::from(first.ts_event),
                        first.open,
                        first.high,
                        first.low,
                        first.close,
                        first.volume,
                    );
                    println!(
                        "  last:  ts={} open={} high={} low={} close={} vol={}",
                        u64::from(last.ts_event),
                        last.open,
                        last.high,
                        last.low,
                        last.close,
                        last.volume,
                    );
                    if !candles.is_sorted_by_key(|c| c.ts_event) {
                        failures.push("candles not sorted by ts_event".into());
                    }
                } else {
                    failures.push("candles empty — unexpected for 5-day window".into());
                }
            }
            Err(e) => {
                failures.push(format!("fetch_candles_raw failed: {e}"));
                println!("  ✗ fetch_candles_raw failed: {e}");
            }
        }
    } else {
        println!("  — skipped (no reliance_token)");
    }

    // -------- Build exec client backed by an ephemeral on-disk store --------
    let store_dir = TempDir::new().context("create temp dir for OrderStore")?;
    let store_path = store_dir.path().join("zerodha_orders.json");
    let exec = Arc::new(ZerodhaExecClient::new(
        http.clone(),
        cache.clone(),
        AccountId::from("ZERODHA-SMOKE"),
        Some(OrderStore::new(&store_path)),
    ));

    // -------- 4. /orders (Phase 5 status reports, empty) --------
    println!("\n[4/7] /orders (Phase 5 generate_order_status_reports)");
    match exec.generate_order_status_reports(ts_now).await {
        Ok(reports) => println!("  ✓ {} reports (empty expected for idle account)", reports.len()),
        Err(e) => {
            failures.push(format!("generate_order_status_reports failed: {e}"));
            println!("  ✗ generate_order_status_reports failed: {e}");
        }
    }

    // -------- 5. /portfolio/positions + /user/margins (Phase 6) --------
    println!("\n[5/7] /portfolio/positions (Phase 6)");
    match exec.generate_position_status_reports(ts_now).await {
        Ok(reports) => println!("  ✓ {} positions (empty expected)", reports.len()),
        Err(e) => {
            failures.push(format!("generate_position_status_reports failed: {e}"));
            println!("  ✗ generate_position_status_reports failed: {e}");
        }
    }

    println!("\n[6/7] /user/margins (Phase 6 account state)");
    match exec.generate_account_state(ts_now).await {
        Ok(state) => {
            let inr = state
                .balances
                .iter()
                .find(|b| b.currency.code.as_str() == "INR");
            match inr {
                Some(b) => println!(
                    "  ✓ INR balance: total={} locked={} free={}",
                    b.total, b.locked, b.free
                ),
                None => println!("  ✓ AccountState returned, no INR balance row (rare)"),
            }
        }
        Err(e) => {
            failures.push(format!("generate_account_state failed: {e}"));
            println!("  ✗ generate_account_state failed: {e}");
        }
    }

    // -------- 7. submit_order error-path verification --------
    println!("\n[7/7] submit_order (expect Kite-side rejection — error-path verification)");
    let submit = SubmitRequest {
        client_order_id: ClientOrderId::from("SMOKE-001"),
        instrument_id: reliance_id,
        order_side: OrderSide::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Day,
        quantity: Quantity::new(1.0, 0),
        price: Some(Price::new(1.0, 1)), // ₹1.0 — never marketable; also unfunded -> rejects before that anyway
        trigger_price: None,
        variety: KiteVariety::Regular,
        product: KiteProduct::Cnc,
    };
    match exec.submit_order(&submit).await {
        Ok(kite_id) => {
            println!("  ! submit_order unexpectedly accepted (kite_order_id={kite_id}) — account is funded?");
            println!("  ! cancelling immediately to avoid live exposure");
            match exec.cancel_order(submit.client_order_id).await {
                Ok(cancel_id) => println!("  ✓ cancel issued, order_id={cancel_id}"),
                Err(e) => {
                    failures.push(format!("cancel after accidental accept failed: {e}"));
                    println!("  ✗ cancel failed: {e}");
                }
            }
        }
        Err(e) => {
            // We *expect* an error here for an unfunded account or out-of-market window. The
            // success criterion is that it classifies into one of our typed ZerodhaError
            // variants, not into a generic "unknown" KiteError.
            let typed = e.downcast::<ZerodhaError>();
            match typed {
                Ok(zerodha_err) => match &zerodha_err {
                    ZerodhaError::KiteError {
                        status,
                        error_type,
                        message,
                    } => println!(
                        "  ✓ classified as KiteError(status={status}, error_type={error_type:?}) — {message}"
                    ),
                    ZerodhaError::MarketClosed(msg) => {
                        println!("  ✓ classified as MarketClosed — {msg}");
                    }
                    ZerodhaError::TokenException(msg) => {
                        failures.push(format!("submit returned TokenException: {msg}"));
                        println!("  ✗ TokenException — token expired? Re-run daily login");
                    }
                    other => {
                        failures.push(format!(
                            "submit error classified as unexpected variant: {other:?}"
                        ));
                        println!("  ? unexpected ZerodhaError variant: {other:?}");
                    }
                },
                Err(generic) => {
                    failures.push(format!(
                        "submit error not a typed ZerodhaError — error-path classifier missed it: {generic}"
                    ));
                    println!("  ✗ NOT a typed ZerodhaError: {generic}");
                }
            }
        }
    }

    // -------- 8. reconcile_startup cold-start --------
    println!("\n[8/8] reconcile_startup (Phase 7 cold-start)");
    let cold_dir = TempDir::new().context("create temp dir for cold-start store")?;
    let cold_store_path: PathBuf = cold_dir.path().join("zerodha_orders.json");
    let cold_exec = ZerodhaExecClient::new(
        http.clone(),
        cache.clone(),
        AccountId::from("ZERODHA-SMOKE"),
        Some(OrderStore::new(&cold_store_path)),
    );
    match cold_exec.reconcile_startup(ts_now).await {
        Ok(reports) => println!(
            "  ✓ reconcile_startup returned {} reports (0 expected on cold start)",
            reports.len()
        ),
        Err(e) => {
            failures.push(format!("reconcile_startup failed: {e}"));
            println!("  ✗ reconcile_startup failed: {e}");
        }
    }

    // -------- Summary --------
    println!("\n----------------------------------------");
    if failures.is_empty() {
        println!("ALL CHECKS PASSED ✓");
        Ok(())
    } else {
        println!("{} FAILURES:", failures.len());
        for f in &failures {
            println!("  - {f}");
        }
        Err(anyhow!("smoke had {} failure(s)", failures.len()))
    }
}

fn wall_clock_ns() -> UnixNanos {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    UnixNanos::from(nanos)
}
