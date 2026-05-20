// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Capture a live Kite ticker session to a fixture file for Phase 3 decoder development.
//!
//! Subscribes to a handful of representative instruments in `full` mode, records every binary
//! frame with a wall-clock timestamp, and writes a length-prefixed binary stream to
//! `tests/fixtures/ws_session_<date>.bin`.
//!
//! Frame format on disk (little-endian):
//!
//! ```text
//! magic: "ZWSC" (4 bytes) -- header sentinel
//! version: u16 = 1
//! reserved: u16 = 0
//! repeated:
//!   timestamp_ns: u64
//!   payload_len: u32
//!   payload: payload_len bytes (raw Kite binary frame, including the outer u16 BE count)
//! ```
//!
//! Prerequisites (same as `zerodha-smoke-connect`):
//!
//! ```text
//! export ZERODHA_API_KEY=...
//! export ZERODHA_API_SECRET=...
//! export ZERODHA_ACCESS_TOKEN=$(python examples/live/zerodha/login.py --print)
//! ```
//!
//! Run with::
//!
//! ```text
//! cargo run --example zerodha-ws-capture --package nautilus-zerodha -- --seconds 120
//! ```

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use nautilus_zerodha::{
    credential::ZerodhaCredentials, live::ZerodhaWsClient, session::ZerodhaSessionManager,
    ws_handler::WsEvent,
};

const MAGIC: &[u8; 4] = b"ZWSC";
const VERSION: u16 = 1;

// Representative instrument set — confirmed live in Phase 1 smoke. Token lookups are stable for
// these blue-chip listings; if you want to widen the capture surface, pull the tokens from
// `instruments_sample.csv` instead of hard-coding here.
const RELIANCE_NSE: u32 = 738561;
const NIFTY_50_INDEX: u32 = 256265;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let seconds = parse_seconds_arg()?;
    let target_path = default_fixture_path();
    println!(
        "[ws_capture] capturing {seconds}s of Kite ticker into {}",
        target_path.display()
    );

    let creds = ZerodhaCredentials::from_env().context("ZERODHA_* env vars missing")?;
    if !creds.has_access_token() {
        return Err(anyhow!(
            "ZERODHA_ACCESS_TOKEN is empty — run the daily login flow first"
        ));
    }
    let session = Arc::new(ZerodhaSessionManager::new(
        creds.api_key.clone(),
        creds.access_token.clone(),
        None,
    ));

    let ws = ZerodhaWsClient::spawn(session, None);

    // Wait for `Connected` before subscribing.
    let deadline = Instant::now() + Duration::from_secs(15);
    while !ws.is_connected() && Instant::now() < deadline {
        if matches!(ws.try_next_event().await, Some(WsEvent::AuthDead)) {
            return Err(anyhow!("auth_dead before subscribe"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !ws.is_connected() {
        return Err(anyhow!("ws did not connect within 15 s"));
    }

    let tokens = vec![RELIANCE_NSE, NIFTY_50_INDEX];
    ws.subscribe(tokens.clone()).await?;
    ws.set_mode("full", tokens.clone()).await?;
    log::info!("subscribed to {tokens:?} in full mode");

    // Open fixture file and write header.
    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent).context("create fixture directory")?;
    }
    let file = File::create(&target_path)
        .with_context(|| format!("create {}", target_path.display()))?;
    let mut writer = BufWriter::new(file);
    writer.write_all(MAGIC)?;
    writer.write_all(&VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;

    let stop_at = Instant::now() + Duration::from_secs(seconds);
    let mut histogram: ahash::HashMap<usize, u64> = ahash::HashMap::default();
    let mut binary_count: u64 = 0;
    let mut json_count: u64 = 0;
    let mut last_log = Instant::now();

    while Instant::now() < stop_at {
        match tokio::time::timeout(Duration::from_millis(500), ws.next_event()).await {
            Ok(Some(WsEvent::Binary(bytes))) => {
                binary_count += 1;
                *histogram.entry(bytes.len()).or_default() += 1;
                let ts_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos() as u64);
                writer.write_all(&ts_ns.to_le_bytes())?;
                writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
                writer.write_all(&bytes)?;

                if last_log.elapsed() > Duration::from_secs(5) {
                    log::info!(
                        "binary={binary_count} json={json_count}, hist so far: {:?}",
                        histogram_sorted(&histogram),
                    );
                    last_log = Instant::now();
                }
            }
            Ok(Some(WsEvent::Json(value))) => {
                json_count += 1;
                log::info!("json frame: {value}");
            }
            Ok(Some(WsEvent::AuthDead)) => {
                log::error!("auth_dead — aborting");
                break;
            }
            Ok(Some(WsEvent::Disconnected | WsEvent::Connected | WsEvent::Rotated) | None)
            | Err(_) => {}
        }
    }

    writer.flush()?;
    drop(writer);
    ws.close().await;

    println!(
        "[ws_capture] done — wrote {binary_count} binary frames ({} bytes/sec avg)",
        std::fs::metadata(&target_path).map_or(0, |m| m.len()) / seconds.max(1),
    );
    println!("[ws_capture] length histogram (size -> count):");
    for (size, count) in histogram_sorted(&histogram) {
        println!("    {size:>5} B  ×  {count}");
    }
    Ok(())
}

fn parse_seconds_arg() -> Result<u64> {
    let mut args = std::env::args();
    args.next(); // binary
    let mut seconds = 60u64;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--seconds" | "-s" => {
                seconds = args
                    .next()
                    .ok_or_else(|| anyhow!("--seconds requires a value"))?
                    .parse()
                    .context("parse --seconds value")?;
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    Ok(seconds)
}

fn default_fixture_path() -> PathBuf {
    let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("ws_session_{date}.bin"))
}

fn histogram_sorted(h: &ahash::HashMap<usize, u64>) -> Vec<(usize, u64)> {
    let mut pairs: Vec<_> = h.iter().map(|(&k, &v)| (k, v)).collect();
    pairs.sort_by_key(|(k, _)| *k);
    pairs
}
