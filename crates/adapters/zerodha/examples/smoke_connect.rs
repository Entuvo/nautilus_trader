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

//! Phase 1 smoke test for the Zerodha adapter.
//!
//! Verifies, end to end against production Kite Connect:
//!
//! 1. The HTTP client's colon-joined `Authorization: token api_key:access_token` header round-
//!    trips against `/user/profile`.
//! 2. The `/instruments` master CSV downloads and the `RELIANCE` NSE equity row is discoverable
//!    by `tradingsymbol`+`exchange` (so we don't hard-code the integer token).
//! 3. The two-layer WebSocket stack connects, subscribes in `full` mode, and streams binary
//!    ticker frames for at least 30 seconds without disconnects.
//!
//! Prerequisites:
//!
//! ```text
//! export ZERODHA_API_KEY=...
//! export ZERODHA_API_SECRET=...
//! export ZERODHA_ACCESS_TOKEN=$(python examples/live/zerodha/login.py --print)
//! ```
//!
//! Run with:
//!
//! ```text
//! cargo run --example zerodha-smoke-connect --package nautilus-zerodha
//! ```

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use nautilus_zerodha::{
    credential::ZerodhaCredentials,
    http::ZerodhaHttpClient,
    live::ZerodhaWsClient,
    session::ZerodhaSessionManager,
    ws_handler::WsEvent,
};
use tokio::time::Instant;

const TARGET_SYMBOL: &str = "RELIANCE";
const TARGET_EXCHANGE: &str = "NSE";
const SMOKE_DURATION_SECS: u64 = 30;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let creds =
        ZerodhaCredentials::from_env().context("resolve ZERODHA_* credentials from env")?;
    if !creds.has_access_token() {
        return Err(anyhow!(
            "ZERODHA_ACCESS_TOKEN is empty — run the daily login helper before smoke testing"
        ));
    }

    let session = Arc::new(ZerodhaSessionManager::new(
        creds.api_key.clone(),
        creds.access_token.clone(),
        None,
    ));

    log::info!("Smoke: probing /user/profile");
    let http = ZerodhaHttpClient::new(session.clone(), None)?;
    let profile = http.user_profile().await.context("/user/profile round-trip")?;
    let user_name = profile
        .get("user_name")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    log::info!("Auth OK — user_name={user_name}");

    log::info!("Smoke: fetching /instruments and locating {TARGET_SYMBOL}.{TARGET_EXCHANGE}");
    let instrument_token =
        discover_instrument_token(&session, TARGET_SYMBOL, TARGET_EXCHANGE).await?;
    log::info!("Discovered {TARGET_SYMBOL}.{TARGET_EXCHANGE} instrument_token={instrument_token}");

    log::info!("Smoke: opening ticker WebSocket");
    let ws = ZerodhaWsClient::spawn(session.clone(), None);

    // Wait briefly for the `Connected` event before subscribing.
    await_connected(&ws).await?;

    ws.subscribe(vec![instrument_token]).await?;
    ws.set_mode("full", vec![instrument_token]).await?;

    let deadline = Instant::now() + Duration::from_secs(SMOKE_DURATION_SECS);
    let mut binary_count: u64 = 0;
    let mut json_count: u64 = 0;
    let mut last_log = Instant::now();

    log::info!("Streaming for {SMOKE_DURATION_SECS}s …");
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), ws.next_event()).await {
            Ok(Some(WsEvent::Binary(bytes))) => {
                binary_count += 1;
                if last_log.elapsed() > Duration::from_secs(5) {
                    log::info!(
                        "binary frames so far: {binary_count} (latest len={})",
                        bytes.len()
                    );
                    last_log = Instant::now();
                }
            }
            Ok(Some(WsEvent::Json(value))) => {
                json_count += 1;
                log::info!("JSON frame: {value}");
            }
            Ok(Some(WsEvent::Connected)) => log::info!("Reconnected"),
            Ok(Some(WsEvent::Disconnected)) => log::warn!("Disconnected"),
            Ok(Some(WsEvent::Rotated)) => log::info!("Session token rotated"),
            Ok(Some(WsEvent::AuthDead)) => {
                log::error!("auth_dead — aborting smoke");
                ws.close().await;
                return Err(anyhow!("session token rotation exhausted retries"));
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }

    log::info!(
        "Smoke complete: {binary_count} binary frames, {json_count} json frames, dropped={}, state={:?}",
        ws.dropped_events(),
        ws.state(),
    );
    ws.close().await;

    if binary_count == 0 {
        return Err(anyhow!(
            "no binary ticker frames in {SMOKE_DURATION_SECS}s — check market hours / token / instrument_token"
        ));
    }
    Ok(())
}

async fn discover_instrument_token(
    session: &Arc<ZerodhaSessionManager>,
    tradingsymbol: &str,
    exchange: &str,
) -> Result<u32> {
    // `/instruments` is a CSV dump, not a JSON envelope — call reqwest directly here rather than
    // route through ZerodhaHttpClient::get which assumes the JSON envelope shape.
    let api_key = session.api_key();
    let access_token = session.access_token();
    let client = reqwest::Client::builder()
        .user_agent("nautilus-zerodha")
        .build()?;
    let csv = client
        .get(format!("{}/instruments", nautilus_zerodha::common::REST_BASE))
        .header("X-Kite-Version", nautilus_zerodha::common::KITE_VERSION)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("token {api_key}:{access_token}"),
        )
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let mut lines = csv.lines();
    let header = lines
        .next()
        .ok_or_else(|| anyhow!("/instruments CSV empty"))?;
    let columns: Vec<&str> = header.split(',').collect();
    let token_idx = columns
        .iter()
        .position(|c| *c == "instrument_token")
        .ok_or_else(|| anyhow!("/instruments missing `instrument_token` column"))?;
    let symbol_idx = columns
        .iter()
        .position(|c| *c == "tradingsymbol")
        .ok_or_else(|| anyhow!("/instruments missing `tradingsymbol` column"))?;
    let exchange_idx = columns
        .iter()
        .position(|c| *c == "exchange")
        .ok_or_else(|| anyhow!("/instruments missing `exchange` column"))?;

    for row in lines {
        let cells: Vec<&str> = row.split(',').collect();
        if cells.get(symbol_idx).copied() == Some(tradingsymbol)
            && cells.get(exchange_idx).copied() == Some(exchange)
        {
            let token = cells
                .get(token_idx)
                .ok_or_else(|| anyhow!("matching row missing instrument_token cell"))?
                .parse::<u32>()
                .context("parse instrument_token")?;
            return Ok(token);
        }
    }
    Err(anyhow!(
        "no row for tradingsymbol={tradingsymbol} exchange={exchange} in /instruments"
    ))
}

async fn await_connected(ws: &ZerodhaWsClient) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if ws.is_connected() {
            return Ok(());
        }
        match tokio::time::timeout(Duration::from_millis(500), ws.next_event()).await {
            Ok(Some(WsEvent::Connected)) => return Ok(()),
            Ok(Some(WsEvent::AuthDead)) => {
                return Err(anyhow!("ws auth_dead before subscribe"));
            }
            _ => {}
        }
    }
    Err(anyhow!("ws did not connect within 15 s"))
}
