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

//! 30-day tick-trade backfill for SPY + SPX + SPXW, ATM ± 20 strikes, writing directly to
//! a Nautilus `ParquetDataCatalog`.
//!
//! Output layout (canonical Nautilus catalog format):
//!
//! ```text
//! $THETADATA_CATALOG_DIR/data/trade_tick/<safe_instrument_id>/<start_ns>-<end_ns>.parquet
//! ```
//!
//! Idempotent: an existing parquet file for an (instrument, time-range) is skipped. Concurrency
//! capped at `THETADATA_CONCURRENCY` (default 4) to stay under Standard-tier limits.
//!
//! Run:
//!
//! ```text
//! cargo run --release --example thetadata-backfill-trades -p nautilus-thetadata
//! ```

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::Duration,
};

use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, Utc, Weekday};
use chrono_tz::America::New_York;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::TradeTick,
    identifiers::InstrumentId,
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use nautilus_thetadata::{
    common::{DEFAULT_HTTP_URL, THETADATA_VENUE},
    enums::OptionRight,
    historical::ThetaDataHistoricalClient,
    symbology::ThetaOptionContract,
};
use tokio::sync::Semaphore;

const ROOTS: &[Root] = &[
    Root { ticker: "SPY", kind: UnderlyingKind::Stock },
    Root { ticker: "SPX", kind: UnderlyingKind::Index },
    Root { ticker: "SPXW", kind: UnderlyingKind::Index },
];

const STRIKES_PER_SIDE: usize = 20;
const DEFAULT_DAYS_BACK: i64 = 30;
const DEFAULT_CONCURRENCY: usize = 4;
const EXPIRATION_LOOKAHEAD_DAYS: i64 = 45;
const PRICE_PRECISION: u8 = 2;
const SIZE_PRECISION: u8 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UnderlyingKind {
    Stock,
    Index,
}

#[derive(Clone, Copy, Debug)]
struct Root {
    ticker: &'static str,
    kind: UnderlyingKind,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog_dir = PathBuf::from(
        std::env::var("THETADATA_CATALOG_DIR")
            .unwrap_or_else(|_| "./data/thetadata-catalog".to_string()),
    );
    std::fs::create_dir_all(&catalog_dir)?;

    let days_back: i64 = std::env::var("THETADATA_DAYS_BACK")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_DAYS_BACK);
    let concurrency: usize = std::env::var("THETADATA_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CONCURRENCY);

    let http = ThetaDataHistoricalClient::new(DEFAULT_HTTP_URL, Duration::from_secs(60))?;
    let catalog = ParquetDataCatalog::new(&catalog_dir, None, None, None, None);
    let semaphore = Arc::new(Semaphore::new(concurrency));

    let today_et = Utc::now().with_timezone(&New_York).date_naive();
    let start = today_et - ChronoDuration::days(days_back);
    let trading_days: Vec<NaiveDate> = (0..=days_back)
        .map(|i| start + ChronoDuration::days(i))
        .filter(is_weekday)
        .filter(|d| *d < today_et)
        .collect();

    println!(
        "[backfill] catalog={} days_back={} trading_days={} concurrency={}",
        catalog_dir.display(),
        days_back,
        trading_days.len(),
        concurrency,
    );

    let req_total = Arc::new(AtomicU64::new(0));
    let trade_total = Arc::new(AtomicU64::new(0));
    let file_total = Arc::new(AtomicU64::new(0));

    for day in &trading_days {
        for root in ROOTS {
            let close = match fetch_close(&http, *root, *day).await {
                Ok(Some(c)) => c,
                Ok(None) => {
                    eprintln!("[backfill] {} {} no close (non-trading day?)", root.ticker, day);
                    continue;
                }
                Err(e) => {
                    eprintln!("[backfill] {} {} close error: {e}", root.ticker, day);
                    continue;
                }
            };

            let expirations = match http.list_expirations(root.ticker).await {
                Ok(exps) => exps,
                Err(e) => {
                    eprintln!("[backfill] {} list_expirations error: {e}", root.ticker);
                    continue;
                }
            };
            let max_exp = *day + ChronoDuration::days(EXPIRATION_LOOKAHEAD_DAYS);
            let active_exps: Vec<NaiveDate> = expirations
                .into_iter()
                .filter_map(|e| NaiveDate::parse_from_str(&e.expiration, "%Y-%m-%d").ok())
                .filter(|d| *d >= *day && *d <= max_exp)
                .collect();

            let mut tasks = Vec::new();
            let mut day_contract_count: u64 = 0;
            let venue = *THETADATA_VENUE;

            for exp in &active_exps {
                let strikes = match http.list_strikes(root.ticker, *exp).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[backfill] {} {} list_strikes error: {e}", root.ticker, exp);
                        continue;
                    }
                };
                let near_atm = pick_atm_strikes(&strikes, close, STRIKES_PER_SIDE);
                for strike in near_atm {
                    for right in [OptionRight::Call, OptionRight::Put] {
                        let contract = match ThetaOptionContract::from_dollar_strike(
                            root.ticker, *exp, strike, right,
                        ) {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("[backfill] bad contract: {e}");
                                continue;
                            }
                        };
                        let instrument_id = contract.to_instrument_id(venue);
                        let permit = semaphore.clone().acquire_owned().await?;
                        let http = http.clone();
                        let day = *day;
                        let req_total = Arc::clone(&req_total);
                        tasks.push(tokio::spawn(async move {
                            let _permit = permit;
                            req_total.fetch_add(1, Ordering::Relaxed);
                            let rows = http.hist_trades(&contract, day, day).await;
                            (instrument_id, rows)
                        }));
                        day_contract_count += 1;
                    }
                }
            }

            // Collect → group by instrument_id → decode → sort → write to catalog.
            let mut grouped: HashMap<InstrumentId, Vec<TradeTick>> = HashMap::new();
            for handle in tasks {
                match handle.await {
                    Ok((instrument_id, Ok(rows))) => {
                        for row in rows {
                            // ts_init = ts_event for catalog determinism. Decode errors are
                            // skipped with a warning (zero-size trades, bad timestamps, ...).
                            match row.to_trade_tick(instrument_id, PRICE_PRECISION, SIZE_PRECISION, UnixNanos::default()) {
                                Ok(mut tick) => {
                                    tick.ts_init = tick.ts_event;
                                    grouped.entry(instrument_id).or_default().push(tick);
                                }
                                Err(_e) => {} // already logged at row source
                            }
                        }
                    }
                    Ok((_, Err(_))) => {} // 472 no-data
                    Err(e) => eprintln!("[backfill] task join error: {e}"),
                }
            }

            let mut day_trade_count: u64 = 0;
            let mut day_file_count: u64 = 0;
            for (instrument_id, mut ticks) in grouped {
                if ticks.is_empty() {
                    continue;
                }
                ticks.sort_by_key(|t| t.ts_event);
                let trade_count = ticks.len();
                // `write_to_parquet` calls `block_on` internally; nest it under `block_in_place`
                // so the multi-threaded runtime tolerates the blocking sub-call.
                let result = tokio::task::block_in_place(|| {
                    catalog.write_to_parquet(ticks, None, None, None)
                });
                match result {
                    Ok(path) => {
                        log::debug!("wrote {} ticks for {instrument_id} → {}", trade_count, path.display());
                        day_trade_count += trade_count as u64;
                        day_file_count += 1;
                    }
                    Err(e) => eprintln!("[backfill] write error for {instrument_id}: {e}"),
                }
            }

            trade_total.fetch_add(day_trade_count, Ordering::Relaxed);
            file_total.fetch_add(day_file_count, Ordering::Relaxed);
            println!(
                "[backfill] {} {} contracts={} trades={} files={} (req_total={} trade_total={} file_total={})",
                root.ticker, day, day_contract_count, day_trade_count, day_file_count,
                req_total.load(Ordering::Relaxed),
                trade_total.load(Ordering::Relaxed),
                file_total.load(Ordering::Relaxed),
            );
        }
    }

    println!(
        "[backfill] DONE total_requests={} total_trades={} total_files={}",
        req_total.load(Ordering::Relaxed),
        trade_total.load(Ordering::Relaxed),
        file_total.load(Ordering::Relaxed),
    );
    // The Nautilus runtime (used internally by ParquetDataCatalog) outlives `#[tokio::main]`'s
    // runtime; letting `main` return causes a benign "drop runtime from async context" panic
    // during shutdown. Hard-exit after all writes have flushed.
    std::process::exit(0);
}

fn is_weekday(d: &NaiveDate) -> bool {
    !matches!(d.weekday(), Weekday::Sat | Weekday::Sun)
}

async fn fetch_close(
    http: &ThetaDataHistoricalClient,
    root: Root,
    day: NaiveDate,
) -> anyhow::Result<Option<f64>> {
    let rows = match root.kind {
        UnderlyingKind::Stock => http.hist_stock_eod(root.ticker, day, day).await,
        UnderlyingKind::Index => {
            http.hist_index_eod(underlying_for_index(root.ticker), day, day).await
        }
    }?;
    Ok(rows.into_iter().next().map(|r| r.close))
}

fn underlying_for_index(ticker: &str) -> &str {
    if ticker == "SPXW" { "SPX" } else { ticker }
}

fn pick_atm_strikes(rows: &[nautilus_thetadata::types::RestStrikeRow], close: f64, per_side: usize) -> Vec<f64> {
    let mut strikes: Vec<f64> = rows.iter().map(|r| r.strike).collect();
    strikes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    strikes.dedup_by(|a, b| (*a - *b).abs() < 1e-9);
    let mut below: Vec<f64> = strikes.iter().copied().filter(|s| *s <= close).collect();
    let above: Vec<f64> = strikes.iter().copied().filter(|s| *s > close).collect();
    below.reverse();
    let mut pick: Vec<f64> = below.into_iter().take(per_side + 1).collect();
    pick.extend(above.into_iter().take(per_side));
    let mut seen = HashSet::new();
    pick.retain(|s| seen.insert((s * 1000.0).round() as i64));
    pick
}
