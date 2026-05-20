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

//! Kite master-CSV row deserializer and `Instrument` mapping.
//!
//! Kite Connect publishes a 12-column dump of every tradeable instrument at
//! `GET /instruments` (~12-15 MB / 140-180 k rows, refreshed daily ~07:30 IST). Schema:
//!
//! ```text
//! instrument_token, exchange_token, tradingsymbol, name, last_price,
//! expiry, strike, tick_size, lot_size, instrument_type, segment, exchange
//! ```
//!
//! The [`KiteInstrument`] struct mirrors this schema verbatim as `String` fields (most robust
//! against schema drift) and the mapping layer parses out typed values during conversion. The
//! mapping produces an [`InstrumentAny`] plus a [`KiteToken`] side-band carrying the integer
//! `instrument_token` (Phase 3 WS) and `tradingsymbol`+`exchange` (Phase 5 REST orders).

use anyhow::{Result, anyhow};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use chrono_tz::Asia::Kolkata;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{AssetClass, OptionKind},
    identifiers::{InstrumentId, Symbol},
    instruments::{Equity, FuturesContract, IndexInstrument, InstrumentAny, OptionContract},
    types::{Currency, Price, Quantity},
};
use serde::Deserialize;
use ustr::Ustr;

use crate::{
    error::ZerodhaError,
    symbology::{InstrumentKind, KiteToken, instrument_id, price_precision_from_tick},
};

/// IST market-open used to anchor `activation_ns` for derivatives (09:15 IST = 03:45 UTC).
const NSE_OPEN_LOCAL: NaiveTime = match NaiveTime::from_hms_opt(9, 15, 0) {
    Some(t) => t,
    None => panic!("invalid hard-coded NSE open time"),
};

/// IST market-close used to anchor `expiration_ns` (15:30 IST = 10:00 UTC).
const NSE_CLOSE_LOCAL: NaiveTime = match NaiveTime::from_hms_opt(15, 30, 0) {
    Some(t) => t,
    None => panic!("invalid hard-coded NSE close time"),
};

/// CSV row from `GET /instruments` (Kite Connect v3).
#[derive(Clone, Debug, Deserialize)]
pub struct KiteInstrument {
    /// Integer ID used on the binary WS ticker stream.
    pub instrument_token: u32,
    /// Per-exchange short ID.
    pub exchange_token: u32,
    /// Human-readable Kite symbol (e.g. `RELIANCE`, `NIFTY26MAYFUT`, `NIFTY 50`).
    pub tradingsymbol: String,
    /// Full instrument name (free-form text from the exchange).
    pub name: String,
    /// Last traded price snapshot at dump time (stale; informational only).
    #[allow(dead_code)]
    pub last_price: String,
    /// Expiry as `YYYY-MM-DD`, empty for cash equities and indices.
    pub expiry: String,
    /// Option strike price as a decimal string, `"0"` for non-options.
    pub strike: String,
    /// Tick size as a decimal string (e.g. `"0.05"`, `"0.0025"`).
    pub tick_size: String,
    /// Lot size — contract multiplier for derivatives, board lot for equities (often `1`).
    pub lot_size: u64,
    /// `EQ | FUT | CE | PE`.
    pub instrument_type: String,
    /// e.g. `NSE | BSE | INDICES | NFO-FUT | NFO-OPT | CDS-FUT | MCX-FUT | …`.
    pub segment: String,
    /// e.g. `NSE | BSE | NFO | BFO | CDS | MCX | NSEIX | GLOBAL`.
    pub exchange: String,
}

/// Parse a Kite `/instruments` CSV dump into rows.
///
/// # Errors
///
/// Returns [`ZerodhaError::InvalidResponse`] if the CSV header is missing or any row fails to
/// deserialize (typed columns: `instrument_token`, `exchange_token`, `lot_size`).
pub fn parse_instruments(csv: &[u8]) -> Result<Vec<KiteInstrument>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::None)
        .from_reader(csv);

    let mut rows = Vec::with_capacity(150_000);
    for (line, record) in reader.deserialize::<KiteInstrument>().enumerate() {
        let row = record.map_err(|e| {
            ZerodhaError::InvalidResponse(format!(
                "instrument row {} failed to deserialize: {e}",
                line + 2 // +1 for 0-index, +1 for header
            ))
        })?;
        rows.push(row);
    }
    Ok(rows)
}

/// Successful mapping of a Kite row to a Nautilus `Instrument`.
#[derive(Clone, Debug)]
pub struct MappedInstrument {
    /// The Nautilus addressing handle.
    pub instrument_id: InstrumentId,
    /// Side-band identifiers (Kite-native `instrument_token` / `tradingsymbol`).
    pub token: KiteToken,
    /// Typed Nautilus instrument.
    pub instrument: InstrumentAny,
}

/// Mapping outcome — `Some(MappedInstrument)` on success, `None` when the row is intentionally
/// dropped (NCO segment, unknown instrument_type, untradeable tick_size on a non-index row).
pub type MappingResult = Result<Option<MappedInstrument>>;

/// Map a single Kite CSV row to a Nautilus [`InstrumentAny`] + side-band [`KiteToken`].
///
/// `ts_init` should be the wall-clock at which the dump was ingested (typically `clock.timestamp_ns()`).
///
/// # Errors
///
/// Returns an error if the row classifies as a tradeable kind but the typed fields fail to
/// parse (e.g. malformed `tick_size`, unparseable `expiry`, non-numeric `strike`).
pub fn map_row(row: &KiteInstrument, ts_init: UnixNanos) -> MappingResult {
    let Some(kind) = InstrumentKind::classify(&row.instrument_type, &row.segment, &row.exchange)
    else {
        return Ok(None);
    };

    // Indices ship with tick_size=0 (untradeable); for everything else a zero tick is a Kite CSV
    // data error per the spec — log it via the error path and let the caller decide whether to
    // skip or surface.
    if kind != InstrumentKind::Index && row.tick_size.trim() == "0" {
        log::warn!(
            "dropping zero-tick {} row instrument_token={}",
            row.instrument_type,
            row.instrument_token,
        );
        return Ok(None);
    }

    let instrument_id = instrument_id(kind, &row.tradingsymbol, &row.exchange)?;
    let token = KiteToken {
        instrument_token: row.instrument_token,
        exchange_token: row.exchange_token,
        tradingsymbol: row.tradingsymbol.clone(),
        exchange: row.exchange.clone(),
        kind,
    };

    let instrument = match kind {
        InstrumentKind::Equity => build_equity(row, instrument_id, ts_init)?,
        InstrumentKind::Future => build_future(row, instrument_id, ts_init)?,
        InstrumentKind::OptionCall => build_option(row, instrument_id, OptionKind::Call, ts_init)?,
        InstrumentKind::OptionPut => build_option(row, instrument_id, OptionKind::Put, ts_init)?,
        InstrumentKind::Index => build_index(row, instrument_id, ts_init)?,
    };

    Ok(Some(MappedInstrument {
        instrument_id,
        token,
        instrument,
    }))
}

fn build_equity(
    row: &KiteInstrument,
    instrument_id: InstrumentId,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    let precision = price_precision_from_tick(&row.tick_size);
    let price_increment = parse_price(&row.tick_size, precision)?;
    let lot_size = lot_size_qty(row.lot_size);

    let inst = Equity::new_checked(
        instrument_id,
        Symbol::from(row.tradingsymbol.as_str()),
        None,           // ISIN unavailable from /instruments
        Currency::INR(),           // INR is the only Kite trading currency
        precision,
        price_increment,
        Some(lot_size),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,           // info
        ts_init,
        ts_init,
    )
    .map_err(|e| anyhow!("Equity::new_checked failed for {}: {e}", row.tradingsymbol))?;
    Ok(InstrumentAny::Equity(inst))
}

fn build_future(
    row: &KiteInstrument,
    instrument_id: InstrumentId,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    let precision = price_precision_from_tick(&row.tick_size);
    let price_increment = parse_price(&row.tick_size, precision)?;
    let lot_size = lot_size_qty(row.lot_size);
    let expiration = parse_expiry(&row.expiry, NSE_CLOSE_LOCAL)?;
    let activation = parse_expiry_with_open(&row.expiry)?;

    let inst = FuturesContract::new_checked(
        instrument_id,
        Symbol::from(row.tradingsymbol.as_str()),
        asset_class_for(&row.exchange, &row.segment, &row.name),
        Some(Ustr::from(&row.exchange)),
        Ustr::from(&row.name),
        activation,
        expiration,
        Currency::INR(),
        precision,
        price_increment,
        Quantity::new(row.lot_size as f64, 0), // multiplier == lot_size for Kite futures
        lot_size,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        ts_init,
        ts_init,
    )
    .map_err(|e| anyhow!("FuturesContract::new_checked failed for {}: {e}", row.tradingsymbol))?;
    Ok(InstrumentAny::FuturesContract(inst))
}

fn build_option(
    row: &KiteInstrument,
    instrument_id: InstrumentId,
    option_kind: OptionKind,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    let precision = price_precision_from_tick(&row.tick_size);
    let price_increment = parse_price(&row.tick_size, precision)?;
    let lot_size = lot_size_qty(row.lot_size);
    let expiration = parse_expiry(&row.expiry, NSE_CLOSE_LOCAL)?;
    let activation = parse_expiry_with_open(&row.expiry)?;
    let strike = parse_price(&row.strike, precision)?;

    let inst = OptionContract::new_checked(
        instrument_id,
        Symbol::from(row.tradingsymbol.as_str()),
        asset_class_for(&row.exchange, &row.segment, &row.name),
        Some(Ustr::from(&row.exchange)),
        Ustr::from(&row.name),
        option_kind,
        strike,
        Currency::INR(),
        activation,
        expiration,
        precision,
        price_increment,
        Quantity::new(row.lot_size as f64, 0),
        lot_size,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        ts_init,
        ts_init,
    )
    .map_err(|e| anyhow!("OptionContract::new_checked failed for {}: {e}", row.tradingsymbol))?;
    Ok(InstrumentAny::OptionContract(inst))
}

fn build_index(
    row: &KiteInstrument,
    instrument_id: InstrumentId,
    ts_init: UnixNanos,
) -> Result<InstrumentAny> {
    // Indices are not directly tradeable; Kite ships tick=0 for them. Use a placeholder tick of
    // 0.01 (the finest tradeable precision on NSE) so the Price invariant holds and downstream
    // consumers can still derive sensible step sizes for derived bars / spreads.
    let precision: u8 = 2;
    let price_increment = Price::new(0.01, precision);

    let inst = IndexInstrument::new_checked(
        instrument_id,
        Symbol::from(row.tradingsymbol.as_str()),
        Currency::INR(),
        precision,
        0,                              // size_precision — index has no size
        price_increment,
        Quantity::new(1.0, 0),
        None,
        ts_init,
        ts_init,
    )
    .map_err(|e| anyhow!("IndexInstrument::new_checked failed for {}: {e}", row.tradingsymbol))?;
    Ok(InstrumentAny::IndexInstrument(inst))
}

fn parse_price(value: &str, precision: u8) -> Result<Price> {
    let parsed: f64 = value.trim().parse().map_err(|e| {
        anyhow!(
            "{}: failed to parse price/tick {:?}: {e}",
            ZerodhaError::InvalidResponse(format!("price field {value:?}")),
            value
        )
    })?;
    Ok(Price::new(parsed, precision))
}

fn lot_size_qty(lot: u64) -> Quantity {
    Quantity::from(lot)
}

/// Parse `YYYY-MM-DD` as IST local time at `local_time`, return `UnixNanos`.
fn parse_expiry(expiry: &str, local_time: NaiveTime) -> Result<UnixNanos> {
    let date = NaiveDate::parse_from_str(expiry.trim(), "%Y-%m-%d")
        .map_err(|e| anyhow!("invalid expiry {expiry:?}: {e}"))?;
    let local = NaiveDateTime::new(date, local_time);
    let utc = Kolkata
        .from_local_datetime(&local)
        .single()
        .ok_or_else(|| anyhow!("ambiguous IST datetime for expiry {expiry:?}"))?;
    Ok(UnixNanos::from(utc.timestamp_nanos_opt().unwrap_or(0) as u64))
}

fn parse_expiry_with_open(expiry: &str) -> Result<UnixNanos> {
    parse_expiry(expiry, NSE_OPEN_LOCAL)
}

fn asset_class_for(exchange: &str, segment: &str, underlying: &str) -> AssetClass {
    match exchange {
        "CDS" if underlying.starts_with("60") => AssetClass::Debt, // 601GS2030 etc.
        "CDS" => AssetClass::FX,
        "MCX" => AssetClass::Commodity,
        "BFO" | "NFO" => {
            if segment.starts_with("NCO") {
                AssetClass::Commodity
            } else {
                AssetClass::Equity
            }
        }
        _ => AssetClass::Equity,
    }
}

// We need the chrono-tz extension trait in scope to call `.from_local_datetime` on `Kolkata`,
// and `Datelike` for date-part accessors used in the refresh-cron calculation.
use chrono::{Datelike, TimeZone};

// ---------------------------------------------------------------------------------------------
// Instrument cache + master-CSV fetch
// ---------------------------------------------------------------------------------------------

/// Validation gate applied to a freshly parsed dump before atomic swap.
///
/// Spec §5/Phase-2 §9: never replace a known-good cache with a corrupt one.
#[derive(Copy, Clone, Debug)]
pub struct ValidationLimits {
    /// Minimum row count expected on any healthy dump (Kite ships ~140-180k).
    pub min_rows: usize,
    /// Whether to require a `RELIANCE-EQ.NSE` instrument as a smoke marker.
    pub require_reliance: bool,
}

impl Default for ValidationLimits {
    fn default() -> Self {
        Self {
            min_rows: 10_000,
            require_reliance: true,
        }
    }
}

/// In-memory instrument cache, double-buffered via [`ArcSwap`] so a refresh can swap atomically
/// without blocking the WS decoder reading on the hot path.
pub struct ZerodhaInstrumentCache {
    /// Decoder-side lookup: `instrument_token` → typed Nautilus instrument.
    pub by_token: arc_swap::ArcSwap<ahash::HashMap<u32, InstrumentAny>>,
    /// Order-side lookup: Nautilus `InstrumentId` → Kite native handles.
    pub by_id: arc_swap::ArcSwap<ahash::HashMap<InstrumentId, KiteToken>>,
    /// `If-None-Match` value from the most recent successful fetch — empty until the first
    /// dump has been ingested.
    etag: std::sync::Mutex<String>,
    /// Wall-clock at which the current snapshot was loaded.
    last_refresh_ns: std::sync::atomic::AtomicU64,
    /// Number of rows in the current snapshot.
    row_count: std::sync::atomic::AtomicUsize,
    /// Number of validation failures observed since startup (exposed for metrics).
    refresh_failed: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for ZerodhaInstrumentCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZerodhaInstrumentCache")
            .field("rows", &self.row_count.load(std::sync::atomic::Ordering::Acquire))
            .field("last_refresh_ns", &self.last_refresh_ns.load(std::sync::atomic::Ordering::Acquire))
            .field(
                "refresh_failures",
                &self.refresh_failed.load(std::sync::atomic::Ordering::Acquire),
            )
            .finish()
    }
}

impl Default for ZerodhaInstrumentCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ZerodhaInstrumentCache {
    /// Build an empty cache. Must be populated with [`Self::load_all`] before use.
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_token: arc_swap::ArcSwap::from_pointee(ahash::HashMap::default()),
            by_id: arc_swap::ArcSwap::from_pointee(ahash::HashMap::default()),
            etag: std::sync::Mutex::new(String::new()),
            last_refresh_ns: std::sync::atomic::AtomicU64::new(0),
            row_count: std::sync::atomic::AtomicUsize::new(0),
            refresh_failed: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Current `ETag` of the cache (empty until the first successful fetch).
    ///
    /// # Panics
    ///
    /// Panics if the internal ETag mutex was poisoned by a prior cache panic.
    pub fn etag(&self) -> String {
        self.etag.lock().expect("etag lock poisoned").clone()
    }

    /// Row count of the current snapshot (0 until first load).
    pub fn row_count(&self) -> usize {
        self.row_count.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Count of failed refreshes since startup.
    pub fn refresh_failures(&self) -> u64 {
        self.refresh_failed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Look up by `instrument_token` (decoder hot path).
    pub fn lookup_by_token(&self, token: u32) -> Option<InstrumentAny> {
        self.by_token.load().get(&token).cloned()
    }

    /// Look up the Kite native handles (`instrument_token`, `tradingsymbol`, `exchange`) by
    /// `InstrumentId`.
    pub fn lookup_by_id(&self, id: &InstrumentId) -> Option<KiteToken> {
        self.by_id.load().get(id).cloned()
    }

    /// Replace the cache with a freshly parsed dump.
    ///
    /// Atomically swaps both maps and updates ETag + row count metadata. The caller is
    /// responsible for validation before invoking — use [`Self::load_all`] for the validated
    /// fetch-and-swap path.
    ///
    /// # Panics
    ///
    /// Panics if the ETag mutex was poisoned by a prior panic inside the cache (which would
    /// indicate corruption elsewhere in the process).
    pub fn install_snapshot(
        &self,
        token_map: ahash::HashMap<u32, InstrumentAny>,
        id_map: ahash::HashMap<InstrumentId, KiteToken>,
        etag: String,
        ts_now_ns: UnixNanos,
    ) {
        let rows = token_map.len();
        self.by_token.store(std::sync::Arc::new(token_map));
        self.by_id.store(std::sync::Arc::new(id_map));
        if let Ok(mut guard) = self.etag.lock() {
            *guard = etag;
        }
        self.row_count
            .store(rows, std::sync::atomic::Ordering::Release);
        self.last_refresh_ns
            .store(u64::from(ts_now_ns), std::sync::atomic::Ordering::Release);
    }

    /// Increment the refresh-failure counter.
    pub fn record_failure(&self) {
        self.refresh_failed
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Outcome of a single `load_all` cycle.
#[derive(Debug)]
pub enum RefreshOutcome {
    /// Fresh dump fetched, validated, and atomically installed.
    Refreshed {
        /// Number of rows mapped into the new cache.
        rows: usize,
        /// Number of CSV rows the adapter intentionally dropped (NCO segment, zero-tick, …).
        dropped: usize,
        /// Number of mapping errors (logged but not fatal).
        errors: usize,
        /// New ETag captured.
        etag: String,
    },
    /// Server returned 304 Not Modified — existing cache retained.
    NotModified,
    /// Validation failed; the prior cache (if any) is retained and the failure counter
    /// incremented.
    ValidationFailed {
        /// Why the dump was rejected.
        reason: String,
    },
}

/// Result type returned to callers; transport errors propagate via [`anyhow::Result`].
pub type LoadResult = Result<RefreshOutcome>;

/// Fetch + parse + validate + atomic-swap.
///
/// Issues `GET /instruments` with the cache's current ETag in `If-None-Match`. On 304 we keep
/// the old cache. On 200 we parse, validate, then atomically install.
///
/// `ts_now_ns` should be the wall-clock at fetch time (typically `clock.timestamp_ns()`).
///
/// # Errors
///
/// Returns a transport / parse error if the dump itself can't be fetched or deserialized.
/// Validation rejections return `Ok(RefreshOutcome::ValidationFailed)` so the daily refresh
/// task can keep going.
pub async fn load_all(
    cache: &ZerodhaInstrumentCache,
    session: &crate::session::ZerodhaSessionManager,
    limits: ValidationLimits,
    ts_now_ns: UnixNanos,
) -> LoadResult {
    let api_key = session.api_key();
    let access_token = session.access_token();
    let etag = cache.etag();

    let client = reqwest::Client::builder()
        .user_agent("nautilus-zerodha")
        .build()
        .map_err(ZerodhaError::Network)?;
    let mut req = client
        .get(format!("{}/instruments", crate::common::REST_BASE))
        .header("X-Kite-Version", crate::common::KITE_VERSION)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("token {api_key}:{access_token}"),
        );
    if !etag.is_empty() {
        req = req.header(reqwest::header::IF_NONE_MATCH, &etag);
    }

    let response = req.send().await.map_err(ZerodhaError::Network)?;
    if response.status().as_u16() == 304 {
        return Ok(RefreshOutcome::NotModified);
    }
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("GET /instruments failed: HTTP {status}: {body}"));
    }
    let new_etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = response.bytes().await.map_err(ZerodhaError::Network)?;

    let rows = parse_instruments(&body)?;
    let mut token_map: ahash::HashMap<u32, InstrumentAny> =
        ahash::HashMap::with_capacity_and_hasher(rows.len(), ahash::RandomState::new());
    let mut id_map: ahash::HashMap<InstrumentId, KiteToken> =
        ahash::HashMap::with_capacity_and_hasher(rows.len(), ahash::RandomState::new());
    let mut dropped = 0_usize;
    let mut errors = 0_usize;
    for row in &rows {
        match map_row(row, ts_now_ns) {
            Ok(Some(mapped)) => {
                token_map.insert(mapped.token.instrument_token, mapped.instrument);
                id_map.insert(mapped.instrument_id, mapped.token);
            }
            Ok(None) => dropped += 1,
            Err(e) => {
                errors += 1;
                log::debug!(
                    "row {} ({}) failed mapping: {e}",
                    row.instrument_token,
                    row.tradingsymbol,
                );
            }
        }
    }

    if let Err(reason) = validate(&id_map, &limits) {
        cache.record_failure();
        return Ok(RefreshOutcome::ValidationFailed { reason });
    }

    let mapped_count = token_map.len();
    cache.install_snapshot(token_map, id_map, new_etag.clone(), ts_now_ns);
    Ok(RefreshOutcome::Refreshed {
        rows: mapped_count,
        dropped,
        errors,
        etag: new_etag,
    })
}

/// Hour-of-day at which the refresh task fires (IST). Kite publishes the new dump ~07:30 IST
/// — we fire at 07:45 IST to give the publish queue 15 min of slack.
pub const REFRESH_HOUR_IST: u32 = 7;
/// Minute-of-hour at which the refresh task fires (IST).
pub const REFRESH_MINUTE_IST: u32 = 45;

/// Compute the [`std::time::Duration`] from `now` until the next 07:45 IST tick.
///
/// If `now` lands past today's IST 07:45 the function returns the gap to tomorrow's window.
///
/// # Panics
///
/// Panics if [`REFRESH_HOUR_IST`] / [`REFRESH_MINUTE_IST`] are reset to invalid values, or if
/// `Asia/Kolkata` ever stops resolving (compile-time invariants of the workspace).
#[must_use]
pub fn duration_until_next_refresh(now: std::time::SystemTime) -> std::time::Duration {
    let now_utc = chrono::DateTime::<chrono::Utc>::from(now);
    let now_ist = now_utc.with_timezone(&Kolkata);
    let target_today = chrono::NaiveDate::from_ymd_opt(
        now_ist.date_naive().year(),
        now_ist.date_naive().month(),
        now_ist.date_naive().day(),
    )
    .and_then(|d| d.and_hms_opt(REFRESH_HOUR_IST, REFRESH_MINUTE_IST, 0))
    .expect("REFRESH_HOUR_IST + REFRESH_MINUTE_IST resolve to a valid NaiveDateTime");

    let target_ist = Kolkata
        .from_local_datetime(&target_today)
        .single()
        .or_else(|| {
            // DST ambiguity — IST has no DST so this branch is unreachable in practice. Bias
            // to "later" to avoid a duplicate refresh on the rare exception.
            Kolkata
                .from_local_datetime(&target_today)
                .latest()
        })
        .expect("Asia/Kolkata + 07:45 must resolve");
    let mut target_utc = target_ist.with_timezone(&chrono::Utc);
    if target_utc <= now_utc {
        target_utc += chrono::Duration::days(1);
    }
    (target_utc - now_utc)
        .to_std()
        .unwrap_or_else(|_| std::time::Duration::from_secs(60))
}

/// Run the daily refresh loop until `shutdown` is signalled.
///
/// The task sleeps until the next 07:45 IST, runs [`load_all`], logs the outcome, and loops.
/// `shutdown` is a `tokio::sync::watch::Receiver<bool>`; flipping it to `true` ends the loop.
pub async fn run_refresh_loop(
    cache: std::sync::Arc<ZerodhaInstrumentCache>,
    session: std::sync::Arc<crate::session::ZerodhaSessionManager>,
    limits: ValidationLimits,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut cycle: u64 = 0;
    loop {
        let sleep_for = duration_until_next_refresh(std::time::SystemTime::now());
        log::info!("instrument refresh: sleeping {sleep_for:?} until next 07:45 IST window");
        tokio::select! {
            biased;
            r = shutdown.changed() => {
                if r.is_err() || *shutdown.borrow() {
                    log::info!("instrument refresh: shutdown signalled, exiting");
                    return;
                }
            }
            () = tokio::time::sleep(sleep_for) => {}
        }

        cycle += 1;
        let ts_now = wall_clock_ns();
        match load_all(cache.as_ref(), session.as_ref(), limits, ts_now).await {
            Ok(outcome) => log::info!("instrument refresh cycle={cycle}: {outcome:?}"),
            Err(e) => {
                cache.record_failure();
                log::warn!("instrument refresh cycle={cycle} transport error: {e:?}");
            }
        }
    }
}

fn wall_clock_ns() -> UnixNanos {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    UnixNanos::from(nanos)
}

fn validate(
    id_map: &ahash::HashMap<InstrumentId, KiteToken>,
    limits: &ValidationLimits,
) -> std::result::Result<(), String> {
    if id_map.len() < limits.min_rows {
        return Err(format!(
            "cache has {} rows, below floor of {}",
            id_map.len(),
            limits.min_rows
        ));
    }
    if limits.require_reliance {
        let needle = InstrumentId::new(Symbol::from("RELIANCE-EQ"), nautilus_model::identifiers::Venue::from("NSE"));
        if !id_map.contains_key(&needle) {
            return Err("cache missing RELIANCE-EQ.NSE marker row".to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn ts() -> UnixNanos {
        UnixNanos::from(1_700_000_000_000_000_000u64)
    }

    #[rstest]
    fn parse_handles_real_reliance_row() {
        let csv = b"instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,strike,tick_size,lot_size,instrument_type,segment,exchange\n\
                    738561,2885,RELIANCE,\"RELIANCE INDUSTRIES\",0,,0,0.1,1,EQ,NSE,NSE\n";
        let rows = parse_instruments(csv).expect("parse");
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.instrument_token, 738561);
        assert_eq!(row.tradingsymbol, "RELIANCE");
        assert_eq!(row.exchange, "NSE");
        assert_eq!(row.tick_size, "0.1");
    }

    #[rstest]
    fn map_reliance_to_equity() {
        let row = KiteInstrument {
            instrument_token: 738561,
            exchange_token: 2885,
            tradingsymbol: "RELIANCE".into(),
            name: "RELIANCE INDUSTRIES".into(),
            last_price: "0".into(),
            expiry: String::new(),
            strike: "0".into(),
            tick_size: "0.1".into(),
            lot_size: 1,
            instrument_type: "EQ".into(),
            segment: "NSE".into(),
            exchange: "NSE".into(),
        };
        let mapped = map_row(&row, ts()).unwrap().expect("equity row");
        assert_eq!(mapped.instrument_id.to_string(), "RELIANCE-EQ.NSE");
        assert!(matches!(mapped.instrument, InstrumentAny::Equity(_)));
        assert_eq!(mapped.token.instrument_token, 738561);
    }

    #[rstest]
    fn map_nifty_future() {
        let row = KiteInstrument {
            instrument_token: 16914178,
            exchange_token: 66071,
            tradingsymbol: "NIFTY26MAYFUT".into(),
            name: "NIFTY".into(),
            last_price: "0".into(),
            expiry: "2026-05-26".into(),
            strike: "0".into(),
            tick_size: "0.1".into(),
            lot_size: 65,
            instrument_type: "FUT".into(),
            segment: "NFO-FUT".into(),
            exchange: "NFO".into(),
        };
        let mapped = map_row(&row, ts()).unwrap().expect("future row");
        assert_eq!(mapped.instrument_id.to_string(), "NIFTY26MAYFUT.NFO");
        assert!(matches!(mapped.instrument, InstrumentAny::FuturesContract(_)));
    }

    #[rstest]
    fn map_nifty_option_call() {
        let row = KiteInstrument {
            instrument_token: 18468610,
            exchange_token: 72143,
            tradingsymbol: "NIFTY26MAY23600CE".into(),
            name: "NIFTY".into(),
            last_price: "0".into(),
            expiry: "2026-05-26".into(),
            strike: "23600".into(),
            tick_size: "0.05".into(),
            lot_size: 65,
            instrument_type: "CE".into(),
            segment: "NFO-OPT".into(),
            exchange: "NFO".into(),
        };
        let mapped = map_row(&row, ts()).unwrap().expect("option row");
        assert_eq!(mapped.instrument_id.to_string(), "NIFTY26MAY23600CE.NFO");
        if let InstrumentAny::OptionContract(opt) = &mapped.instrument {
            assert_eq!(opt.option_kind, OptionKind::Call);
            assert_eq!(opt.strike_price.as_f64(), 23600.0);
        } else {
            panic!("expected OptionContract");
        }
    }

    #[rstest]
    fn map_index_routes_by_segment_not_instrument_type() {
        let row = KiteInstrument {
            instrument_token: 256265,
            exchange_token: 1001,
            tradingsymbol: "NIFTY 50".into(),
            name: "NIFTY 50".into(),
            last_price: "0".into(),
            expiry: String::new(),
            strike: "0".into(),
            tick_size: "0".into(), // indices have tick=0 — must not be rejected
            lot_size: 0,
            instrument_type: "EQ".into(),
            segment: "INDICES".into(),
            exchange: "NSE".into(),
        };
        let mapped = map_row(&row, ts()).unwrap().expect("index row");
        assert_eq!(mapped.instrument_id.to_string(), "NIFTY-50.NSE_INDEX");
        assert!(matches!(mapped.instrument, InstrumentAny::IndexInstrument(_)));
    }

    #[rstest]
    fn map_zero_tick_non_index_dropped() {
        let row = KiteInstrument {
            instrument_token: 1,
            exchange_token: 1,
            tradingsymbol: "BROKEN".into(),
            name: "BROKEN".into(),
            last_price: "0".into(),
            expiry: String::new(),
            strike: "0".into(),
            tick_size: "0".into(),
            lot_size: 1,
            instrument_type: "EQ".into(),
            segment: "NSE".into(),
            exchange: "NSE".into(),
        };
        assert!(map_row(&row, ts()).unwrap().is_none());
    }

    #[rstest]
    fn map_nco_segment_dropped() {
        let row = KiteInstrument {
            instrument_token: 9,
            exchange_token: 9,
            tradingsymbol: "NCOTHING26MAYFUT".into(),
            name: "NCOTHING".into(),
            last_price: "0".into(),
            expiry: "2026-05-26".into(),
            strike: "0".into(),
            tick_size: "0.05".into(),
            lot_size: 1,
            instrument_type: "FUT".into(),
            segment: "NCO-FUT".into(),
            exchange: "NCO".into(),
        };
        assert!(map_row(&row, ts()).unwrap().is_none());
    }

    #[rstest]
    fn duration_until_next_refresh_skips_past_today_window() {
        use chrono::{TimeZone, Utc};
        // 04:00 UTC = 09:30 IST — already past 07:45 IST, expect ~22h gap to tomorrow.
        let now = Utc.with_ymd_and_hms(2026, 5, 20, 4, 0, 0).unwrap();
        let sleep = duration_until_next_refresh(now.into());
        let hrs = sleep.as_secs() / 3600;
        assert!((21..=23).contains(&hrs), "expected ~22h gap, got {hrs}h");
    }

    #[rstest]
    fn duration_until_next_refresh_targets_today_when_before_window() {
        use chrono::{TimeZone, Utc};
        // 00:00 UTC = 05:30 IST — before today's 07:45 IST, expect ~2h15m gap.
        let now = Utc.with_ymd_and_hms(2026, 5, 20, 0, 0, 0).unwrap();
        let sleep = duration_until_next_refresh(now.into());
        let minutes = sleep.as_secs() / 60;
        assert!(
            (130..=140).contains(&minutes),
            "expected ~135min gap, got {minutes}min"
        );
    }

    #[rstest]
    fn validate_rejects_undersized_cache() {
        let map: ahash::HashMap<InstrumentId, KiteToken> = ahash::HashMap::default();
        let err = validate(&map, &ValidationLimits::default()).unwrap_err();
        assert!(err.contains("below floor"), "got: {err}");
    }

    #[rstest]
    fn validate_rejects_missing_reliance() {
        let mut map: ahash::HashMap<InstrumentId, KiteToken> =
            ahash::HashMap::with_capacity_and_hasher(11_000, ahash::RandomState::new());
        // Fill with placeholder ids so we clear the size floor but never insert RELIANCE-EQ.NSE.
        for i in 0..11_000_u32 {
            let id = InstrumentId::new(
                Symbol::from(format!("STUB{i}").as_str()),
                nautilus_model::identifiers::Venue::from("NSE"),
            );
            map.insert(
                id,
                KiteToken {
                    instrument_token: i,
                    exchange_token: i,
                    tradingsymbol: format!("STUB{i}"),
                    exchange: "NSE".into(),
                    kind: InstrumentKind::Equity,
                },
            );
        }
        let err = validate(&map, &ValidationLimits::default()).unwrap_err();
        assert!(err.contains("RELIANCE"), "got: {err}");
    }

    #[rstest]
    fn install_snapshot_then_lookup_round_trips() {
        let cache = ZerodhaInstrumentCache::new();
        let row = KiteInstrument {
            instrument_token: 738561,
            exchange_token: 2885,
            tradingsymbol: "RELIANCE".into(),
            name: "RELIANCE INDUSTRIES".into(),
            last_price: "0".into(),
            expiry: String::new(),
            strike: "0".into(),
            tick_size: "0.1".into(),
            lot_size: 1,
            instrument_type: "EQ".into(),
            segment: "NSE".into(),
            exchange: "NSE".into(),
        };
        let mapped = map_row(&row, ts()).unwrap().unwrap();
        let mut token_map: ahash::HashMap<u32, InstrumentAny> =
            ahash::HashMap::with_capacity_and_hasher(1, ahash::RandomState::new());
        let mut id_map: ahash::HashMap<InstrumentId, KiteToken> =
            ahash::HashMap::with_capacity_and_hasher(1, ahash::RandomState::new());
        token_map.insert(mapped.token.instrument_token, mapped.instrument.clone());
        id_map.insert(mapped.instrument_id, mapped.token);
        cache.install_snapshot(token_map, id_map, "etag-X".into(), ts());

        assert_eq!(cache.row_count(), 1);
        assert_eq!(cache.etag(), "etag-X");
        assert!(cache.lookup_by_token(738561).is_some());
        let id =
            InstrumentId::new(Symbol::from("RELIANCE-EQ"), nautilus_model::identifiers::Venue::from("NSE"));
        let tok = cache.lookup_by_id(&id).unwrap();
        assert_eq!(tok.instrument_token, 738561);
    }

    #[rstest]
    fn parse_corrupt_csv_returns_error() {
        // Garbage with too few columns on the data row — csv::Deserialize will surface an error.
        let garbage = b"not,a,valid,csv\n12,oops,no,more,columns";
        assert!(
            parse_instruments(garbage).is_err(),
            "expected parse error on corrupt CSV",
        );
    }

    #[rstest]
    fn validation_failure_preserves_existing_cache() {
        // Pre-populate the cache with one good RELIANCE row.
        let cache = ZerodhaInstrumentCache::new();
        let row = KiteInstrument {
            instrument_token: 738561,
            exchange_token: 2885,
            tradingsymbol: "RELIANCE".into(),
            name: "RELIANCE INDUSTRIES".into(),
            last_price: "0".into(),
            expiry: String::new(),
            strike: "0".into(),
            tick_size: "0.1".into(),
            lot_size: 1,
            instrument_type: "EQ".into(),
            segment: "NSE".into(),
            exchange: "NSE".into(),
        };
        let mapped = map_row(&row, ts()).unwrap().unwrap();
        let mut token_map: ahash::HashMap<u32, InstrumentAny> =
            ahash::HashMap::with_capacity_and_hasher(1, ahash::RandomState::new());
        let mut id_map: ahash::HashMap<InstrumentId, KiteToken> =
            ahash::HashMap::with_capacity_and_hasher(1, ahash::RandomState::new());
        token_map.insert(mapped.token.instrument_token, mapped.instrument.clone());
        id_map.insert(mapped.instrument_id, mapped.token);
        cache.install_snapshot(token_map, id_map, "good-etag".into(), ts());
        assert_eq!(cache.row_count(), 1);

        // Simulate a corrupt refresh: validate rejects → record_failure → cache untouched.
        let bad: ahash::HashMap<InstrumentId, KiteToken> = ahash::HashMap::default();
        assert!(validate(&bad, &ValidationLimits::default()).is_err());
        cache.record_failure();

        assert_eq!(cache.row_count(), 1, "old cache must survive failed refresh");
        assert_eq!(cache.etag(), "good-etag");
        assert_eq!(cache.refresh_failures(), 1);
        assert!(
            cache
                .lookup_by_id(&InstrumentId::new(
                    Symbol::from("RELIANCE-EQ"),
                    nautilus_model::identifiers::Venue::from("NSE"),
                ))
                .is_some(),
            "RELIANCE-EQ.NSE must still resolve",
        );
    }

    #[rstest]
    fn parse_committed_sample_fixture_round_trips() {
        // Committed ~3k-row sample covering all routing paths (NSE+BSE EQ, NFO FUT/CE/PE,
        // INDICES, CDS, MCX). Catches regressions on the parse + mapping layer in CI without
        // needing the full 11 MB live dump.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/instruments_sample.csv");
        let bytes = std::fs::read(&path).expect("read sample fixture");
        let rows = parse_instruments(&bytes).expect("parse");
        assert!(rows.len() > 2_000, "sample too small: {}", rows.len());

        let mut mapped = 0_usize;
        let mut dropped = 0_usize;
        let mut errors = 0_usize;
        let mut kinds: ahash::HashMap<&'static str, usize> = ahash::HashMap::default();
        for row in &rows {
            match map_row(row, ts()) {
                Ok(Some(m)) => {
                    mapped += 1;
                    let label = match m.token.kind {
                        InstrumentKind::Equity => "equity",
                        InstrumentKind::Future => "future",
                        InstrumentKind::OptionCall => "call",
                        InstrumentKind::OptionPut => "put",
                        InstrumentKind::Index => "index",
                    };
                    *kinds.entry(label).or_default() += 1;
                }
                Ok(None) => dropped += 1,
                Err(_) => errors += 1,
            }
        }
        assert!(mapped > 2_000, "mapped count too low: {mapped}");
        // We deliberately included every routing path; each must show up.
        for label in ["equity", "future", "call", "put", "index"] {
            assert!(
                kinds.get(label).copied().unwrap_or(0) > 0,
                "no {label} rows mapped — sample fixture missing coverage",
            );
        }
        log::info!(
            "sample fixture parse: {} rows -> mapped={mapped} dropped={dropped} errors={errors}",
            rows.len(),
        );
    }

    #[rstest]
    fn parse_full_live_fixture_when_present() {
        // Operator-captured dump (not committed; obtain via the smoke flow). Skipped in CI.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/instruments_2026-05-20.csv");
        if !path.exists() {
            return;
        }
        let bytes = std::fs::read(&path).expect("read fixture");
        let rows = parse_instruments(&bytes).expect("parse");
        assert!(rows.len() > 100_000, "live fixture too small: {}", rows.len());
        let mut mapped = 0_usize;
        let mut errors = 0_usize;
        for row in &rows {
            match map_row(row, ts()) {
                Ok(Some(_)) => mapped += 1,
                Ok(None) => {}
                Err(_) => errors += 1,
            }
        }
        assert!(mapped > 100_000, "mapped count too low: {mapped}");
        assert!(
            errors < rows.len() / 100,
            "more than 1% of rows errored: {errors} / {}",
            rows.len()
        );
    }
}
