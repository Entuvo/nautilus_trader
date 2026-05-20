// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Historical bar / candle fetch + Nautilus `Bar` conversion (spec §5/Phase-4).
//!
//! # Endpoint
//!
//! `GET https://api.kite.trade/instruments/historical/{instrument_token}/{interval}`
//!
//! Query string: `from=YYYY-MM-DD%20HH:MM:SS`, `to=YYYY-MM-DD%20HH:MM:SS`, optional `oi=1` for
//! derivatives.
//!
//! Response shape (Kite v3):
//!
//! ```json
//! {
//!   "status": "success",
//!   "data": {
//!     "candles": [
//!       ["2025-05-15T09:15:00+0530", 19800.0, 19850.0, 19795.0, 19825.0, 12345],
//!       ["2025-05-15T09:20:00+0530", 19825.0, 19840.0, 19815.0, 19830.0,  9876],
//!       ...
//!     ]
//!   }
//! }
//! ```
//!
//! Each candle is a heterogeneous array: `[ts, open, high, low, close, volume]`, with a trailing
//! `oi` value when `oi=1` is requested.
//!
//! # Chunking (Kite per-request limits)
//!
//! | Resolution          | Max chunk |
//! |---------------------|-----------|
//! | minute              | 60 days   |
//! | 3 / 5 / 10 / 15-min | 100 days  |
//! | 30 / 60-min         | 200 days  |
//! | day                 | 2000 days |
//!
//! Wrong chunk size produces silent gaps. [`KiteResolution::chunk_days`] is the source of truth.
//!
//! # Timestamps
//!
//! - **Intraday**: Kite stamps with the `+0530` offset. We parse via [`chrono::DateTime::parse_from_str`]
//!   then convert to UTC. `ts_event` is the bar-open instant (Kite's native convention).
//! - **Daily**: Kite emits a bare date string (`"2025-05-15"`). We anchor to **NSE session open
//!   09:15 IST = 03:45 UTC**. This matches IBKR's Indian-equities adapter, openalgo's daily map,
//!   and strategy intuition (bar belongs to "the trading session that started at 09:15").
//!
//! # OI on derivatives
//!
//! `oi=1` is requested by default for futures and options (caller-controlled via the `oi`
//! parameter to [`fetch_candles_raw`]). The trailing OI value lands on [`KiteCandle::oi`] when
//! present; v1 doesn't yet wire OI through to a Nautilus type — it's stored on the candle for
//! Phase 5+ consumers.

use anyhow::{Result, anyhow};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Asia::Kolkata;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::bar::{Bar, BarType},
    types::{Price, Quantity},
};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

use crate::{
    common::{KITE_VERSION, REST_BASE},
    error::ZerodhaError,
    session::ZerodhaSessionManager,
};

/// Anchor time for daily bars — 09:15 IST is when the NSE / BSE cash session opens.
const NSE_OPEN_LOCAL: NaiveTime = match NaiveTime::from_hms_opt(9, 15, 0) {
    Some(t) => t,
    None => panic!("invalid NSE_OPEN_LOCAL"),
};

/// Resolution accepted by `GET /instruments/historical`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum KiteResolution {
    /// 1-minute candles.
    Minute,
    /// 3-minute candles.
    ThreeMinute,
    /// 5-minute candles.
    FiveMinute,
    /// 10-minute candles.
    TenMinute,
    /// 15-minute candles.
    FifteenMinute,
    /// 30-minute candles.
    ThirtyMinute,
    /// 60-minute candles.
    SixtyMinute,
    /// Daily candles.
    Day,
}

impl KiteResolution {
    /// The string Kite expects in the `/historical/{token}/{interval}` path.
    #[must_use]
    pub const fn as_kite_str(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::ThreeMinute => "3minute",
            Self::FiveMinute => "5minute",
            Self::TenMinute => "10minute",
            Self::FifteenMinute => "15minute",
            Self::ThirtyMinute => "30minute",
            Self::SixtyMinute => "60minute",
            Self::Day => "day",
        }
    }

    /// Kite's per-request day limit. Crossing this returns an error from the API.
    #[must_use]
    pub const fn chunk_days(self) -> i64 {
        match self {
            Self::Minute => 60,
            Self::ThreeMinute | Self::FiveMinute | Self::TenMinute | Self::FifteenMinute => 100,
            Self::ThirtyMinute | Self::SixtyMinute => 200,
            Self::Day => 2000,
        }
    }

    /// Whether candles for this resolution carry an intraday timestamp (versus a bare date).
    #[must_use]
    pub const fn is_intraday(self) -> bool {
        !matches!(self, Self::Day)
    }
}

/// One row from `data.candles`.
#[derive(Clone, Debug, PartialEq)]
pub struct KiteCandle {
    /// Bar-open instant in UTC nanos. For daily bars this is `09:15 IST` of the candle date.
    pub ts_event: UnixNanos,
    /// Open price.
    pub open: f64,
    /// High price.
    pub high: f64,
    /// Low price.
    pub low: f64,
    /// Close price.
    pub close: f64,
    /// Traded volume during the bar (0 for indices).
    pub volume: f64,
    /// Open interest at bar-close. Present only when `oi=1` was requested.
    pub oi: Option<f64>,
}

/// Pause between chunked requests so a multi-year backfill doesn't trip the API quota.
pub const CHUNK_GAP: Duration = Duration::from_millis(250);

/// Fetch + stitch + dedupe candles across the `[from, to]` window.
///
/// `from` / `to` are wall-clock UTC instants; the chunker splits into `chunk_days`-wide windows
/// and issues one HTTP call per chunk. Boundary candles are deduped by `ts_event` so the join
/// between adjacent chunks doesn't double-count.
///
/// # Errors
///
/// - [`ZerodhaError::Network`] on transport failure.
/// - [`ZerodhaError::KiteError`] / [`ZerodhaError::TokenException`] on a Kite-side rejection.
/// - [`ZerodhaError::InvalidResponse`] if a chunk body fails to deserialize.
pub async fn fetch_candles_raw(
    session: &ZerodhaSessionManager,
    instrument_token: u32,
    resolution: KiteResolution,
    from: chrono::DateTime<Utc>,
    to: chrono::DateTime<Utc>,
    oi: bool,
) -> Result<Vec<KiteCandle>> {
    if to < from {
        return Err(anyhow!("fetch_candles_raw: `to` < `from`"));
    }

    let client = Client::builder()
        .user_agent("nautilus-zerodha")
        .build()
        .map_err(ZerodhaError::Network)?;
    let chunk_span = chrono::Duration::days(resolution.chunk_days());

    let mut acc: Vec<KiteCandle> = Vec::new();
    let mut window_start = from;
    let mut first_chunk = true;

    while window_start <= to {
        let window_end = (window_start + chunk_span).min(to);
        if !first_chunk {
            tokio::time::sleep(CHUNK_GAP).await;
        }
        first_chunk = false;

        let chunk = fetch_chunk(
            &client,
            session,
            instrument_token,
            resolution,
            window_start,
            window_end,
            oi,
        )
        .await?;
        acc.extend(chunk);

        if window_end == to {
            break;
        }
        window_start = window_end + chrono::Duration::seconds(1);
    }

    // Defensive dedupe + sort. Kite returns sorted within a chunk, but boundary overlap can
    // produce duplicates after stitching.
    acc.sort_by_key(|c| c.ts_event);
    acc.dedup_by_key(|c| c.ts_event);
    Ok(acc)
}

async fn fetch_chunk(
    client: &Client,
    session: &ZerodhaSessionManager,
    instrument_token: u32,
    resolution: KiteResolution,
    from: chrono::DateTime<Utc>,
    to: chrono::DateTime<Utc>,
    oi: bool,
) -> Result<Vec<KiteCandle>> {
    let from_str = from.format("%Y-%m-%d %H:%M:%S").to_string();
    let to_str = to.format("%Y-%m-%d %H:%M:%S").to_string();
    let api_key = session.api_key();
    let access_token = session.access_token();

    let mut request = client
        .get(format!(
            "{REST_BASE}/instruments/historical/{instrument_token}/{}",
            resolution.as_kite_str()
        ))
        .header("X-Kite-Version", KITE_VERSION)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("token {api_key}:{access_token}"),
        )
        .query(&[("from", from_str.as_str()), ("to", to_str.as_str())]);
    if oi {
        request = request.query(&[("oi", "1")]);
    }
    let response = request.send().await.map_err(ZerodhaError::Network)?;
    let status = response.status();
    let body = response.text().await.map_err(ZerodhaError::Network)?;
    if !status.is_success() {
        return Err(anyhow!(
            "GET /instruments/historical HTTP {}: {body}",
            status.as_u16()
        ));
    }

    parse_candles_envelope(&body, resolution)
}

#[derive(Deserialize)]
struct HistoricalEnvelope {
    status: Option<String>,
    data: Option<HistoricalData>,
    error_type: Option<String>,
    message: Option<String>,
}

#[derive(Deserialize)]
struct HistoricalData {
    candles: Vec<Vec<serde_json::Value>>,
}

fn parse_candles_envelope(body: &str, resolution: KiteResolution) -> Result<Vec<KiteCandle>> {
    let env: HistoricalEnvelope = serde_json::from_str(body).map_err(|e| {
        ZerodhaError::InvalidResponse(format!("/historical body not JSON: {e}"))
    })?;
    if env.status.as_deref() == Some("error") {
        return Err(ZerodhaError::from_kite_body(
            400,
            env.error_type.as_deref().unwrap_or("Unknown"),
            env.message.as_deref().unwrap_or(""),
        )
        .into());
    }
    let data = env.data.ok_or_else(|| {
        ZerodhaError::InvalidResponse("/historical missing `data` field".to_string())
    })?;

    let mut out = Vec::with_capacity(data.candles.len());
    for row in data.candles {
        out.push(parse_candle_row(&row, resolution)?);
    }
    Ok(out)
}

fn parse_candle_row(row: &[serde_json::Value], resolution: KiteResolution) -> Result<KiteCandle> {
    if row.len() < 6 {
        return Err(anyhow!("candle row too short: {row:?}"));
    }
    let ts_str = row[0]
        .as_str()
        .ok_or_else(|| anyhow!("candle[0] is not a string: {:?}", row[0]))?;
    let ts_event = parse_candle_timestamp(ts_str, resolution.is_intraday())?;
    let open = read_f64(&row[1])?;
    let high = read_f64(&row[2])?;
    let low = read_f64(&row[3])?;
    let close = read_f64(&row[4])?;
    let volume = read_f64(&row[5])?;
    let oi = row.get(6).map(read_f64).transpose()?;

    Ok(KiteCandle {
        ts_event,
        open,
        high,
        low,
        close,
        volume,
        oi,
    })
}

fn read_f64(value: &serde_json::Value) -> Result<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|v| v as f64))
        .or_else(|| value.as_u64().map(|v| v as f64))
        .ok_or_else(|| anyhow!("expected numeric, got {value:?}"))
}

/// Parse a Kite candle timestamp to UTC nanos.
///
/// - Intraday: `"2025-05-15T09:15:00+0530"` — chrono parses with `%z`, we just convert to UTC.
/// - Daily: `"2025-05-15"` — anchor at 09:15 IST (NSE session open), then convert.
///
/// # Errors
///
/// Returns an error if the string can't be parsed in the expected format.
pub fn parse_candle_timestamp(s: &str, is_intraday: bool) -> Result<UnixNanos> {
    if is_intraday {
        let dt = chrono::DateTime::parse_from_str(s.trim(), "%Y-%m-%dT%H:%M:%S%z")
            .map_err(|e| anyhow!("invalid intraday timestamp {s:?}: {e}"))?;
        let utc = dt.with_timezone(&Utc);
        Ok(UnixNanos::from(
            utc.timestamp_nanos_opt().unwrap_or(0) as u64,
        ))
    } else {
        let date = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
            .map_err(|e| anyhow!("invalid daily date {s:?}: {e}"))?;
        let local = NaiveDateTime::new(date, NSE_OPEN_LOCAL);
        let ist = Kolkata
            .from_local_datetime(&local)
            .single()
            .ok_or_else(|| anyhow!("ambiguous IST datetime for date {s:?}"))?;
        let utc = ist.with_timezone(&Utc);
        Ok(UnixNanos::from(
            utc.timestamp_nanos_opt().unwrap_or(0) as u64,
        ))
    }
}

/// Convert a parsed [`KiteCandle`] to a Nautilus [`Bar`].
///
/// `bar_type` identifies the instrument + spec; `price_precision` / `size_precision` should
/// match the instrument's configuration. `ts_init` is the wall-clock at which the bar was
/// ingested (typically `clock.timestamp_ns()`).
#[must_use]
pub fn to_bar(
    candle: &KiteCandle,
    bar_type: BarType,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> Bar {
    Bar::new(
        bar_type,
        Price::new(candle.open, price_precision),
        Price::new(candle.high, price_precision),
        Price::new(candle.low, price_precision),
        Price::new(candle.close, price_precision),
        Quantity::new(candle.volume, size_precision),
        candle.ts_event,
        ts_init,
    )
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, TimeZone, Timelike};
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(KiteResolution::Minute, "minute", 60)]
    #[case(KiteResolution::ThreeMinute, "3minute", 100)]
    #[case(KiteResolution::FiveMinute, "5minute", 100)]
    #[case(KiteResolution::FifteenMinute, "15minute", 100)]
    #[case(KiteResolution::ThirtyMinute, "30minute", 200)]
    #[case(KiteResolution::SixtyMinute, "60minute", 200)]
    #[case(KiteResolution::Day, "day", 2000)]
    fn resolution_table(
        #[case] resolution: KiteResolution,
        #[case] kite_str: &str,
        #[case] chunk_days: i64,
    ) {
        assert_eq!(resolution.as_kite_str(), kite_str);
        assert_eq!(resolution.chunk_days(), chunk_days);
    }

    #[rstest]
    fn intraday_timestamp_round_trips_ist_offset() {
        // 09:15 IST = 03:45 UTC
        let ts = parse_candle_timestamp("2025-05-15T09:15:00+0530", true).unwrap();
        let expected = Utc.with_ymd_and_hms(2025, 5, 15, 3, 45, 0).unwrap();
        assert_eq!(
            u64::from(ts),
            expected.timestamp_nanos_opt().unwrap() as u64
        );
    }

    #[rstest]
    fn intraday_5min_bar_at_session_open() {
        // 09:15-09:20 5-minute bar — ts_event must be exactly 09:15 IST (= 03:45 UTC).
        let ts = parse_candle_timestamp("2025-05-15T09:15:00+0530", true).unwrap();
        let utc = chrono::DateTime::<Utc>::from_timestamp(
            (u64::from(ts) / 1_000_000_000) as i64,
            0,
        )
        .unwrap();
        assert_eq!(utc.hour(), 3);
        assert_eq!(utc.minute(), 45);
        assert_eq!(utc.year(), 2025);
        assert_eq!(utc.month(), 5);
        assert_eq!(utc.day(), 15);
    }

    #[rstest]
    fn daily_bar_anchors_to_ist_session_open() {
        // "2025-05-15" daily bar → 2025-05-15 09:15 IST = 2025-05-15 03:45 UTC
        let ts = parse_candle_timestamp("2025-05-15", false).unwrap();
        let expected = Utc.with_ymd_and_hms(2025, 5, 15, 3, 45, 0).unwrap();
        assert_eq!(
            u64::from(ts),
            expected.timestamp_nanos_opt().unwrap() as u64,
        );
    }

    #[rstest]
    fn parse_envelope_with_oi() {
        let body = r#"{"status":"success","data":{"candles":[
            ["2025-05-15T09:15:00+0530",19800.0,19850.0,19795.0,19825.0,12345,5500],
            ["2025-05-15T09:20:00+0530",19825.0,19840.0,19815.0,19830.0,9876,5600]
        ]}}"#;
        let candles = parse_candles_envelope(body, KiteResolution::FiveMinute).unwrap();
        assert_eq!(candles.len(), 2);
        let first = &candles[0];
        assert!((first.open - 19_800.0).abs() < 1e-6);
        assert!((first.close - 19_825.0).abs() < 1e-6);
        assert_eq!(first.volume, 12_345.0);
        assert_eq!(first.oi, Some(5500.0));
    }

    #[rstest]
    fn parse_envelope_without_oi() {
        let body = r#"{"status":"success","data":{"candles":[
            ["2025-05-15T09:15:00+0530",19800.0,19850.0,19795.0,19825.0,12345]
        ]}}"#;
        let candles = parse_candles_envelope(body, KiteResolution::FiveMinute).unwrap();
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].oi, None);
    }

    #[rstest]
    fn parse_envelope_daily_format() {
        let body = r#"{"status":"success","data":{"candles":[
            ["2025-05-15",19800.0,19850.0,19795.0,19825.0,123456]
        ]}}"#;
        let candles = parse_candles_envelope(body, KiteResolution::Day).unwrap();
        assert_eq!(candles.len(), 1);
        let expected_ts = parse_candle_timestamp("2025-05-15", false).unwrap();
        assert_eq!(candles[0].ts_event, expected_ts);
    }

    #[rstest]
    fn parse_envelope_propagates_kite_error() {
        let body = r#"{"status":"error","error_type":"TokenException","message":"bad token"}"#;
        let err = parse_candles_envelope(body, KiteResolution::Minute).unwrap_err();
        let inner = err.downcast::<ZerodhaError>().unwrap();
        assert!(matches!(inner, ZerodhaError::TokenException(_)));
    }

    #[rstest]
    fn to_bar_round_trips_ohlcv() {
        use nautilus_model::{
            data::bar::BarSpecification,
            enums::{AggregationSource, BarAggregation, PriceType},
            identifiers::{InstrumentId, Symbol, Venue},
        };
        let id = InstrumentId::new(Symbol::from("NIFTY26MAYFUT"), Venue::from("NFO"));
        let spec = BarSpecification::new(5, BarAggregation::Minute, PriceType::Last);
        let bt = BarType::new(id, spec, AggregationSource::External);

        let candle = KiteCandle {
            ts_event: parse_candle_timestamp("2025-05-15T09:15:00+0530", true).unwrap(),
            open: 19_800.05,
            high: 19_850.10,
            low: 19_795.20,
            close: 19_825.55,
            volume: 12_345.0,
            oi: Some(5_500.0),
        };
        let ts_init = UnixNanos::from(1_700_000_000_000_000_000u64);
        let bar = to_bar(&candle, bt, 2, 0, ts_init);
        assert!((bar.open.as_f64() - 19_800.05).abs() < 0.01);
        assert!((bar.close.as_f64() - 19_825.55).abs() < 0.01);
        assert_eq!(bar.volume.as_f64(), 12_345.0);
        assert_eq!(bar.ts_event, candle.ts_event);
        assert_eq!(bar.ts_init, ts_init);
    }
}
