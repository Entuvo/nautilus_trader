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

//! HTTP REST client for the local ThetaTerminal v3 API.
//!
//! The Terminal exposes a stateless HTTP server at `http://127.0.0.1:25503/v3` (configurable).
//! Authentication is owned by the Terminal process — requests carry no signing or bearer tokens.
//!
//! Multi-day historical requests are capped at 1 month per call; chunking by date window is the
//! responsibility of the calling layer.
//!
//! All endpoints are invoked with `format=ndjson` so responses can be parsed line-by-line.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use reqwest::{Client, Response, StatusCode};
use serde::de::DeserializeOwned;

use crate::{
    enums::{Interval, OptionRight},
    symbology::ThetaOptionContract,
    types::{
        RestContractRow, RestEodRow, RestExpirationRow, RestOhlcRow, RestQuoteRow, RestStrikeRow,
        RestTradeRow,
    },
};

/// Maximum date span per historical request (ThetaData multi-day cap).
pub const MAX_HISTORY_SPAN_DAYS: i64 = 30;

/// REST client over the local ThetaTerminal.
#[derive(Clone, Debug)]
pub struct ThetaDataHistoricalClient {
    base_url: String,
    http: Client,
}

impl ThetaDataHistoricalClient {
    /// Creates a new client.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying `reqwest::Client` cannot be constructed.
    pub fn new(base_url: impl Into<String>, timeout: Duration) -> Result<Self> {
        let http = Client::builder()
            .timeout(timeout)
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self {
            base_url: base_url.into(),
            http,
        })
    }

    /// Returns the configured base URL.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns a reference to the underlying HTTP client.
    #[must_use]
    pub fn http(&self) -> &Client {
        &self.http
    }

    /// Lists every option contract for the underlying symbol with expirations on or after `date`.
    ///
    /// ThetaTerminal v3 does **not** expose a single `/v3/option/list/contracts` endpoint
    /// (despite earlier doc claims). This method composes the result client-side from
    /// `list_expirations` + `list_strikes`, expanding each (expiration, strike) pair into a
    /// call and a put.
    ///
    /// The returned rows carry both fields populated even though the underlying `list_strikes`
    /// response omits expiration — we inject the expiration that was queried.
    ///
    /// # Errors
    ///
    /// Returns an error on any non-2xx status, NDJSON parse failure, or invalid expiration
    /// string returned by the server.
    pub async fn list_contracts(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<Vec<RestContractRow>> {
        let expirations = self.list_expirations(symbol).await?;
        let mut out: Vec<RestContractRow> = Vec::new();
        for exp in expirations {
            let exp_date = NaiveDate::parse_from_str(&exp.expiration, "%Y-%m-%d")
                .with_context(|| format!("invalid expiration {:?}", exp.expiration))?;
            if exp_date < date {
                continue;
            }
            let exp_yyyymmdd = exp_date;
            let strikes = self.list_strikes(symbol, exp_yyyymmdd).await?;
            for s in strikes {
                for right in ["call", "put"] {
                    out.push(RestContractRow {
                        symbol: s.symbol.clone(),
                        expiration: exp.expiration.clone(),
                        strike: s.strike,
                        right: right.to_string(),
                    });
                }
            }
        }
        Ok(out)
    }

    /// Lists every available expiration date for the given underlying symbol.
    ///
    /// Calls `GET /option/list/expirations?symbol=<symbol>&format=ndjson`.
    ///
    /// # Errors
    ///
    /// Returns an error on non-2xx status or NDJSON parse failure.
    pub async fn list_expirations(&self, symbol: &str) -> Result<Vec<RestExpirationRow>> {
        let url = format!("{}/option/list/expirations", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&[("symbol", symbol), ("format", "ndjson")])
            .send()
            .await
            .with_context(|| format!("list_expirations request to {url} failed"))?;
        Self::parse_ndjson_response(response).await
    }

    /// Lists every strike for the given underlying symbol on the given expiration date.
    ///
    /// Calls `GET /option/list/strikes?symbol=<symbol>&expiration=<YYYYMMDD>&format=ndjson`.
    ///
    /// # Errors
    ///
    /// Returns an error on non-2xx status or NDJSON parse failure.
    pub async fn list_strikes(
        &self,
        symbol: &str,
        expiration: NaiveDate,
    ) -> Result<Vec<RestStrikeRow>> {
        let url = format!("{}/option/list/strikes", self.base_url);
        let exp_str = expiration.format("%Y%m%d").to_string();
        let response = self
            .http
            .get(&url)
            .query(&[
                ("symbol", symbol),
                ("expiration", &exp_str),
                ("format", "ndjson"),
            ])
            .send()
            .await
            .with_context(|| format!("list_strikes request to {url} failed"))?;
        Self::parse_ndjson_response(response).await
    }

    /// Fetches historical quotes for a single option contract over the date range.
    ///
    /// Calls `GET /option/history/quote`. Sub-minute intervals are only valid for single-day
    /// requests — passing one with `start_date != end_date` returns an error.
    ///
    /// Date ranges spanning more than [`MAX_HISTORY_SPAN_DAYS`] days are rejected. Callers
    /// should pre-chunk via [`chunk_date_range`].
    ///
    /// # Errors
    ///
    /// Returns an error on non-2xx status, NDJSON parse failure, or constraint violation.
    pub async fn hist_quotes(
        &self,
        contract: &ThetaOptionContract,
        start_date: NaiveDate,
        end_date: NaiveDate,
        interval: Interval,
    ) -> Result<Vec<RestQuoteRow>> {
        check_history_constraints(start_date, end_date, interval)?;
        let url = format!("{}/option/history/quote", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&hist_query_params(contract, start_date, end_date, Some(interval)))
            .send()
            .await
            .with_context(|| format!("hist_quotes request to {url} failed"))?;
        Self::parse_ndjson_response(response).await
    }

    /// Fetches historical trades for a single option contract over the date range.
    ///
    /// Calls `GET /option/history/trade`. The trade endpoint does not take an `interval`.
    ///
    /// # Errors
    ///
    /// Returns an error on non-2xx status, NDJSON parse failure, or date-span violation.
    pub async fn hist_trades(
        &self,
        contract: &ThetaOptionContract,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<RestTradeRow>> {
        check_date_span(start_date, end_date)?;
        let url = format!("{}/option/history/trade", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&hist_query_params(contract, start_date, end_date, None))
            .send()
            .await
            .with_context(|| format!("hist_trades request to {url} failed"))?;
        Self::parse_ndjson_response(response).await
    }

    /// Fetches historical OHLC bars for a single option contract over the date range.
    ///
    /// Calls `GET /option/history/ohlc`.
    ///
    /// # Errors
    ///
    /// Returns an error on non-2xx status, NDJSON parse failure, or constraint violation.
    pub async fn hist_ohlc(
        &self,
        contract: &ThetaOptionContract,
        start_date: NaiveDate,
        end_date: NaiveDate,
        interval: Interval,
    ) -> Result<Vec<RestOhlcRow>> {
        check_history_constraints(start_date, end_date, interval)?;
        let url = format!("{}/option/history/ohlc", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&hist_query_params(contract, start_date, end_date, Some(interval)))
            .send()
            .await
            .with_context(|| format!("hist_ohlc request to {url} failed"))?;
        Self::parse_ndjson_response(response).await
    }

    /// Fetches end-of-day OHLCV for a stock (e.g. `SPY`) over a date range.
    ///
    /// Calls `GET /stock/history/eod`.
    ///
    /// # Errors
    ///
    /// Returns an error on non-2xx status or NDJSON parse failure.
    pub async fn hist_stock_eod(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<RestEodRow>> {
        let url = format!("{}/stock/history/eod", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&[
                ("symbol", symbol),
                ("start_date", &start.format("%Y%m%d").to_string()),
                ("end_date", &end.format("%Y%m%d").to_string()),
                ("format", "ndjson"),
            ])
            .send()
            .await
            .with_context(|| format!("hist_stock_eod request to {url} failed"))?;
        Self::parse_ndjson_response(response).await
    }

    /// Fetches end-of-day OHLCV for an index (e.g. `SPX`) over a date range.
    ///
    /// Calls `GET /index/history/eod`.
    ///
    /// # Errors
    ///
    /// Returns an error on non-2xx status or NDJSON parse failure.
    pub async fn hist_index_eod(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<RestEodRow>> {
        let url = format!("{}/index/history/eod", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&[
                ("symbol", symbol),
                ("start_date", &start.format("%Y%m%d").to_string()),
                ("end_date", &end.format("%Y%m%d").to_string()),
                ("format", "ndjson"),
            ])
            .send()
            .await
            .with_context(|| format!("hist_index_eod request to {url} failed"))?;
        Self::parse_ndjson_response(response).await
    }

    // TODO(phase-3): calendar_today + calendar_on_date for trading-session metadata.

    async fn parse_ndjson_response<T: DeserializeOwned>(response: Response) -> Result<Vec<T>> {
        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read response body")?;
        if !status.is_success() {
            bail!(
                "thetadata terminal returned {status}: {snippet}",
                snippet = truncate_for_log(&body),
            );
        }
        parse_ndjson_rows(&body)
    }
}

/// Parses an NDJSON body into a vector of `T`.
///
/// Empty lines are skipped. The first parse error short-circuits with line context.
///
/// # Errors
///
/// Returns an error annotated with the 1-indexed line number when any row fails to deserialize.
pub fn parse_ndjson_rows<T: DeserializeOwned>(body: &str) -> Result<Vec<T>> {
    let mut out = Vec::new();
    for (idx, line) in body.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let row: T = serde_json::from_str(trimmed)
            .with_context(|| format!("failed to parse NDJSON row {}", idx + 1))?;
        out.push(row);
    }
    Ok(out)
}

fn truncate_for_log(s: &str) -> String {
    const MAX: usize = 256;
    if s.len() <= MAX {
        s.to_owned()
    } else {
        format!("{}…", &s[..MAX])
    }
}

impl RestContractRow {
    /// Decodes this row into a canonical [`ThetaOptionContract`].
    ///
    /// # Errors
    ///
    /// Returns an error if the row's `expiration`, `strike`, or `right` cannot be parsed.
    pub fn to_option_contract(&self) -> Result<ThetaOptionContract> {
        let expiration = NaiveDate::parse_from_str(&self.expiration, "%Y-%m-%d")
            .with_context(|| format!("invalid expiration date {:?}", self.expiration))?;
        let right = match self.right.as_str() {
            "call" | "C" => OptionRight::Call,
            "put" | "P" => OptionRight::Put,
            other => bail!("unknown right {other:?}"),
        };
        ThetaOptionContract::from_dollar_strike(&self.symbol, expiration, self.strike, right)
    }
}

/// HTTP status codes for which a retry might succeed (transient).
///
/// Used by the higher-level data client when implementing retry logic. Defined here so the
/// classification policy lives next to the HTTP code that produced it.
#[must_use]
pub const fn is_retriable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 500..=599)
}

/// Splits a date range into chunks of at most [`MAX_HISTORY_SPAN_DAYS`] days each.
///
/// Yields `(start, end)` pairs in ascending order, end-inclusive. Useful when issuing requests
/// across more than one ThetaData multi-day window.
///
/// # Errors
///
/// Returns an error if `end < start`.
pub fn chunk_date_range(start: NaiveDate, end: NaiveDate) -> Result<Vec<(NaiveDate, NaiveDate)>> {
    if end < start {
        bail!("end date {end} precedes start {start}");
    }
    let mut chunks = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let chunk_end_candidate = cursor + chrono::Duration::days(MAX_HISTORY_SPAN_DAYS - 1);
        let chunk_end = if chunk_end_candidate > end {
            end
        } else {
            chunk_end_candidate
        };
        chunks.push((cursor, chunk_end));
        cursor = chunk_end + chrono::Duration::days(1);
    }
    Ok(chunks)
}

fn check_date_span(start_date: NaiveDate, end_date: NaiveDate) -> Result<()> {
    if end_date < start_date {
        bail!("end_date {end_date} precedes start_date {start_date}");
    }
    let span = (end_date - start_date).num_days();
    if span >= MAX_HISTORY_SPAN_DAYS {
        bail!(
            "date span of {} days exceeds the ThetaData {}-day multi-day cap; chunk via \
             chunk_date_range first",
            span + 1,
            MAX_HISTORY_SPAN_DAYS,
        );
    }
    Ok(())
}

fn check_history_constraints(
    start_date: NaiveDate,
    end_date: NaiveDate,
    interval: Interval,
) -> Result<()> {
    check_date_span(start_date, end_date)?;
    if interval.is_sub_minute() && start_date != end_date {
        bail!(
            "sub-minute interval {} requires a single-day request (start == end)",
            interval.as_query(),
        );
    }
    Ok(())
}

fn hist_query_params(
    contract: &ThetaOptionContract,
    start_date: NaiveDate,
    end_date: NaiveDate,
    interval: Option<Interval>,
) -> Vec<(&'static str, String)> {
    let mut params = vec![
        ("symbol", contract.root.clone()),
        ("expiration", contract.expiration.format("%Y%m%d").to_string()),
        ("strike", contract.rest_strike()),
        ("right", contract.right.as_rest_query().to_string()),
        ("start_date", start_date.format("%Y%m%d").to_string()),
        ("end_date", end_date.format("%Y%m%d").to_string()),
        ("format", "ndjson".to_string()),
    ];
    if let Some(i) = interval {
        params.push(("interval", i.as_query().to_string()));
    }
    params
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;
    use crate::common::DEFAULT_HTTP_URL;

    #[rstest]
    fn test_client_constructs_with_defaults() {
        let client =
            ThetaDataHistoricalClient::new(DEFAULT_HTTP_URL, Duration::from_secs(30)).unwrap();
        assert_eq!(client.base_url(), DEFAULT_HTTP_URL);
    }

    #[rstest]
    fn test_parse_ndjson_rows_basic() {
        let body = r#"{"symbol":"AAPL","expiration":"2024-03-15","strike":180.0,"right":"call"}
{"symbol":"AAPL","expiration":"2024-03-15","strike":180.0,"right":"put"}"#;
        let rows: Vec<RestContractRow> = parse_ndjson_rows(body).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].symbol, "AAPL");
        assert_eq!(rows[0].right, "call");
        assert_eq!(rows[1].right, "put");
    }

    #[rstest]
    fn test_parse_ndjson_rows_skips_blank_lines() {
        let body = "\n{\"symbol\":\"AAPL\",\"expiration\":\"2024-03-15\",\"strike\":180.0,\"right\":\"call\"}\n\n";
        let rows: Vec<RestContractRow> = parse_ndjson_rows(body).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[rstest]
    fn test_parse_ndjson_rows_empty_body() {
        let rows: Vec<RestContractRow> = parse_ndjson_rows("").unwrap();
        assert!(rows.is_empty());
    }

    #[rstest]
    fn test_parse_ndjson_rows_annotates_line_number() {
        let body = "{\"symbol\":\"AAPL\",\"expiration\":\"2024-03-15\",\"strike\":180.0,\"right\":\"call\"}\nnot-json";
        let err = parse_ndjson_rows::<RestContractRow>(body).unwrap_err();
        assert!(err.to_string().contains("row 2"));
    }

    #[rstest]
    fn test_rest_contract_row_to_option_contract() {
        let row = RestContractRow {
            symbol: "SPXW".to_string(),
            expiration: "2025-03-15".to_string(),
            strike: 480.0,
            right: "call".to_string(),
        };
        let contract = row.to_option_contract().unwrap();
        assert_eq!(contract.root, "SPXW");
        assert_eq!(contract.strike_thousandths, 480_000);
        assert_eq!(contract.right, OptionRight::Call);
    }

    #[rstest]
    fn test_rest_contract_row_accepts_wire_right() {
        let row = RestContractRow {
            symbol: "SPXW".to_string(),
            expiration: "2025-03-15".to_string(),
            strike: 480.0,
            right: "P".to_string(),
        };
        assert_eq!(row.to_option_contract().unwrap().right, OptionRight::Put);
    }

    #[rstest]
    fn test_rest_contract_row_rejects_unknown_right() {
        let row = RestContractRow {
            symbol: "SPXW".to_string(),
            expiration: "2025-03-15".to_string(),
            strike: 480.0,
            right: "neither".to_string(),
        };
        let err = row.to_option_contract().unwrap_err();
        assert!(err.to_string().contains("neither"));
    }

    #[rstest]
    fn test_rest_contract_row_rejects_invalid_date() {
        let row = RestContractRow {
            symbol: "SPXW".to_string(),
            expiration: "March 15 2025".to_string(),
            strike: 480.0,
            right: "call".to_string(),
        };
        assert!(row.to_option_contract().is_err());
    }

    #[rstest]
    #[case(StatusCode::OK, false)]
    #[case(StatusCode::BAD_REQUEST, false)]
    #[case(StatusCode::UNAUTHORIZED, false)]
    #[case(StatusCode::TOO_MANY_REQUESTS, true)]
    #[case(StatusCode::INTERNAL_SERVER_ERROR, true)]
    #[case(StatusCode::BAD_GATEWAY, true)]
    #[case(StatusCode::SERVICE_UNAVAILABLE, true)]
    fn test_is_retriable_status(#[case] status: StatusCode, #[case] expected: bool) {
        assert_eq!(is_retriable_status(status), expected);
    }

    #[rstest]
    fn test_truncate_for_log_short() {
        assert_eq!(truncate_for_log("short"), "short");
    }

    #[rstest]
    fn test_truncate_for_log_long() {
        let s = "x".repeat(300);
        let truncated = truncate_for_log(&s);
        assert!(truncated.ends_with('…'));
        assert!(truncated.len() < s.len() + 4);
    }

    fn sample_contract() -> ThetaOptionContract {
        ThetaOptionContract::from_dollar_strike(
            "AAPL",
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            180.0,
            OptionRight::Call,
        )
        .unwrap()
    }

    #[rstest]
    fn test_hist_query_params_includes_required_fields() {
        let params = hist_query_params(
            &sample_contract(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
            Some(Interval::M5),
        );
        let map: std::collections::HashMap<&str, String> = params.into_iter().collect();
        assert_eq!(map.get("symbol").unwrap(), "AAPL");
        assert_eq!(map.get("expiration").unwrap(), "20240315");
        assert_eq!(map.get("strike").unwrap(), "180.000");
        assert_eq!(map.get("right").unwrap(), "call");
        assert_eq!(map.get("start_date").unwrap(), "20240105");
        assert_eq!(map.get("end_date").unwrap(), "20240110");
        assert_eq!(map.get("format").unwrap(), "ndjson");
        assert_eq!(map.get("interval").unwrap(), "5m");
    }

    #[rstest]
    fn test_hist_query_params_omits_interval_when_none() {
        let params = hist_query_params(
            &sample_contract(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
            None,
        );
        let map: std::collections::HashMap<&str, String> = params.into_iter().collect();
        assert!(!map.contains_key("interval"));
    }

    #[rstest]
    fn test_check_date_span_rejects_inverted() {
        let start = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert!(check_date_span(start, end).is_err());
    }

    #[rstest]
    fn test_check_date_span_rejects_over_cap() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = start + chrono::Duration::days(MAX_HISTORY_SPAN_DAYS);
        assert!(check_date_span(start, end).is_err());
    }

    #[rstest]
    fn test_check_date_span_accepts_max() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = start + chrono::Duration::days(MAX_HISTORY_SPAN_DAYS - 1);
        assert!(check_date_span(start, end).is_ok());
    }

    #[rstest]
    fn test_check_history_constraints_rejects_sub_minute_multi_day() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let err = check_history_constraints(start, end, Interval::Tick).unwrap_err();
        assert!(err.to_string().contains("sub-minute"));
    }

    #[rstest]
    fn test_check_history_constraints_allows_sub_minute_single_day() {
        let day = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert!(check_history_constraints(day, day, Interval::Tick).is_ok());
    }

    #[rstest]
    fn test_chunk_date_range_single_chunk() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let chunks = chunk_date_range(start, end).unwrap();
        assert_eq!(chunks, vec![(start, end)]);
    }

    #[rstest]
    fn test_chunk_date_range_splits_across_cap() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let chunks = chunk_date_range(start, end).unwrap();
        assert!(chunks.len() >= 3);
        // Each chunk except possibly the last is exactly MAX_HISTORY_SPAN_DAYS long.
        for window in chunks.windows(2) {
            let (_, prev_end) = window[0];
            let (next_start, _) = window[1];
            assert_eq!(next_start, prev_end + chrono::Duration::days(1));
        }
        // Full range covered.
        assert_eq!(chunks.first().unwrap().0, start);
        assert_eq!(chunks.last().unwrap().1, end);
    }

    #[rstest]
    fn test_chunk_date_range_rejects_inverted() {
        let start = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert!(chunk_date_range(start, end).is_err());
    }
}
