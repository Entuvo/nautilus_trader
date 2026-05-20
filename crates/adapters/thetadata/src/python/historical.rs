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

//! Python bindings for the ThetaData HTTP (historical) client.
//!
//! Exposes a `ThetaDataHttpClient` pyclass that wraps the Rust
//! [`crate::historical::ThetaDataHistoricalClient`] and returns Nautilus pyo3 types
//! (`QuoteTick`, `TradeTick`, `Bar`, `OptionContract`) so the Python `ThetaDataDataClient`
//! can route historical requests onto the engine without re-implementing HTTP, NDJSON parsing,
//! or wire-format decoding.

use std::time::Duration;

use chrono::NaiveDate;
use nautilus_core::{
    UnixNanos,
    python::{to_pyruntime_err, to_pyvalue_err},
};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    identifiers::{InstrumentId, Venue},
    instruments::OptionContract,
};
use pyo3::{conversion::IntoPyObjectExt, prelude::*, types::PyList};

use crate::{
    common::{DEFAULT_HTTP_URL, THETADATA_VENUE},
    enums::Interval,
    historical::{ThetaDataHistoricalClient, chunk_date_range},
    instruments::build_option_contract,
    symbology::ThetaOptionContract,
};

/// HTTP client over the local ThetaTerminal v3 API, callable from Python.
#[pyo3_stub_gen::derive::gen_stub_pyclass]
#[pyclass(module = "nautilus_trader.core.nautilus_pyo3.thetadata", from_py_object)]
#[derive(Clone, Debug)]
pub struct ThetaDataHttpClient {
    inner: ThetaDataHistoricalClient,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ThetaDataHttpClient {
    /// Creates a new HTTP client targeting the local ThetaTerminal v3 endpoint.
    #[new]
    #[pyo3(signature = (http_url = DEFAULT_HTTP_URL.to_string(), timeout_secs = 30))]
    fn py_new(http_url: String, timeout_secs: u64) -> PyResult<Self> {
        let inner = ThetaDataHistoricalClient::new(http_url, Duration::from_secs(timeout_secs))
            .map_err(to_pyvalue_err)?;
        Ok(Self { inner })
    }

    /// Returns the configured base URL.
    #[getter]
    #[pyo3(name = "base_url")]
    fn py_base_url(&self) -> &str {
        self.inner.base_url()
    }

    fn __repr__(&self) -> String {
        format!("ThetaDataHttpClient(base_url={:?})", self.inner.base_url())
    }

    /// Builds a Nautilus [`OptionContract`] from an OCC-encoded `InstrumentId` without an HTTP
    /// round trip.
    ///
    /// Strike, expiration, right, and underlying are recovered from the symbol; the static
    /// option defaults (multiplier 100, tick 0.01 USD, lot 1, expiration approximated at
    /// 21:00 UTC on the contract date) are applied. Useful for instrument-provider single
    /// loads where bulk listing would be wasteful.
    #[staticmethod]
    #[pyo3(name = "option_contract_from_id")]
    #[pyo3(signature = (instrument_id, venue=None))]
    fn py_option_contract_from_id(
        instrument_id: InstrumentId,
        venue: Option<Venue>,
    ) -> PyResult<OptionContract> {
        let theta = ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())
            .map_err(to_pyvalue_err)?;
        let resolved_venue = venue.unwrap_or(*THETADATA_VENUE);
        crate::instruments::build_from_canonical(&theta, resolved_venue, now_unix_nanos())
            .map_err(to_pyvalue_err)
    }

    /// Lists every available expiration date for the given underlying symbol.
    ///
    /// Returns a list of `YYYY-MM-DD` strings.
    #[pyo3(name = "list_expirations")]
    fn py_list_expirations<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = client.list_expirations(&symbol).await.map_err(to_pyvalue_err)?;
            Python::attach(|py| {
                let items: Vec<String> = rows.into_iter().map(|r| r.expiration).collect();
                items.into_py_any(py)
            })
        })
    }

    /// Lists every strike (in decimal dollars) for the given underlying on the given expiration.
    ///
    /// `expiration` accepts either `YYYY-MM-DD` or `YYYYMMDD`.
    #[pyo3(name = "list_strikes")]
    fn py_list_strikes<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        expiration: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let expiration_date = parse_iso_or_compact_date(&expiration).map_err(to_pyvalue_err)?;
            let rows = client
                .list_strikes(&symbol, expiration_date)
                .await
                .map_err(to_pyvalue_err)?;
            Python::attach(|py| {
                let items: Vec<f64> = rows.into_iter().map(|r| r.strike).collect();
                items.into_py_any(py)
            })
        })
    }

    /// Lists every option contract for the underlying with expirations on or after `date`.
    ///
    /// Returns a list of Nautilus [`OptionContract`] instruments built with OCC-standard
    /// defaults (multiplier 100, tick 0.01 USD, lot 1). Venue defaults to `THETADATA`.
    ///
    /// `date` accepts `YYYY-MM-DD` or `YYYYMMDD`.
    #[pyo3(name = "list_contracts")]
    #[pyo3(signature = (symbol, date, venue=None))]
    fn py_list_contracts<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        date: String,
        venue: Option<Venue>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let date_value = parse_iso_or_compact_date(&date).map_err(to_pyvalue_err)?;
            let rows = client
                .list_contracts(&symbol, date_value)
                .await
                .map_err(to_pyvalue_err)?;
            let resolved_venue = venue.unwrap_or(*THETADATA_VENUE);
            let ts_init = now_unix_nanos();
            let mut contracts: Vec<OptionContract> = Vec::with_capacity(rows.len());
            for row in &rows {
                let contract =
                    build_option_contract(row, resolved_venue, ts_init).map_err(to_pyvalue_err)?;
                contracts.push(contract);
            }
            Python::attach(|py| {
                let py_items: PyResult<Vec<_>> =
                    contracts.into_iter().map(|c| c.into_py_any(py)).collect();
                PyList::new(py, py_items?)
                    .map_err(to_pyruntime_err)
                    .map(|list| list.into_any().unbind())
            })
        })
    }

    /// Fetches historical quotes for a single option contract over the date range.
    ///
    /// `instrument_id` must use the OCC-style symbology produced by
    /// `ThetaOptionContract::to_instrument_id` (e.g. `SPXW250315C00480000.THETADATA`).
    /// `start_date`/`end_date` accept `YYYY-MM-DD` or `YYYYMMDD`. `interval` accepts the
    /// ThetaData query strings: `tick`, `10ms`, `100ms`, `500ms`, `1s`…`30s`, `1m`…`30m`, `1h`.
    #[pyo3(name = "hist_quotes")]
    #[pyo3(signature = (
        instrument_id,
        start_date,
        end_date,
        interval,
        price_precision = 2,
        size_precision = 0,
        limit = None,
    ))]
    fn py_hist_quotes<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
        start_date: String,
        end_date: String,
        interval: String,
        price_precision: u8,
        size_precision: u8,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contract = ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())
                .map_err(to_pyvalue_err)?;
            let start = parse_iso_or_compact_date(&start_date).map_err(to_pyvalue_err)?;
            let end = parse_iso_or_compact_date(&end_date).map_err(to_pyvalue_err)?;
            let interval_enum = parse_interval(&interval).map_err(to_pyvalue_err)?;
            let ts_init = now_unix_nanos();
            let mut out: Vec<QuoteTick> = Vec::new();
            for (chunk_start, chunk_end) in chunk_date_range(start, end).map_err(to_pyvalue_err)? {
                let chunk_interval = if interval_enum.is_sub_minute() && chunk_start != chunk_end {
                    Interval::M1
                } else {
                    interval_enum
                };
                let rows = client
                    .hist_quotes(&contract, chunk_start, chunk_end, chunk_interval)
                    .await
                    .map_err(to_pyvalue_err)?;
                for row in rows {
                    match row.to_quote_tick(instrument_id, price_precision, size_precision, ts_init)
                    {
                        Ok(tick) => out.push(tick),
                        Err(e) => log::warn!("skip quote row: {e}"),
                    }
                    if limit.is_some_and(|l| out.len() >= l) {
                        out.truncate(limit.unwrap());
                        return ticks_to_py(out);
                    }
                }
            }
            ticks_to_py(out)
        })
    }

    /// Fetches historical trades for a single option contract over the date range.
    #[pyo3(name = "hist_trades")]
    #[pyo3(signature = (
        instrument_id,
        start_date,
        end_date,
        price_precision = 2,
        size_precision = 0,
        limit = None,
    ))]
    fn py_hist_trades<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
        start_date: String,
        end_date: String,
        price_precision: u8,
        size_precision: u8,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contract = ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())
                .map_err(to_pyvalue_err)?;
            let start = parse_iso_or_compact_date(&start_date).map_err(to_pyvalue_err)?;
            let end = parse_iso_or_compact_date(&end_date).map_err(to_pyvalue_err)?;
            let ts_init = now_unix_nanos();
            let mut out: Vec<TradeTick> = Vec::new();
            for (chunk_start, chunk_end) in chunk_date_range(start, end).map_err(to_pyvalue_err)? {
                let rows = client
                    .hist_trades(&contract, chunk_start, chunk_end)
                    .await
                    .map_err(to_pyvalue_err)?;
                for row in rows {
                    match row.to_trade_tick(instrument_id, price_precision, size_precision, ts_init)
                    {
                        Ok(tick) => out.push(tick),
                        Err(e) => log::warn!("skip trade row: {e}"),
                    }
                    if limit.is_some_and(|l| out.len() >= l) {
                        out.truncate(limit.unwrap());
                        return trades_to_py(out);
                    }
                }
            }
            trades_to_py(out)
        })
    }

    /// Fetches historical OHLC bars for a single option contract.
    ///
    /// `bar_type.instrument_id` must use the OCC-style symbology. `interval` accepts the
    /// ThetaData query strings (same as `hist_quotes`).
    #[pyo3(name = "hist_ohlc")]
    #[pyo3(signature = (
        bar_type,
        start_date,
        end_date,
        interval,
        price_precision = 2,
        size_precision = 0,
        limit = None,
    ))]
    fn py_hist_ohlc<'py>(
        &self,
        py: Python<'py>,
        bar_type: BarType,
        start_date: String,
        end_date: String,
        interval: String,
        price_precision: u8,
        size_precision: u8,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contract = ThetaOptionContract::from_symbol(bar_type.instrument_id().symbol.as_str())
                .map_err(to_pyvalue_err)?;
            let start = parse_iso_or_compact_date(&start_date).map_err(to_pyvalue_err)?;
            let end = parse_iso_or_compact_date(&end_date).map_err(to_pyvalue_err)?;
            let interval_enum = parse_interval(&interval).map_err(to_pyvalue_err)?;
            if interval_enum.is_sub_minute() && start != end {
                return Err(to_pyvalue_err(format!(
                    "sub-minute bar interval {} requires a single-day range",
                    interval_enum.as_query()
                )));
            }
            let ts_init = now_unix_nanos();
            let mut out: Vec<Bar> = Vec::new();
            for (chunk_start, chunk_end) in chunk_date_range(start, end).map_err(to_pyvalue_err)? {
                let rows = client
                    .hist_ohlc(&contract, chunk_start, chunk_end, interval_enum)
                    .await
                    .map_err(to_pyvalue_err)?;
                for row in rows {
                    match row.to_bar(bar_type, price_precision, size_precision, ts_init) {
                        Ok(bar) => out.push(bar),
                        Err(e) => log::warn!("skip ohlc row: {e}"),
                    }
                    if limit.is_some_and(|l| out.len() >= l) {
                        out.truncate(limit.unwrap());
                        return bars_to_py(out);
                    }
                }
            }
            bars_to_py(out)
        })
    }

    /// Fetches end-of-day OHLCV for a stock (e.g. `SPY`).
    ///
    /// Returns a list of `(date_yyyymmdd: str, open, high, low, close, volume, count)` tuples
    /// for the dates requested. The Terminal returns one row per requested day; missing dates
    /// (holidays) are simply absent from the result.
    #[pyo3(name = "hist_stock_eod")]
    fn py_hist_stock_eod<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        start_date: String,
        end_date: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let start = parse_iso_or_compact_date(&start_date).map_err(to_pyvalue_err)?;
            let end = parse_iso_or_compact_date(&end_date).map_err(to_pyvalue_err)?;
            let rows = client.hist_stock_eod(&symbol, start, end).await.map_err(to_pyvalue_err)?;
            eod_rows_to_py(rows)
        })
    }

    /// Fetches end-of-day OHLCV for an index (e.g. `SPX`). NBBO fields are zero for indices.
    #[pyo3(name = "hist_index_eod")]
    fn py_hist_index_eod<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        start_date: String,
        end_date: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let start = parse_iso_or_compact_date(&start_date).map_err(to_pyvalue_err)?;
            let end = parse_iso_or_compact_date(&end_date).map_err(to_pyvalue_err)?;
            let rows = client.hist_index_eod(&symbol, start, end).await.map_err(to_pyvalue_err)?;
            eod_rows_to_py(rows)
        })
    }
}

fn ticks_to_py(ticks: Vec<QuoteTick>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let items: PyResult<Vec<_>> = ticks.into_iter().map(|t| t.into_py_any(py)).collect();
        PyList::new(py, items?)
            .map_err(to_pyruntime_err)
            .map(|list| list.into_any().unbind())
    })
}

fn trades_to_py(ticks: Vec<TradeTick>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let items: PyResult<Vec<_>> = ticks.into_iter().map(|t| t.into_py_any(py)).collect();
        PyList::new(py, items?)
            .map_err(to_pyruntime_err)
            .map(|list| list.into_any().unbind())
    })
}

fn bars_to_py(bars: Vec<Bar>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let items: PyResult<Vec<_>> = bars.into_iter().map(|b| b.into_py_any(py)).collect();
        PyList::new(py, items?)
            .map_err(to_pyruntime_err)
            .map(|list| list.into_any().unbind())
    })
}

fn eod_rows_to_py(rows: Vec<crate::types::RestEodRow>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let items: PyResult<Vec<_>> = rows
            .into_iter()
            .map(|r| {
                (
                    r.last_trade,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    r.volume,
                    r.count,
                )
                    .into_py_any(py)
            })
            .collect();
        PyList::new(py, items?)
            .map_err(to_pyruntime_err)
            .map(|list| list.into_any().unbind())
    })
}

fn parse_iso_or_compact_date(s: &str) -> anyhow::Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y%m%d"))
        .map_err(|e| anyhow::anyhow!("invalid date {s:?}: {e}"))
}

fn parse_interval(s: &str) -> anyhow::Result<Interval> {
    let normalized = s.trim().to_lowercase();
    let v = match normalized.as_str() {
        "tick" => Interval::Tick,
        "10ms" => Interval::Ms10,
        "100ms" => Interval::Ms100,
        "500ms" => Interval::Ms500,
        "1s" => Interval::S1,
        "5s" => Interval::S5,
        "10s" => Interval::S10,
        "15s" => Interval::S15,
        "30s" => Interval::S30,
        "1m" => Interval::M1,
        "5m" => Interval::M5,
        "10m" => Interval::M10,
        "15m" => Interval::M15,
        "30m" => Interval::M30,
        "1h" => Interval::H1,
        _ => anyhow::bail!("unknown interval {s:?}"),
    };
    Ok(v)
}

fn now_unix_nanos() -> UnixNanos {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    UnixNanos::from(nanos)
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;

    #[rstest]
    fn test_parse_iso_or_compact_date_accepts_iso() {
        let d = parse_iso_or_compact_date("2025-03-15").unwrap();
        assert_eq!(d, NaiveDate::from_ymd_opt(2025, 3, 15).unwrap());
    }

    #[rstest]
    fn test_parse_iso_or_compact_date_accepts_compact() {
        let d = parse_iso_or_compact_date("20250315").unwrap();
        assert_eq!(d, NaiveDate::from_ymd_opt(2025, 3, 15).unwrap());
    }

    #[rstest]
    fn test_parse_iso_or_compact_date_rejects_garbage() {
        assert!(parse_iso_or_compact_date("March 15 2025").is_err());
    }

    #[rstest]
    #[case("tick", Interval::Tick)]
    #[case("10ms", Interval::Ms10)]
    #[case("1s", Interval::S1)]
    #[case("1m", Interval::M1)]
    #[case("5m", Interval::M5)]
    #[case("1h", Interval::H1)]
    fn test_parse_interval_round_trip(#[case] s: &str, #[case] expected: Interval) {
        assert_eq!(parse_interval(s).unwrap(), expected);
    }

    #[rstest]
    fn test_parse_interval_normalizes_case() {
        assert_eq!(parse_interval("  5M  ").unwrap(), Interval::M5);
    }

    #[rstest]
    fn test_parse_interval_rejects_unknown() {
        assert!(parse_interval("3m").is_err());
    }
}
