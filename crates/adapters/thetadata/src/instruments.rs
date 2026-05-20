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

//! Instrument construction and provider for the ThetaData adapter.
//!
//! ThetaData's contract-listing endpoints return only `(root, expiration, strike, right)`. The
//! Nautilus [`OptionContract`] instrument additionally needs multiplier, tick size, lot size,
//! and expiration timestamp — none of which ThetaData publishes. The provider hardcodes
//! OCC-standard defaults (multiplier 100, tick 0.01 USD, lot 1, expiration 21:00 UTC) and
//! leaves precision-overrides for future work once the `/calendar/on_date` endpoint is wired
//! and a per-root precision table is introduced.

use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::AssetClass,
    identifiers::Venue,
    instruments::OptionContract,
    types::{Currency, Price, Quantity},
};
use ustr::Ustr;

use crate::{
    common::THETADATA_VENUE,
    historical::ThetaDataHistoricalClient,
    symbology::ThetaOptionContract,
    types::RestContractRow,
};

const PRICE_PRECISION: u8 = 2;
const PRICE_INCREMENT: f64 = 0.01;
const DEFAULT_MULTIPLIER: u64 = 100;
const DEFAULT_LOT_SIZE: u64 = 1;
const OPRA_MIC: &str = "OPRA";
const NS_PER_MS: u64 = 1_000_000;

/// Approximate UTC close time for US-listed options.
///
/// US options close at 16:00 America/New_York, which is 21:00 UTC during Standard Time and
/// 20:00 UTC during Daylight Time. Using a fixed 21:00 UTC introduces up to one hour of skew
/// during the summer half-year. TODO(phase-3): wire `chrono-tz` and resolve the precise close
/// from `/calendar/on_date`.
const CLOSE_TIME_MS_FROM_MIDNIGHT_UTC: u64 = 21 * 3_600 * 1_000;

/// Loader that turns ThetaData contract rows into Nautilus [`OptionContract`] instruments.
#[derive(Clone, Debug)]
pub struct ThetaDataInstrumentProvider {
    http: ThetaDataHistoricalClient,
    venue: Venue,
}

impl ThetaDataInstrumentProvider {
    /// Creates a new provider that uses the given historical client.
    #[must_use]
    pub fn new(http: ThetaDataHistoricalClient) -> Self {
        Self {
            http,
            venue: *THETADATA_VENUE,
        }
    }

    /// Creates a provider that emits instruments under a custom venue.
    ///
    /// Useful for tests, or when the consumer routes ThetaData feeds onto a venue identifier
    /// owned by a different adapter (e.g. `OPRA`).
    #[must_use]
    pub fn with_venue(http: ThetaDataHistoricalClient, venue: Venue) -> Self {
        Self { http, venue }
    }

    /// Loads every option contract for the underlying on the given date and builds Nautilus
    /// [`OptionContract`] instances.
    ///
    /// `ts_init` is the timestamp stamped on the resulting instruments. Pass the live clock's
    /// current time in production; pass a fixed value for replay determinism.
    ///
    /// # Errors
    ///
    /// Returns an error if the REST call fails or any row cannot be decoded.
    pub async fn load_contracts(
        &self,
        symbol: &str,
        date: NaiveDate,
        ts_init: UnixNanos,
    ) -> Result<Vec<OptionContract>> {
        let rows = self.http.list_contracts(symbol, date).await?;
        rows.iter()
            .map(|row| build_option_contract(row, self.venue, ts_init))
            .collect()
    }
}

/// Builds an [`OptionContract`] from a single contract row.
///
/// # Errors
///
/// Returns an error if the row's date/strike/right cannot be decoded.
pub fn build_option_contract(
    row: &RestContractRow,
    venue: Venue,
    ts_init: UnixNanos,
) -> Result<OptionContract> {
    let theta = row.to_option_contract()?;
    build_from_canonical(&theta, venue, ts_init)
}

/// Builds an [`OptionContract`] from a canonical [`ThetaOptionContract`].
///
/// # Errors
///
/// Returns an error if any of the Nautilus value constructors reject the inputs (negative
/// strike, etc.) or if the expiration cannot be converted to a `UnixNanos`.
pub fn build_from_canonical(
    theta: &ThetaOptionContract,
    venue: Venue,
    ts_init: UnixNanos,
) -> Result<OptionContract> {
    let instrument_id = theta.to_instrument_id(venue);
    let raw_symbol = instrument_id.symbol;
    let strike_dollars = theta.strike_thousandths as f64 / 1_000.0;
    let strike_price = Price::new(strike_dollars, PRICE_PRECISION);
    let price_increment = Price::new(PRICE_INCREMENT, PRICE_PRECISION);
    let multiplier = Quantity::from(DEFAULT_MULTIPLIER);
    let lot_size = Quantity::from(DEFAULT_LOT_SIZE);
    let expiration_ns = expiration_to_unix_nanos(theta.expiration)?;

    Ok(OptionContract::new(
        instrument_id,
        raw_symbol,
        AssetClass::Equity,
        Some(Ustr::from(OPRA_MIC)),
        Ustr::from(&theta.root),
        theta.right.into(),
        strike_price,
        Currency::USD(),
        UnixNanos::default(),
        expiration_ns,
        PRICE_PRECISION,
        price_increment,
        multiplier,
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
    ))
}

/// Converts an expiration date to a `UnixNanos` representing approximate 21:00 UTC on that day.
///
/// See `CLOSE_TIME_MS_FROM_MIDNIGHT_UTC` for the timezone caveat.
fn expiration_to_unix_nanos(date: NaiveDate) -> Result<UnixNanos> {
    let midnight = NaiveDateTime::new(date, NaiveTime::MIN);
    let unix_ms = midnight.and_utc().timestamp_millis();
    if unix_ms < 0 {
        anyhow::bail!("expiration date {date} predates the unix epoch");
    }
    let ms = (unix_ms as u64)
        .checked_add(CLOSE_TIME_MS_FROM_MIDNIGHT_UTC)
        .context("expiration overflow")?;
    if ms >= u64::MAX / NS_PER_MS {
        anyhow::bail!("expiration far beyond representable range");
    }
    Ok(UnixNanos::from(ms * NS_PER_MS))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use nautilus_model::enums::OptionKind;
    use rstest::*;

    use super::*;
    use crate::enums::OptionRight;

    fn sample_row() -> RestContractRow {
        RestContractRow {
            symbol: "SPXW".to_string(),
            expiration: "2025-03-15".to_string(),
            strike: 480.0,
            right: "call".to_string(),
        }
    }

    #[rstest]
    fn test_build_option_contract_basic() {
        let row = sample_row();
        let venue = *THETADATA_VENUE;
        let contract = build_option_contract(&row, venue, UnixNanos::from(1_000_u64)).unwrap();

        assert_eq!(contract.id.venue, venue);
        assert_eq!(contract.id.symbol.as_str(), "SPXW250315C00480000");
        assert_eq!(contract.option_kind, OptionKind::Call);
        assert_eq!(contract.underlying.as_str(), "SPXW");
        assert_eq!(contract.exchange.unwrap().as_str(), "OPRA");
        assert_eq!(contract.asset_class, AssetClass::Equity);
        assert_eq!(contract.multiplier, Quantity::from(100));
        assert_eq!(contract.lot_size, Quantity::from(1));
        assert_eq!(contract.price_precision, 2);
        assert_eq!(contract.strike_price.to_string(), "480.00");
        assert_eq!(contract.ts_init, UnixNanos::from(1_000_u64));
    }

    #[rstest]
    fn test_build_option_contract_put() {
        let mut row = sample_row();
        row.right = "put".to_string();
        let contract =
            build_option_contract(&row, *THETADATA_VENUE, UnixNanos::default()).unwrap();
        assert_eq!(contract.option_kind, OptionKind::Put);
    }

    #[rstest]
    fn test_expiration_to_unix_nanos_known_date() {
        // 2025-03-15 21:00:00 UTC → 1742072400 seconds since epoch.
        let ns = expiration_to_unix_nanos(NaiveDate::from_ymd_opt(2025, 3, 15).unwrap()).unwrap();
        assert_eq!(u64::from(ns), 1_742_072_400_000_000_000);
    }

    #[rstest]
    fn test_expiration_rejects_pre_epoch() {
        let result = expiration_to_unix_nanos(NaiveDate::from_ymd_opt(1969, 1, 1).unwrap());
        assert!(result.is_err());
    }

    #[rstest]
    fn test_build_from_canonical_uses_supplied_venue() {
        let theta = ThetaOptionContract::from_dollar_strike(
            "AAPL",
            NaiveDate::from_ymd_opt(2024, 1, 19).unwrap(),
            172.5,
            OptionRight::Call,
        )
        .unwrap();
        let custom = Venue::new(Ustr::from("OPRA"));
        let contract = build_from_canonical(&theta, custom, UnixNanos::default()).unwrap();
        assert_eq!(contract.id.venue, custom);
    }

    #[rstest]
    #[case("2025-03-15")]
    #[case("not-a-date")]
    fn test_build_option_contract_validates_row(#[case] expiration: &str) {
        let row = RestContractRow {
            symbol: "SPXW".to_string(),
            expiration: expiration.to_string(),
            strike: 480.0,
            right: "call".to_string(),
        };
        let result = build_option_contract(&row, *THETADATA_VENUE, UnixNanos::default());
        assert_eq!(result.is_ok(), expiration == "2025-03-15");
    }

    #[rstest]
    fn test_provider_constructs() {
        let http = ThetaDataHistoricalClient::new(
            "http://127.0.0.1:25503/v3",
            std::time::Duration::from_secs(30),
        )
        .unwrap();
        let provider = ThetaDataInstrumentProvider::new(http);
        let _ = provider; // just verify construction
    }

}
