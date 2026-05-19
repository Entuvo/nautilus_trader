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

//! Symbology helpers for ThetaData option contracts.
//!
//! ThetaData uses two distinct on-the-wire strike encodings:
//!
//! - **REST**: decimal dollars (e.g. `220.000`).
//! - **WebSocket**: integer scaled by 1 000 (OCC thousandths-of-a-dollar — `$480 → 480_000`).
//!   (The v3 docs page erroneously documents this as ×10 000 / `$480 → 4_800_000` — the
//!   actual wire format is ×1 000, confirmed by ThetaData support.)
//!
//! Internally we canonicalize on the OCC convention (strike in thousandths of a dollar,
//! 8 zero-padded digits) and provide converters to both wire encodings plus a Nautilus
//! [`InstrumentId`].

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use nautilus_model::identifiers::{InstrumentId, Symbol, Venue};
use serde::{Deserialize, Serialize};

use crate::enums::OptionRight;

/// Canonical representation of a ThetaData option contract.
///
/// Strike is stored as integer thousandths of a dollar (OCC convention).
/// `$220.00` is represented as `220_000`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ThetaOptionContract {
    /// Underlying root symbol (e.g. `"SPXW"`, `"AAPL"`).
    pub root: String,
    /// Expiration date.
    pub expiration: NaiveDate,
    /// Strike in thousandths of a dollar (OCC convention).
    pub strike_thousandths: u64,
    /// Call or put.
    pub right: OptionRight,
}

impl ThetaOptionContract {
    /// Creates a new contract from a decimal-dollar strike. Returns an error if the strike does
    /// not represent a whole number of thousandths of a dollar.
    ///
    /// # Errors
    ///
    /// Returns an error if `strike_dollars` is negative or has sub-thousandth precision that
    /// would lose information when canonicalized.
    pub fn from_dollar_strike(
        root: impl Into<String>,
        expiration: NaiveDate,
        strike_dollars: f64,
        right: OptionRight,
    ) -> Result<Self> {
        if !strike_dollars.is_finite() || strike_dollars < 0.0 {
            bail!("strike must be finite and non-negative, got {strike_dollars}");
        }
        let scaled = strike_dollars * 1_000.0;
        let rounded = scaled.round();
        if (scaled - rounded).abs() > 1e-6 {
            bail!(
                "strike {strike_dollars} cannot be represented exactly in thousandths of a dollar"
            );
        }
        Ok(Self {
            root: root.into(),
            expiration,
            strike_thousandths: rounded as u64,
            right,
        })
    }

    /// Strike encoded for the ThetaData REST API (decimal dollars with 3-place precision).
    #[must_use]
    pub fn rest_strike(&self) -> String {
        let dollars = self.strike_thousandths / 1_000;
        let frac = self.strike_thousandths % 1_000;
        format!("{dollars}.{frac:03}")
    }

    /// Strike encoded for the ThetaData WebSocket subscribe message.
    ///
    /// The scale is **thousandths of a dollar** (OCC convention) — same as the canonical
    /// internal representation, no multiplier. `$740 → 740_000`.
    ///
    /// Note: the v3 streaming docs page describes this as "1/10 of a cent" with an example
    /// `$480 → 4_800_000` — that example was confirmed inaccurate by ThetaData support;
    /// the actual wire format is `$480 → 480_000` (×1 000, not ×10 000).
    #[must_use]
    pub const fn ws_strike(&self) -> u64 {
        self.strike_thousandths
    }

    /// Expiration formatted as the `YYYYMMDD` integer used in WebSocket frames and as the
    /// preferred REST format.
    #[must_use]
    pub fn ws_expiration(&self) -> u32 {
        let y = self.expiration.format("%Y").to_string().parse::<u32>().unwrap_or(0);
        let m = self.expiration.format("%m").to_string().parse::<u32>().unwrap_or(0);
        let d = self.expiration.format("%d").to_string().parse::<u32>().unwrap_or(0);
        y * 10_000 + m * 100 + d
    }

    /// Builds a Nautilus [`InstrumentId`] using OCC-style symbology: `ROOTYYMMDD[C|P]SSSSSSSS`
    /// where strike is the 8-digit zero-padded thousandths-of-a-dollar value.
    ///
    /// Example: `SPXW250315C04800000`
    #[must_use]
    pub fn to_instrument_id(&self, venue: Venue) -> InstrumentId {
        let yy = self.expiration.format("%y");
        let mmdd = self.expiration.format("%m%d");
        let symbol_str = format!(
            "{root}{yy}{mmdd}{right}{strike:08}",
            root = self.root,
            right = self.right.as_wire(),
            strike = self.strike_thousandths,
        );
        InstrumentId::new(Symbol::new(&symbol_str), venue)
    }

    /// Parses a Nautilus OCC-style symbol back into a contract.
    ///
    /// # Errors
    ///
    /// Returns an error if the symbol does not match the OCC format. The root portion may be
    /// any length ≥ 1, but the trailing `YYMMDD[C|P]SSSSSSSS` (15 chars) suffix is required.
    pub fn from_symbol(symbol: &str) -> Result<Self> {
        if symbol.len() < 16 {
            bail!("symbol too short for OCC encoding: {symbol}");
        }
        let (root, rest) = symbol.split_at(symbol.len() - 15);
        let yy: i32 = rest[0..2].parse().context("invalid YY in OCC symbol")?;
        let mm: u32 = rest[2..4].parse().context("invalid MM in OCC symbol")?;
        let dd: u32 = rest[4..6].parse().context("invalid DD in OCC symbol")?;
        let year = 2_000 + yy;
        let expiration = NaiveDate::from_ymd_opt(year, mm, dd)
            .with_context(|| format!("invalid date {year:04}-{mm:02}-{dd:02}"))?;
        let right = match &rest[6..7] {
            "C" => OptionRight::Call,
            "P" => OptionRight::Put,
            other => bail!("invalid right in OCC symbol: {other}"),
        };
        let strike_thousandths: u64 = rest[7..15]
            .parse()
            .context("invalid strike in OCC symbol")?;
        Ok(Self {
            root: root.to_owned(),
            expiration,
            strike_thousandths,
            right,
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;
    use crate::common::THETADATA_VENUE;

    fn sample() -> ThetaOptionContract {
        ThetaOptionContract::from_dollar_strike(
            "SPXW",
            NaiveDate::from_ymd_opt(2025, 3, 15).unwrap(),
            480.0,
            OptionRight::Call,
        )
        .unwrap()
    }

    #[rstest]
    fn test_rest_strike_formatting() {
        assert_eq!(sample().rest_strike(), "480.000");
        let c = ThetaOptionContract::from_dollar_strike(
            "AAPL",
            NaiveDate::from_ymd_opt(2024, 1, 19).unwrap(),
            172.5,
            OptionRight::Put,
        )
        .unwrap();
        assert_eq!(c.rest_strike(), "172.500");
    }

    #[rstest]
    fn test_ws_strike_uses_occ_thousandths() {
        // $480 = 480 × 1000 thousandths of a dollar (OCC convention).
        // Per ThetaData support: the v3 docs example of `$480 → 4_800_000` is incorrect.
        assert_eq!(sample().ws_strike(), 480_000);
    }

    #[rstest]
    fn test_ws_expiration_format() {
        assert_eq!(sample().ws_expiration(), 20_250_315);
    }

    #[rstest]
    fn test_instrument_id_roundtrip() {
        let original = sample();
        let venue = *THETADATA_VENUE;
        let id = original.to_instrument_id(venue);
        assert_eq!(id.symbol.as_str(), "SPXW250315C00480000");

        let parsed = ThetaOptionContract::from_symbol(id.symbol.as_str()).unwrap();
        assert_eq!(parsed, original);
    }

    #[rstest]
    fn test_from_dollar_strike_rejects_subthousandth() {
        let err = ThetaOptionContract::from_dollar_strike(
            "AAPL",
            NaiveDate::from_ymd_opt(2024, 1, 19).unwrap(),
            172.5005,
            OptionRight::Put,
        )
        .unwrap_err();
        assert!(err.to_string().contains("thousandths"));
    }

    #[rstest]
    fn test_from_symbol_rejects_short() {
        assert!(ThetaOptionContract::from_symbol("X").is_err());
    }
}
