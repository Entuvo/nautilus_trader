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

//! Venue-native enums for the ThetaData adapter, with mappers to Nautilus enums.

use nautilus_model::enums::{AggressorSide, OptionKind};
use serde::{Deserialize, Serialize};

/// Option contract right (call or put).
///
/// ThetaData uses `"C"` / `"P"` over the wire in both REST and WebSocket payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OptionRight {
    #[serde(rename = "C")]
    Call,
    #[serde(rename = "P")]
    Put,
}

impl OptionRight {
    /// Returns the single-character ThetaData wire code.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Call => "C",
            Self::Put => "P",
        }
    }

    /// Returns the lowercase REST query value (`call` or `put`).
    #[must_use]
    pub const fn as_rest_query(self) -> &'static str {
        match self {
            Self::Call => "call",
            Self::Put => "put",
        }
    }
}

impl From<OptionRight> for OptionKind {
    fn from(value: OptionRight) -> Self {
        match value {
            OptionRight::Call => Self::Call,
            OptionRight::Put => Self::Put,
        }
    }
}

/// Trade condition codes that explicitly signal aggressor side.
///
/// Per the ThetaData trade-conditions reference, only codes 145 and 146 directly indicate trade
/// initiation. Every other code maps to [`AggressorSide::NoAggressor`].
pub const COND_BID_AGGRESSOR: u32 = 145;
pub const COND_ASK_AGGRESSOR: u32 = 146;

/// Maps a ThetaData trade condition code to a Nautilus [`AggressorSide`].
#[must_use]
pub fn aggressor_from_condition(condition: u32) -> AggressorSide {
    match condition {
        COND_BID_AGGRESSOR => AggressorSide::Buyer,
        COND_ASK_AGGRESSOR => AggressorSide::Seller,
        _ => AggressorSide::NoAggressor,
    }
}

/// Trade condition codes that should be excluded from volume / OHLC aggregation.
///
/// These are cancellation reports, "already accounted for" detail trades, blanked prices, and
/// stopped trades. Code 109 (`IMPLIED`) is **not** in this list — it updates volume but not OHLC,
/// which is handled separately by bar aggregators.
pub const VOLUME_EXCLUDED_CONDITIONS: &[u32] = &[
    26, // OPEN_DETAIL
    27, // INTRA_DETAIL
    40, // CANC
    41, // CANC_LAST
    42, // CANC_OPEN
    43, // CANC_ONLY
    44, // CANC_STPD
    49, // BLANK_PRICE
];

/// Returns true when the condition should be excluded from volume aggregation.
#[must_use]
pub fn is_volume_excluded(condition: u32) -> bool {
    VOLUME_EXCLUDED_CONDITIONS.contains(&condition)
}

/// OPRA options exchange code.
///
/// Stock exchange codes are decoded separately — see the historical-quote / historical-trade
/// response handlers.
pub const EXCHANGE_OPRA: u32 = 10;

/// Bar-aggregation intervals supported by the ThetaData historical OHLC and quote endpoints.
///
/// Maps to the `interval` query parameter. Sub-minute intervals are only valid for single-day
/// requests — the historical client enforces this when more than one day is requested.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Interval {
    Tick,
    Ms10,
    Ms100,
    Ms500,
    S1,
    S5,
    S10,
    S15,
    S30,
    M1,
    M5,
    M10,
    M15,
    M30,
    H1,
}

impl Interval {
    /// Returns the wire-string form (matches the ThetaData `interval` query param).
    #[must_use]
    pub const fn as_query(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Ms10 => "10ms",
            Self::Ms100 => "100ms",
            Self::Ms500 => "500ms",
            Self::S1 => "1s",
            Self::S5 => "5s",
            Self::S10 => "10s",
            Self::S15 => "15s",
            Self::S30 => "30s",
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M10 => "10m",
            Self::M15 => "15m",
            Self::M30 => "30m",
            Self::H1 => "1h",
        }
    }

    /// Whether this interval is sub-minute (and therefore restricted to single-day requests).
    #[must_use]
    pub const fn is_sub_minute(self) -> bool {
        matches!(
            self,
            Self::Tick
                | Self::Ms10
                | Self::Ms100
                | Self::Ms500
                | Self::S1
                | Self::S5
                | Self::S10
                | Self::S15
                | Self::S30,
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;

    #[rstest]
    fn test_option_right_wire() {
        assert_eq!(OptionRight::Call.as_wire(), "C");
        assert_eq!(OptionRight::Put.as_wire(), "P");
    }

    #[rstest]
    fn test_option_right_rest_query() {
        assert_eq!(OptionRight::Call.as_rest_query(), "call");
        assert_eq!(OptionRight::Put.as_rest_query(), "put");
    }

    #[rstest]
    #[case(145, AggressorSide::Buyer)]
    #[case(146, AggressorSide::Seller)]
    #[case(0, AggressorSide::NoAggressor)]
    #[case(18, AggressorSide::NoAggressor)]
    fn test_aggressor_from_condition(#[case] code: u32, #[case] expected: AggressorSide) {
        assert_eq!(aggressor_from_condition(code), expected);
    }

    #[rstest]
    #[case(0, false)]
    #[case(26, true)]
    #[case(27, true)]
    #[case(40, true)]
    #[case(49, true)]
    #[case(109, false)] // IMPLIED — volume updates, OHLC excluded elsewhere
    fn test_is_volume_excluded(#[case] code: u32, #[case] expected: bool) {
        assert_eq!(is_volume_excluded(code), expected);
    }

    #[rstest]
    #[case(Interval::Tick, "tick")]
    #[case(Interval::Ms100, "100ms")]
    #[case(Interval::S30, "30s")]
    #[case(Interval::M1, "1m")]
    #[case(Interval::H1, "1h")]
    fn test_interval_as_query(#[case] interval: Interval, #[case] expected: &str) {
        assert_eq!(interval.as_query(), expected);
    }

    #[rstest]
    #[case(Interval::Tick, true)]
    #[case(Interval::S30, true)]
    #[case(Interval::M1, false)]
    #[case(Interval::H1, false)]
    fn test_interval_is_sub_minute(#[case] interval: Interval, #[case] expected: bool) {
        assert_eq!(interval.is_sub_minute(), expected);
    }
}
