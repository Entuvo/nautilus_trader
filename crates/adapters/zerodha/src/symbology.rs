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

//! `InstrumentId` ↔ Kite trading-symbol mapping.
//!
//! Symbology rules (spec §5/Phase-2 §7, refined against the live `/instruments` fixture):
//!
//! - **Equity**: Nautilus `Symbol = "<kite_tradingsymbol>-EQ"`, `Venue = exchange`.
//!   Suffix disambiguates from same-root futures (e.g. `RELIANCE-EQ.NSE` vs.
//!   `RELIANCE26MAYFUT.NFO`). Kite tradingsymbols may already contain `-` (e.g.
//!   `GOLDSTAR-ST`); the disambiguating `-EQ` is appended verbatim.
//! - **Future**: `Symbol = <kite_tradingsymbol>`, `Venue = exchange` (e.g. `NIFTY26MAYFUT.NFO`).
//! - **Option**: `Symbol = <kite_tradingsymbol>`, `Venue = exchange` (e.g. `NIFTY26MAY23600CE.NFO`).
//! - **Index**: `Symbol = <kite_tradingsymbol>` with whitespace → `-`,
//!   `Venue = "<exchange>_INDEX"` (e.g. `NIFTY 50` → `NIFTY-50.NSE_INDEX`).
//!
//! No openalgo-style semantic renames (`NIFTY 50 → NIFTY` etc.). Divergence from Kite docs is
//! worse than divergence from openalgo (spec §7).

use nautilus_model::identifiers::{InstrumentId, Symbol, Venue};

use crate::error::ZerodhaError;

/// Suffix appended to Kite equity tradingsymbols when forming the Nautilus `Symbol`.
pub const EQUITY_SYMBOL_SUFFIX: &str = "-EQ";

/// Suffix appended to the Kite exchange when forming the Nautilus index `Venue`.
pub const INDEX_VENUE_SUFFIX: &str = "_INDEX";

/// Side-band identifiers we must remember per instrument: Phase 3 WS needs `instrument_token`,
/// Phase 5 REST orders need `tradingsymbol` + `exchange`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KiteToken {
    /// Integer ID Kite uses on the binary WS channel.
    pub instrument_token: u32,
    /// Per-exchange short ID (less used; kept for completeness).
    pub exchange_token: u32,
    /// Human-readable Kite symbol (e.g. `RELIANCE`, `NIFTY26MAYFUT`, `NIFTY 50`).
    pub tradingsymbol: String,
    /// Kite exchange (e.g. `NSE`, `BSE`, `NFO`, `BFO`, `CDS`, `MCX`).
    pub exchange: String,
    /// Asset-class classification derived from `(instrument_type, segment)`.
    pub kind: InstrumentKind,
}

/// Asset-class classification derived from Kite's `(instrument_type, segment)` pair.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum InstrumentKind {
    /// Cash equity (`instrument_type=EQ, segment=NSE | BSE`).
    Equity,
    /// Futures contract on equity / index / commodity / currency.
    Future,
    /// Call option (`instrument_type=CE`).
    OptionCall,
    /// Put option (`instrument_type=PE`).
    OptionPut,
    /// Index (`segment=INDICES`, or composite exchanges `NSEIX` / `GLOBAL`).
    ///
    /// Routed by `segment`/`exchange` rather than `instrument_type` because Kite ships indices
    /// with `instrument_type=EQ` and `tick_size=0`.
    Index,
}

impl InstrumentKind {
    /// Classify a Kite CSV row into the matching adapter kind.
    ///
    /// Returns `None` for rows the adapter intentionally drops — currently the `NCO` (commodity
    /// options on NSE) segment per the spec scope clarification, plus any unknown
    /// `instrument_type`.
    #[must_use]
    pub fn classify(
        instrument_type: &str,
        segment: &str,
        exchange: &str,
    ) -> Option<Self> {
        // Spec scope: drop NCO (low volume, untested in openalgo).
        if exchange == "NCO" || segment.starts_with("NCO") {
            return None;
        }

        // Index dispatch wins over instrument_type because indices ship as EQ rows.
        if segment == "INDICES" || exchange == "NSEIX" || exchange == "GLOBAL" {
            return Some(Self::Index);
        }

        match instrument_type {
            "EQ" => Some(Self::Equity),
            "FUT" => Some(Self::Future),
            "CE" => Some(Self::OptionCall),
            "PE" => Some(Self::OptionPut),
            _ => None,
        }
    }
}

/// Compose the Nautilus `InstrumentId` for the given Kite row metadata.
///
/// # Errors
///
/// Returns [`ZerodhaError::InvalidResponse`] if the tradingsymbol is empty or the exchange code
/// is not ASCII (which would break Nautilus's `Venue` validation).
pub fn instrument_id(
    kind: InstrumentKind,
    tradingsymbol: &str,
    exchange: &str,
) -> Result<InstrumentId, ZerodhaError> {
    if tradingsymbol.is_empty() {
        return Err(ZerodhaError::InvalidResponse(
            "empty tradingsymbol".to_string(),
        ));
    }
    if !exchange.is_ascii() {
        return Err(ZerodhaError::InvalidResponse(format!(
            "non-ascii exchange: {exchange:?}"
        )));
    }

    let (symbol, venue) = match kind {
        InstrumentKind::Equity => (
            format!("{tradingsymbol}{EQUITY_SYMBOL_SUFFIX}"),
            exchange.to_string(),
        ),
        InstrumentKind::Future | InstrumentKind::OptionCall | InstrumentKind::OptionPut => {
            (tradingsymbol.to_string(), exchange.to_string())
        }
        InstrumentKind::Index => (
            normalize_index_symbol(tradingsymbol),
            format!("{exchange}{INDEX_VENUE_SUFFIX}"),
        ),
    };

    Ok(InstrumentId::new(
        Symbol::from(symbol.as_str()),
        Venue::from(venue.as_str()),
    ))
}

/// Whitespace-only normalization for index trading symbols (`NIFTY 50` → `NIFTY-50`).
#[must_use]
pub fn normalize_index_symbol(tradingsymbol: &str) -> String {
    let mut out = String::with_capacity(tradingsymbol.len());
    let mut prev_dash = false;
    for ch in tradingsymbol.chars() {
        if ch.is_whitespace() {
            if !prev_dash {
                out.push('-');
                prev_dash = true;
            }
        } else {
            out.push(ch);
            prev_dash = ch == '-';
        }
    }
    out.trim_matches('-').to_string()
}

/// Derive the Nautilus `price_precision` (decimal places) from a Kite `tick_size` string.
///
/// Examples: `"0.05"` → 2, `"0.0025"` → 4, `"0.1"` → 1, `"0.10"` → 1, `"1"` → 0.
///
/// `0` (no tradeable tick — common for indices) returns 0; callers that care must filter
/// such rows separately.
#[must_use]
pub fn price_precision_from_tick(tick: &str) -> u8 {
    let trimmed = tick.trim();
    let Some((_, frac)) = trimmed.split_once('.') else {
        return 0;
    };
    // "0.10" → "1" → 1; "0.0025" → "0025" → 4
    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        0
    } else {
        frac.len() as u8
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::reliance_nse_eq("EQ", "NSE", "NSE", Some(InstrumentKind::Equity))]
    #[case::nifty_future("FUT", "NFO-FUT", "NFO", Some(InstrumentKind::Future))]
    #[case::nifty_call("CE", "NFO-OPT", "NFO", Some(InstrumentKind::OptionCall))]
    #[case::nifty_put("PE", "NFO-OPT", "NFO", Some(InstrumentKind::OptionPut))]
    #[case::nifty_index("EQ", "INDICES", "NSE", Some(InstrumentKind::Index))]
    #[case::sensex_index("EQ", "INDICES", "BSE", Some(InstrumentKind::Index))]
    #[case::nseix_global("EQ", "INDICES", "NSEIX", Some(InstrumentKind::Index))]
    #[case::nco_dropped("FUT", "NCO-FUT", "NCO", None)]
    #[case::unknown_kind("XX", "FOO", "NSE", None)]
    fn classify_matrix(
        #[case] instrument_type: &str,
        #[case] segment: &str,
        #[case] exchange: &str,
        #[case] expected: Option<InstrumentKind>,
    ) {
        assert_eq!(
            InstrumentKind::classify(instrument_type, segment, exchange),
            expected,
        );
    }

    #[rstest]
    fn instrument_id_equity_appends_eq_suffix() {
        let id = instrument_id(InstrumentKind::Equity, "RELIANCE", "NSE").unwrap();
        assert_eq!(id.symbol.to_string(), "RELIANCE-EQ");
        assert_eq!(id.venue.to_string(), "NSE");
    }

    #[rstest]
    fn instrument_id_equity_preserves_native_hyphens() {
        let id = instrument_id(InstrumentKind::Equity, "GOLDSTAR-ST", "NSE").unwrap();
        assert_eq!(id.symbol.to_string(), "GOLDSTAR-ST-EQ");
    }

    #[rstest]
    fn instrument_id_future_no_suffix() {
        let id = instrument_id(InstrumentKind::Future, "NIFTY26MAYFUT", "NFO").unwrap();
        assert_eq!(id.symbol.to_string(), "NIFTY26MAYFUT");
        assert_eq!(id.venue.to_string(), "NFO");
    }

    #[rstest]
    fn instrument_id_option_no_suffix() {
        let id = instrument_id(InstrumentKind::OptionCall, "NIFTY26MAY23600CE", "NFO").unwrap();
        assert_eq!(id.symbol.to_string(), "NIFTY26MAY23600CE");
    }

    #[rstest]
    fn instrument_id_index_normalises_whitespace_and_adds_venue_suffix() {
        let id = instrument_id(InstrumentKind::Index, "NIFTY 50", "NSE").unwrap();
        assert_eq!(id.symbol.to_string(), "NIFTY-50");
        assert_eq!(id.venue.to_string(), "NSE_INDEX");
    }

    #[rstest]
    fn instrument_id_index_collapses_multiple_spaces() {
        let id = instrument_id(InstrumentKind::Index, "NIFTY  MIDCAP  100", "NSE").unwrap();
        assert_eq!(id.symbol.to_string(), "NIFTY-MIDCAP-100");
    }

    #[rstest]
    fn instrument_id_rejects_empty_tradingsymbol() {
        assert!(instrument_id(InstrumentKind::Equity, "", "NSE").is_err());
    }

    #[rstest]
    #[case::two_dp("0.05", 2)]
    #[case::four_dp("0.0025", 4)]
    #[case::one_dp("0.1", 1)]
    #[case::one_dp_trailing_zero("0.10", 1)]
    #[case::integer("1", 0)]
    #[case::zero("0", 0)]
    #[case::three_dp("0.001", 3)]
    fn price_precision_matrix(#[case] tick: &str, #[case] expected: u8) {
        assert_eq!(price_precision_from_tick(tick), expected);
    }
}
