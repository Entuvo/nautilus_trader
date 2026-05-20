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

//! Wire DTO → Nautilus domain type decoders.
//!
//! Two timestamp conventions to bridge — both treat the source as **America/New_York wall clock**
//! and convert to UTC `UnixNanos` via DST-aware `chrono-tz`:
//!
//! - **REST** rows carry ISO 8601 millisecond strings (no offset), e.g.
//!   `"2023-12-19T09:30:22.025"`.
//! - **WebSocket** frames split the timestamp into `date` (`YYYYMMDD` int) and `ms_of_day`
//!   (milliseconds since 00:00:00 ET).
//!
//! Ambiguous local times during the fall-back DST transition resolve to the earlier UTC
//! instant (standard time); non-existent times during the spring-forward transition are
//! rejected.

use anyhow::{Context, Result, anyhow, bail};
use chrono::{LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use chrono_tz::{America::New_York, Tz};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    enums::AggressorSide,
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};

use crate::{
    enums::aggressor_from_condition,
    types::{RestOhlcRow, RestQuoteRow, RestTradeRow, WsQuoteFrame, WsTradeFrame},
};

const MS_TO_NS: u64 = 1_000_000;
const ET_TZ: Tz = New_York;

/// Combines a `YYYYMMDD` date integer and `ms_of_day` (Eastern Time) into a `UnixNanos`.
///
/// # Errors
///
/// Returns an error if the date integer is not a valid calendar date, `ms_of_day` is out of
/// range for a single day (86 400 000 ms), or the local time does not exist (spring-forward gap).
pub fn ws_timestamp_to_unix_nanos(date: u32, ms_of_day: u64) -> Result<UnixNanos> {
    if ms_of_day >= 86_400_000 {
        bail!("ms_of_day out of range: {ms_of_day}");
    }
    let year = (date / 10_000) as i32;
    let month = (date / 100) % 100;
    let day = date % 100;
    let naive_date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| anyhow!("invalid date: {date}"))?;
    let hours = (ms_of_day / 3_600_000) as u32;
    let mins = ((ms_of_day % 3_600_000) / 60_000) as u32;
    let secs = ((ms_of_day % 60_000) / 1_000) as u32;
    let millis = (ms_of_day % 1_000) as u32;
    let naive_time = NaiveTime::from_hms_milli_opt(hours, mins, secs, millis)
        .ok_or_else(|| anyhow!("invalid time-of-day: {ms_of_day}"))?;
    et_naive_to_unix_nanos(NaiveDateTime::new(naive_date, naive_time))
}

/// Parses an ISO 8601 millisecond timestamp returned by the REST API and converts to `UnixNanos`.
///
/// Accepts `"YYYY-MM-DDTHH:MM:SS.SSS"` (no offset). The value is interpreted as
/// America/New_York wall clock.
///
/// # Errors
///
/// Returns an error if the string cannot be parsed or the local time does not exist.
pub fn parse_rest_timestamp(s: &str) -> Result<UnixNanos> {
    let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.3f")
        .with_context(|| format!("invalid REST timestamp {s:?}"))?;
    et_naive_to_unix_nanos(naive)
}

/// Converts a naive ET wall-clock datetime to `UnixNanos`.
///
/// # Errors
///
/// Returns an error if the local time does not exist (DST spring-forward gap). Ambiguous times
/// (fall-back overlap) resolve to the earlier UTC instant.
pub fn et_naive_to_unix_nanos(naive: NaiveDateTime) -> Result<UnixNanos> {
    let resolved = match ET_TZ.from_local_datetime(&naive) {
        LocalResult::Single(dt) => dt,
        LocalResult::Ambiguous(earlier, _) => earlier,
        LocalResult::None => bail!("local time {naive} does not exist in America/New_York"),
    };
    let millis = resolved.timestamp_millis();
    if millis < 0 {
        bail!("timestamp pre-epoch");
    }
    Ok(UnixNanos::from((millis as u64) * MS_TO_NS))
}

impl RestQuoteRow {
    /// Decodes this row into a Nautilus [`QuoteTick`].
    ///
    /// `price_precision` and `size_precision` come from the instrument metadata (typically
    /// `(2, 0)` for US options).
    ///
    /// # Errors
    ///
    /// Returns an error if the row's timestamp cannot be parsed.
    pub fn to_quote_tick(
        &self,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> Result<QuoteTick> {
        let ts_event = parse_rest_timestamp(&self.timestamp)?;
        let bid_price = Price::new(self.bid, price_precision);
        let ask_price = Price::new(self.ask, price_precision);
        let bid_size = Quantity::new(f64::from(self.bid_size), size_precision);
        let ask_size = Quantity::new(f64::from(self.ask_size), size_precision);
        QuoteTick::new_checked(
            instrument_id,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            ts_event,
            ts_init,
        )
        .map_err(Into::into)
    }
}

impl RestTradeRow {
    /// Decodes this row into a Nautilus [`TradeTick`].
    ///
    /// The aggressor side is derived from the trade condition code (codes 145/146 only).
    ///
    /// # Errors
    ///
    /// Returns an error if the row's timestamp cannot be parsed or the size is non-positive.
    pub fn to_trade_tick(
        &self,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> Result<TradeTick> {
        let ts_event = parse_rest_timestamp(&self.timestamp)?;
        let price = Price::new(self.price, price_precision);
        let size = Quantity::new(self.size as f64, size_precision);
        let aggressor: AggressorSide = aggressor_from_condition(self.condition);
        let trade_id = TradeId::new(self.sequence.to_string().as_str());
        TradeTick::new_checked(instrument_id, price, size, aggressor, trade_id, ts_event, ts_init)
            .map_err(Into::into)
    }
}

impl WsQuoteFrame {
    /// Decodes this inbound WebSocket quote into a Nautilus [`QuoteTick`].
    ///
    /// # Errors
    ///
    /// Returns an error if the embedded `date + ms_of_day` cannot be combined into a valid
    /// timestamp.
    pub fn to_quote_tick(
        &self,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> Result<QuoteTick> {
        let ts_event = ws_timestamp_to_unix_nanos(self.quote.date, self.quote.ms_of_day)?;
        let bid_price = Price::new(self.quote.bid, price_precision);
        let ask_price = Price::new(self.quote.ask, price_precision);
        let bid_size = Quantity::new(f64::from(self.quote.bid_size), size_precision);
        let ask_size = Quantity::new(f64::from(self.quote.ask_size), size_precision);
        QuoteTick::new_checked(
            instrument_id,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            ts_event,
            ts_init,
        )
        .map_err(Into::into)
    }
}

impl WsTradeFrame {
    /// Decodes this inbound WebSocket trade into a Nautilus [`TradeTick`].
    ///
    /// Aggressor side derives from the trade condition code (codes 145/146 only).
    ///
    /// # Errors
    ///
    /// Returns an error if the timestamp cannot be combined or the size is non-positive.
    pub fn to_trade_tick(
        &self,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> Result<TradeTick> {
        let ts_event = ws_timestamp_to_unix_nanos(self.trade.date, self.trade.ms_of_day)?;
        let price = Price::new(self.trade.price, price_precision);
        let size = Quantity::new(self.trade.size as f64, size_precision);
        let aggressor = aggressor_from_condition(self.trade.condition);
        let trade_id = TradeId::new(self.trade.sequence.to_string().as_str());
        TradeTick::new_checked(instrument_id, price, size, aggressor, trade_id, ts_event, ts_init)
            .map_err(Into::into)
    }
}

impl RestOhlcRow {
    /// Decodes this row into a Nautilus [`Bar`].
    ///
    /// `bar_type` carries the instrument id, aggregation, and price-type — construct it at the
    /// call site from the historical request parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if the row's timestamp cannot be parsed or OHLC invariants
    /// (`high >= low`, etc.) are violated.
    pub fn to_bar(
        &self,
        bar_type: BarType,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> Result<Bar> {
        let ts_event = parse_rest_timestamp(&self.timestamp)?;
        let open = Price::new(self.open, price_precision);
        let high = Price::new(self.high, price_precision);
        let low = Price::new(self.low, price_precision);
        let close = Price::new(self.close, price_precision);
        let volume = Quantity::new(self.volume as f64, size_precision);
        Bar::new_checked(bar_type, open, high, low, close, volume, ts_event, ts_init)
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::BarType,
        enums::{BarAggregation, PriceType},
        identifiers::{InstrumentId, Symbol, Venue},
    };
    use rstest::*;
    use ustr::Ustr;

    use super::*;

    fn make_instrument_id() -> InstrumentId {
        InstrumentId::new(
            Symbol::new("AAPL240315C00180000"),
            Venue::new(Ustr::from("THETADATA")),
        )
    }

    #[rstest]
    fn test_ws_timestamp_combines_date_and_ms_in_et() {
        // 2023-12-19 09:30:22.025 ET (EST, UTC-5) → 2023-12-19 14:30:22.025 UTC
        // = 1_702_996_222_025 ms since epoch.
        let unix = ws_timestamp_to_unix_nanos(20_231_219, 34_222_025).unwrap();
        assert_eq!(u64::from(unix), 1_702_996_222_025_000_000);
    }

    #[rstest]
    fn test_ws_timestamp_rejects_out_of_range_ms() {
        assert!(ws_timestamp_to_unix_nanos(20_231_219, 86_400_000).is_err());
    }

    #[rstest]
    fn test_ws_timestamp_rejects_invalid_date() {
        assert!(ws_timestamp_to_unix_nanos(20_231_232, 0).is_err());
    }

    #[rstest]
    fn test_ws_timestamp_summer_uses_edt_offset() {
        // 2024-07-15 09:30:00 ET (EDT, UTC-4) → 13:30:00 UTC.
        let unix = ws_timestamp_to_unix_nanos(20_240_715, 34_200_000).unwrap();
        // Sanity check: should be 4 hours after midnight UTC + 9.5h.
        let expected_secs = chrono::NaiveDate::from_ymd_opt(2024, 7, 15)
            .unwrap()
            .and_hms_opt(13, 30, 0)
            .unwrap()
            .and_utc()
            .timestamp() as u64
            * 1_000_000_000;
        assert_eq!(u64::from(unix), expected_secs);
    }

    #[rstest]
    fn test_parse_rest_timestamp_basic() {
        let unix = parse_rest_timestamp("2023-12-19T09:30:22.025").unwrap();
        assert_eq!(u64::from(unix), 1_702_996_222_025_000_000);
    }

    #[rstest]
    fn test_parse_rest_timestamp_rejects_garbage() {
        assert!(parse_rest_timestamp("not-a-timestamp").is_err());
    }

    #[rstest]
    fn test_rest_quote_row_to_quote_tick() {
        let row = RestQuoteRow {
            symbol: "AAPL".into(),
            expiration: "2024-03-15".into(),
            strike: 180.0,
            right: "call".into(),
            timestamp: "2024-03-15T10:00:00.000".into(),
            bid_size: 5,
            bid_exchange: 10,
            bid: 1.23,
            bid_condition: 50,
            ask_size: 7,
            ask_exchange: 10,
            ask: 1.25,
            ask_condition: 50,
        };
        let tick = row.to_quote_tick(make_instrument_id(), 2, 0, UnixNanos::default()).unwrap();
        assert_eq!(tick.bid_price.to_string(), "1.23");
        assert_eq!(tick.ask_price.to_string(), "1.25");
        assert_eq!(tick.bid_size, Quantity::from(5));
        assert_eq!(tick.ask_size, Quantity::from(7));
    }

    #[rstest]
    #[case(0, AggressorSide::NoAggressor)]
    #[case(145, AggressorSide::Buyer)]
    #[case(146, AggressorSide::Seller)]
    fn test_rest_trade_row_to_trade_tick_aggressor_mapping(
        #[case] condition: u32,
        #[case] expected: AggressorSide,
    ) {
        let row = RestTradeRow {
            symbol: "AAPL".into(),
            expiration: "2024-03-15".into(),
            strike: 180.0,
            right: "call".into(),
            timestamp: "2024-03-15T10:00:00.000".into(),
            price: 1.24,
            size: 10,
            exchange: 10,
            condition,
            sequence: 42,
        };
        let tick = row.to_trade_tick(make_instrument_id(), 2, 0, UnixNanos::default()).unwrap();
        assert_eq!(tick.aggressor_side, expected);
        assert_eq!(tick.trade_id.to_string(), "42");
        assert_eq!(tick.size, Quantity::from(10));
    }

    #[rstest]
    fn test_rest_trade_row_rejects_zero_size() {
        let row = RestTradeRow {
            symbol: "AAPL".into(),
            expiration: "2024-03-15".into(),
            strike: 180.0,
            right: "call".into(),
            timestamp: "2024-03-15T10:00:00.000".into(),
            price: 1.24,
            size: 0,
            exchange: 10,
            condition: 0,
            sequence: 42,
        };
        assert!(row.to_trade_tick(make_instrument_id(), 2, 0, UnixNanos::default()).is_err());
    }

    #[rstest]
    fn test_rest_ohlc_row_to_bar() {
        let row = RestOhlcRow {
            symbol: "AAPL".into(),
            expiration: "2024-03-15".into(),
            strike: 180.0,
            right: "call".into(),
            timestamp: "2024-03-15T10:00:00.000".into(),
            open: 1.20,
            high: 1.30,
            low: 1.18,
            close: 1.25,
            volume: 1000,
            count: 50,
            vwap: 1.24,
        };
        let bar_type = BarType::new(
            make_instrument_id(),
            nautilus_model::data::BarSpecification::new(
                1,
                BarAggregation::Minute,
                PriceType::Last,
            ),
            nautilus_model::enums::AggregationSource::External,
        );
        let bar = row.to_bar(bar_type, 2, 0, UnixNanos::default()).unwrap();
        assert_eq!(bar.open.to_string(), "1.20");
        assert_eq!(bar.high.to_string(), "1.30");
        assert_eq!(bar.low.to_string(), "1.18");
        assert_eq!(bar.close.to_string(), "1.25");
        assert_eq!(bar.volume, Quantity::from(1000));
    }

    #[rstest]
    fn test_ws_quote_frame_to_quote_tick() {
        use crate::types::{WsContract, WsHeader, WsQuoteBody};
        let frame = WsQuoteFrame {
            header: WsHeader { status: "CONNECTED".into(), kind: "QUOTE".into() },
            contract: WsContract {
                security_type: "OPTION".into(),
                root: "SPXW".into(),
                expiration: 20_240_315,
                strike: 480_000,
                right: "C".into(),
            },
            quote: WsQuoteBody {
                ms_of_day: 34_222_025,
                bid_size: 7,
                bid_exchange: 5,
                bid: 110.2,
                bid_condition: 50,
                ask_size: 7,
                ask_exchange: 5,
                ask: 110.5,
                ask_condition: 50,
                date: 20_231_219,
            },
        };
        let tick = frame
            .to_quote_tick(make_instrument_id(), 2, 0, UnixNanos::default())
            .unwrap();
        assert_eq!(tick.bid_price.to_string(), "110.20");
        assert_eq!(tick.ask_price.to_string(), "110.50");
        assert_eq!(tick.bid_size, Quantity::from(7));
    }

    #[rstest]
    #[case(0, AggressorSide::NoAggressor)]
    #[case(145, AggressorSide::Buyer)]
    #[case(146, AggressorSide::Seller)]
    fn test_ws_trade_frame_to_trade_tick_aggressor(
        #[case] condition: u32,
        #[case] expected: AggressorSide,
    ) {
        use crate::types::{WsContract, WsHeader};
        let frame = WsTradeFrame {
            header: WsHeader { status: "CONNECTED".into(), kind: "TRADE".into() },
            contract: WsContract {
                security_type: "OPTION".into(),
                root: "AAPL".into(),
                expiration: 20_231_222,
                strike: 2_000_000,
                right: "C".into(),
            },
            trade: crate::types::WsTradeBody {
                ms_of_day: 34_389_945,
                sequence: 42,
                size: 10,
                condition,
                price: 0.31,
                exchange: 31,
                date: 20_231_219,
            },
        };
        let tick = frame
            .to_trade_tick(make_instrument_id(), 2, 0, UnixNanos::default())
            .unwrap();
        assert_eq!(tick.aggressor_side, expected);
        assert_eq!(tick.trade_id.to_string(), "42");
    }

    #[rstest]
    fn test_rest_ohlc_row_rejects_invariant_violation() {
        let row = RestOhlcRow {
            symbol: "AAPL".into(),
            expiration: "2024-03-15".into(),
            strike: 180.0,
            right: "call".into(),
            timestamp: "2024-03-15T10:00:00.000".into(),
            open: 1.20,
            high: 1.10, // < open — invalid
            low: 1.05,
            close: 1.15,
            volume: 100,
            count: 5,
            vwap: 1.12,
        };
        let bar_type = BarType::new(
            make_instrument_id(),
            nautilus_model::data::BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
            nautilus_model::enums::AggregationSource::External,
        );
        assert!(row.to_bar(bar_type, 2, 0, UnixNanos::default()).is_err());
    }
}
