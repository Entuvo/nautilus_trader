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

//! End-to-end REST + decode smoke test.
//!
//! Pulls one trading day of 1-minute quotes for a near-ATM AAPL option and prints the decoded
//! Nautilus `QuoteTick` for the first/last bar. Runs without market hours since it's historical.
//!
//! Run with:
//!
//! ```text
//! cargo run --example thetadata-hist-tester -p nautilus-thetadata
//! ```

use std::time::Duration;

use chrono::NaiveDate;
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::{InstrumentId, Symbol, Venue};
use nautilus_thetadata::{
    common::{DEFAULT_HTTP_URL, THETADATA_VENUE},
    enums::{Interval, OptionRight},
    historical::ThetaDataHistoricalClient,
    symbology::ThetaOptionContract,
};
use ustr::Ustr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1) Construct the REST client against the local Terminal.
    let http = ThetaDataHistoricalClient::new(DEFAULT_HTTP_URL, Duration::from_secs(30))?;
    println!("[hist-tester] connected to {}", http.base_url());

    // 2) Build the contract: AAPL 2026-05-22 $285 call.
    let contract = ThetaOptionContract::from_dollar_strike(
        "AAPL",
        NaiveDate::from_ymd_opt(2026, 5, 22).unwrap(),
        285.0,
        OptionRight::Call,
    )?;

    let instrument_id = InstrumentId::new(
        Symbol::new(format!(
            "{}{}{}{}{:08}",
            contract.root,
            contract.expiration.format("%y"),
            contract.expiration.format("%m%d"),
            contract.right.as_wire(),
            contract.strike_thousandths,
        )),
        *THETADATA_VENUE,
    );
    let _venue: Venue = *THETADATA_VENUE; // silence unused-import warning on ustr/Venue if any
    let _ = Ustr::from("OPRA");
    println!("[hist-tester] target: {instrument_id}");

    // 3) Request 1-minute quotes for last Friday (2026-05-15).
    let day = NaiveDate::from_ymd_opt(2026, 5, 15).unwrap();
    let rows = http.hist_quotes(&contract, day, day, Interval::M1).await?;
    println!("[hist-tester] fetched {} rows", rows.len());

    if rows.is_empty() {
        println!("[hist-tester] no rows returned — this is unexpected for a liquid AAPL strike");
        return Ok(());
    }

    // 4) Decode first and last rows into Nautilus QuoteTicks.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| UnixNanos::from(d.as_nanos() as u64))
        .unwrap_or_default();

    let first_tick = rows[0].to_quote_tick(instrument_id, 2, 0, now)?;
    let last_tick = rows[rows.len() - 1].to_quote_tick(instrument_id, 2, 0, now)?;

    println!("[hist-tester] first  quote @ {}: bid={} ask={} bid_size={} ask_size={}",
        u64::from(first_tick.ts_event),
        first_tick.bid_price,
        first_tick.ask_price,
        first_tick.bid_size,
        first_tick.ask_size,
    );
    println!("[hist-tester] last   quote @ {}: bid={} ask={} bid_size={} ask_size={}",
        u64::from(last_tick.ts_event),
        last_tick.bid_price,
        last_tick.ask_price,
        last_tick.bid_size,
        last_tick.ask_size,
    );

    Ok(())
}
