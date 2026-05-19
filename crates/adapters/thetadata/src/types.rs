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

//! Wire-format DTOs for the ThetaData adapter.

use serde::{Deserialize, Serialize};

// -------------------------------------------------------------------------------------------------
// REST response DTOs
// -------------------------------------------------------------------------------------------------

/// One row of `/v3/option/list/contracts`.
///
/// `expiration` is `YYYY-MM-DD`. `strike` is decimal dollars. `right` is `"call"` or `"put"`.
/// The `symbol` field carries the underlying root, not a fully-qualified contract identifier —
/// the (root, expiration, strike, right) tuple uniquely identifies the option.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RestContractRow {
    pub symbol: String,
    pub expiration: String,
    pub strike: f64,
    pub right: String,
}

/// One row of `/v3/{stock,index}/history/eod`.
///
/// Verified against live Terminal 2026-05 build: returns one row per requested date with
/// open/high/low/close + an EOD snapshot of last-trade NBBO. Index endpoints leave the NBBO
/// fields as zero.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RestEodRow {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    #[serde(default)]
    pub volume: u64,
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub last_trade: String,
    #[serde(default)]
    pub created: String,
}

/// One row of `/v3/option/list/expirations`.
///
/// Verified against live Terminal 2026-05 build: `{"symbol":"AAPL","expiration":"2012-06-01"}`.
/// `expiration` is `YYYY-MM-DD`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RestExpirationRow {
    pub symbol: String,
    pub expiration: String,
}

/// One row of `/v3/option/list/strikes`.
///
/// Verified against live Terminal 2026-05 build: `{"symbol":"AAPL","strike":282.500}`.
/// The endpoint does **not** echo the `expiration` parameter back — the caller already knows it.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RestStrikeRow {
    pub symbol: String,
    pub strike: f64,
}

/// One row of `/v3/option/history/quote` in `ndjson` format.
///
/// `timestamp` is ISO 8601 millisecond precision in Eastern Time.
/// Prices are floating-point dollars.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RestQuoteRow {
    pub symbol: String,
    pub expiration: String,
    pub strike: f64,
    pub right: String,
    pub timestamp: String,
    pub bid_size: u32,
    pub bid_exchange: u32,
    pub bid: f64,
    pub bid_condition: u32,
    pub ask_size: u32,
    pub ask_exchange: u32,
    pub ask: f64,
    pub ask_condition: u32,
}

/// One row of `/v3/option/history/trade`.
///
/// `sequence` is signed (`i64`) because the v3 wire format encodes OPRA sequence numbers as
/// signed 32-bit casts widened to 64 bits — values can come back negative for some sessions.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RestTradeRow {
    pub symbol: String,
    pub expiration: String,
    pub strike: f64,
    pub right: String,
    pub timestamp: String,
    pub price: f64,
    pub size: u64,
    pub exchange: u32,
    pub condition: u32,
    pub sequence: i64,
}

/// One row of `/v3/option/history/ohlc`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RestOhlcRow {
    pub symbol: String,
    pub expiration: String,
    pub strike: f64,
    pub right: String,
    pub timestamp: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
    pub count: u64,
    pub vwap: f64,
}

// -------------------------------------------------------------------------------------------------
// WebSocket frame DTOs
// -------------------------------------------------------------------------------------------------

/// Header common to every WebSocket inbound frame.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsHeader {
    pub status: String,
    #[serde(rename = "type")]
    pub kind: String,
}

/// Contract block embedded in `QUOTE`, `TRADE`, and `OHLC` frames.
///
/// Note: `strike` here is the WebSocket encoding (integer ×10 000).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsContract {
    pub security_type: String,
    pub root: String,
    pub expiration: u32,
    pub strike: u64,
    pub right: String,
}

/// Inbound `QUOTE` frame body.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsQuoteBody {
    pub ms_of_day: u64,
    pub bid_size: u32,
    pub bid_exchange: u32,
    pub bid: f64,
    pub bid_condition: u32,
    pub ask_size: u32,
    pub ask_exchange: u32,
    pub ask: f64,
    pub ask_condition: u32,
    pub date: u32,
}

/// Inbound `TRADE` frame body.
///
/// `sequence` is signed (`i64`) — see `RestTradeRow` for the reason.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsTradeBody {
    pub ms_of_day: u64,
    pub sequence: i64,
    pub size: u64,
    pub condition: u32,
    pub price: f64,
    pub exchange: u32,
    pub date: u32,
}

/// Inbound `OHLC` frame body — session-cumulative open/high/low/close + running volume/count.
///
/// The Terminal pushes these as a side-effect of TRADE-stream subscriptions (per the "Full
/// Trade Stream" docs: each trade also triggers an OHLC summary message). The OHLC is
/// cumulative over the session, not aligned to bar intervals.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsOhlcBody {
    pub ms_of_day: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
    pub count: u64,
    pub date: u32,
}

/// Top-level `QUOTE` frame.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsQuoteFrame {
    pub header: WsHeader,
    pub contract: WsContract,
    pub quote: WsQuoteBody,
}

/// Top-level `OHLC` frame (session-summary update piggybacking on a trade subscription).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsOhlcFrame {
    pub header: WsHeader,
    pub contract: WsContract,
    pub ohlc: WsOhlcBody,
}

/// Top-level `TRADE` frame.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsTradeFrame {
    pub header: WsHeader,
    pub contract: WsContract,
    pub trade: WsTradeBody,
}

/// Standalone `STATUS` heartbeat frame, emitted every second.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsStatusFrame {
    pub header: WsHeader,
}

/// Standalone `STATE` frame — session-state notification (e.g. `START` at session open).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WsStateFrame {
    pub header: WsHeader,
    /// Verbatim state token from the header, e.g. `"START"`.
    pub state: Option<String>,
}

/// Tag union used by the WebSocket router to dispatch decoded frames.
#[derive(Clone, Debug)]
pub enum WsFrame {
    Quote(WsQuoteFrame),
    Trade(WsTradeFrame),
    Ohlc(WsOhlcFrame),
    Status(WsStatusFrame),
    State(WsStateFrame),
}
