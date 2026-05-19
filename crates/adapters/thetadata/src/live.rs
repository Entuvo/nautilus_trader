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

//! WebSocket client for the local ThetaTerminal streaming endpoint.
//!
//! Architecture:
//!
//! - **Single connection.** ThetaTerminal allows only one client at a time on
//!   `ws://127.0.0.1:25520/v1/events`; the adapter multiplexes every subscription through it.
//! - **Two-layer pattern.** The outer [`ThetaDataWsClient`] wraps
//!   `nautilus_network::WebSocketClient` (which owns the I/O task, reconnect with backoff,
//!   and message dispatch). This module supplies the ThetaData-specific protocol:
//!   subscribe/unsubscribe envelopes, a subscription registry, and inbound frame routing.
//! - **Reconnect = resubscribe.** A `post_reconnection` callback replays every active
//!   subscription from the registry through `WebSocketClient::send_text` (dispatched onto
//!   the shared Tokio runtime so the synchronous callback returns immediately).
//! - **Heartbeat.** The Terminal emits a `STATUS` frame every second; we surface those as
//!   `WsFrame::Status` to the consumer rather than swallowing them.
//! - **No blocking on slow consumers.** Decoded frames go through an unbounded MPSC channel.

use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, AtomicU8, Ordering},
};

use anyhow::{Context, Result, anyhow};
use indexmap::IndexMap;
use nautilus_common::live::get_runtime;
use nautilus_network::{
    transport::Message,
    websocket::{WebSocketClient, WebSocketConfig},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    symbology::ThetaOptionContract,
    types::{WsFrame, WsOhlcFrame, WsQuoteFrame, WsStateFrame, WsStatusFrame, WsTradeFrame},
};

/// Stream type — drives the `req_type` field of the subscribe envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamKind {
    Quote,
    Trade,
}

impl StreamKind {
    /// Returns the wire-string form (`"QUOTE"` or `"TRADE"`).
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Quote => "QUOTE",
            Self::Trade => "TRADE",
        }
    }
}

/// Key identifying a single-contract subscription. Used by [`SubscriptionRegistry`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionKey {
    pub kind: StreamKind,
    pub root: String,
    pub expiration: u32,
    pub strike: u64,
    pub right: String,
}

impl SubscriptionKey {
    /// Builds a [`SubscriptionKey`] for an option contract.
    #[must_use]
    pub fn for_option(kind: StreamKind, contract: &ThetaOptionContract) -> Self {
        Self {
            kind,
            root: contract.root.clone(),
            expiration: contract.ws_expiration(),
            strike: contract.ws_strike(),
            right: contract.right.as_wire().to_string(),
        }
    }
}

/// Connection state values stored in the lock-free state holder.
///
/// Kept for backward compatibility with the earlier scaffold — the authoritative state now
/// lives inside `nautilus_network::WebSocketClient::connection_mode`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnState {
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
    Closing = 3,
}

impl From<u8> for ConnState {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Connecting,
            2 => Self::Connected,
            3 => Self::Closing,
            _ => Self::Disconnected,
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Subscription registry
// -------------------------------------------------------------------------------------------------

/// Tracks active subscriptions so the client can replay them on reconnect.
///
/// Subscriptions are deduplicated by [`SubscriptionKey`]; calling `add` for an existing key is
/// idempotent and returns `false` to signal no envelope needs to be sent.
#[derive(Debug, Default)]
pub struct SubscriptionRegistry {
    entries: IndexMap<SubscriptionKey, u64>,
}

impl SubscriptionRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a subscription. Returns `true` if it was newly inserted.
    ///
    /// `id` is the envelope id stamped on the outgoing subscribe message.
    pub fn add(&mut self, key: SubscriptionKey, id: u64) -> bool {
        if self.entries.contains_key(&key) {
            return false;
        }
        self.entries.insert(key, id);
        true
    }

    /// Removes a subscription. Returns `true` if it existed.
    pub fn remove(&mut self, key: &SubscriptionKey) -> bool {
        self.entries.shift_remove(key).is_some()
    }

    /// Returns the number of active subscriptions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` when there are no active subscriptions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the subscribe payloads for every active entry, in insertion order.
    ///
    /// Used by the reconnect-replay path. The `id` field stamped on each payload is the same
    /// id that was used on the original subscribe.
    #[must_use]
    pub fn replay_payloads(&self) -> Vec<String> {
        self.entries
            .iter()
            .map(|(key, id)| build_subscribe_payload(true, *id, key))
            .collect()
    }
}

// -------------------------------------------------------------------------------------------------
// Wire envelope serialization
// -------------------------------------------------------------------------------------------------

/// Builds the JSON subscribe/unsubscribe envelope for a single-contract subscription.
///
/// Schema (per ThetaData v3 docs):
///
/// ```json
/// {
///   "msg_type": "STREAM",
///   "sec_type": "OPTION",
///   "req_type": "QUOTE"|"TRADE",
///   "add":     true|false,
///   "id":      <int>,
///   "contract": {
///     "root":       "SPXW",
///     "expiration": 20240315,
///     "strike":     480000,
///     "right":      "C"|"P"
///   }
/// }
/// ```
#[must_use]
pub fn build_subscribe_payload(add: bool, id: u64, key: &SubscriptionKey) -> String {
    let value = serde_json::json!({
        "msg_type": "STREAM",
        "sec_type": "OPTION",
        "req_type": key.kind.as_wire(),
        "add": add,
        "id": id,
        "contract": {
            "root": key.root,
            "expiration": key.expiration,
            "strike": key.strike,
            "right": key.right,
        },
    });
    serde_json::to_string(&value).expect("envelope value is always serializable")
}

// -------------------------------------------------------------------------------------------------
// Inbound frame parsing
// -------------------------------------------------------------------------------------------------

/// Parses an inbound text frame into a typed [`WsFrame`].
///
/// Dispatch is by `header.type`:
///
/// - `STATUS` → [`WsStatusFrame`]
/// - `QUOTE`  → [`WsQuoteFrame`]
/// - `TRADE`  → [`WsTradeFrame`]
///
/// # Errors
///
/// Returns an error if the JSON is malformed, the `header.type` field is missing, or the body
/// fails to deserialize into the type indicated by the header.
pub fn parse_ws_frame(text: &str) -> Result<WsFrame> {
    let value: Value = serde_json::from_str(text).context("inbound frame is not valid JSON")?;
    let kind = value
        .get("header")
        .and_then(|h| h.get("type"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("frame missing header.type"))?;
    match kind {
        "STATUS" => {
            let frame: WsStatusFrame =
                serde_json::from_value(value).context("invalid STATUS frame")?;
            Ok(WsFrame::Status(frame))
        }
        "QUOTE" => {
            let frame: WsQuoteFrame =
                serde_json::from_value(value).context("invalid QUOTE frame")?;
            Ok(WsFrame::Quote(frame))
        }
        "TRADE" => {
            let frame: WsTradeFrame =
                serde_json::from_value(value).context("invalid TRADE frame")?;
            Ok(WsFrame::Trade(frame))
        }
        "OHLC" => {
            // Session-cumulative OHLC summary auto-pushed by trade subscriptions. Not aligned
            // to bar intervals — included for completeness; consumers needing `Bar` data
            // should use the historical request path or aggregate from quotes/trades.
            let frame: WsOhlcFrame =
                serde_json::from_value(value).context("invalid OHLC frame")?;
            Ok(WsFrame::Ohlc(frame))
        }
        "STATE" => {
            // Session-state notification (e.g. `state: "START"` at market open).
            let state = value
                .get("header")
                .and_then(|h| h.get("state"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            let header: crate::types::WsHeader = serde_json::from_value(
                value
                    .get("header")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({"status":"","type":"STATE"})),
            )
            .context("invalid STATE header")?;
            Ok(WsFrame::State(WsStateFrame { header, state }))
        }
        "REQ_RESPONSE" => {
            // Subscribe/unsubscribe acknowledgement from the Terminal. Logged at debug so
            // operators can surface auth/subscription errors that the Terminal returns this
            // way, without spamming production logs.
            log::debug!("REQ_RESPONSE: {text}");
            Ok(WsFrame::Status(WsStatusFrame {
                header: crate::types::WsHeader {
                    status: "REQ_RESPONSE".to_string(),
                    kind: "STATUS".to_string(),
                },
            }))
        }
        other => anyhow::bail!("unknown frame type {other:?}: {}", truncate_for_log(text)),
    }
}

fn truncate_for_log(s: &str) -> String {
    const MAX: usize = 512;
    if s.len() <= MAX {
        s.to_owned()
    } else {
        format!("{}…", &s[..MAX])
    }
}

// -------------------------------------------------------------------------------------------------
// Outer client
// -------------------------------------------------------------------------------------------------

/// Outer WebSocket client — orchestrates subscriptions, replay, and event dispatch.
///
/// Construct with [`ThetaDataWsClient::new`], drive with [`Self::connect`], then issue
/// `subscribe_*` / `unsubscribe_*` calls. Inbound frames arrive on the receiver returned from
/// `new`.
#[derive(Debug)]
pub struct ThetaDataWsClient {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    config: WebSocketConfig,
    ws: OnceLock<WebSocketClient>,
    registry: Arc<Mutex<SubscriptionRegistry>>,
    events_tx: mpsc::UnboundedSender<WsFrame>,
    next_id: AtomicU64,
    state: Arc<AtomicU8>,
}

impl ThetaDataWsClient {
    /// Creates a new client (not yet connected) and returns the channel its frames will land on.
    ///
    /// Call [`Self::connect`] to actually open the connection.
    #[must_use]
    pub fn new(config: WebSocketConfig) -> (Self, mpsc::UnboundedReceiver<WsFrame>) {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(Inner {
            config,
            ws: OnceLock::new(),
            registry: Arc::new(Mutex::new(SubscriptionRegistry::new())),
            events_tx,
            next_id: AtomicU64::new(0),
            state: Arc::new(AtomicU8::new(ConnState::Disconnected as u8)),
        });
        (Self { inner }, events_rx)
    }

    /// Returns the current connection state.
    #[must_use]
    pub fn state(&self) -> ConnState {
        ConnState::from(self.inner.state.load(Ordering::Acquire))
    }

    /// Returns the number of active subscriptions.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.inner
            .registry
            .lock()
            .expect("registry mutex poisoned")
            .len()
    }

    /// Opens the WebSocket connection.
    ///
    /// Must be called before any `subscribe_*` calls; calling it twice is an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying connection fails.
    pub async fn connect(&self) -> Result<()> {
        if self.inner.ws.get().is_some() {
            anyhow::bail!("ThetaDataWsClient is already connected");
        }
        self.inner
            .state
            .store(ConnState::Connecting as u8, Ordering::Release);

        let events_tx = self.inner.events_tx.clone();
        let message_handler: Arc<dyn Fn(Message) + Send + Sync> = Arc::new(move |msg: Message| {
            let bytes = match msg {
                Message::Text(b) | Message::Binary(b) => b,
                Message::Ping(_) | Message::Pong(_) | Message::Close(_) => return,
            };
            let text = match std::str::from_utf8(&bytes) {
                Ok(s) => s,
                Err(e) => {
                    log::warn!("dropping non-utf8 frame: {e}");
                    return;
                }
            };
            match parse_ws_frame(text) {
                Ok(frame) => {
                    if events_tx.send(frame).is_err() {
                        log::warn!("ThetaDataWsClient event consumer dropped; closing");
                    }
                }
                Err(e) => log::warn!("failed to parse inbound ThetaData frame: {e}"),
            }
        });

        // The post_reconnect callback needs to reach back into the same OnceLock that holds
        // the WebSocketClient (so it can call `send_text`). We give the closure an Arc clone
        // of `inner` and read `inner.ws.get()` at callback time — `inner.ws` is set below
        // after `connect()` returns, and the WebSocketClient invokes post_reconnect only AFTER
        // a successful reconnect, by which point the OnceLock has been populated.
        let inner_for_reconnect = Arc::clone(&self.inner);
        let post_reconnect: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let payloads = {
                let guard = match inner_for_reconnect.registry.lock() {
                    Ok(g) => g,
                    Err(e) => {
                        log::error!("registry mutex poisoned during reconnect: {e}");
                        return;
                    }
                };
                guard.replay_payloads()
            };
            if payloads.is_empty() {
                return;
            }
            let inner = Arc::clone(&inner_for_reconnect);
            get_runtime().spawn(async move {
                let Some(ws) = inner.ws.get() else {
                    log::warn!("reconnect fired before WebSocketClient was stored");
                    return;
                };
                for payload in payloads {
                    if let Err(e) = ws.send_text(payload, None).await {
                        log::error!("failed to replay subscription: {e}");
                    }
                }
            });
        });

        let client = WebSocketClient::connect(
            self.inner.config.clone(),
            Some(message_handler),
            None,
            Some(post_reconnect),
            vec![],
            None,
        )
        .await
        .context("WebSocket connect failed")?;

        self.inner
            .ws
            .set(client)
            .map_err(|_| anyhow!("ThetaDataWsClient was already connected"))?;
        self.inner
            .state
            .store(ConnState::Connected as u8, Ordering::Release);
        Ok(())
    }

    /// Closes the WebSocket connection.
    ///
    /// Idempotent — calling on an already-closed client returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns an error if the close frame send fails.
    pub async fn close(&self) -> Result<()> {
        let Some(ws) = self.inner.ws.get() else {
            return Ok(());
        };
        self.inner
            .state
            .store(ConnState::Closing as u8, Ordering::Release);
        ws.send_close_message()
            .await
            .map_err(|e| anyhow!("close failed: {e}"))?;
        self.inner
            .state
            .store(ConnState::Disconnected as u8, Ordering::Release);
        Ok(())
    }

    /// Subscribes to quotes for the given option contract.
    ///
    /// Idempotent — re-subscribing returns immediately without re-sending.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is not connected or the send fails.
    pub async fn subscribe_quotes(&self, contract: &ThetaOptionContract) -> Result<()> {
        self.subscribe(StreamKind::Quote, contract).await
    }

    /// Subscribes to trades for the given option contract.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is not connected or the send fails.
    pub async fn subscribe_trades(&self, contract: &ThetaOptionContract) -> Result<()> {
        self.subscribe(StreamKind::Trade, contract).await
    }

    /// Unsubscribes from quotes for the given option contract.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is not connected or the send fails.
    pub async fn unsubscribe_quotes(&self, contract: &ThetaOptionContract) -> Result<()> {
        self.unsubscribe(StreamKind::Quote, contract).await
    }

    /// Unsubscribes from trades for the given option contract.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is not connected or the send fails.
    pub async fn unsubscribe_trades(&self, contract: &ThetaOptionContract) -> Result<()> {
        self.unsubscribe(StreamKind::Trade, contract).await
    }

    async fn subscribe(&self, kind: StreamKind, contract: &ThetaOptionContract) -> Result<()> {
        let key = SubscriptionKey::for_option(kind, contract);
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let inserted = self
            .inner
            .registry
            .lock()
            .expect("registry mutex poisoned")
            .add(key.clone(), id);
        if !inserted {
            return Ok(());
        }
        let payload = build_subscribe_payload(true, id, &key);
        self.send_text(payload).await
    }

    async fn unsubscribe(&self, kind: StreamKind, contract: &ThetaOptionContract) -> Result<()> {
        let key = SubscriptionKey::for_option(kind, contract);
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let existed = self
            .inner
            .registry
            .lock()
            .expect("registry mutex poisoned")
            .remove(&key);
        if !existed {
            return Ok(());
        }
        let payload = build_subscribe_payload(false, id, &key);
        self.send_text(payload).await
    }

    async fn send_text(&self, payload: String) -> Result<()> {
        let ws = self
            .inner
            .ws
            .get()
            .ok_or_else(|| anyhow!("ThetaDataWsClient is not connected"))?;
        log::debug!("ws-out: {payload}");
        ws.send_text(payload, None)
            .await
            .map_err(|e| anyhow!("send_text failed: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rstest::*;
    use serde_json::Value;

    use super::*;
    use crate::enums::OptionRight;

    fn sample_contract() -> ThetaOptionContract {
        ThetaOptionContract::from_dollar_strike(
            "SPXW",
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            480.0,
            OptionRight::Call,
        )
        .unwrap()
    }

    #[rstest]
    fn test_conn_state_from_byte() {
        assert_eq!(ConnState::from(0), ConnState::Disconnected);
        assert_eq!(ConnState::from(1), ConnState::Connecting);
        assert_eq!(ConnState::from(2), ConnState::Connected);
        assert_eq!(ConnState::from(3), ConnState::Closing);
        assert_eq!(ConnState::from(99), ConnState::Disconnected);
    }

    #[rstest]
    fn test_stream_kind_wire() {
        assert_eq!(StreamKind::Quote.as_wire(), "QUOTE");
        assert_eq!(StreamKind::Trade.as_wire(), "TRADE");
    }

    #[rstest]
    fn test_subscription_key_from_contract() {
        let key = SubscriptionKey::for_option(StreamKind::Quote, &sample_contract());
        assert_eq!(key.root, "SPXW");
        assert_eq!(key.expiration, 20_240_315);
        assert_eq!(key.strike, 480_000);
        assert_eq!(key.right, "C");
        assert_eq!(key.kind, StreamKind::Quote);
    }

    #[rstest]
    fn test_build_subscribe_payload_shape() {
        let key = SubscriptionKey::for_option(StreamKind::Quote, &sample_contract());
        let json = build_subscribe_payload(true, 7, &key);
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["msg_type"], "STREAM");
        assert_eq!(v["sec_type"], "OPTION");
        assert_eq!(v["req_type"], "QUOTE");
        assert_eq!(v["add"], true);
        assert_eq!(v["id"], 7);
        assert_eq!(v["contract"]["root"], "SPXW");
        assert_eq!(v["contract"]["expiration"], 20_240_315);
        assert_eq!(v["contract"]["strike"], 480_000);
        assert_eq!(v["contract"]["right"], "C");
    }

    #[rstest]
    fn test_build_unsubscribe_payload_shape() {
        let key = SubscriptionKey::for_option(StreamKind::Trade, &sample_contract());
        let json = build_subscribe_payload(false, 9, &key);
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["req_type"], "TRADE");
        assert_eq!(v["add"], false);
        assert_eq!(v["id"], 9);
    }

    #[rstest]
    fn test_registry_add_is_idempotent() {
        let mut reg = SubscriptionRegistry::new();
        let key = SubscriptionKey::for_option(StreamKind::Quote, &sample_contract());
        assert!(reg.add(key.clone(), 1));
        assert!(!reg.add(key.clone(), 2));
        assert_eq!(reg.len(), 1);
    }

    #[rstest]
    fn test_registry_remove_returns_false_when_missing() {
        let mut reg = SubscriptionRegistry::new();
        let key = SubscriptionKey::for_option(StreamKind::Quote, &sample_contract());
        assert!(!reg.remove(&key));
        reg.add(key.clone(), 0);
        assert!(reg.remove(&key));
        assert!(reg.is_empty());
    }

    #[rstest]
    fn test_registry_replay_payloads_preserves_order_and_ids() {
        let mut reg = SubscriptionRegistry::new();
        let key_q = SubscriptionKey::for_option(StreamKind::Quote, &sample_contract());
        let key_t = SubscriptionKey::for_option(StreamKind::Trade, &sample_contract());
        reg.add(key_q.clone(), 11);
        reg.add(key_t.clone(), 22);

        let payloads = reg.replay_payloads();
        assert_eq!(payloads.len(), 2);
        let first: Value = serde_json::from_str(&payloads[0]).unwrap();
        let second: Value = serde_json::from_str(&payloads[1]).unwrap();
        assert_eq!(first["req_type"], "QUOTE");
        assert_eq!(first["id"], 11);
        assert_eq!(first["add"], true);
        assert_eq!(second["req_type"], "TRADE");
        assert_eq!(second["id"], 22);
        assert_eq!(second["add"], true);
    }

    #[rstest]
    fn test_parse_ws_frame_status() {
        let text = r#"{"header":{"status":"CONNECTED","type":"STATUS"}}"#;
        match parse_ws_frame(text).unwrap() {
            WsFrame::Status(s) => assert_eq!(s.header.status, "CONNECTED"),
            other => panic!("expected status, got {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_ws_frame_quote() {
        let text = r#"{
            "header":{"status":"CONNECTED","type":"QUOTE"},
            "contract":{"security_type":"OPTION","root":"SPXW","expiration":20240315,"strike":480000,"right":"C"},
            "quote":{"ms_of_day":26622025,"bid_size":7,"bid_exchange":5,"bid":110.2,"bid_condition":50,"ask_size":7,"ask_exchange":5,"ask":110.5,"ask_condition":50,"date":20231219}
        }"#;
        match parse_ws_frame(text).unwrap() {
            WsFrame::Quote(q) => {
                assert_eq!(q.contract.root, "SPXW");
                assert_eq!(q.quote.bid, 110.2);
                assert_eq!(q.quote.ms_of_day, 26_622_025);
            }
            other => panic!("expected quote, got {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_ws_frame_trade() {
        let text = r#"{
            "header":{"status":"CONNECTED","type":"TRADE"},
            "contract":{"security_type":"OPTION","root":"AAPL","expiration":20231222,"strike":200000,"right":"C"},
            "trade":{"ms_of_day":34389945,"sequence":772942264,"size":10,"condition":18,"price":0.31,"exchange":31,"date":20231219}
        }"#;
        match parse_ws_frame(text).unwrap() {
            WsFrame::Trade(t) => {
                assert_eq!(t.trade.sequence, 772_942_264);
                assert_eq!(t.trade.condition, 18);
            }
            other => panic!("expected trade, got {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_ws_frame_ohlc() {
        let text = r#"{
            "header":{"type":"OHLC","status":"CONNECTED"},
            "contract":{"security_type":"OPTION","root":"SPXW","expiration":20260520,"strike":7400000,"right":"C"},
            "ohlc":{"ms_of_day":73971008,"open":28.0,"high":56.1,"low":15.7,"close":40.8,"volume":2763,"count":783,"date":20260518}
        }"#;
        match parse_ws_frame(text).unwrap() {
            WsFrame::Ohlc(o) => {
                assert_eq!(o.contract.root, "SPXW");
                assert_eq!(o.ohlc.close, 40.8);
                assert_eq!(o.ohlc.count, 783);
                assert_eq!(o.ohlc.volume, 2763);
            }
            other => panic!("expected ohlc, got {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_ws_frame_state() {
        let text = r#"{"header":{"type":"STATE","status":"CONNECTED","state":"START"}}"#;
        match parse_ws_frame(text).unwrap() {
            WsFrame::State(s) => assert_eq!(s.state.as_deref(), Some("START")),
            other => panic!("expected state, got {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_ws_frame_state_missing_state_field() {
        let text = r#"{"header":{"type":"STATE","status":"CONNECTED"}}"#;
        match parse_ws_frame(text).unwrap() {
            WsFrame::State(s) => assert!(s.state.is_none()),
            other => panic!("expected state, got {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_ws_frame_rejects_unknown_type() {
        let text = r#"{"header":{"status":"X","type":"GREEK"}}"#;
        let err = parse_ws_frame(text).unwrap_err();
        assert!(err.to_string().contains("GREEK"));
    }

    #[rstest]
    fn test_parse_ws_frame_rejects_missing_type() {
        let text = r#"{"header":{"status":"X"}}"#;
        let err = parse_ws_frame(text).unwrap_err();
        assert!(err.to_string().contains("header.type"));
    }

    #[rstest]
    fn test_parse_ws_frame_rejects_garbage() {
        assert!(parse_ws_frame("not json").is_err());
    }

    #[rstest]
    fn test_client_constructs_with_unconnected_state() {
        let cfg = WebSocketConfig::builder()
            .url("ws://127.0.0.1:25520/v1/events".to_string())
            .build();
        let (client, _rx) = ThetaDataWsClient::new(cfg);
        assert_eq!(client.state(), ConnState::Disconnected);
        assert_eq!(client.subscription_count(), 0);
    }
}
