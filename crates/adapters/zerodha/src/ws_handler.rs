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

//! Inner stateless WebSocket I/O loop for the Kite ticker.
//!
//! Owns the `tokio_tungstenite` socket and is restarted by [`crate::live`] on every reconnect.
//! Splits inbound traffic into two streams:
//!
//! - **Binary** frames flow to [`WsEvent::Binary`] — the Phase 3 decoder lives downstream and
//!   length-switches on the packet variants (8/28/32/44/184 bytes).
//! - **Text JSON** frames flow to [`WsEvent::Json`] — these are Kite postback / error envelopes,
//!   not market data; we surface them as-is for the outer layer to route.
//!
//! Outbound traffic is driven by [`WsCommand`] over an `mpsc` channel from
//! [`crate::live::ZerodhaWsClient`].
//!
//! Reconnect policy mirrors the spec:
//!
//! - URL rebuilt from [`ZerodhaSessionManager`] on every (re)connect.
//! - Backoff 1 s → 60 s ×1.5 between attempts.
//! - HTTP 401/403 / `error_type=TokenException` → trigger
//!   [`ZerodhaSessionManager::rotate`], await [`ZerodhaSessionManager::rotation_notify`], then
//!   reconnect with the new token in the query string.

use std::{
    sync::{
        Arc, RwLock,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use ahash::HashSet;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::{
    sync::mpsc,
    time::{Instant, sleep},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, http::StatusCode, protocol::frame::coding::CloseCode},
};

use crate::{
    common::{WS_BACKOFF_INITIAL, WS_BACKOFF_MAX, WS_BACKOFF_MULTIPLIER, WS_BASE},
    error::ZerodhaError,
    session::ZerodhaSessionManager,
};

/// Connection state, exposed via [`Arc<AtomicU8>`] so the outer layer can read it without locks.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectionState {
    /// No socket open.
    Disconnected = 0,
    /// Establishing the socket / TLS handshake.
    Connecting = 1,
    /// Socket up and ready for subscribe / mode commands.
    Connected = 2,
    /// Shutdown requested; handler will exit after draining commands.
    Closing = 3,
    /// Authentication rotation exhausted retries; handler has exited.
    AuthDead = 4,
}

impl ConnectionState {
    /// Decode a raw u8 (e.g. read from the shared [`AtomicU8`]).
    #[must_use]
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Connecting,
            2 => Self::Connected,
            3 => Self::Closing,
            4 => Self::AuthDead,
            _ => Self::Disconnected,
        }
    }
}

/// Outbound commands accepted by the handler.
#[derive(Clone, Debug)]
pub enum WsCommand {
    /// Subscribe one or more instrument tokens (Kite payload `{"a":"subscribe","v":[…]}`).
    Subscribe(Vec<u32>),
    /// Unsubscribe one or more instrument tokens.
    Unsubscribe(Vec<u32>),
    /// Switch the streaming mode for a subset of tokens (`"ltp" | "quote" | "full"`).
    SetMode { mode: String, tokens: Vec<u32> },
    /// Drain and shut down; handler exits the run loop after sending the final close frame.
    Close,
}

/// Inbound events emitted by the handler.
#[derive(Clone, Debug)]
pub enum WsEvent {
    /// Connection established (replays subscriptions automatically before firing).
    Connected,
    /// Connection dropped (may reconnect; check the state atomic to disambiguate).
    Disconnected,
    /// Binary frame (Kite ticker tick).
    Binary(Vec<u8>),
    /// Text JSON frame (Kite error / postback envelope).
    Json(serde_json::Value),
    /// Session token rotated successfully; subscriptions will be re-established.
    Rotated,
    /// `ZerodhaSessionManager` exhausted rotation retries; the handler is exiting.
    AuthDead,
}

/// Spawn the handler loop.
///
/// The loop owns the WebSocket; the caller owns `command_tx` (to send commands) and `event_rx`
/// (to receive ticks). When `Close` is received or the command channel drops, the loop exits.
///
/// # Panics
///
/// Panics if the subscription `RwLock` is poisoned by a prior crash — at that point the
/// handler can't safely replay subscriptions and the caller must reconstruct the client.
pub async fn run_handler(
    session: Arc<ZerodhaSessionManager>,
    base_url: String,
    state: Arc<AtomicU8>,
    subscriptions: Arc<RwLock<HashSet<u32>>>,
    mut command_rx: mpsc::Receiver<WsCommand>,
    event_tx: mpsc::Sender<WsEvent>,
    dropped_events: Arc<std::sync::atomic::AtomicU64>,
) {
    let mut backoff = WS_BACKOFF_INITIAL;

    loop {
        if session.is_auth_dead() {
            state.store(ConnectionState::AuthDead as u8, Ordering::Release);
            send_or_drop(&event_tx, WsEvent::AuthDead, &dropped_events).await;
            break;
        }

        state.store(ConnectionState::Connecting as u8, Ordering::Release);
        let url = build_ws_url(&base_url, &session);

        let connect_result = connect_async(url.as_str()).await;
        let (mut ws, response) = match connect_result {
            Ok(pair) => pair,
            Err(e) => {
                if is_auth_failure(&e) {
                    log::warn!("Kite WS handshake rejected as auth failure: {e}");
                    if !rotate_token(&session, &state, &event_tx, &dropped_events).await {
                        break;
                    }
                    backoff = WS_BACKOFF_INITIAL;
                    continue;
                }
                log::warn!("Kite WS connect failed, backing off {backoff:?}: {e}");
                state.store(ConnectionState::Disconnected as u8, Ordering::Release);
                send_or_drop(&event_tx, WsEvent::Disconnected, &dropped_events).await;
                if !sleep_or_close(backoff, &mut command_rx).await {
                    break;
                }
                backoff = next_backoff(backoff);
                continue;
            }
        };

        // The TLS upgrade may carry an auth-failure status even though `connect_async` returned
        // Ok — check the handshake response.
        if response.status() == StatusCode::UNAUTHORIZED
            || response.status() == StatusCode::FORBIDDEN
        {
            log::warn!(
                "Kite WS handshake returned auth status {}, rotating",
                response.status()
            );
            if !rotate_token(&session, &state, &event_tx, &dropped_events).await {
                break;
            }
            backoff = WS_BACKOFF_INITIAL;
            continue;
        }

        state.store(ConnectionState::Connected as u8, Ordering::Release);
        send_or_drop(&event_tx, WsEvent::Connected, &dropped_events).await;

        // Replay subscriptions so a reconnect doesn't silently lose ticks. The read guard is
        // explicitly bound in a sub-scope so it drops before any `.await` below — std's
        // `RwLockReadGuard` is `!Send`.
        let resubscribe: Vec<u32> = {
            let guard = subscriptions.read().expect("subscriptions lock poisoned");
            guard.iter().copied().collect()
        };
        if !resubscribe.is_empty() {
            let frame = json!({"a": "subscribe", "v": resubscribe}).to_string();
            if let Err(e) = ws.send(Message::Text(frame.into())).await {
                log::warn!("Kite WS resubscribe send failed: {e}");
            }
        }

        backoff = WS_BACKOFF_INITIAL;
        let disposition = run_session(
            &mut ws,
            &session,
            &subscriptions,
            &mut command_rx,
            &event_tx,
            &dropped_events,
        )
        .await;

        state.store(ConnectionState::Disconnected as u8, Ordering::Release);
        send_or_drop(&event_tx, WsEvent::Disconnected, &dropped_events).await;

        // Best-effort graceful close.
        let _ = ws.close(None).await;

        match disposition {
            SessionDisposition::Close => break,
            SessionDisposition::Rotate => {
                if !rotate_token(&session, &state, &event_tx, &dropped_events).await {
                    break;
                }
                backoff = WS_BACKOFF_INITIAL;
            }
            SessionDisposition::Reconnect => {
                if !sleep_or_close(backoff, &mut command_rx).await {
                    break;
                }
                backoff = next_backoff(backoff);
            }
        }
    }
}

#[derive(Debug)]
enum SessionDisposition {
    Close,
    Rotate,
    Reconnect,
}

async fn run_session(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    session: &Arc<ZerodhaSessionManager>,
    subscriptions: &Arc<RwLock<HashSet<u32>>>,
    command_rx: &mut mpsc::Receiver<WsCommand>,
    event_tx: &mpsc::Sender<WsEvent>,
    dropped_events: &Arc<std::sync::atomic::AtomicU64>,
) -> SessionDisposition {
    // Kite's ticker keeps the connection alive with periodic 1-byte heartbeats from the server;
    // we drive a 30 s ping in our direction as belt-and-suspenders against silent NAT timeouts.
    let mut ping_deadline = Instant::now() + Duration::from_secs(30);

    loop {
        tokio::select! {
            biased;

            cmd = command_rx.recv() => match cmd {
                None | Some(WsCommand::Close) => return SessionDisposition::Close,
                Some(WsCommand::Subscribe(tokens)) => {
                    if !tokens.is_empty() {
                        {
                            let mut guard = subscriptions.write().expect("subs lock");
                            guard.extend(tokens.iter().copied());
                        }
                        let frame = json!({"a": "subscribe", "v": tokens}).to_string();
                        if let Err(e) = ws.send(Message::Text(frame.into())).await {
                            log::warn!("Kite WS subscribe send failed: {e}");
                            return SessionDisposition::Reconnect;
                        }
                    }
                }
                Some(WsCommand::Unsubscribe(tokens)) => {
                    if !tokens.is_empty() {
                        {
                            let mut guard = subscriptions.write().expect("subs lock");
                            for t in &tokens { guard.remove(t); }
                        }
                        let frame = json!({"a": "unsubscribe", "v": tokens}).to_string();
                        if let Err(e) = ws.send(Message::Text(frame.into())).await {
                            log::warn!("Kite WS unsubscribe send failed: {e}");
                            return SessionDisposition::Reconnect;
                        }
                    }
                }
                Some(WsCommand::SetMode { mode, tokens }) => {
                    if !tokens.is_empty() {
                        let frame = json!({"a": "mode", "v": [mode, tokens]}).to_string();
                        if let Err(e) = ws.send(Message::Text(frame.into())).await {
                            log::warn!("Kite WS mode send failed: {e}");
                            return SessionDisposition::Reconnect;
                        }
                    }
                }
            },

            () = session.rotation_notify.notified() => {
                // Token rotation completed elsewhere (HTTP path detected token death first).
                // Drop the socket and reconnect with the new URL.
                send_or_drop(event_tx, WsEvent::Rotated, dropped_events).await;
                return SessionDisposition::Reconnect;
            }

            () = tokio::time::sleep_until(ping_deadline) => {
                if let Err(e) = ws.send(Message::Ping(Vec::new().into())).await {
                    log::warn!("Kite WS ping send failed: {e}");
                    return SessionDisposition::Reconnect;
                }
                ping_deadline = Instant::now() + Duration::from_secs(30);
            }

            frame = ws.next() => match frame {
                Some(Ok(Message::Binary(bytes))) => {
                    send_or_drop(event_tx, WsEvent::Binary(bytes.to_vec()), dropped_events).await;
                }
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<serde_json::Value>(&text) {
                        Ok(value) => {
                            if is_token_exception(&value) {
                                log::warn!("Kite WS text frame indicates token death: {text}");
                                return SessionDisposition::Rotate;
                            }
                            send_or_drop(event_tx, WsEvent::Json(value), dropped_events).await;
                        }
                        Err(e) => {
                            log::warn!("Kite WS text frame not JSON: {e} body={text}");
                        }
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    if let Err(e) = ws.send(Message::Pong(payload)).await {
                        log::warn!("Kite WS pong send failed: {e}");
                        return SessionDisposition::Reconnect;
                    }
                }
                Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                Some(Ok(Message::Close(frame))) => {
                    let auth_close = frame.as_ref().is_some_and(|f| {
                        let code = u16::from(f.code);
                        code == u16::from(CloseCode::Policy) || code == 4401 || code == 4403
                    });
                    if auth_close {
                        log::warn!("Kite WS server closed with auth code: {frame:?}");
                        return SessionDisposition::Rotate;
                    }
                    log::info!("Kite WS server closed: {frame:?}");
                    return SessionDisposition::Reconnect;
                }
                Some(Err(e)) => {
                    log::warn!("Kite WS recv error: {e}");
                    return SessionDisposition::Reconnect;
                }
                None => {
                    log::info!("Kite WS stream ended");
                    return SessionDisposition::Reconnect;
                }
            }
        }
    }
}

async fn rotate_token(
    session: &Arc<ZerodhaSessionManager>,
    state: &Arc<AtomicU8>,
    event_tx: &mpsc::Sender<WsEvent>,
    dropped_events: &Arc<std::sync::atomic::AtomicU64>,
) -> bool {
    match session.rotate().await {
        Ok(_) => {
            send_or_drop(event_tx, WsEvent::Rotated, dropped_events).await;
            true
        }
        Err(e) => {
            log::error!("Kite session rotation failed, declaring auth_dead: {e}");
            state.store(ConnectionState::AuthDead as u8, Ordering::Release);
            send_or_drop(event_tx, WsEvent::AuthDead, dropped_events).await;
            false
        }
    }
}

async fn send_or_drop(
    event_tx: &mpsc::Sender<WsEvent>,
    event: WsEvent,
    dropped_events: &Arc<std::sync::atomic::AtomicU64>,
) {
    if let Err(mpsc::error::TrySendError::Full(_dropped)) = event_tx.try_send(event.clone()) {
        // Drop-oldest by counting and continuing — the receiver is back-pressured so further
        // sends would deadlock the I/O loop.
        dropped_events.fetch_add(1, Ordering::Relaxed);
    } else if event_tx.try_send(event).is_err() {
        // Receiver gone; nothing we can do.
    }
}

async fn sleep_or_close(backoff: Duration, command_rx: &mut mpsc::Receiver<WsCommand>) -> bool {
    tokio::select! {
        () = sleep(backoff) => true,
        cmd = command_rx.recv() => matches!(cmd, Some(c) if !matches!(c, WsCommand::Close)),
    }
}

fn next_backoff(current: Duration) -> Duration {
    current.mul_f64(WS_BACKOFF_MULTIPLIER).min(WS_BACKOFF_MAX)
}

/// Assemble the Kite ticker URL with `api_key` + `access_token` query params.
///
/// # Panics
///
/// Panics if [`WS_BASE`] is not a valid URL — a compile-time invariant of the workspace
/// constant, never reached at runtime.
#[must_use]
pub fn build_ws_url(base_url: &str, session: &ZerodhaSessionManager) -> url::Url {
    let mut url = url::Url::parse(base_url)
        .unwrap_or_else(|_| url::Url::parse(WS_BASE).expect("WS_BASE is a valid URL"));
    let api_key = session.api_key();
    let token = session.access_token();
    {
        let mut q = url.query_pairs_mut();
        q.clear();
        q.append_pair("api_key", &api_key);
        q.append_pair("access_token", &token);
    }
    url
}

fn is_auth_failure(err: &tokio_tungstenite::tungstenite::Error) -> bool {
    use tokio_tungstenite::tungstenite::Error as E;
    if let E::Http(response) = err {
        return response.status() == StatusCode::UNAUTHORIZED
            || response.status() == StatusCode::FORBIDDEN;
    }
    false
}

fn is_token_exception(value: &serde_json::Value) -> bool {
    value.get("type").and_then(|v| v.as_str()) == Some("error")
        && value
            .get("data")
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.to_lowercase().contains("token"))
}

// Silence the unused-import lint when `ZerodhaError` only appears in the docs.
#[allow(dead_code)]
fn _doc_anchor(_: ZerodhaError) {}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn next_backoff_grows_until_max() {
        let mut b = WS_BACKOFF_INITIAL;
        for _ in 0..20 {
            b = next_backoff(b);
        }
        assert_eq!(b, WS_BACKOFF_MAX);
    }

    #[rstest]
    fn build_ws_url_carries_credentials() {
        let session = ZerodhaSessionManager::new("apik".into(), "tokN".into(), None);
        let url = build_ws_url(WS_BASE, &session);
        let qs: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(qs.get("api_key").map(String::as_str), Some("apik"));
        assert_eq!(qs.get("access_token").map(String::as_str), Some("tokN"));
    }

    #[rstest]
    fn token_exception_detection() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"type":"error","data":"invalid access token"}"#).unwrap();
        assert!(is_token_exception(&v));

        let v: serde_json::Value =
            serde_json::from_str(r#"{"type":"order","data":"OPEN"}"#).unwrap();
        assert!(!is_token_exception(&v));
    }

    #[rstest]
    fn connection_state_round_trip() {
        for raw in 0..=4 {
            let state = ConnectionState::from_u8(raw);
            assert_eq!(state as u8, raw);
        }
    }
}
