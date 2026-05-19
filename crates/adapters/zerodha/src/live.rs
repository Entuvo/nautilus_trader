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

//! Outer (Python-facing) Kite ticker WebSocket client.
//!
//! Holds the cheap-to-clone handles into the [`ws_handler`](crate::ws_handler) task: the command
//! channel, the connection-state atomic, and the subscription set used to replay subscriptions
//! after a reconnect.
//!
//! Channels are bounded with a drop-oldest policy: when the consumer falls behind during an open
//! auction or expiry-day burst, the count of dropped events is exposed via
//! [`ZerodhaWsClient::dropped_events`] rather than silently OOMing the process.

use std::{
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, AtomicU8, Ordering},
    },
};

use ahash::HashSet;
use anyhow::Result;
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};

use crate::{
    common::{WS_BASE, WS_EVENT_CHANNEL_CAPACITY, WS_MAX_SUBSCRIPTIONS},
    error::ZerodhaError,
    session::ZerodhaSessionManager,
    ws_handler::{ConnectionState, WsCommand, WsEvent, run_handler},
};

/// Outer ticker client.
pub struct ZerodhaWsClient {
    session: Arc<ZerodhaSessionManager>,
    state: Arc<AtomicU8>,
    subscriptions: Arc<RwLock<HashSet<u32>>>,
    command_tx: mpsc::Sender<WsCommand>,
    event_rx: Mutex<mpsc::Receiver<WsEvent>>,
    dropped_events: Arc<AtomicU64>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for ZerodhaWsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZerodhaWsClient")
            .field("state", &self.state())
            .field("subscription_count", &self.subscription_count())
            .field("dropped_events", &self.dropped_events())
            .finish()
    }
}

impl ZerodhaWsClient {
    /// Spawn the inner handler task and return a handle.
    ///
    /// `base_url` defaults to [`WS_BASE`] when `None` — tests pass a mock server's URL here.
    #[must_use]
    pub fn spawn(session: Arc<ZerodhaSessionManager>, base_url: Option<String>) -> Self {
        let base_url = base_url.unwrap_or_else(|| WS_BASE.to_string());
        let state = Arc::new(AtomicU8::new(ConnectionState::Disconnected as u8));
        let subscriptions: Arc<RwLock<HashSet<u32>>> = Arc::new(RwLock::new(HashSet::default()));
        let (command_tx, command_rx) = mpsc::channel::<WsCommand>(64);
        let (event_tx, event_rx) = mpsc::channel::<WsEvent>(WS_EVENT_CHANNEL_CAPACITY);
        let dropped_events = Arc::new(AtomicU64::new(0));

        let handle = tokio::spawn(run_handler(
            session.clone(),
            base_url,
            state.clone(),
            subscriptions.clone(),
            command_rx,
            event_tx,
            dropped_events.clone(),
        ));

        Self {
            session,
            state,
            subscriptions,
            command_tx,
            event_rx: Mutex::new(event_rx),
            dropped_events,
            handle: Mutex::new(Some(handle)),
        }
    }

    /// Current connection state read straight from the shared atomic.
    #[must_use]
    pub fn state(&self) -> ConnectionState {
        ConnectionState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Whether the client believes the socket is currently open.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.state() == ConnectionState::Connected
    }

    /// Number of instrument tokens currently subscribed.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.read().map_or(0, |s| s.len())
    }

    /// Snapshot of subscribed instrument tokens.
    #[must_use]
    pub fn subscriptions(&self) -> Vec<u32> {
        self.subscriptions
            .read()
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Count of dropped events (consumer back-pressure metric).
    #[must_use]
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Acquire)
    }

    /// Shared session manager (so the matching HTTP client can hold the same `Arc`).
    #[must_use]
    pub fn session(&self) -> Arc<ZerodhaSessionManager> {
        self.session.clone()
    }

    /// Subscribe one or more instrument tokens.
    ///
    /// # Errors
    ///
    /// Returns [`ZerodhaError::SubscriptionLimitExceeded`] if the operation would push the total
    /// subscription count past Kite's per-connection cap of
    /// [`WS_MAX_SUBSCRIPTIONS`](crate::common::WS_MAX_SUBSCRIPTIONS).
    ///
    /// # Panics
    ///
    /// Panics if the internal subscription lock has been poisoned by a prior crash inside the
    /// handler task — at that point the client is in an undefined state and recovery requires a
    /// fresh `spawn()`.
    pub async fn subscribe(&self, tokens: Vec<u32>) -> Result<()> {
        let projected = {
            let guard = self.subscriptions.read().expect("subs lock");
            tokens.iter().filter(|t| !guard.contains(t)).count() + guard.len()
        };
        if projected > WS_MAX_SUBSCRIPTIONS {
            return Err(ZerodhaError::SubscriptionLimitExceeded {
                requested: projected,
                cap: WS_MAX_SUBSCRIPTIONS,
            }
            .into());
        }
        self.send_command(WsCommand::Subscribe(tokens)).await
    }

    /// Unsubscribe one or more instrument tokens.
    ///
    /// # Errors
    ///
    /// Returns an error if the handler task has exited and no longer accepts commands.
    pub async fn unsubscribe(&self, tokens: Vec<u32>) -> Result<()> {
        self.send_command(WsCommand::Unsubscribe(tokens)).await
    }

    /// Change the streaming mode for a subset of tokens (`"ltp" | "quote" | "full"`).
    ///
    /// # Errors
    ///
    /// Returns an error if the handler task has exited and no longer accepts commands.
    pub async fn set_mode(&self, mode: &str, tokens: Vec<u32>) -> Result<()> {
        self.send_command(WsCommand::SetMode {
            mode: mode.to_string(),
            tokens,
        })
        .await
    }

    /// Pull the next event from the inbound channel. Returns `None` after the handler exits.
    pub async fn next_event(&self) -> Option<WsEvent> {
        let mut guard = self.event_rx.lock().await;
        guard.recv().await
    }

    /// Try to pull an event without awaiting; returns `None` if no event is available right now.
    pub async fn try_next_event(&self) -> Option<WsEvent> {
        let mut guard = self.event_rx.lock().await;
        guard.try_recv().ok()
    }

    /// Send `Close` and await the handler task. The client is unusable after this returns.
    pub async fn close(&self) {
        let _ = self.command_tx.send(WsCommand::Close).await;
        let handle = {
            let mut guard = self.handle.lock().await;
            guard.take()
        };
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }

    async fn send_command(&self, command: WsCommand) -> Result<()> {
        self.command_tx
            .send(command)
            .await
            .map_err(|e| anyhow::anyhow!("ws handler stopped: {e}"))
    }
}

impl Drop for ZerodhaWsClient {
    fn drop(&mut self) {
        // Best-effort shutdown signal; doesn't await the handler — that's `close()`'s job.
        let _ = self.command_tx.try_send(WsCommand::Close);
        if let Ok(mut guard) = self.handle.try_lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_above_cap_errors() {
        let session = Arc::new(ZerodhaSessionManager::new("k".into(), "t".into(), None));
        // Point at a dummy URL so the connect attempt fails fast — we only care about the
        // command-side cap enforcement.
        let client = ZerodhaWsClient::spawn(session, Some("ws://127.0.0.1:1".to_string()));

        let too_many: Vec<u32> = (0..=(WS_MAX_SUBSCRIPTIONS as u32)).collect();
        let err = client.subscribe(too_many).await.expect_err("over cap");
        let inner = err.downcast::<ZerodhaError>().unwrap();
        assert!(matches!(inner, ZerodhaError::SubscriptionLimitExceeded { .. }));

        client.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_drains_handler() {
        let session = Arc::new(ZerodhaSessionManager::new("k".into(), "t".into(), None));
        let client = ZerodhaWsClient::spawn(session, Some("ws://127.0.0.1:1".to_string()));

        // Give the handler one cycle to fail-connect and emit Disconnected.
        tokio::time::sleep(Duration::from_millis(50)).await;

        client.close().await;
        assert_eq!(client.state(), ConnectionState::Disconnected);
    }
}
