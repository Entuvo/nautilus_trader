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

//! Process-singleton access-token holder shared across the data + execution clients.
//!
//! Both `ZerodhaDataClient` and `ZerodhaExecutionClient` read the `access_token` on every request
//! and both can independently detect token death (HTTP 401/403 or `error_type=TokenException`).
//! Without a shared holder, a token rotation by one client leaves the other using a dead token
//! until its next failure — easy to lose orders in the gap.
//!
//! Design highlights (spec §3.1.1):
//! - [`arc_swap::ArcSwap`] for the token so reads are wait-free and the swap is atomic.
//! - CAS-style guard via `last_rotation_ns`: concurrent rotations inside a 5 s window await the
//!   in-flight rotation instead of stampeding the provider.
//! - Provider invocation is dispatched via [`tokio::task::spawn_blocking`] so a slow Python
//!   callback (`input()`, login HTTP round-trip) doesn't park the executor.
//! - [`tokio::sync::Notify`] barrier wakes WS clients to drain pending sends and reconnect with
//!   the new URL after a successful swap.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicU8, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use arc_swap::ArcSwap;
use tokio::sync::{Mutex, Notify};

use crate::{
    common::{SESSION_ROTATION_RETRIES, SESSION_ROTATION_WINDOW},
    error::ZerodhaError,
};

/// Provider closure invoked to fetch a fresh `access_token`.
///
/// Runs inside [`tokio::task::spawn_blocking`], so it may block (HTTP, file IO, Python GIL
/// acquisition) without parking the runtime.
pub type TokenProvider = Arc<dyn Fn() -> Result<String> + Send + Sync + 'static>;

/// Session-manager state.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
enum SessionState {
    /// Holding a valid token (or believed-valid; only Kite can confirm).
    Healthy = 0,
    /// A rotation is in flight; concurrent callers should await `rotation_notify` rather than
    /// re-invoking the provider.
    Rotating = 1,
    /// Rotation exhausted retries; the session is dead until the host restarts with fresh creds.
    AuthDead = 2,
}

impl SessionState {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Rotating,
            2 => Self::AuthDead,
            _ => Self::Healthy,
        }
    }
}

/// Shared session manager — both clients hold an `Arc<ZerodhaSessionManager>`.
pub struct ZerodhaSessionManager {
    api_key: Arc<String>,
    access_token: ArcSwap<String>,
    provider: Mutex<Option<TokenProvider>>,
    last_rotation_ns: AtomicU64,
    state: AtomicU8,
    /// Notified after a successful rotation so the WS handler can wake, drain pending sends, and
    /// reconnect with the new token-in-URL.
    pub rotation_notify: Notify,
}

impl std::fmt::Debug for ZerodhaSessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZerodhaSessionManager")
            .field("api_key", &"****")
            .field("access_token_present", &!self.access_token.load().is_empty())
            .field("state", &self.current_state())
            .finish()
    }
}

impl ZerodhaSessionManager {
    /// Build a session manager from the resolved credentials. The provider is optional; without
    /// one, `rotate()` returns [`ZerodhaError::AuthDead`] immediately.
    #[must_use]
    pub fn new(api_key: String, access_token: String, provider: Option<TokenProvider>) -> Self {
        Self {
            api_key: Arc::new(api_key),
            access_token: ArcSwap::new(Arc::new(access_token)),
            provider: Mutex::new(provider),
            last_rotation_ns: AtomicU64::new(0),
            state: AtomicU8::new(SessionState::Healthy as u8),
            rotation_notify: Notify::new(),
        }
    }

    /// Hand-out the API key (cheap, no atomics).
    #[must_use]
    pub fn api_key(&self) -> Arc<String> {
        self.api_key.clone()
    }

    /// Snapshot the current access token. The returned `Arc` is independent of any subsequent
    /// rotation, so it is safe to use across an await point.
    #[must_use]
    pub fn access_token(&self) -> Arc<String> {
        self.access_token.load_full()
    }

    /// Atomically install a new access token (e.g. the caller just ran the login helper).
    pub fn install_token(&self, token: String) {
        self.access_token.store(Arc::new(token));
        self.state
            .store(SessionState::Healthy as u8, Ordering::Release);
        self.rotation_notify.notify_waiters();
    }

    /// Swap in a new provider (e.g. when the strategy attaches one after startup).
    pub async fn set_provider(&self, provider: Option<TokenProvider>) {
        let mut guard = self.provider.lock().await;
        *guard = provider;
    }

    /// Current state — `Healthy`, `Rotating`, or `AuthDead`.
    #[must_use]
    pub fn current_state(&self) -> &'static str {
        match SessionState::from_u8(self.state.load(Ordering::Acquire)) {
            SessionState::Healthy => "healthy",
            SessionState::Rotating => "rotating",
            SessionState::AuthDead => "auth_dead",
        }
    }

    /// Whether the session has been declared dead and should not be reused.
    #[must_use]
    pub fn is_auth_dead(&self) -> bool {
        SessionState::from_u8(self.state.load(Ordering::Acquire)) == SessionState::AuthDead
    }

    /// Rotate the access token.
    ///
    /// Behaviour:
    /// - If a rotation completed within [`SESSION_ROTATION_WINDOW`], returns immediately with the
    ///   current token (assumes the prior rotation already replaced the dead one).
    /// - If a rotation is in flight, awaits [`Self::rotation_notify`] and returns the resulting
    ///   token.
    /// - Otherwise CAS-promotes the state to `Rotating`, invokes the provider with bounded retry
    ///   (1 s → 60 s × 1.5, up to [`SESSION_ROTATION_RETRIES`] attempts), installs the new token,
    ///   and notifies waiters.
    /// - On terminal failure marks the session `AuthDead` and returns
    ///   [`ZerodhaError::AuthDead`].
    ///
    /// # Errors
    ///
    /// - [`ZerodhaError::AuthDead`] if no provider is configured or retries are exhausted.
    /// - Any error propagated from the provider closure if every attempt failed.
    pub async fn rotate(&self) -> Result<Arc<String>> {
        // Fast path: a recent rotation already succeeded; nothing to do.
        if self.recently_rotated() {
            return Ok(self.access_token());
        }
        if self.is_auth_dead() {
            return Err(ZerodhaError::AuthDead.into());
        }

        // CAS-promote Healthy → Rotating. Concurrent callers that lose the CAS wait on the
        // rotation_notify barrier instead of triggering a second provider invocation.
        let won_cas = self
            .state
            .compare_exchange(
                SessionState::Healthy as u8,
                SessionState::Rotating as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();

        if !won_cas {
            self.rotation_notify.notified().await;
            if self.is_auth_dead() {
                return Err(ZerodhaError::AuthDead.into());
            }
            return Ok(self.access_token());
        }

        let result = self.run_provider_with_retry().await;

        match &result {
            Ok(token) => {
                self.access_token.store(Arc::new(token.clone()));
                self.last_rotation_ns
                    .store(now_ns(), Ordering::Release);
                self.state
                    .store(SessionState::Healthy as u8, Ordering::Release);
            }
            Err(_) => {
                self.state
                    .store(SessionState::AuthDead as u8, Ordering::Release);
            }
        }
        self.rotation_notify.notify_waiters();

        result.map(Arc::new)
    }

    async fn run_provider_with_retry(&self) -> Result<String> {
        let provider = {
            let guard = self.provider.lock().await;
            match guard.as_ref() {
                Some(p) => p.clone(),
                None => return Err(ZerodhaError::AuthDead.into()),
            }
        };

        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);
        let mut last_err: Option<anyhow::Error> = None;

        for attempt in 1..=SESSION_ROTATION_RETRIES {
            let provider = provider.clone();
            let result = tokio::task::spawn_blocking(move || provider())
                .await
                .map_err(|e| anyhow::anyhow!("token provider task panicked: {e}"))
                .and_then(|r| r);

            match result {
                Ok(token) if !token.is_empty() => return Ok(token),
                Ok(_) => {
                    last_err = Some(anyhow::anyhow!(
                        "token provider returned an empty token (attempt {attempt})"
                    ));
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }

            if attempt < SESSION_ROTATION_RETRIES {
                tokio::time::sleep(backoff).await;
                backoff = (backoff.mul_f64(1.5)).min(max_backoff);
            }
        }

        Err(last_err.unwrap_or_else(|| ZerodhaError::AuthDead.into()))
    }

    fn recently_rotated(&self) -> bool {
        let last = self.last_rotation_ns.load(Ordering::Acquire);
        if last == 0 {
            return false;
        }
        let elapsed_ns = now_ns().saturating_sub(last);
        elapsed_ns < SESSION_ROTATION_WINDOW.as_nanos() as u64
    }
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn closure_provider(calls: Arc<AtomicUsize>, output: &'static str) -> TokenProvider {
        Arc::new(move || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(output.to_string())
        })
    }

    fn failing_provider(calls: Arc<AtomicUsize>) -> TokenProvider {
        Arc::new(move || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(anyhow::anyhow!("simulated provider failure"))
        })
    }

    #[tokio::test]
    async fn install_token_publishes_immediately() {
        let mgr = ZerodhaSessionManager::new("key".into(), "old".into(), None);
        assert_eq!(mgr.access_token().as_str(), "old");

        mgr.install_token("new".into());
        assert_eq!(mgr.access_token().as_str(), "new");
        assert_eq!(mgr.current_state(), "healthy");
    }

    #[tokio::test]
    async fn rotate_without_provider_marks_auth_dead() {
        let mgr = ZerodhaSessionManager::new("key".into(), "old".into(), None);
        let err = mgr.rotate().await.expect_err("should fail without provider");
        assert!(matches!(
            err.downcast::<ZerodhaError>().unwrap(),
            ZerodhaError::AuthDead
        ));
        assert!(mgr.is_auth_dead());
    }

    #[tokio::test]
    async fn rotate_installs_new_token_and_notifies() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mgr = Arc::new(ZerodhaSessionManager::new(
            "key".into(),
            "old".into(),
            Some(closure_provider(calls.clone(), "fresh")),
        ));

        let waiter_mgr = mgr.clone();
        let waiter = tokio::spawn(async move { waiter_mgr.rotation_notify.notified().await });

        // Give the waiter a moment to register.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let token = mgr.rotate().await.expect("rotate");
        assert_eq!(token.as_str(), "fresh");
        assert_eq!(mgr.access_token().as_str(), "fresh");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // The notify barrier should fire so the WS handler can wake.
        tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("notify should fire after rotation")
            .expect("waiter task ok");
    }

    #[tokio::test]
    async fn concurrent_rotate_calls_share_one_provider_invocation() {
        let calls = Arc::new(AtomicUsize::new(0));

        // Provider sleeps so we can race two callers into it.
        let provider: TokenProvider = {
            let calls = calls.clone();
            Arc::new(move || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(80));
                Ok("shared".to_string())
            })
        };
        let mgr = Arc::new(ZerodhaSessionManager::new(
            "key".into(),
            "old".into(),
            Some(provider),
        ));

        let a = tokio::spawn({
            let mgr = mgr.clone();
            async move { mgr.rotate().await }
        });
        let b = tokio::spawn({
            let mgr = mgr.clone();
            async move {
                // Skew slightly so b loses the CAS and ends up waiting on rotation_notify.
                tokio::time::sleep(Duration::from_millis(10)).await;
                mgr.rotate().await
            }
        });

        let (ra, rb) = tokio::join!(a, b);
        let tok_a = ra.unwrap().expect("a ok");
        let tok_b = rb.unwrap().expect("b ok");
        assert_eq!(tok_a.as_str(), "shared");
        assert_eq!(tok_b.as_str(), "shared");
        // Exactly one provider invocation across both callers.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn rotate_marks_auth_dead_after_exhausting_retries() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mgr = ZerodhaSessionManager::new(
            "key".into(),
            "old".into(),
            Some(failing_provider(calls.clone())),
        );

        let err = mgr.rotate().await.expect_err("provider always fails");
        assert!(err.to_string().contains("simulated provider failure"));
        assert!(mgr.is_auth_dead());
        assert_eq!(calls.load(Ordering::SeqCst), SESSION_ROTATION_RETRIES as usize);
    }
}
