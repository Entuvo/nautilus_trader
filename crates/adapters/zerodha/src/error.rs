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

//! Domain error types for the Zerodha adapter.
//!
//! All fallible adapter operations return [`anyhow::Result`] (per the workspace rust conventions),
//! but the inner cause is one of the variants below so callers can match on a stable taxonomy
//! without string-sniffing.

use thiserror::Error;

/// Errors emitted by the Zerodha adapter.
#[derive(Debug, Error)]
pub enum ZerodhaError {
    /// Transport-level failure (DNS, connection refused, TLS, timeout, …).
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),

    /// Kite Connect returned a structured error in the response body.
    #[error("kite error [{status} {error_type}]: {message}")]
    KiteError {
        /// HTTP status code returned alongside the structured body.
        status: u16,
        /// Kite's `error_type` field (e.g. `TokenException`, `InputException`, `NetworkException`).
        error_type: String,
        /// Kite's `message` field.
        message: String,
    },

    /// `access_token` rejected by Kite — the daily token has expired or been revoked.
    ///
    /// Triggers a `ZerodhaSessionManager::rotate()` attempt; persistent failure surfaces as
    /// `auth_dead`.
    #[error("token rejected: {0}")]
    TokenException(String),

    /// Kite returned HTTP 429. Adaptive backoff applies (see `common::ORDER_POLL_MAX_INTERVAL`).
    #[error("rate limited: {0}")]
    RateLimited(String),

    /// Market-hours guard from Kite (`error_type=NetworkException`, message matches the
    /// "Trading is not allowed at this time" template).
    ///
    /// Distinguished from generic `Network` errors so strategies can pick a retry policy.
    #[error("market closed: {0}")]
    MarketClosed(String),

    /// `ZerodhaSessionManager` rotation failed `SESSION_ROTATION_RETRIES` times in a row.
    #[error("auth_dead — session token rotation exhausted retries")]
    AuthDead,

    /// Tried to issue more than `WS_MAX_SUBSCRIPTIONS` against a single ticker connection.
    #[error("subscription limit exceeded: requested {requested}, cap {cap}")]
    SubscriptionLimitExceeded {
        /// Total instrument-token count requested (including resubscriptions).
        requested: usize,
        /// Kite's documented per-connection cap (see `common::WS_MAX_SUBSCRIPTIONS`).
        cap: usize,
    },

    /// JSON parse failure on a Kite response.
    #[error("invalid response: {0}")]
    InvalidResponse(String),

    /// Required environment variable is missing or empty (see `credential.rs`).
    #[error("missing credential: {0}")]
    MissingCredential(String),
}

impl ZerodhaError {
    /// Classify a parsed Kite error body into the most specific [`ZerodhaError`] variant.
    ///
    /// The HTTP status is supplied separately because Kite includes the `error_type` /
    /// `message` fields in the JSON body alongside any 4xx / 5xx status.
    #[must_use]
    pub fn from_kite_body(status: u16, error_type: &str, message: &str) -> Self {
        match error_type {
            "TokenException" => Self::TokenException(message.to_string()),
            "NetworkException"
                if message
                    .to_lowercase()
                    .contains("trading is not allowed at this time") =>
            {
                Self::MarketClosed(message.to_string())
            }
            _ => Self::KiteError {
                status,
                error_type: error_type.to_string(),
                message: message.to_string(),
            },
        }
    }

    /// Whether the error indicates the access token must be rotated.
    #[must_use]
    pub const fn is_token_dead(&self) -> bool {
        matches!(self, Self::TokenException(_))
    }
}
