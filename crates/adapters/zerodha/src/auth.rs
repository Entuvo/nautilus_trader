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

//! Kite Connect daily-login helper.
//!
//! Port of openalgo's `authenticate_broker` (`broker/zerodha/api/auth_api.py:8-63`). The flow:
//!
//! 1. User completes the browser-based Kite login → receives a single-use `request_token`.
//! 2. Host POSTs `/session/token` with `checksum = sha256(api_key + request_token + api_secret)`.
//! 3. Kite returns `data.access_token` (valid until ~06:00 IST next trading day).
//!
//! The token returned here is then either stamped onto `ZerodhaCredentials::access_token` or
//! supplied to `ZerodhaSessionManager::rotate()`.

use anyhow::Result;
use sha2::{Digest, Sha256};

use crate::{common::KITE_VERSION, error::ZerodhaError};

/// Kite Connect session-exchange endpoint.
pub const SESSION_TOKEN_URL: &str = "https://api.kite.trade/session/token";

/// Compute the Kite session checksum.
///
/// `checksum = sha256(api_key + request_token + api_secret)` hex-encoded.
#[must_use]
pub fn session_checksum(api_key: &str, request_token: &str, api_secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    hasher.update(request_token.as_bytes());
    hasher.update(api_secret.as_bytes());
    hex::encode(hasher.finalize())
}

/// Exchange a single-use `request_token` for a daily `access_token`.
///
/// Builds its own `reqwest::Client`; the call is one-shot per login so connection pooling is not
/// worth the API surface. Header `X-Kite-Version: 3` matches Kite's documented contract.
///
/// # Errors
///
/// - [`ZerodhaError::Network`] on transport failure.
/// - [`ZerodhaError::KiteError`] / [`ZerodhaError::TokenException`] / [`ZerodhaError::InvalidResponse`]
///   when Kite rejects the exchange (most commonly: stale `request_token`, wrong `api_secret`,
///   or a checksum mismatch).
pub async fn exchange_request_token(
    api_key: &str,
    api_secret: &str,
    request_token: &str,
) -> Result<String> {
    let checksum = session_checksum(api_key, request_token, api_secret);
    let client = reqwest::Client::builder()
        .user_agent("nautilus-zerodha")
        .build()
        .map_err(ZerodhaError::Network)?;

    let response = client
        .post(SESSION_TOKEN_URL)
        .header("X-Kite-Version", KITE_VERSION)
        .form(&[
            ("api_key", api_key),
            ("request_token", request_token),
            ("checksum", checksum.as_str()),
        ])
        .send()
        .await
        .map_err(ZerodhaError::Network)?;

    let status = response.status();
    let body = response.text().await.map_err(ZerodhaError::Network)?;
    parse_session_response(status.as_u16(), &body)
}

fn parse_session_response(status: u16, body: &str) -> Result<String> {
    let value: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        ZerodhaError::InvalidResponse(format!("session/token body not JSON: {e}"))
    })?;

    if value.get("status").and_then(|v| v.as_str()) == Some("error") {
        let error_type = value
            .get("error_type")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown");
        let message = value
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return Err(ZerodhaError::from_kite_body(status, error_type, message).into());
    }

    value
        .get("data")
        .and_then(|d| d.get("access_token"))
        .and_then(|t| t.as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            ZerodhaError::InvalidResponse(
                "session/token response missing data.access_token".to_string(),
            )
            .into()
        })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn checksum_matches_openalgo_reference() {
        // hashlib.sha256(b"keytokensecret").hexdigest()
        let expected = "08a03d928417ea4085557933d3b187ff2a3515b039d6054dbd230c95d978a17a";
        assert_eq!(session_checksum("key", "token", "secret"), expected);
    }

    #[rstest]
    fn parse_success() {
        let body = r#"{"status":"success","data":{"access_token":"tok"}}"#;
        let token = parse_session_response(200, body).expect("parse");
        assert_eq!(token, "tok");
    }

    #[rstest]
    fn parse_token_exception() {
        let body = r#"{"status":"error","error_type":"TokenException","message":"bad token"}"#;
        let err = parse_session_response(403, body).expect_err("should error");
        let inner = err
            .downcast::<ZerodhaError>()
            .expect("downcast to ZerodhaError");
        assert!(matches!(inner, ZerodhaError::TokenException(_)));
    }

    #[rstest]
    fn parse_missing_token_field() {
        let body = r#"{"status":"success","data":{}}"#;
        let err = parse_session_response(200, body).expect_err("should error");
        assert!(err.to_string().contains("access_token"));
    }
}
