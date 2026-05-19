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

//! Signed Kite Connect v3 REST client.
//!
//! The auth header is `Authorization: token {api_key}:{access_token}` (colon-joined). This
//! diverges from openalgo's single-token form — openalgo proxies through its own server, while
//! we hit `api.kite.trade` directly and the v3 docs require the colon form.
//!
//! Token resolution happens **per request**: we read [`ZerodhaSessionManager::access_token`] on
//! every call, so a rotation propagates without needing to rebuild the client. A response with
//! `error_type=TokenException` triggers `manager.rotate()` and is retried once with the new
//! token.

use std::{num::NonZeroU32, sync::Arc};

use anyhow::Result;
use nautilus_network::ratelimiter::{RateLimiter, clock::MonotonicClock, quota::Quota};
use reqwest::{Method, Response, StatusCode};
use serde::de::DeserializeOwned;

use crate::{
    common::{KITE_VERSION, REST_BASE, SUBMIT_RATE_PER_SEC},
    error::ZerodhaError,
    session::ZerodhaSessionManager,
};

/// Rate-limit bucket key for order submission (Kite cap: 10/sec/user).
pub const SUBMIT_BUCKET: &str = "submit";

/// Kite Connect v3 HTTP client.
pub struct ZerodhaHttpClient {
    inner: reqwest::Client,
    base_url: String,
    session: Arc<ZerodhaSessionManager>,
    rate_limiter: Arc<RateLimiter<String, MonotonicClock>>,
}

impl std::fmt::Debug for ZerodhaHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZerodhaHttpClient")
            .field("base_url", &self.base_url)
            .field("session", &self.session)
            .finish()
    }
}

impl ZerodhaHttpClient {
    /// Build the HTTP client.
    ///
    /// `base_url` defaults to [`REST_BASE`] when `None` — tests substitute a mock-server URL.
    ///
    /// # Errors
    ///
    /// Returns [`ZerodhaError::Network`] if the underlying `reqwest::Client` build fails.
    ///
    /// # Panics
    ///
    /// Panics if [`SUBMIT_RATE_PER_SEC`] is set to zero or yields an invalid per-second quota.
    /// Both are compile-time invariants of the workspace constants.
    pub fn new(session: Arc<ZerodhaSessionManager>, base_url: Option<String>) -> Result<Self> {
        let inner = reqwest::Client::builder()
            .user_agent("nautilus-zerodha")
            .build()
            .map_err(ZerodhaError::Network)?;

        let submit_quota = Quota::per_second(
            NonZeroU32::new(SUBMIT_RATE_PER_SEC)
                .expect("SUBMIT_RATE_PER_SEC must be non-zero"),
        )
        .expect("SUBMIT_RATE_PER_SEC yields a valid per-second quota");
        let rate_limiter = Arc::new(RateLimiter::new_with_quota(
            None,
            vec![(SUBMIT_BUCKET.to_string(), submit_quota)],
        ));

        Ok(Self {
            inner,
            base_url: base_url.unwrap_or_else(|| REST_BASE.to_string()),
            session,
            rate_limiter,
        })
    }

    /// Build the colon-joined `Authorization: token {api_key}:{access_token}` header value.
    fn auth_header(&self) -> String {
        let key = self.session.api_key();
        let token = self.session.access_token();
        format!("token {key}:{token}")
    }

    /// GET with no body.
    ///
    /// # Errors
    ///
    /// See [`Self::request`].
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.request(Method::GET, path, None, None).await
    }

    /// POST with `application/x-www-form-urlencoded` body.
    ///
    /// # Errors
    ///
    /// See [`Self::request`].
    pub async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        self.request(Method::POST, path, Some(form), None).await
    }

    /// POST against the order-submit rate-limit bucket.
    ///
    /// # Errors
    ///
    /// See [`Self::request`].
    pub async fn post_order<T: DeserializeOwned>(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        self.request(Method::POST, path, Some(form), Some(SUBMIT_BUCKET))
            .await
    }

    /// PUT with form body.
    ///
    /// # Errors
    ///
    /// See [`Self::request`].
    pub async fn put<T: DeserializeOwned>(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        self.request(Method::PUT, path, Some(form), Some(SUBMIT_BUCKET))
            .await
    }

    /// DELETE with no body (Kite cancel endpoint).
    ///
    /// # Errors
    ///
    /// See [`Self::request`].
    pub async fn delete<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.request(Method::DELETE, path, None, Some(SUBMIT_BUCKET))
            .await
    }

    /// Fetch the user profile — used by smoke tests to verify the auth header works.
    ///
    /// # Errors
    ///
    /// See [`Self::request`].
    pub async fn user_profile(&self) -> Result<serde_json::Value> {
        self.get("/user/profile").await
    }

    /// Core dispatch: rate-limit, send, retry once on `TokenException`, parse Kite envelope.
    ///
    /// # Errors
    ///
    /// - [`ZerodhaError::Network`] for transport-level failures.
    /// - [`ZerodhaError::KiteError`] / [`ZerodhaError::TokenException`] /
    ///   [`ZerodhaError::MarketClosed`] / [`ZerodhaError::RateLimited`] for structured Kite
    ///   responses.
    /// - [`ZerodhaError::InvalidResponse`] when the response body is malformed.
    /// - [`ZerodhaError::AuthDead`] when token rotation fails after a `TokenException`.
    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        form: Option<&[(&str, &str)]>,
        rate_limit_key: Option<&str>,
    ) -> Result<T> {
        if let Some(key) = rate_limit_key {
            self.rate_limiter.until_key_ready(&key.to_string()).await;
        }

        let url = format!("{}{}", self.base_url, path);
        let send_once = |auth: String| {
            let mut req = self.inner.request(method.clone(), &url);
            req = req
                .header("X-Kite-Version", KITE_VERSION)
                .header(reqwest::header::AUTHORIZATION, auth);
            if let Some(body) = form {
                req = req.form(body);
            }
            req.send()
        };

        let response = send_once(self.auth_header())
            .await
            .map_err(ZerodhaError::Network)?;
        let (status, body) = read_response(response).await?;

        match classify(status, &body) {
            ResponseClass::Ok => parse_kite_data(&body),
            ResponseClass::TokenExpired => {
                self.session.rotate().await?;
                let retry = send_once(self.auth_header())
                    .await
                    .map_err(ZerodhaError::Network)?;
                let (retry_status, retry_body) = read_response(retry).await?;
                match classify(retry_status, &retry_body) {
                    ResponseClass::Ok => parse_kite_data(&retry_body),
                    ResponseClass::TokenExpired => Err(ZerodhaError::AuthDead.into()),
                    ResponseClass::KiteError {
                        status,
                        error_type,
                        message,
                    } => Err(ZerodhaError::from_kite_body(status, &error_type, &message).into()),
                    ResponseClass::HttpError(s) => Err(ZerodhaError::KiteError {
                        status: s,
                        error_type: "HttpError".into(),
                        message: retry_body,
                    }
                    .into()),
                }
            }
            ResponseClass::KiteError {
                status,
                error_type,
                message,
            } => Err(ZerodhaError::from_kite_body(status, &error_type, &message).into()),
            ResponseClass::HttpError(s) if s == StatusCode::TOO_MANY_REQUESTS.as_u16() => {
                Err(ZerodhaError::RateLimited(body).into())
            }
            ResponseClass::HttpError(s) => Err(ZerodhaError::KiteError {
                status: s,
                error_type: "HttpError".into(),
                message: body,
            }
            .into()),
        }
    }
}

async fn read_response(response: Response) -> Result<(u16, String)> {
    let status = response.status().as_u16();
    let body = response.text().await.map_err(ZerodhaError::Network)?;
    Ok((status, body))
}

#[derive(Debug)]
enum ResponseClass {
    Ok,
    TokenExpired,
    KiteError {
        status: u16,
        error_type: String,
        message: String,
    },
    HttpError(u16),
}

fn classify(status: u16, body: &str) -> ResponseClass {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body)
        && value.get("status").and_then(|v| v.as_str()) == Some("error")
    {
        let error_type = value
            .get("error_type")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();
        let message = value
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if error_type == "TokenException" {
            return ResponseClass::TokenExpired;
        }
        return ResponseClass::KiteError {
            status,
            error_type,
            message,
        };
    }

    if (200..300).contains(&status) {
        ResponseClass::Ok
    } else if status == StatusCode::UNAUTHORIZED.as_u16() || status == StatusCode::FORBIDDEN.as_u16()
    {
        ResponseClass::TokenExpired
    } else {
        ResponseClass::HttpError(status)
    }
}

fn parse_kite_data<T: DeserializeOwned>(body: &str) -> Result<T> {
    let envelope: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        ZerodhaError::InvalidResponse(format!("response body not JSON: {e}"))
    })?;
    let data = envelope.get("data").ok_or_else(|| {
        ZerodhaError::InvalidResponse("response missing `data` field".to_string())
    })?;
    serde_json::from_value::<T>(data.clone()).map_err(|e| {
        ZerodhaError::InvalidResponse(format!("failed to deserialize `data`: {e}")).into()
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn classify_routes_token_exception_to_rotation() {
        let body = r#"{"status":"error","error_type":"TokenException","message":"x"}"#;
        assert!(matches!(classify(403, body), ResponseClass::TokenExpired));
    }

    #[rstest]
    fn classify_routes_403_without_body_to_rotation() {
        assert!(matches!(classify(403, ""), ResponseClass::TokenExpired));
    }

    #[rstest]
    fn classify_separates_kite_errors_from_token_exception() {
        let body = r#"{"status":"error","error_type":"InputException","message":"bad qty"}"#;
        let class = classify(400, body);
        let ResponseClass::KiteError { error_type, .. } = class else {
            panic!("expected KiteError variant, got {class:?}");
        };
        assert_eq!(error_type, "InputException");
    }

    #[rstest]
    fn classify_passes_2xx_envelope() {
        let body = r#"{"status":"success","data":{}}"#;
        assert!(matches!(classify(200, body), ResponseClass::Ok));
    }

    #[rstest]
    fn parse_kite_data_extracts_user_profile() {
        let body = r#"{"status":"success","data":{"user_name":"X"}}"#;
        let value: serde_json::Value = parse_kite_data(body).unwrap();
        assert_eq!(value.get("user_name").and_then(|v| v.as_str()), Some("X"));
    }

    #[tokio::test]
    async fn auth_header_uses_colon_join_and_current_token() {
        let session = Arc::new(ZerodhaSessionManager::new(
            "api_key".into(),
            "tok".into(),
            None,
        ));
        let client = ZerodhaHttpClient::new(session.clone(), None).unwrap();
        assert_eq!(client.auth_header(), "token api_key:tok");

        session.install_token("rotated".into());
        assert_eq!(client.auth_header(), "token api_key:rotated");
    }
}
