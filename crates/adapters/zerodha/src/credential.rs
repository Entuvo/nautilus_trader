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

//! Kite Connect credential resolution.
//!
//! Long-lived secrets live in env vars: `ZERODHA_API_KEY`, `ZERODHA_API_SECRET`. The daily
//! `access_token` is rotated separately (env var `ZERODHA_ACCESS_TOKEN` or an explicit override
//! supplied by the caller after running the login helper). Kite has no sandbox, so there are no
//! `_TESTNET` variants.

use std::env;

use anyhow::Result;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::ZerodhaError;

/// Environment variable holding the long-lived Kite Connect API key.
pub const ENV_API_KEY: &str = "ZERODHA_API_KEY";

/// Environment variable holding the long-lived Kite Connect API secret.
pub const ENV_API_SECRET: &str = "ZERODHA_API_SECRET";

/// Environment variable holding the daily `access_token` (rotated by the login helper).
pub const ENV_ACCESS_TOKEN: &str = "ZERODHA_ACCESS_TOKEN";

/// Resolved Kite credentials.
///
/// The secret-bearing fields are zeroized on drop so a panic or process tear-down doesn't leave
/// residue in freed pages.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct ZerodhaCredentials {
    /// Public API key issued by Kite Connect.
    pub api_key: String,
    /// API secret issued by Kite Connect — used to derive the session checksum during login.
    pub api_secret: String,
    /// Current `access_token`. May be empty at startup if the caller will supply it later via
    /// the session manager's provider callback.
    pub access_token: String,
}

impl std::fmt::Debug for ZerodhaCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZerodhaCredentials")
            .field("api_key", &mask(&self.api_key))
            .field("api_secret", &"****")
            .field("access_token", &mask(&self.access_token))
            .finish()
    }
}

impl ZerodhaCredentials {
    /// Resolve credentials entirely from environment variables.
    ///
    /// `ZERODHA_API_KEY` and `ZERODHA_API_SECRET` are mandatory; `ZERODHA_ACCESS_TOKEN` is
    /// optional (a caller supplying a session manager provider can resolve it lazily).
    ///
    /// # Errors
    ///
    /// Returns [`ZerodhaError::MissingCredential`] when either of the required vars is unset or
    /// empty.
    pub fn from_env() -> Result<Self> {
        let api_key = require_env(ENV_API_KEY)?;
        let api_secret = require_env(ENV_API_SECRET)?;
        let access_token = env::var(ENV_ACCESS_TOKEN).unwrap_or_default();
        Ok(Self {
            api_key,
            api_secret,
            access_token,
        })
    }

    /// Build credentials from explicit overrides, falling back to env vars for any field left as
    /// `None`.
    ///
    /// `api_key` and `api_secret` must resolve to a non-empty string from one of the two
    /// sources; `access_token` may be empty.
    ///
    /// # Errors
    ///
    /// Returns [`ZerodhaError::MissingCredential`] when a required field resolves to empty after
    /// both override and env fallback.
    pub fn from_config(
        api_key: Option<String>,
        api_secret: Option<String>,
        access_token: Option<String>,
    ) -> Result<Self> {
        let api_key = resolve_required(api_key, ENV_API_KEY)?;
        let api_secret = resolve_required(api_secret, ENV_API_SECRET)?;
        let access_token = access_token.unwrap_or_else(|| {
            env::var(ENV_ACCESS_TOKEN).unwrap_or_default()
        });
        Ok(Self {
            api_key,
            api_secret,
            access_token,
        })
    }

    /// Whether a non-empty `access_token` is currently held.
    #[must_use]
    pub fn has_access_token(&self) -> bool {
        !self.access_token.is_empty()
    }
}

fn require_env(name: &str) -> Result<String> {
    let value = env::var(name).unwrap_or_default();
    if value.is_empty() {
        Err(ZerodhaError::MissingCredential(name.to_string()).into())
    } else {
        Ok(value)
    }
}

fn resolve_required(override_value: Option<String>, env_name: &str) -> Result<String> {
    if let Some(value) = override_value.filter(|s| !s.is_empty()) {
        return Ok(value);
    }
    require_env(env_name)
}

fn mask(value: &str) -> String {
    let len = value.len();
    if len <= 4 {
        "****".to_string()
    } else {
        format!("{}****", &value[..len.saturating_sub(4).min(4)])
    }
}

#[cfg(test)]
#[allow(unsafe_code)] // env::set_var / env::remove_var are unsafe in Rust 2024 (thread-shared).
mod tests {
    use std::sync::Mutex;

    use rstest::rstest;

    use super::*;

    // Serialize tests so they don't race on the shared process env.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        // SAFETY: tests hold ENV_LOCK so no other thread is racing on the env table.
        unsafe { env::remove_var(ENV_API_KEY) };
        // SAFETY: see above.
        unsafe { env::remove_var(ENV_API_SECRET) };
        // SAFETY: see above.
        unsafe { env::remove_var(ENV_ACCESS_TOKEN) };
    }

    fn set_env(key: &str, value: &str) {
        // SAFETY: ENV_LOCK is held by the calling test.
        unsafe { env::set_var(key, value) };
    }

    #[rstest]
    fn from_env_resolves_all_three() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        set_env(ENV_API_KEY, "k");
        set_env(ENV_API_SECRET, "s");
        set_env(ENV_ACCESS_TOKEN, "t");

        let creds = ZerodhaCredentials::from_env().expect("creds");
        assert_eq!(creds.api_key, "k");
        assert_eq!(creds.api_secret, "s");
        assert_eq!(creds.access_token, "t");
        assert!(creds.has_access_token());

        clear_env();
    }

    #[rstest]
    fn from_env_allows_missing_access_token() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        set_env(ENV_API_KEY, "k");
        set_env(ENV_API_SECRET, "s");

        let creds = ZerodhaCredentials::from_env().expect("creds");
        assert!(!creds.has_access_token());

        clear_env();
    }

    #[rstest]
    fn from_env_errors_when_api_key_missing() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        set_env(ENV_API_SECRET, "s");

        let err = ZerodhaCredentials::from_env().expect_err("should fail");
        assert!(err.to_string().contains(ENV_API_KEY));

        clear_env();
    }

    #[rstest]
    fn from_config_prefers_overrides_over_env() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        set_env(ENV_API_KEY, "env_key");
        set_env(ENV_API_SECRET, "env_secret");

        let creds = ZerodhaCredentials::from_config(
            Some("override_key".into()),
            None,
            Some("override_token".into()),
        )
        .expect("creds");
        assert_eq!(creds.api_key, "override_key");
        assert_eq!(creds.api_secret, "env_secret");
        assert_eq!(creds.access_token, "override_token");

        clear_env();
    }
}
