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

//! Common constants and credential handling for the ThetaData adapter.

use std::{
    fmt::Debug,
    fs,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use anyhow::{Context, Result};
use nautilus_core::string::secret::REDACTED;
use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;
use zeroize::ZeroizeOnDrop;

/// Venue identifier string.
pub const THETADATA: &str = "THETADATA";

/// Static venue instance.
pub static THETADATA_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(THETADATA)));

/// Static client ID instance.
pub static THETADATA_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(THETADATA)));

/// Default base URL for the local ThetaTerminal HTTP server (v3 API).
pub const DEFAULT_HTTP_URL: &str = "http://127.0.0.1:25503/v3";

/// Default WebSocket URL for the local ThetaTerminal streaming endpoint.
///
/// Note: only a single WebSocket connection may be open at a time — all subscriptions must be
/// multiplexed through one client.
pub const DEFAULT_WS_URL: &str = "ws://127.0.0.1:25520/v1/events";

/// Environment variable used to override the credentials file location.
///
/// When set, this path overrides the default `./creds.txt` lookup.
pub const ENV_CREDS_FILE: &str = "THETADATA_CREDENTIALS_FILE";

/// Default credentials filename when no path override is provided.
pub const DEFAULT_CREDS_FILENAME: &str = "creds.txt";

/// Credentials for a ThetaData account, loaded from a `creds.txt` file.
///
/// The file format expected by `ThetaTerminalv3.jar`:
///
/// ```text
/// your.email@example.com
/// your-password
/// ```
///
/// The Terminal owns authentication — this struct only carries credentials needed to start the
/// Terminal process, or to validate its configuration. The adapter never sends credentials over
/// the wire itself.
#[derive(Clone, ZeroizeOnDrop)]
pub struct Credential {
    email: Box<[u8]>,
    password: Box<[u8]>,
}

impl Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Credential))
            .field("email", &REDACTED)
            .field("password", &REDACTED)
            .finish()
    }
}

impl Credential {
    /// Creates a new [`Credential`] from an email and password.
    #[must_use]
    pub fn new(email: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            email: email.into().into_bytes().into_boxed_slice(),
            password: password.into().into_bytes().into_boxed_slice(),
        }
    }

    /// Loads credentials from a `creds.txt` file.
    ///
    /// Resolution order:
    /// 1. The `path` argument if provided.
    /// 2. The `THETADATA_CREDENTIALS_FILE` env var.
    /// 3. `./creds.txt` in the current working directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the file is missing, unreadable, or does not contain at least two
    /// non-empty lines (email then password).
    pub fn from_file(path: Option<&Path>) -> Result<Self> {
        let resolved: PathBuf = match path {
            Some(p) => p.to_path_buf(),
            None => match std::env::var(ENV_CREDS_FILE) {
                Ok(v) => PathBuf::from(v),
                Err(_) => PathBuf::from(DEFAULT_CREDS_FILENAME),
            },
        };

        let content = fs::read_to_string(&resolved)
            .with_context(|| format!("failed to read credentials file at {}", resolved.display()))?;

        let mut lines = content.lines();
        let email = lines
            .next()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .context("credentials file is missing the email on line 1")?
            .to_owned();
        let password = lines
            .next()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .context("credentials file is missing the password on line 2")?
            .to_owned();

        Ok(Self::new(email, password))
    }

    /// Returns the email address.
    ///
    /// # Panics
    ///
    /// Never panics — the email is always valid UTF-8 since it was constructed from a `String`.
    #[must_use]
    pub fn email(&self) -> &str {
        std::str::from_utf8(&self.email).expect("email is valid UTF-8")
    }

    /// Returns the password.
    ///
    /// # Panics
    ///
    /// Never panics — the password is always valid UTF-8 since it was constructed from a `String`.
    #[must_use]
    pub fn password(&self) -> &str {
        std::str::from_utf8(&self.password).expect("password is valid UTF-8")
    }

    /// Returns a masked version of the email for logging.
    #[must_use]
    pub fn email_masked(&self) -> String {
        nautilus_core::string::secret::mask_api_key(self.email())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use rstest::*;
    use tempfile::NamedTempFile;

    use super::*;

    #[rstest]
    fn test_credential_debug_redaction() {
        let credential = Credential::new("user@example.com", "supersecret");
        let debug_str = format!("{credential:?}");
        assert!(debug_str.contains(REDACTED));
        assert!(!debug_str.contains("user@example.com"));
        assert!(!debug_str.contains("supersecret"));
    }

    #[rstest]
    fn test_credential_from_file_ok() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "user@example.com").unwrap();
        writeln!(file, "hunter2").unwrap();

        let cred = Credential::from_file(Some(file.path())).unwrap();
        assert_eq!(cred.email(), "user@example.com");
        assert_eq!(cred.password(), "hunter2");
    }

    #[rstest]
    fn test_credential_from_file_strips_whitespace() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "  user@example.com  ").unwrap();
        writeln!(file, "  hunter2  ").unwrap();

        let cred = Credential::from_file(Some(file.path())).unwrap();
        assert_eq!(cred.email(), "user@example.com");
        assert_eq!(cred.password(), "hunter2");
    }

    #[rstest]
    fn test_credential_from_file_missing_password() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "user@example.com").unwrap();

        let err = Credential::from_file(Some(file.path())).unwrap_err();
        assert!(err.to_string().contains("password"));
    }

    #[rstest]
    fn test_credential_from_file_missing_file() {
        let err =
            Credential::from_file(Some(Path::new("/nonexistent/path/creds.txt"))).unwrap_err();
        assert!(err.to_string().contains("failed to read credentials file"));
    }
}
