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

//! Configuration DTOs for the ThetaData adapter.

use std::{any::Any, path::PathBuf};

use bon::Builder;
use nautilus_common::factories::ClientConfig;
use nautilus_model::identifiers::ClientId;
use serde::{Deserialize, Serialize};

use crate::common::{DEFAULT_HTTP_URL, DEFAULT_WS_URL};

/// ThetaData subscription tier — drives subscription-count enforcement and feature availability.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThetaDataTier {
    /// $40/mo: US options real-time, 4yr history, 1-minute bars.
    Value,
    /// $80/mo: tick-level OPRA NBBO, 8yr history, 15 K concurrent stream subs.
    #[default]
    Standard,
    /// $160/mo: full OPRA trade stream, 12yr history.
    Pro,
}

impl ThetaDataTier {
    /// Returns the maximum number of concurrent single-contract stream subscriptions allowed
    /// for this tier. `None` means unlimited (Value and Pro).
    #[must_use]
    pub const fn max_concurrent_streams(self) -> Option<u32> {
        match self {
            Self::Standard => Some(15_000),
            Self::Value | Self::Pro => None,
        }
    }

    /// Whether this tier supports the bulk trade stream (`STREAM_BULK`).
    #[must_use]
    pub const fn supports_bulk_stream(self) -> bool {
        matches!(self, Self::Pro)
    }
}

/// Configuration for a ThetaData live market data client.
///
/// Credentials are **not** carried here — the local ThetaTerminal owns authentication via its
/// `creds.txt` file. See [`crate::common::Credential::from_file`].
#[derive(Clone, Debug, Builder, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.thetadata",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.thetadata")
)]
pub struct ThetaDataDataClientConfig {
    /// Optional explicit client id. Defaults to `THETADATA` when omitted.
    pub client_id: Option<ClientId>,

    /// HTTP base URL for the ThetaTerminal REST API.
    #[builder(default = DEFAULT_HTTP_URL.to_owned())]
    pub http_url: String,

    /// WebSocket URL for the ThetaTerminal streaming endpoint.
    #[builder(default = DEFAULT_WS_URL.to_owned())]
    pub ws_url: String,

    /// Subscription tier — used for local enforcement of stream-count caps.
    #[builder(default)]
    pub tier: ThetaDataTier,

    /// Optional path to a `creds.txt` file. When `None`, resolves via the
    /// `THETADATA_CREDENTIALS_FILE` env var, then `./creds.txt`.
    pub creds_file: Option<PathBuf>,

    /// Maximum number of reconnect attempts before surfacing an error.
    #[builder(default = 10)]
    pub max_reconnects: u32,

    /// HTTP request timeout in seconds.
    #[builder(default = 30)]
    pub http_timeout_secs: u64,
}

impl ClientConfig for ThetaDataDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;

    #[rstest]
    fn test_tier_default_is_standard() {
        assert_eq!(ThetaDataTier::default(), ThetaDataTier::Standard);
    }

    #[rstest]
    fn test_tier_stream_caps() {
        assert_eq!(ThetaDataTier::Value.max_concurrent_streams(), None);
        assert_eq!(
            ThetaDataTier::Standard.max_concurrent_streams(),
            Some(15_000)
        );
        assert_eq!(ThetaDataTier::Pro.max_concurrent_streams(), None);
    }

    #[rstest]
    fn test_tier_bulk_stream_support() {
        assert!(!ThetaDataTier::Value.supports_bulk_stream());
        assert!(!ThetaDataTier::Standard.supports_bulk_stream());
        assert!(ThetaDataTier::Pro.supports_bulk_stream());
    }

    #[rstest]
    fn test_config_builder_defaults() {
        let cfg = ThetaDataDataClientConfig::builder().build();
        assert_eq!(cfg.http_url, DEFAULT_HTTP_URL);
        assert_eq!(cfg.ws_url, DEFAULT_WS_URL);
        assert_eq!(cfg.tier, ThetaDataTier::Standard);
        assert_eq!(cfg.max_reconnects, 10);
        assert_eq!(cfg.http_timeout_secs, 30);
        assert!(cfg.creds_file.is_none());
    }
}
