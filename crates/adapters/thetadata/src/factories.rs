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

//! Factory for wiring a [`ThetaDataDataClient`] into a `LiveNode`.

use std::{cell::RefCell, rc::Rc};

use anyhow::anyhow;
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{common::THETADATA, config::ThetaDataDataClientConfig, data::ThetaDataDataClient};

/// Factory for creating [`ThetaDataDataClient`] instances.
///
/// Wired into a `LiveNode` via `add_data_client(name, factory, config)`. The factory is a
/// zero-sized unit type; it carries no state of its own.
#[derive(Clone, Copy, Debug, Default)]
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
pub struct ThetaDataDataClientFactory;

impl ThetaDataDataClientFactory {
    /// Creates a new factory instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for ThetaDataDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let cfg = config
            .as_any()
            .downcast_ref::<ThetaDataDataClientConfig>()
            .ok_or_else(|| {
                anyhow!(
                    "ThetaDataDataClientFactory expected ThetaDataDataClientConfig, got {config:?}"
                )
            })?
            .clone();

        // Honor the explicit client_id only if it was already set; otherwise stamp from `name`.
        let cfg = ThetaDataDataClientConfig {
            client_id: cfg.client_id.or(Some(ClientId::from(name))),
            ..cfg
        };
        let client = ThetaDataDataClient::new(cfg)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &str {
        THETADATA
    }

    fn config_type(&self) -> &'static str {
        "ThetaDataDataClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;

    #[rstest]
    fn test_factory_name_and_config_type() {
        let f = ThetaDataDataClientFactory::new();
        assert_eq!(f.name(), "THETADATA");
        assert_eq!(f.config_type(), "ThetaDataDataClientConfig");
    }
}
