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

//! Smoke-test runner for the ThetaData adapter.
//!
//! Prerequisites:
//!
//! 1. ThetaTerminalv3 running locally — `java -jar ThetaTerminalv3.jar`
//!    (requires Java 21 and a `creds.txt` with your account email + password).
//! 2. Set `THETADATA_INSTRUMENT_ID` to an OCC-encoded option, e.g.
//!    `SPXW250620C00480000.THETADATA`. Defaults to that value when unset.
//!
//! Run with:
//!
//! ```text
//! cargo run --example thetadata-data-tester --package nautilus-thetadata
//! ```

use nautilus_common::{
    actor::{DataActor, DataActorCore, data_actor::DataActorConfig},
    enums::{Environment, LogColor},
    log_info, nautilus_actor,
};
use nautilus_core::env::get_env_var;
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{QuoteTick, TradeTick},
    identifiers::{ClientId, InstrumentId, TraderId},
    stubs::TestDefault,
};
use nautilus_thetadata::{
    common::THETADATA_CLIENT_ID,
    config::ThetaDataDataClientConfig,
    factories::ThetaDataDataClientFactory,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let trader_id = TraderId::test_default();
    let node_name = "THETADATA-TESTER-001".to_string();

    // Default to next Friday's AAPL 200-strike call — replace with any currently-listed
    // contract via THETADATA_INSTRUMENT_ID. Encoding: ROOT YY MM DD C|P STRIKE×1000 (8 digits).
    // Default: AAPL 2026-05-22 $285 call (near-ATM as of 2026-05-18). Override with
    // THETADATA_INSTRUMENT_ID. Encoding: ROOT YY MM DD C|P STRIKE×1000 (8 digits).
    let instrument_id_str = get_env_var("THETADATA_INSTRUMENT_ID")
        .unwrap_or_else(|_| "AAPL260522C00285000.THETADATA".to_string());
    let instrument_id = InstrumentId::from(instrument_id_str.as_str());

    let theta_config = ThetaDataDataClientConfig::builder().build();
    let factory = ThetaDataDataClientFactory::new();

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name(node_name)
        .with_load_state(false)
        .with_save_state(false)
        .with_delay_post_stop_secs(2)
        .add_data_client(None, Box::new(factory), Box::new(theta_config))?
        .build()?;

    let actor_config = ThetaDataSubscriberActorConfig::new(
        *THETADATA_CLIENT_ID,
        vec![instrument_id],
    );
    let actor = ThetaDataSubscriberActor::new(actor_config);
    node.add_actor(actor)?;
    node.run().await?;
    Ok(())
}

/// Configuration for the ThetaData subscriber actor.
#[derive(Debug, Clone)]
pub struct ThetaDataSubscriberActorConfig {
    /// Base data actor configuration.
    pub base: DataActorConfig,
    /// Client ID to use for subscriptions.
    pub client_id: ClientId,
    /// Instrument IDs to subscribe to.
    pub instrument_ids: Vec<InstrumentId>,
}

impl ThetaDataSubscriberActorConfig {
    /// Creates a new [`ThetaDataSubscriberActorConfig`].
    #[must_use]
    pub fn new(client_id: ClientId, instrument_ids: Vec<InstrumentId>) -> Self {
        Self {
            base: DataActorConfig::default(),
            client_id,
            instrument_ids,
        }
    }
}

/// Smoke-test actor — subscribes to quotes + trades, logs each as it arrives.
#[derive(Debug)]
pub struct ThetaDataSubscriberActor {
    core: DataActorCore,
    config: ThetaDataSubscriberActorConfig,
    pub received_quotes: Vec<QuoteTick>,
    pub received_trades: Vec<TradeTick>,
}

nautilus_actor!(ThetaDataSubscriberActor);

impl DataActor for ThetaDataSubscriberActor {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let instrument_ids = self.config.instrument_ids.clone();
        let client_id = self.config.client_id;
        for instrument_id in instrument_ids {
            self.subscribe_quotes(instrument_id, Some(client_id), None);
            self.subscribe_trades(instrument_id, Some(client_id), None);
        }
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        let instrument_ids = self.config.instrument_ids.clone();
        let client_id = self.config.client_id;
        for instrument_id in instrument_ids {
            self.unsubscribe_quotes(instrument_id, Some(client_id), None);
            self.unsubscribe_trades(instrument_id, Some(client_id), None);
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        log_info!("{quote:?}", color = LogColor::Cyan);
        self.received_quotes.push(*quote);
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        log_info!("{trade:?}", color = LogColor::Cyan);
        self.received_trades.push(*trade);
        Ok(())
    }
}

impl ThetaDataSubscriberActor {
    /// Creates a new subscriber actor.
    #[must_use]
    pub fn new(config: ThetaDataSubscriberActorConfig) -> Self {
        Self {
            core: DataActorCore::new(config.base.clone()),
            config,
            received_quotes: Vec::new(),
            received_trades: Vec::new(),
        }
    }
}
