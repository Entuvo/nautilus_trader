// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Live market data client — implements `nautilus_common::clients::DataClient` (Phase 8 step 14a).
//!
//! Composes:
//!
//! - [`crate::session::ZerodhaSessionManager`] — token holder shared with the exec client.
//! - [`crate::live::ZerodhaWsClient`] — outer WebSocket client (two-layer per
//!   `adapter-architecture`).
//! - [`crate::instruments::ZerodhaInstrumentCache`] — for precision lookup and Phase 2
//!   refresh.
//! - [`crate::data::ZerodhaDataDispatcher`] — Kite-tick → Nautilus-tick converter + sub
//!   tracker.
//!
//! # Scope
//!
//! This is the **minimum** trait impl needed to plug into Nautilus's live runner:
//! lifecycle + the three `subscribe_*` / `unsubscribe_*` paths the dispatcher knows about
//! (`quotes`, `trades`, `book_depth10`). Every other `DataClient` method inherits its
//! default impl from the trait, which logs an "unimplemented" warning. Step 14b fills in
//! `request_bars` (Phase 4 historical) and the actual dispatcher → runner forwarder task.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::{
        DataEvent,
        data::{
            SubscribeBookDepth10, SubscribeQuotes, SubscribeTrades, UnsubscribeBookDepth10,
            UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_model::{
    data::Data,
    identifiers::{ClientId, Venue},
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    config::ZerodhaDataClientConfig,
    data::{DispatchSinks, ZerodhaDataDispatcher},
    factories::SharedDeps,
    live::ZerodhaWsClient,
};

/// Live market data client.
pub struct ZerodhaDataClient {
    client_id: ClientId,
    venue: Venue,
    is_connected: Arc<AtomicBool>,
    ws: Arc<ZerodhaWsClient>,
    dispatcher: Arc<ZerodhaDataDispatcher>,
    sink_rxs: std::sync::Mutex<Option<SinkReceivers>>,
    data_sender: mpsc::UnboundedSender<DataEvent>,
    forwarder_task: std::sync::Mutex<Option<JoinHandle<()>>>,
    #[allow(dead_code)]
    config: ZerodhaDataClientConfig,
}

struct SinkReceivers {
    quotes: mpsc::Receiver<nautilus_model::data::QuoteTick>,
    trades: mpsc::Receiver<nautilus_model::data::TradeTick>,
    depths: mpsc::Receiver<nautilus_model::data::depth::OrderBookDepth10>,
}

impl std::fmt::Debug for ZerodhaDataClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZerodhaDataClient")
            .field("client_id", &self.client_id)
            .field("venue", &self.venue)
            .field(
                "is_connected",
                &self.is_connected.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl ZerodhaDataClient {
    /// Construct the client. Does **not** open the WebSocket — call [`DataClient::connect`].
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket / dispatcher channels can't be set up.
    pub fn new(
        client_id: ClientId,
        venue: Venue,
        shared: &Arc<SharedDeps>,
        config: ZerodhaDataClientConfig,
    ) -> Result<Self> {
        let ws = Arc::new(ZerodhaWsClient::spawn(
            shared.session.clone(),
            Some(config.ws_url.clone()),
        ));

        let (qtx, qrx) = mpsc::channel(crate::common::WS_EVENT_CHANNEL_CAPACITY);
        let (ttx, trx) = mpsc::channel(crate::common::WS_EVENT_CHANNEL_CAPACITY);
        let (dtx, drx) = mpsc::channel(crate::common::WS_EVENT_CHANNEL_CAPACITY);
        let sinks = DispatchSinks {
            quotes: qtx,
            trades: ttx,
            depths: dtx,
        };
        let dispatcher = Arc::new(ZerodhaDataDispatcher::new(
            shared.cache.clone(),
            ws.clone(),
            sinks,
            2, // default INR precision; per-instrument precision comes from the cache
        ));

        // get_data_event_sender uses a thread-local set by the live runner; the factory call
        // path runs on that thread, so capturing the sender here is safe.
        let data_sender = get_data_event_sender();

        Ok(Self {
            client_id,
            venue,
            is_connected: Arc::new(AtomicBool::new(false)),
            ws,
            dispatcher,
            sink_rxs: std::sync::Mutex::new(Some(SinkReceivers {
                quotes: qrx,
                trades: trx,
                depths: drx,
            })),
            data_sender,
            forwarder_task: std::sync::Mutex::new(None),
            config,
        })
    }

    /// Spawn the dispatcher→runner forwarder. Runs once on first `connect()`; subsequent
    /// calls are no-ops.
    fn spawn_forwarder(&self) -> Result<()> {
        let sinks = {
            let mut guard = self
                .sink_rxs
                .lock()
                .map_err(|e| anyhow!("sink_rxs lock poisoned: {e}"))?;
            guard.take()
        };
        let Some(SinkReceivers {
            mut quotes,
            mut trades,
            mut depths,
        }) = sinks
        else {
            // Already spawned.
            return Ok(());
        };
        let sender = self.data_sender.clone();
        let task = get_runtime().spawn(async move {
            loop {
                tokio::select! {
                    Some(q) = quotes.recv() => {
                        if sender.send(DataEvent::Data(Data::Quote(q))).is_err() {
                            log::warn!("ZerodhaDataClient forwarder: data_sender closed");
                            break;
                        }
                    }
                    Some(t) = trades.recv() => {
                        if sender.send(DataEvent::Data(Data::Trade(t))).is_err() { break; }
                    }
                    Some(d) = depths.recv() => {
                        if sender.send(DataEvent::Data(Data::Depth10(Box::new(d)))).is_err() {
                            break;
                        }
                    }
                    else => break,
                }
            }
        });
        if let Ok(mut guard) = self.forwarder_task.lock() {
            *guard = Some(task);
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl DataClient for ZerodhaDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue)
    }

    fn start(&mut self) -> Result<()> {
        log::debug!("ZerodhaDataClient starting (client_id={})", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        log::debug!("ZerodhaDataClient stopping");
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn dispose(&mut self) -> Result<()> {
        self.stop()
    }

    async fn connect(&mut self) -> Result<()> {
        // The WebSocket starts spawning the handler task on construction; here we just wait
        // for it to come up. ws.is_connected() flips to true after the handshake completes.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !self.ws.is_connected() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        if !self.ws.is_connected() {
            return Err(anyhow!(
                "ZerodhaDataClient: WebSocket did not connect within 15 s"
            ));
        }
        // Spawn the forwarder on first connect so dispatcher ticks flow to the runner.
        self.spawn_forwarder()?;
        self.is_connected.store(true, Ordering::Relaxed);
        log::info!("ZerodhaDataClient connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.ws.close().await;
        if let Ok(mut guard) = self.forwarder_task.lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
        self.is_connected.store(false, Ordering::Relaxed);
        log::info!("ZerodhaDataClient disconnected");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        let id = cmd.instrument_id;
        nautilus_common::live::get_runtime().spawn(async move {
            if let Err(e) = dispatcher.subscribe_quote_ticks(&id).await {
                log::warn!("subscribe_quote_ticks({id}) failed: {e}");
            }
        });
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        let id = cmd.instrument_id;
        nautilus_common::live::get_runtime().spawn(async move {
            if let Err(e) = dispatcher.unsubscribe_quote_ticks(&id).await {
                log::warn!("unsubscribe_quote_ticks({id}) failed: {e}");
            }
        });
        Ok(())
    }


    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        let id = cmd.instrument_id;
        nautilus_common::live::get_runtime().spawn(async move {
            if let Err(e) = dispatcher.subscribe_trade_ticks(&id).await {
                log::warn!("subscribe_trade_ticks({id}) failed: {e}");
            }
        });
        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        let id = cmd.instrument_id;
        nautilus_common::live::get_runtime().spawn(async move {
            if let Err(e) = dispatcher.unsubscribe_trade_ticks(&id).await {
                log::warn!("unsubscribe_trade_ticks({id}) failed: {e}");
            }
        });
        Ok(())
    }

    fn subscribe_book_depth10(&mut self, cmd: SubscribeBookDepth10) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        let id = cmd.instrument_id;
        nautilus_common::live::get_runtime().spawn(async move {
            if let Err(e) = dispatcher.subscribe_order_book(&id).await {
                log::warn!("subscribe_order_book({id}) failed: {e}");
            }
        });
        Ok(())
    }

    fn unsubscribe_book_depth10(&mut self, cmd: &UnsubscribeBookDepth10) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        let id = cmd.instrument_id;
        nautilus_common::live::get_runtime().spawn(async move {
            if let Err(e) = dispatcher.unsubscribe_order_book(&id).await {
                log::warn!("unsubscribe_order_book({id}) failed: {e}");
            }
        });
        Ok(())
    }
}
