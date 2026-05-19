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

//! Live market data client for ThetaData.
//!
//! Composes the [`ThetaDataHistoricalClient`] (REST) and [`ThetaDataWsClient`] (WebSocket) into
//! a single [`DataClient`] implementation. Inbound WS frames are decoded to Nautilus ticks and
//! forwarded onto the runner's `DataEvent::Data` channel; subscribe/unsubscribe commands route
//! to the WS client; historical requests bypass the WS path entirely.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::{
        DataEvent,
        data::{
            BarsResponse, DataResponse, InstrumentsResponse, QuotesResponse, RequestBars,
            RequestInstruments, RequestQuotes, RequestTrades, SubscribeQuotes, SubscribeTrades,
            TradesResponse, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, Data, QuoteTick, TradeTick},
    enums::BarAggregation,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::InstrumentAny,
};
use nautilus_network::websocket::WebSocketConfig;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    common::{THETADATA_CLIENT_ID, THETADATA_VENUE},
    config::ThetaDataDataClientConfig,
    enums::Interval,
    historical::{ThetaDataHistoricalClient, chunk_date_range},
    instruments::build_option_contract,
    live::ThetaDataWsClient,
    symbology::ThetaOptionContract,
    types::{WsFrame, WsQuoteFrame, WsTradeFrame},
};

/// Default price precision when none is known per instrument (US options trade in cents).
const DEFAULT_PRICE_PRECISION: u8 = 2;
/// Default size precision (option contracts trade in integer units).
const DEFAULT_SIZE_PRECISION: u8 = 0;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ContractKey {
    root: String,
    expiration: u32,
    strike: u64,
    right: String,
}

impl ContractKey {
    fn from_contract(contract: &ThetaOptionContract) -> Self {
        Self {
            root: contract.root.clone(),
            expiration: contract.ws_expiration(),
            strike: contract.ws_strike(),
            right: contract.right.as_wire().to_string(),
        }
    }

    fn from_ws_quote(frame: &WsQuoteFrame) -> Self {
        Self {
            root: frame.contract.root.clone(),
            expiration: frame.contract.expiration,
            strike: frame.contract.strike,
            right: frame.contract.right.clone(),
        }
    }

    fn from_ws_trade(frame: &WsTradeFrame) -> Self {
        Self {
            root: frame.contract.root.clone(),
            expiration: frame.contract.expiration,
            strike: frame.contract.strike,
            right: frame.contract.right.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct InstrumentMetadata {
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
}

/// Live market data client for ThetaData.
#[derive(Debug)]
pub struct ThetaDataDataClient {
    client_id: ClientId,
    venue: Venue,
    is_connected: Arc<AtomicBool>,
    historical: ThetaDataHistoricalClient,
    ws: Arc<ThetaDataWsClient>,
    events_rx: Mutex<Option<mpsc::UnboundedReceiver<WsFrame>>>,
    instruments: Arc<Mutex<HashMap<ContractKey, InstrumentMetadata>>>,
    data_sender: mpsc::UnboundedSender<DataEvent>,
    forwarder_task: Mutex<Option<JoinHandle<()>>>,
}

impl ThetaDataDataClient {
    /// Creates a new ThetaData data client.
    ///
    /// Constructs the historical (REST) and live (WebSocket) clients but does not open the
    /// WebSocket connection — call [`DataClient::connect`] to do that.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be constructed.
    pub fn new(config: ThetaDataDataClientConfig) -> Result<Self> {
        let client_id = config.client_id.unwrap_or(*THETADATA_CLIENT_ID);
        let venue = *THETADATA_VENUE;
        let historical = ThetaDataHistoricalClient::new(
            &config.http_url,
            Duration::from_secs(config.http_timeout_secs),
        )?;
        let ws_config = WebSocketConfig::builder().url(config.ws_url.clone()).build();
        let (ws_client, events_rx) = ThetaDataWsClient::new(ws_config);
        Ok(Self {
            client_id,
            venue,
            is_connected: Arc::new(AtomicBool::new(false)),
            historical,
            ws: Arc::new(ws_client),
            events_rx: Mutex::new(Some(events_rx)),
            instruments: Arc::new(Mutex::new(HashMap::new())),
            data_sender: get_data_event_sender(),
            forwarder_task: Mutex::new(None),
        })
    }

    /// Returns the wrapped historical client (useful for direct REST calls outside the
    /// engine's request path — e.g. backfills written to the Parquet catalog).
    #[must_use]
    pub fn historical(&self) -> &ThetaDataHistoricalClient {
        &self.historical
    }

    fn register(&self, instrument_id: InstrumentId) -> Result<()> {
        let contract = ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())?;
        let key = ContractKey::from_contract(&contract);
        let meta = InstrumentMetadata {
            instrument_id,
            price_precision: DEFAULT_PRICE_PRECISION,
            size_precision: DEFAULT_SIZE_PRECISION,
        };
        self.instruments
            .lock()
            .map_err(|_| anyhow!("instruments mutex poisoned"))?
            .insert(key, meta);
        Ok(())
    }

    fn contract_for(&self, instrument_id: InstrumentId) -> Result<ThetaOptionContract> {
        ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())
    }

    fn spawn_forwarder(&self) -> Result<()> {
        let mut slot = self
            .events_rx
            .lock()
            .map_err(|_| anyhow!("events_rx mutex poisoned"))?;
        let rx = slot
            .take()
            .ok_or_else(|| anyhow!("WebSocket forwarder already spawned"))?;
        let instruments = Arc::clone(&self.instruments);
        let data_sender = self.data_sender.clone();
        let handle = get_runtime().spawn(run_forwarder(rx, instruments, data_sender));
        let mut task_slot = self
            .forwarder_task
            .lock()
            .map_err(|_| anyhow!("forwarder_task mutex poisoned"))?;
        *task_slot = Some(handle);
        Ok(())
    }
}

async fn run_forwarder(
    mut rx: mpsc::UnboundedReceiver<WsFrame>,
    instruments: Arc<Mutex<HashMap<ContractKey, InstrumentMetadata>>>,
    data_sender: mpsc::UnboundedSender<DataEvent>,
) {
    while let Some(frame) = rx.recv().await {
        match frame {
            WsFrame::Status(_) => {} // heartbeat — drop
            WsFrame::State(s) => {
                log::debug!("session state: {:?}", s.state);
            }
            WsFrame::Ohlc(o) => {
                // Cumulative session OHLC, not bar-interval aligned. Keep at debug — consumers
                // that need bars should use historical requests or aggregate from ticks.
                log::debug!(
                    "ohlc {}/{}/{}/{} close={} count={}",
                    o.contract.root,
                    o.contract.expiration,
                    o.contract.strike,
                    o.contract.right,
                    o.ohlc.close,
                    o.ohlc.count,
                );
            }
            WsFrame::Quote(quote) => {
                let key = ContractKey::from_ws_quote(&quote);
                let meta = lookup_metadata(&instruments, &key);
                let Some(meta) = meta else {
                    log::debug!(
                        "ignoring quote for unsubscribed contract {}/{}/{}/{}",
                        quote.contract.root,
                        quote.contract.expiration,
                        quote.contract.strike,
                        quote.contract.right,
                    );
                    continue;
                };
                let ts_init = now_unix_nanos();
                match quote.to_quote_tick(
                    meta.instrument_id,
                    meta.price_precision,
                    meta.size_precision,
                    ts_init,
                ) {
                    Ok(tick) => {
                        let data: Data = tick.into();
                        if data_sender.send(DataEvent::Data(data)).is_err() {
                            log::warn!("DataEvent receiver dropped; forwarder exiting");
                            return;
                        }
                    }
                    Err(e) => log::warn!("failed to decode quote tick: {e}"),
                }
            }
            WsFrame::Trade(trade) => {
                let key = ContractKey::from_ws_trade(&trade);
                let meta = lookup_metadata(&instruments, &key);
                let Some(meta) = meta else {
                    log::debug!(
                        "ignoring trade for unsubscribed contract {}/{}/{}/{}",
                        trade.contract.root,
                        trade.contract.expiration,
                        trade.contract.strike,
                        trade.contract.right,
                    );
                    continue;
                };
                let ts_init = now_unix_nanos();
                match trade.to_trade_tick(
                    meta.instrument_id,
                    meta.price_precision,
                    meta.size_precision,
                    ts_init,
                ) {
                    Ok(tick) => {
                        let data: Data = tick.into();
                        if data_sender.send(DataEvent::Data(data)).is_err() {
                            log::warn!("DataEvent receiver dropped; forwarder exiting");
                            return;
                        }
                    }
                    Err(e) => log::warn!("failed to decode trade tick: {e}"),
                }
            }
        }
    }
}

fn lookup_metadata(
    instruments: &Arc<Mutex<HashMap<ContractKey, InstrumentMetadata>>>,
    key: &ContractKey,
) -> Option<InstrumentMetadata> {
    instruments.lock().ok()?.get(key).cloned()
}

fn now_unix_nanos() -> UnixNanos {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default();
    UnixNanos::from(secs)
}

#[async_trait(?Send)]
impl DataClient for ThetaDataDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue)
    }

    fn start(&mut self) -> Result<()> {
        log::debug!("ThetaDataDataClient starting");
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        log::debug!("ThetaDataDataClient stopping");
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
        self.ws.connect().await.context("WebSocket connect failed")?;
        self.spawn_forwarder()?;
        self.is_connected.store(true, Ordering::Relaxed);
        log::info!("ThetaDataDataClient connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.ws.close().await.ok();
        if let Ok(mut slot) = self.forwarder_task.lock() {
            if let Some(handle) = slot.take() {
                handle.abort();
            }
        }
        self.is_connected.store(false, Ordering::Relaxed);
        log::info!("ThetaDataDataClient disconnected");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> Result<()> {
        let instrument_id = cmd.instrument_id;
        self.register(instrument_id)?;
        let contract = self.contract_for(instrument_id)?;
        let ws = Arc::clone(&self.ws);
        get_runtime().spawn(async move {
            if let Err(e) = ws.subscribe_quotes(&contract).await {
                log::error!("subscribe_quotes failed: {e}");
            }
        });
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> Result<()> {
        let contract = self.contract_for(cmd.instrument_id)?;
        let ws = Arc::clone(&self.ws);
        get_runtime().spawn(async move {
            if let Err(e) = ws.unsubscribe_quotes(&contract).await {
                log::error!("unsubscribe_quotes failed: {e}");
            }
        });
        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> Result<()> {
        let instrument_id = cmd.instrument_id;
        self.register(instrument_id)?;
        let contract = self.contract_for(instrument_id)?;
        let ws = Arc::clone(&self.ws);
        get_runtime().spawn(async move {
            if let Err(e) = ws.subscribe_trades(&contract).await {
                log::error!("subscribe_trades failed: {e}");
            }
        });
        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> Result<()> {
        let contract = self.contract_for(cmd.instrument_id)?;
        let ws = Arc::clone(&self.ws);
        get_runtime().spawn(async move {
            if let Err(e) = ws.unsubscribe_trades(&contract).await {
                log::error!("unsubscribe_trades failed: {e}");
            }
        });
        Ok(())
    }

    fn request_quotes(&self, request: RequestQuotes) -> Result<()> {
        let contract = self.contract_for(request.instrument_id)?;
        let historical = self.historical.clone();
        let data_sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let instrument_id = request.instrument_id;
        let request_id = request.request_id;
        let limit = request.limit.map(|n| n.get());
        let start = match resolve_start(request.start) {
            Ok(d) => d,
            Err(e) => return Err(e),
        };
        let end = resolve_end(request.end);
        let params = request.params.clone();
        let ts_init_resp = now_unix_nanos();

        get_runtime().spawn(async move {
            match fetch_quotes_chunked(
                &historical,
                &contract,
                instrument_id,
                start,
                end,
                limit,
            )
            .await
            {
                Ok(ticks) => {
                    let response = DataResponse::Quotes(QuotesResponse {
                        correlation_id: request_id,
                        client_id,
                        instrument_id,
                        data: ticks,
                        start: Some(date_to_unix_nanos_et(start)),
                        end: Some(UnixNanos::from(
                            u64::from(date_to_unix_nanos_et(end)) + DAY_NANOS_U64 - 1,
                        )),
                        ts_init: ts_init_resp,
                        params,
                    });
                    if let Err(e) = data_sender.send(DataEvent::Response(response)) {
                        log::error!("failed to send quotes response: {e}");
                    }
                }
                Err(e) => log::error!("request_quotes failed: {e}"),
            }
        });
        Ok(())
    }

    fn request_trades(&self, request: RequestTrades) -> Result<()> {
        let contract = self.contract_for(request.instrument_id)?;
        let historical = self.historical.clone();
        let data_sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let instrument_id = request.instrument_id;
        let request_id = request.request_id;
        let limit = request.limit.map(|n| n.get());
        let start = match resolve_start(request.start) {
            Ok(d) => d,
            Err(e) => return Err(e),
        };
        let end = resolve_end(request.end);
        let params = request.params.clone();
        let ts_init_resp = now_unix_nanos();

        get_runtime().spawn(async move {
            match fetch_trades_chunked(
                &historical,
                &contract,
                instrument_id,
                start,
                end,
                limit,
            )
            .await
            {
                Ok(ticks) => {
                    let response = DataResponse::Trades(TradesResponse {
                        correlation_id: request_id,
                        client_id,
                        instrument_id,
                        data: ticks,
                        start: Some(date_to_unix_nanos_et(start)),
                        end: Some(UnixNanos::from(
                            u64::from(date_to_unix_nanos_et(end)) + DAY_NANOS_U64 - 1,
                        )),
                        ts_init: ts_init_resp,
                        params,
                    });
                    if let Err(e) = data_sender.send(DataEvent::Response(response)) {
                        log::error!("failed to send trades response: {e}");
                    }
                }
                Err(e) => log::error!("request_trades failed: {e}"),
            }
        });
        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> Result<()> {
        let bar_type = request.bar_type;
        let instrument_id = bar_type.instrument_id();
        let contract = self.contract_for(instrument_id)?;
        let interval = bar_type_to_interval(&bar_type)
            .with_context(|| format!("unsupported bar_type {bar_type:?}"))?;
        let historical = self.historical.clone();
        let data_sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let limit = request.limit.map(|n| n.get());
        let start = match resolve_start(request.start) {
            Ok(d) => d,
            Err(e) => return Err(e),
        };
        let end = resolve_end(request.end);
        let params = request.params.clone();
        let ts_init_resp = now_unix_nanos();

        get_runtime().spawn(async move {
            match fetch_bars_chunked(
                &historical,
                &contract,
                bar_type,
                interval,
                start,
                end,
                limit,
            )
            .await
            {
                Ok(bars) => {
                    let response = DataResponse::Bars(BarsResponse {
                        correlation_id: request_id,
                        client_id,
                        bar_type,
                        data: bars,
                        ts_init: ts_init_resp,
                        start: Some(date_to_unix_nanos_et(start)),
                        end: Some(UnixNanos::from(
                            u64::from(date_to_unix_nanos_et(end)) + DAY_NANOS_U64 - 1,
                        )),
                        params,
                    });
                    if let Err(e) = data_sender.send(DataEvent::Response(response)) {
                        log::error!("failed to send bars response: {e}");
                    }
                }
                Err(e) => log::error!("request_bars failed: {e}"),
            }
        });
        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> Result<()> {
        let venue = request.venue.unwrap_or(self.venue);
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let params = request.params.clone();
        let ts_init_resp = now_unix_nanos();
        let historical = self.historical.clone();
        let data_sender = self.data_sender.clone();

        // For instrument discovery, ThetaData lists per (root, date). The request gives us no
        // root — that comes from the `params` map. Phase-2's `params` flow isn't wired yet, so
        // for now we emit an empty response and log a TODO. TODO(phase-3): once
        // PARAMS_THETADATA_ROOT / PARAMS_THETADATA_DATE are agreed, fan out a list-contracts
        // call per root in params and aggregate the instruments here.
        let underlyings = extract_roots_from_params(request.params.as_ref());
        let date = extract_date_from_params(request.params.as_ref())
            .or_else(|| Some(today_et()));

        get_runtime().spawn(async move {
            let mut all: Vec<InstrumentAny> = Vec::new();
            if let Some(date) = date {
                for root in underlyings {
                    match historical.list_contracts(&root, date).await {
                        Ok(rows) => {
                            for row in rows {
                                match build_option_contract(&row, venue, ts_init_resp) {
                                    Ok(oc) => all.push(InstrumentAny::OptionContract(oc)),
                                    Err(e) => log::warn!("skip row {row:?}: {e}"),
                                }
                            }
                        }
                        Err(e) => log::error!("list_contracts({root}, {date}) failed: {e}"),
                    }
                }
            }
            let response = DataResponse::Instruments(InstrumentsResponse {
                correlation_id: request_id,
                client_id,
                venue,
                data: all,
                start: None,
                end: None,
                ts_init: ts_init_resp,
                params,
            });
            if let Err(e) = data_sender.send(DataEvent::Response(response)) {
                log::error!("failed to send instruments response: {e}");
            }
        });
        Ok(())
    }
}

// -------------------------------------------------------------------------------------------------
// Date + chunking helpers
// -------------------------------------------------------------------------------------------------

const DAY_NANOS_U64: u64 = 86_400_000_000_000;

fn resolve_start(start: Option<DateTime<Utc>>) -> Result<NaiveDate> {
    start
        .map(|dt| dt.with_timezone(&New_York).date_naive())
        .ok_or_else(|| anyhow!("ThetaData historical requests require an explicit `start`"))
}

fn resolve_end(end: Option<DateTime<Utc>>) -> NaiveDate {
    end.map(|dt| dt.with_timezone(&New_York).date_naive())
        .unwrap_or_else(today_et)
}

fn today_et() -> NaiveDate {
    chrono::Utc::now().with_timezone(&New_York).date_naive()
}

fn date_to_unix_nanos_et(d: NaiveDate) -> UnixNanos {
    let naive = d.and_hms_opt(0, 0, 0).expect("00:00 is always valid");
    let dt = New_York
        .from_local_datetime(&naive)
        .single()
        .unwrap_or_else(|| chrono::Utc.from_utc_datetime(&naive).with_timezone(&New_York));
    let ms = dt.timestamp_millis().max(0) as u64;
    UnixNanos::from(ms * 1_000_000)
}

fn bar_type_to_interval(bar_type: &BarType) -> Option<Interval> {
    let spec = bar_type.spec();
    let step = spec.step.get();
    match (spec.aggregation, step) {
        (BarAggregation::Second, 1) => Some(Interval::S1),
        (BarAggregation::Second, 5) => Some(Interval::S5),
        (BarAggregation::Second, 10) => Some(Interval::S10),
        (BarAggregation::Second, 15) => Some(Interval::S15),
        (BarAggregation::Second, 30) => Some(Interval::S30),
        (BarAggregation::Minute, 1) => Some(Interval::M1),
        (BarAggregation::Minute, 5) => Some(Interval::M5),
        (BarAggregation::Minute, 10) => Some(Interval::M10),
        (BarAggregation::Minute, 15) => Some(Interval::M15),
        (BarAggregation::Minute, 30) => Some(Interval::M30),
        (BarAggregation::Hour, 1) => Some(Interval::H1),
        _ => None,
    }
}

async fn fetch_quotes_chunked(
    http: &ThetaDataHistoricalClient,
    contract: &ThetaOptionContract,
    instrument_id: InstrumentId,
    start: NaiveDate,
    end: NaiveDate,
    limit: Option<usize>,
) -> Result<Vec<QuoteTick>> {
    let chunks = chunk_date_range(start, end)?;
    let mut out: Vec<QuoteTick> = Vec::new();
    let ts_init = now_unix_nanos();
    for (chunk_start, chunk_end) in chunks {
        // Tick interval is the only one valid for multi-day across a chunk's full span if the
        // chunk is a single day. Otherwise we use 1m. For raw quotes the only sensible default
        // is tick on single-day chunks; on multi-day chunks we must accept loss-of-resolution.
        let interval = if chunk_start == chunk_end {
            Interval::Tick
        } else {
            Interval::M1
        };
        let rows = http.hist_quotes(contract, chunk_start, chunk_end, interval).await?;
        for row in rows {
            match row.to_quote_tick(instrument_id, DEFAULT_PRICE_PRECISION, DEFAULT_SIZE_PRECISION, ts_init) {
                Ok(tick) => out.push(tick),
                Err(e) => log::warn!("skip quote row: {e}"),
            }
            if limit.is_some_and(|l| out.len() >= l) {
                out.truncate(limit.unwrap());
                return Ok(out);
            }
        }
    }
    Ok(out)
}

async fn fetch_trades_chunked(
    http: &ThetaDataHistoricalClient,
    contract: &ThetaOptionContract,
    instrument_id: InstrumentId,
    start: NaiveDate,
    end: NaiveDate,
    limit: Option<usize>,
) -> Result<Vec<TradeTick>> {
    let chunks = chunk_date_range(start, end)?;
    let mut out: Vec<TradeTick> = Vec::new();
    let ts_init = now_unix_nanos();
    for (chunk_start, chunk_end) in chunks {
        let rows = http.hist_trades(contract, chunk_start, chunk_end).await?;
        for row in rows {
            match row.to_trade_tick(instrument_id, DEFAULT_PRICE_PRECISION, DEFAULT_SIZE_PRECISION, ts_init) {
                Ok(tick) => out.push(tick),
                Err(e) => log::warn!("skip trade row: {e}"),
            }
            if limit.is_some_and(|l| out.len() >= l) {
                out.truncate(limit.unwrap());
                return Ok(out);
            }
        }
    }
    Ok(out)
}

async fn fetch_bars_chunked(
    http: &ThetaDataHistoricalClient,
    contract: &ThetaOptionContract,
    bar_type: BarType,
    interval: Interval,
    start: NaiveDate,
    end: NaiveDate,
    limit: Option<usize>,
) -> Result<Vec<Bar>> {
    if interval.is_sub_minute() && start != end {
        anyhow::bail!(
            "sub-minute bar interval {} requires a single-day range",
            interval.as_query()
        );
    }
    let chunks = chunk_date_range(start, end)?;
    let mut out: Vec<Bar> = Vec::new();
    let ts_init = now_unix_nanos();
    for (chunk_start, chunk_end) in chunks {
        let rows = http.hist_ohlc(contract, chunk_start, chunk_end, interval).await?;
        for row in rows {
            match row.to_bar(bar_type, DEFAULT_PRICE_PRECISION, DEFAULT_SIZE_PRECISION, ts_init) {
                Ok(bar) => out.push(bar),
                Err(e) => log::warn!("skip ohlc row: {e}"),
            }
            if limit.is_some_and(|l| out.len() >= l) {
                out.truncate(limit.unwrap());
                return Ok(out);
            }
        }
    }
    Ok(out)
}

const PARAMS_THETADATA_ROOTS: &str = "thetadata_roots";
const PARAMS_THETADATA_DATE: &str = "thetadata_date";

fn extract_roots_from_params(params: Option<&nautilus_core::Params>) -> Vec<String> {
    params
        .and_then(|p| p.get_str(PARAMS_THETADATA_ROOTS))
        .map(|s| s.split(',').map(|t| t.trim().to_owned()).filter(|t| !t.is_empty()).collect())
        .unwrap_or_default()
}

fn extract_date_from_params(params: Option<&nautilus_core::Params>) -> Option<NaiveDate> {
    let s = params.and_then(|p| p.get_str(PARAMS_THETADATA_DATE))?;
    NaiveDate::parse_from_str(s, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .ok()
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rstest::*;

    use super::*;
    use crate::enums::OptionRight;

    fn sample_contract() -> ThetaOptionContract {
        ThetaOptionContract::from_dollar_strike(
            "SPXW",
            NaiveDate::from_ymd_opt(2025, 3, 15).unwrap(),
            480.0,
            OptionRight::Call,
        )
        .unwrap()
    }

    #[rstest]
    fn test_contract_key_roundtrip_via_contract() {
        let contract = sample_contract();
        let key = ContractKey::from_contract(&contract);
        assert_eq!(key.root, "SPXW");
        assert_eq!(key.expiration, 20_250_315);
        assert_eq!(key.strike, 480_000);
        assert_eq!(key.right, "C");
    }

    #[rstest]
    fn test_contract_key_from_ws_quote_matches_from_contract() {
        use crate::types::{WsContract, WsHeader, WsQuoteBody};
        let contract = sample_contract();
        let key_from_contract = ContractKey::from_contract(&contract);

        let frame = WsQuoteFrame {
            header: WsHeader { status: "CONNECTED".into(), kind: "QUOTE".into() },
            contract: WsContract {
                security_type: "OPTION".into(),
                root: "SPXW".into(),
                expiration: 20_250_315,
                strike: 480_000,
                right: "C".into(),
            },
            quote: WsQuoteBody {
                ms_of_day: 0,
                bid_size: 0,
                bid_exchange: 0,
                bid: 0.0,
                bid_condition: 0,
                ask_size: 0,
                ask_exchange: 0,
                ask: 0.0,
                ask_condition: 0,
                date: 20_250_315,
            },
        };
        let key_from_ws = ContractKey::from_ws_quote(&frame);
        assert_eq!(key_from_contract, key_from_ws);
    }
}
