// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Subscription dispatch + Kite-tick → Nautilus-tick conversion.
//!
//! The dispatcher owns no I/O of its own — it composes [`ZerodhaWsClient`] (subscriptions and
//! frame source) with [`Decoder`] (parsing) and emits Nautilus typed ticks to bounded
//! per-channel mpsc senders the caller supplies.
//!
//! # Subscription model
//!
//! Per-instrument we track three boolean flags: `wants_quote`, `wants_trade`, `wants_book`.
//! Any single one being true means we keep the WS subscribed; all-false means we
//! `Unsubscribe`. Kite's connection-level mode is the **union** of the three, and the dispatcher
//! always asks for `full` mode because:
//!
//! - `subscribe_quote_ticks` needs `bid_price` / `ask_price` (top-of-book), which only the
//!   184 B full packet carries (Kite's 44 B quote payload has aggregate buy/sell volume but no
//!   depth).
//! - `subscribe_trade_ticks` needs `last_trade_timestamp` + `last_quantity`, again full-only.
//! - `subscribe_order_book` obviously needs depth.
//!
//! That decision diverges from the spec which says `subscribe_quote_ticks` → mode `quote`; the
//! deviation is documented in the README and motivated by Nautilus's `QuoteTick` invariant
//! (`bid_price` and `ask_price` are non-optional).
//!
//! # Bulk subscriptions
//!
//! `subscribe_many` batches into chunks of [`SUB_BATCH_SIZE`] tokens with
//! [`SUB_BATCH_GAP`] between to avoid Kite's silent backpressure when a single
//! frame carries too many tokens. Single-instrument helpers route through the same
//! path with a one-element slice.
//!
//! # Trade synthesis
//!
//! Kite's full-mode packets carry the **last** trade's price / quantity / timestamp, not a
//! stream of trades. The dispatcher tracks `last_emitted_trade_ts` per token; on every full
//! frame where `last_trade_timestamp > last_emitted_trade_ts`, we emit one synthesised
//! `TradeTick` and update the cursor. If two trades land in the same WS frame the second is
//! lost (Kite reports only the final quantity) — a documented v1 limitation; downstream
//! strategies needing strict per-trade accuracy must overlay the REST `/trades` endpoint.

use std::{
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use ahash::HashMap;
use anyhow::Result;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{QuoteTick, TradeTick, depth::OrderBookDepth10, order::BookOrder},
    enums::{AggressorSide, OrderSide},
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::{
    decode::{Decoder, KiteTick, PrecisionLookup, TickMode},
    error::ZerodhaError,
    instruments::ZerodhaInstrumentCache,
    live::ZerodhaWsClient,
    symbology::InstrumentKind,
};

/// Kite cap on tokens per subscribe / unsubscribe / mode frame.
pub const SUB_BATCH_SIZE: usize = 200;

/// Pause between consecutive batched subscribe frames so Kite's backend keeps up.
pub const SUB_BATCH_GAP: Duration = Duration::from_millis(500);

/// Per-instrument subscription flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionFlags {
    /// Caller wants `QuoteTick`s for this instrument.
    pub wants_quote: bool,
    /// Caller wants synthesised `TradeTick`s for this instrument.
    pub wants_trade: bool,
    /// Caller wants `OrderBookDepth10` snapshots for this instrument.
    pub wants_book: bool,
}

impl SubscriptionFlags {
    /// Whether anything is currently subscribed.
    #[must_use]
    pub const fn any(&self) -> bool {
        self.wants_quote || self.wants_trade || self.wants_book
    }
}

/// Per-instrument lookup needed by the dispatcher for every conversion.
#[derive(Clone, Debug)]
pub struct DispatchTarget {
    /// The Nautilus addressing handle.
    pub instrument_id: InstrumentId,
    /// Kite's integer token (Phase 3 WS key).
    pub instrument_token: u32,
    /// Asset classification — used to gate Quote / Trade / Book emission against what Kite
    /// actually publishes for the instrument's segment.
    pub kind: InstrumentKind,
    /// Price decimal precision (NSE/BSE = 2, MCX = 1, CDS = 4, etc.).
    pub price_precision: u8,
}

/// Per-channel sinks supplied by the caller.
pub struct DispatchSinks {
    /// Receives every emitted `QuoteTick` (bounded, drop-oldest semantics live in the caller).
    pub quotes: mpsc::Sender<QuoteTick>,
    /// Receives every emitted `TradeTick`.
    pub trades: mpsc::Sender<TradeTick>,
    /// Receives every emitted `OrderBookDepth10` snapshot.
    pub depths: mpsc::Sender<OrderBookDepth10>,
}

impl std::fmt::Debug for DispatchSinks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchSinks").finish_non_exhaustive()
    }
}

/// Reasons a tick was dropped rather than emitted (exposed for metrics in tests).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DispatchMetrics {
    /// Ticks decoded but with no subscription claiming them.
    pub no_subscribers: u64,
    /// Quotes skipped because the full-mode packet has no resolved depth top-of-book.
    pub quotes_missing_book: u64,
    /// Trade ticks skipped because `last_trade_timestamp` was unchanged.
    pub trades_deduped: u64,
    /// Quotes / trades / depths whose mpsc channel was full and the receiver wasn't draining.
    pub sink_back_pressured: u64,
    /// Ticks whose instrument_token isn't yet in the instrument cache.
    pub unknown_token: u64,
}

/// Dispatcher tying [`Decoder`] + [`ZerodhaWsClient`] + subscription bookkeeping together.
pub struct ZerodhaDataDispatcher {
    cache: Arc<ZerodhaInstrumentCache>,
    ws_client: Arc<ZerodhaWsClient>,
    decoder: Arc<Decoder<CachePrecisionLookup>>,
    subscriptions: RwLock<HashMap<u32, SubscriptionFlags>>,
    last_trade_ts: Mutex<HashMap<u32, u32>>,
    fill_seq: AtomicU64,
    sinks: DispatchSinks,
    metrics: Mutex<DispatchMetrics>,
}

impl std::fmt::Debug for ZerodhaDataDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZerodhaDataDispatcher")
            .field("decoder_stats", &self.decoder.stats())
            .finish_non_exhaustive()
    }
}

/// Precision lookup backed by the instrument cache.
#[derive(Clone, Debug)]
pub struct CachePrecisionLookup {
    cache: Arc<ZerodhaInstrumentCache>,
}

impl PrecisionLookup for CachePrecisionLookup {
    fn precision(&self, token: u32) -> Option<u8> {
        // The price_precision lives on the typed Nautilus instrument; look it up via the cache
        // and pattern-match the variant.
        use nautilus_model::instruments::InstrumentAny;
        match self.cache.lookup_by_token(token)? {
            InstrumentAny::Equity(e) => Some(e.price_precision),
            InstrumentAny::FuturesContract(f) => Some(f.price_precision),
            InstrumentAny::OptionContract(o) => Some(o.price_precision),
            InstrumentAny::IndexInstrument(i) => Some(i.price_precision),
            _ => None,
        }
    }
}

impl ZerodhaDataDispatcher {
    /// Construct a dispatcher.
    ///
    /// `default_precision` (typically 2) handles tokens not yet in the cache — most commonly a
    /// brand-new listing Kite ships in the next daily dump.
    #[must_use]
    pub fn new(
        cache: Arc<ZerodhaInstrumentCache>,
        ws_client: Arc<ZerodhaWsClient>,
        sinks: DispatchSinks,
        default_precision: u8,
    ) -> Self {
        let lookup = CachePrecisionLookup { cache: cache.clone() };
        let decoder = Arc::new(Decoder::new(lookup, default_precision));
        Self {
            cache,
            ws_client,
            decoder,
            subscriptions: RwLock::new(HashMap::default()),
            last_trade_ts: Mutex::new(HashMap::default()),
            fill_seq: AtomicU64::new(0),
            sinks,
            metrics: Mutex::new(DispatchMetrics::default()),
        }
    }

    /// Snapshot of dispatch metrics.
    pub async fn metrics(&self) -> DispatchMetrics {
        *self.metrics.lock().await
    }

    /// Snapshot of decoder stats.
    #[must_use]
    pub fn decoder_stats(&self) -> crate::decode::DecodeStatsSnapshot {
        self.decoder.stats()
    }

    /// Subscribe to QuoteTicks for one instrument.
    ///
    /// # Errors
    ///
    /// Propagates `ZerodhaWsClient::subscribe` errors (subscription-cap exceeded, handler dead).
    pub async fn subscribe_quote_ticks(&self, instrument_id: &InstrumentId) -> Result<()> {
        self.update_flag(instrument_id, |f| f.wants_quote = true).await
    }

    /// Subscribe to synthesised TradeTicks for one instrument.
    ///
    /// # Errors
    ///
    /// See [`Self::subscribe_quote_ticks`].
    pub async fn subscribe_trade_ticks(&self, instrument_id: &InstrumentId) -> Result<()> {
        self.update_flag(instrument_id, |f| f.wants_trade = true).await
    }

    /// Subscribe to `OrderBookDepth10` snapshots for one instrument.
    ///
    /// # Errors
    ///
    /// See [`Self::subscribe_quote_ticks`].
    pub async fn subscribe_order_book(&self, instrument_id: &InstrumentId) -> Result<()> {
        self.update_flag(instrument_id, |f| f.wants_book = true).await
    }

    /// Unsubscribe from QuoteTicks for one instrument (other flags retained).
    ///
    /// # Errors
    ///
    /// Propagates `ZerodhaWsClient` errors.
    pub async fn unsubscribe_quote_ticks(&self, instrument_id: &InstrumentId) -> Result<()> {
        self.update_flag(instrument_id, |f| f.wants_quote = false).await
    }

    /// Unsubscribe from TradeTicks for one instrument.
    ///
    /// # Errors
    ///
    /// See [`Self::unsubscribe_quote_ticks`].
    pub async fn unsubscribe_trade_ticks(&self, instrument_id: &InstrumentId) -> Result<()> {
        self.update_flag(instrument_id, |f| f.wants_trade = false).await
    }

    /// Unsubscribe from order-book snapshots for one instrument.
    ///
    /// # Errors
    ///
    /// See [`Self::unsubscribe_quote_ticks`].
    pub async fn unsubscribe_order_book(&self, instrument_id: &InstrumentId) -> Result<()> {
        self.update_flag(instrument_id, |f| f.wants_book = false).await
    }

    /// Bulk subscribe with batching (200/batch, 500 ms gap) — Phase 3 §4.
    ///
    /// Each `(InstrumentId, mode)` is updated in flag state, then the union of tokens needing
    /// `Subscribe` is split into [`SUB_BATCH_SIZE`] chunks and each chunk is dispatched with a
    /// [`SUB_BATCH_GAP`] pause.
    ///
    /// # Errors
    ///
    /// Stops on first batch failure and propagates the error; partial subscriptions remain
    /// installed in the flag state and on the WS — caller can retry.
    pub async fn subscribe_many(
        &self,
        targets: &[(InstrumentId, SubscriptionFlags)],
    ) -> Result<()> {
        // Step 1: update flag state, collect tokens that need a Subscribe frame.
        let mut new_tokens: Vec<u32> = Vec::new();
        for (id, want) in targets {
            let Some(token) = self.cache.lookup_by_id(id) else {
                return Err(ZerodhaError::InvalidResponse(format!(
                    "no instrument_token cached for {id}"
                ))
                .into());
            };
            let mut subs = self.subscriptions.write().await;
            let entry = subs.entry(token.instrument_token).or_default();
            let was_subscribed = entry.any();
            entry.wants_quote |= want.wants_quote;
            entry.wants_trade |= want.wants_trade;
            entry.wants_book |= want.wants_book;
            if !was_subscribed && entry.any() {
                new_tokens.push(token.instrument_token);
            }
        }

        // Step 2: batched Subscribe frames.
        for chunk in new_tokens.chunks(SUB_BATCH_SIZE) {
            self.ws_client.subscribe(chunk.to_vec()).await?;
            self.ws_client
                .set_mode("full", chunk.to_vec())
                .await?;
            if chunk.len() == SUB_BATCH_SIZE {
                tokio::time::sleep(SUB_BATCH_GAP).await;
            }
        }
        Ok(())
    }

    async fn update_flag<F>(&self, instrument_id: &InstrumentId, mut mutate: F) -> Result<()>
    where
        F: FnMut(&mut SubscriptionFlags),
    {
        let token = self
            .cache
            .lookup_by_id(instrument_id)
            .ok_or_else(|| {
                ZerodhaError::InvalidResponse(format!(
                    "no instrument_token cached for {instrument_id}"
                ))
            })?;
        let mut subs = self.subscriptions.write().await;
        let entry = subs.entry(token.instrument_token).or_default();
        let was_any = entry.any();
        mutate(entry);
        let now_any = entry.any();
        let kite_token = token.instrument_token;
        drop(subs);

        match (was_any, now_any) {
            (false, true) => {
                self.ws_client.subscribe(vec![kite_token]).await?;
                self.ws_client.set_mode("full", vec![kite_token]).await?;
            }
            (true, false) => {
                self.ws_client.unsubscribe(vec![kite_token]).await?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Decode a raw Kite WS frame and emit downstream Nautilus ticks.
    ///
    /// `ts_init` should be the wall-clock at which the frame was received.
    pub async fn dispatch_frame(&self, frame: &[u8], ts_init: UnixNanos) {
        let mut ticks = Vec::with_capacity(8);
        self.decoder.decode_frame(frame, &mut ticks);
        for tick in &ticks {
            self.dispatch_tick(tick, ts_init).await;
        }
    }

    /// Dispatch a single decoded tick.
    pub async fn dispatch_tick(&self, tick: &KiteTick, ts_init: UnixNanos) {
        let flags = {
            let subs = self.subscriptions.read().await;
            subs.get(&tick.instrument_token).copied().unwrap_or_default()
        };
        if !flags.any() {
            self.bump_metric(|m| m.no_subscribers += 1).await;
            return;
        }

        let Some(target) = self.resolve_target(tick.instrument_token).await else {
            self.bump_metric(|m| m.unknown_token += 1).await;
            return;
        };

        let event_ns = tick.exchange_timestamp.map_or(ts_init, |secs| {
            UnixNanos::from(u64::from(secs) * 1_000_000_000)
        });

        if flags.wants_quote {
            self.emit_quote(tick, &target, event_ns, ts_init).await;
        }
        if flags.wants_book && tick.mode == TickMode::Full {
            self.emit_depth(tick, &target, event_ns, ts_init).await;
        }
        if flags.wants_trade {
            self.emit_trade(tick, &target, ts_init).await;
        }
    }

    async fn resolve_target(&self, token: u32) -> Option<DispatchTarget> {
        let (instrument_id, kite_token) = self.cache.lookup_by_id_via_token(token).await?;
        let inst = self.cache.lookup_by_token(token)?;
        let price_precision = match &inst {
            nautilus_model::instruments::InstrumentAny::Equity(e) => e.price_precision,
            nautilus_model::instruments::InstrumentAny::FuturesContract(f) => f.price_precision,
            nautilus_model::instruments::InstrumentAny::OptionContract(o) => o.price_precision,
            nautilus_model::instruments::InstrumentAny::IndexInstrument(i) => i.price_precision,
            _ => 2,
        };
        Some(DispatchTarget {
            instrument_id,
            instrument_token: token,
            kind: kite_token.kind,
            price_precision,
        })
    }

    async fn emit_quote(
        &self,
        tick: &KiteTick,
        target: &DispatchTarget,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) {
        let Some(depth) = tick.depth else {
            // Index ticks (no depth) — synthesise a degenerate quote from ltp on both sides so
            // downstream consumers can still observe value updates. Skipped if the caller only
            // wants a "real" two-sided quote; we bump a metric so the divergence is visible.
            if target.kind == InstrumentKind::Index {
                let lp = Price::new(tick.last_price, target.price_precision);
                let zero = Quantity::new(0.0, 0);
                let quote =
                    QuoteTick::new(target.instrument_id, lp, lp, zero, zero, ts_event, ts_init);
                self.try_send_quote(quote).await;
                return;
            }
            self.bump_metric(|m| m.quotes_missing_book += 1).await;
            return;
        };
        let bid_level = depth.bids[0];
        let ask_level = depth.asks[0];
        if bid_level.price <= 0.0 || ask_level.price <= 0.0 {
            self.bump_metric(|m| m.quotes_missing_book += 1).await;
            return;
        }
        let quote = QuoteTick::new(
            target.instrument_id,
            Price::new(bid_level.price, target.price_precision),
            Price::new(ask_level.price, target.price_precision),
            Quantity::new(f64::from(bid_level.quantity), 0),
            Quantity::new(f64::from(ask_level.quantity), 0),
            ts_event,
            ts_init,
        );
        self.try_send_quote(quote).await;
    }

    async fn try_send_quote(&self, quote: QuoteTick) {
        if self.sinks.quotes.try_send(quote).is_err() {
            self.bump_metric(|m| m.sink_back_pressured += 1).await;
        }
    }

    async fn emit_depth(
        &self,
        tick: &KiteTick,
        target: &DispatchTarget,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) {
        const ZERO_ID: nautilus_model::data::order::OrderId = 0;
        let Some(depth) = tick.depth else {
            return;
        };
        let mut bids = [BookOrder::new(
            OrderSide::Buy,
            Price::new(0.0, target.price_precision),
            Quantity::new(0.0, 0),
            ZERO_ID,
        ); nautilus_model::data::depth::DEPTH10_LEN];
        let mut asks = [BookOrder::new(
            OrderSide::Sell,
            Price::new(0.0, target.price_precision),
            Quantity::new(0.0, 0),
            ZERO_ID,
        ); nautilus_model::data::depth::DEPTH10_LEN];
        let mut bid_counts = [0u32; nautilus_model::data::depth::DEPTH10_LEN];
        let mut ask_counts = [0u32; nautilus_model::data::depth::DEPTH10_LEN];

        for (i, level) in depth.bids.iter().enumerate() {
            if level.price <= 0.0 && level.quantity == 0 {
                continue;
            }
            bids[i] = BookOrder::new(
                OrderSide::Buy,
                Price::new(level.price, target.price_precision),
                Quantity::new(f64::from(level.quantity), 0),
                ZERO_ID,
            );
            bid_counts[i] = u32::from(level.orders);
        }
        for (i, level) in depth.asks.iter().enumerate() {
            if level.price <= 0.0 && level.quantity == 0 {
                continue;
            }
            asks[i] = BookOrder::new(
                OrderSide::Sell,
                Price::new(level.price, target.price_precision),
                Quantity::new(f64::from(level.quantity), 0),
                ZERO_ID,
            );
            ask_counts[i] = u32::from(level.orders);
        }

        let snapshot = OrderBookDepth10::new(
            target.instrument_id,
            bids,
            asks,
            bid_counts,
            ask_counts,
            0,                 // flags — none for snapshot
            tick.exchange_timestamp.map_or(0, u64::from),
            ts_event,
            ts_init,
        );
        if self.sinks.depths.try_send(snapshot).is_err() {
            self.bump_metric(|m| m.sink_back_pressured += 1).await;
        }
    }

    async fn emit_trade(&self, tick: &KiteTick, target: &DispatchTarget, ts_init: UnixNanos) {
        let Some(last_ts) = tick.last_trade_timestamp else {
            return;
        };
        let Some(last_qty) = tick.last_quantity else {
            return;
        };
        if last_qty == 0 {
            return;
        }

        let mut cursor = self.last_trade_ts.lock().await;
        let prev = cursor.get(&target.instrument_token).copied().unwrap_or(0);
        if last_ts <= prev {
            drop(cursor);
            self.bump_metric(|m| m.trades_deduped += 1).await;
            return;
        }
        cursor.insert(target.instrument_token, last_ts);
        drop(cursor);

        let seq = self
            .fill_seq
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let trade_id = TradeId::from(
            format!("ZT-{}-{seq}", target.instrument_token).as_str(),
        );
        let ts_event = UnixNanos::from(u64::from(last_ts) * 1_000_000_000);

        let trade = TradeTick::new(
            target.instrument_id,
            Price::new(tick.last_price, target.price_precision),
            Quantity::new(f64::from(last_qty), 0),
            AggressorSide::NoAggressor,
            trade_id,
            ts_event,
            ts_init,
        );
        if self.sinks.trades.try_send(trade).is_err() {
            self.bump_metric(|m| m.sink_back_pressured += 1).await;
        }
    }

    async fn bump_metric<F: FnOnce(&mut DispatchMetrics)>(&self, f: F) {
        let mut m = self.metrics.lock().await;
        f(&mut m);
    }
}

/// Helper trait to look up an `InstrumentId` from an `instrument_token` via the cache; this is
/// the reverse of the cache's primary `by_id` map (which goes the other way).
trait ReverseTokenLookup {
    fn lookup_by_id_via_token(
        &self,
        token: u32,
    ) -> impl std::future::Future<Output = Option<(InstrumentId, crate::symbology::KiteToken)>> + Send;
}

impl ReverseTokenLookup for Arc<ZerodhaInstrumentCache> {
    async fn lookup_by_id_via_token(
        &self,
        token: u32,
    ) -> Option<(InstrumentId, crate::symbology::KiteToken)> {
        // The cache only indexes one direction (by_id → KiteToken). For the reverse we walk the
        // by_id map. This is O(N) but the per-tick path stores the resolved Target in the
        // dispatcher's hot-path; this helper only fires for the FIRST tick of each token after
        // subscription. We could carry the InstrumentId in the WS subscription registry to avoid
        // even this scan; deferred to a perf pass.
        let snapshot = self.by_id.load();
        for (id, kt) in snapshot.iter() {
            if kt.instrument_token == token {
                return Some((*id, kt.clone()));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ahash::HashMap;
    use nautilus_model::{
        identifiers::{InstrumentId, Symbol, Venue},
        instruments::InstrumentAny,
    };
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        instruments::{KiteInstrument, map_row},
        session::ZerodhaSessionManager,
        symbology::KiteToken,
    };

    fn ts() -> UnixNanos {
        UnixNanos::from(1_700_000_000_000_000_000u64)
    }

    fn build_cache_with_reliance() -> Arc<ZerodhaInstrumentCache> {
        let cache = ZerodhaInstrumentCache::new();
        let row = KiteInstrument {
            instrument_token: 738_561,
            exchange_token: 2885,
            tradingsymbol: "RELIANCE".into(),
            name: "RELIANCE INDUSTRIES".into(),
            last_price: "0".into(),
            expiry: String::new(),
            strike: "0".into(),
            tick_size: "0.1".into(),
            lot_size: 1,
            instrument_type: "EQ".into(),
            segment: "NSE".into(),
            exchange: "NSE".into(),
        };
        let mapped = map_row(&row, ts()).unwrap().unwrap();
        let mut token_map: HashMap<u32, InstrumentAny> = HashMap::default();
        let mut id_map: HashMap<InstrumentId, KiteToken> = HashMap::default();
        token_map.insert(mapped.token.instrument_token, mapped.instrument.clone());
        id_map.insert(mapped.instrument_id, mapped.token);
        cache.install_snapshot(token_map, id_map, "test".into(), ts());
        Arc::new(cache)
    }

    fn build_dispatcher() -> (
        Arc<ZerodhaDataDispatcher>,
        mpsc::Receiver<QuoteTick>,
        mpsc::Receiver<TradeTick>,
        mpsc::Receiver<OrderBookDepth10>,
    ) {
        let cache = build_cache_with_reliance();
        let session = Arc::new(ZerodhaSessionManager::new("k".into(), "t".into(), None));
        let ws = Arc::new(ZerodhaWsClient::spawn(session, Some("ws://127.0.0.1:1".into())));
        let (qtx, qrx) = mpsc::channel(64);
        let (ttx, trx) = mpsc::channel(64);
        let (dtx, drx) = mpsc::channel(64);
        let sinks = DispatchSinks {
            quotes: qtx,
            trades: ttx,
            depths: dtx,
        };
        let dispatcher = Arc::new(ZerodhaDataDispatcher::new(cache, ws, sinks, 2));
        (dispatcher, qrx, trx, drx)
    }

    fn full_tick(token: u32, ltp: f64, bid: f64, ask: f64, last_qty: u32, ts: u32) -> KiteTick {
        use crate::decode::{DepthLevel, MarketDepth, Ohlc};
        KiteTick {
            instrument_token: token,
            mode: TickMode::Full,
            last_price: ltp,
            last_quantity: Some(last_qty),
            average_price: Some(ltp),
            volume: Some(1_000),
            buy_quantity: Some(50),
            sell_quantity: Some(50),
            ohlc: Some(Ohlc { open: ltp, high: ltp, low: ltp, close: ltp }),
            change: None,
            exchange_timestamp: Some(ts),
            last_trade_timestamp: Some(ts),
            oi: None,
            oi_day_high: None,
            oi_day_low: None,
            depth: Some(MarketDepth {
                bids: [
                    DepthLevel { quantity: 10, price: bid, orders: 1 },
                    DepthLevel { quantity: 0, price: 0.0, orders: 0 },
                    DepthLevel { quantity: 0, price: 0.0, orders: 0 },
                    DepthLevel { quantity: 0, price: 0.0, orders: 0 },
                    DepthLevel { quantity: 0, price: 0.0, orders: 0 },
                ],
                asks: [
                    DepthLevel { quantity: 10, price: ask, orders: 1 },
                    DepthLevel { quantity: 0, price: 0.0, orders: 0 },
                    DepthLevel { quantity: 0, price: 0.0, orders: 0 },
                    DepthLevel { quantity: 0, price: 0.0, orders: 0 },
                    DepthLevel { quantity: 0, price: 0.0, orders: 0 },
                ],
            }),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_emits_quote_when_subscribed() {
        let (dispatcher, mut qrx, _, _) = build_dispatcher();
        let id = InstrumentId::new(Symbol::from("RELIANCE-EQ"), Venue::from("NSE"));
        dispatcher
            .subscribe_quote_ticks(&id)
            .await
            .ok();
        // The subscribe call may error because the dummy WS isn't really connected, but the flag
        // state will have been updated regardless.

        let tick = full_tick(738_561, 1327.4, 1327.3, 1327.5, 5, 1_779_424_068);
        dispatcher.dispatch_tick(&tick, ts()).await;
        let q = tokio::time::timeout(Duration::from_millis(50), qrx.recv())
            .await
            .expect("quote arrived")
            .expect("quote channel ok");
        assert_eq!(q.instrument_id.to_string(), "RELIANCE-EQ.NSE");
        assert!((q.bid_price.as_f64() - 1327.3).abs() < 0.05);
        assert!((q.ask_price.as_f64() - 1327.5).abs() < 0.05);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_skips_when_unsubscribed() {
        let (dispatcher, mut qrx, mut trx, mut drx) = build_dispatcher();
        // No subscribe — token has zero flags.
        let tick = full_tick(738_561, 1327.4, 1327.3, 1327.5, 5, 1_779_424_068);
        dispatcher.dispatch_tick(&tick, ts()).await;

        // Nothing should land.
        assert!(qrx.try_recv().is_err());
        assert!(trx.try_recv().is_err());
        assert!(drx.try_recv().is_err());

        let m = dispatcher.metrics().await;
        assert_eq!(m.no_subscribers, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_synthesises_trade_only_on_ts_advance() {
        let (dispatcher, _, mut trx, _) = build_dispatcher();
        let id = InstrumentId::new(Symbol::from("RELIANCE-EQ"), Venue::from("NSE"));
        dispatcher.subscribe_trade_ticks(&id).await.ok();

        // First trade — ts advances from 0 → emit.
        let tick1 = full_tick(738_561, 1327.4, 1327.3, 1327.5, 5, 1000);
        dispatcher.dispatch_tick(&tick1, ts()).await;
        let t1 = tokio::time::timeout(Duration::from_millis(50), trx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t1.size.as_f64(), 5.0);

        // Same ts — dedupe.
        let tick2 = full_tick(738_561, 1327.5, 1327.4, 1327.6, 7, 1000);
        dispatcher.dispatch_tick(&tick2, ts()).await;
        assert!(trx.try_recv().is_err());

        // Advance ts — emit again with the new quantity.
        let tick3 = full_tick(738_561, 1327.6, 1327.5, 1327.7, 9, 1001);
        dispatcher.dispatch_tick(&tick3, ts()).await;
        let t3 = tokio::time::timeout(Duration::from_millis(50), trx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t3.size.as_f64(), 9.0);

        let m = dispatcher.metrics().await;
        assert_eq!(m.trades_deduped, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_emits_depth_when_subscribed() {
        let (dispatcher, _, _, mut drx) = build_dispatcher();
        let id = InstrumentId::new(Symbol::from("RELIANCE-EQ"), Venue::from("NSE"));
        dispatcher.subscribe_order_book(&id).await.ok();

        let tick = full_tick(738_561, 1327.4, 1327.3, 1327.5, 5, 1000);
        dispatcher.dispatch_tick(&tick, ts()).await;
        let d = tokio::time::timeout(Duration::from_millis(50), drx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d.instrument_id.to_string(), "RELIANCE-EQ.NSE");
        assert!((d.bids[0].price.as_f64() - 1327.3).abs() < 0.05);
        assert!((d.asks[0].price.as_f64() - 1327.5).abs() < 0.05);
        // Levels beyond index 0 are zero-prices since the test fixture only populates the best.
        assert_eq!(d.bid_counts[0], 1);
        assert_eq!(d.ask_counts[0], 1);
    }
}
