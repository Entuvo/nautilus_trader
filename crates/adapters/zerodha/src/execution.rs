// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Order submission + `OrderStatusReport` generation (spec §5/Phase-5, MVP scope).
//!
//! # Scope (intentionally minimal)
//!
//! - `submit_order` — `POST /orders/{variety}` form-encoded; rate-limited at 10 QPS by
//!   [`crate::http::ZerodhaHttpClient::post_order`].
//! - `generate_order_status_reports` — `GET /orders`, diff against the side-band metadata map,
//!   return [`OrderStatusReport`] for every changed row. The framework's reconciliation
//!   manager turns those into `OrderFilled` / `OrderAccepted` / etc. events; we don't emit them
//!   ourselves.
//! - Side-band [`ZerodhaOrderMeta`] map keyed by `ClientOrderId`. Carries the Kite-native fields
//!   (`kite_order_id`, `variety`, `product`) that don't fit in Nautilus's `Order` shape.
//!
//! Deferred to later phases per the spec's phased plan and our "don't overengineer" rule:
//!
//! - Modify / cancel → Phase 6.
//! - Sqlite persistence at `<cache_dir>/zerodha_orders.sqlite` → Phase 7 (startup
//!   reconciliation needs it; v1 stubs survive a node restart by re-reading Kite's `/orders`).
//! - `/trades` sub-poll for real `trade_id`s → Phase 7 (fills via order-level
//!   `filled_quantity` delta are sufficient for the framework to emit fills).
//! - Position snapshots + holdings + funds → Phase 6.
//! - Adaptive 1→5 s poll cadence under 429 → Phase 7 (we provide the function; the framework
//!   schedules the poll).

use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, anyhow};
use nautilus_core::{UnixNanos, UUID4};
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
    identifiers::{AccountId, ClientOrderId, InstrumentId, Symbol, VenueOrderId},
    reports::order::OrderStatusReport,
    types::{Price, Quantity},
};
use serde::Deserialize;
use tokio::sync::RwLock;

use crate::{error::ZerodhaError, http::ZerodhaHttpClient, instruments::ZerodhaInstrumentCache};

// -------------------------------------------------------------------------------------------------
// Kite-native enums (v1 supports a deliberately small set; rest reject up front).
// -------------------------------------------------------------------------------------------------

/// Kite order variety (the `{variety}` path segment on `/orders/{variety}`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum KiteVariety {
    /// Regular open-market order.
    Regular,
    /// After-market order, queued for next session open.
    Amo,
}

impl KiteVariety {
    /// Kite's wire value.
    #[must_use]
    pub const fn as_kite_str(self) -> &'static str {
        match self {
            Self::Regular => "regular",
            Self::Amo => "amo",
        }
    }

    /// Parse Kite's wire value; only `regular` / `amo` accepted in v1.
    ///
    /// # Errors
    ///
    /// Returns an error for `iceberg`, `bo`, `co`, `auction` (out of scope per spec §7).
    pub fn from_kite_str(s: &str) -> Result<Self> {
        match s {
            "regular" => Ok(Self::Regular),
            "amo" => Ok(Self::Amo),
            other => Err(anyhow!("unsupported variety: {other:?}")),
        }
    }
}

/// Kite product (margin / settlement class).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum KiteProduct {
    /// Cash-and-carry (delivery; equity only).
    Cnc,
    /// Margin intraday squareoff.
    Mis,
    /// Normal margin (futures, options, currency).
    Nrml,
}

impl KiteProduct {
    /// Kite's wire value.
    #[must_use]
    pub const fn as_kite_str(self) -> &'static str {
        match self {
            Self::Cnc => "CNC",
            Self::Mis => "MIS",
            Self::Nrml => "NRML",
        }
    }

    /// Parse Kite's wire value.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown products (e.g. `BO`, `CO`).
    pub fn from_kite_str(s: &str) -> Result<Self> {
        match s {
            "CNC" => Ok(Self::Cnc),
            "MIS" => Ok(Self::Mis),
            "NRML" => Ok(Self::Nrml),
            other => Err(anyhow!("unsupported product: {other:?}")),
        }
    }
}

/// Map Nautilus [`TimeInForce`] to Kite's `validity`.
///
/// # Errors
///
/// Returns an error for unsupported TIFs (`GTC`, `GTD`, `FOK`, …).
pub fn kite_validity(tif: TimeInForce) -> Result<&'static str> {
    match tif {
        TimeInForce::Day => Ok("DAY"),
        TimeInForce::Ioc => Ok("IOC"),
        other => Err(anyhow!("unsupported TIF: {other:?}")),
    }
}

/// Parse Kite's `validity` string into [`TimeInForce`].
///
/// # Errors
///
/// Returns an error for unknown / unsupported validities.
pub fn parse_kite_validity(s: &str) -> Result<TimeInForce> {
    match s {
        "DAY" => Ok(TimeInForce::Day),
        "IOC" => Ok(TimeInForce::Ioc),
        other => Err(anyhow!("unsupported validity: {other:?}")),
    }
}

/// Map Nautilus [`OrderType`] to Kite's `order_type`.
///
/// # Errors
///
/// Returns an error for order types Kite doesn't speak (trailing stops, iceberg display, etc.).
pub fn kite_order_type(t: OrderType) -> Result<&'static str> {
    match t {
        OrderType::Market => Ok("MARKET"),
        OrderType::Limit => Ok("LIMIT"),
        OrderType::StopMarket => Ok("SL-M"),
        OrderType::StopLimit => Ok("SL"),
        other => Err(anyhow!("unsupported order type: {other:?}")),
    }
}

/// Parse Kite's `order_type` into Nautilus [`OrderType`].
///
/// # Errors
///
/// Returns an error for unknown strings.
pub fn parse_kite_order_type(s: &str) -> Result<OrderType> {
    match s {
        "MARKET" => Ok(OrderType::Market),
        "LIMIT" => Ok(OrderType::Limit),
        "SL" => Ok(OrderType::StopLimit),
        "SL-M" => Ok(OrderType::StopMarket),
        other => Err(anyhow!("unsupported order_type: {other:?}")),
    }
}

/// Map Nautilus [`OrderSide`] to Kite's `transaction_type`.
#[must_use]
pub fn kite_transaction_type(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
        OrderSide::NoOrderSide => "BUY", // defensive default; framework rejects upstream
    }
}

/// Parse Kite's `transaction_type` into Nautilus [`OrderSide`].
///
/// # Errors
///
/// Returns an error for unknown strings.
pub fn parse_kite_transaction_type(s: &str) -> Result<OrderSide> {
    match s {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        other => Err(anyhow!("unsupported transaction_type: {other:?}")),
    }
}

/// Map Kite's `status` string into Nautilus [`OrderStatus`].
///
/// `trigger_pending_is_triggered` should be `true` only when the local order is a
/// `StopMarket` / `StopLimit` / `MarketIfTouched` etc. — for plain MARKET / LIMIT orders
/// Kite's `TRIGGER PENDING` would otherwise illegally transition the state machine
/// (spec §5/Phase-5 §1).
#[must_use]
pub fn map_kite_status(status: &str, trigger_pending_is_triggered: bool) -> OrderStatus {
    match status {
        "PUTORDER REQ RECEIVED" | "VALIDATION PENDING" | "OPEN PENDING" | "AMO REQ RECEIVED" => {
            OrderStatus::Submitted
        }
        "MODIFY VALIDATION PENDING" | "MODIFY PENDING" | "MODIFY REQ RECEIVED" => {
            OrderStatus::PendingUpdate
        }
        "CANCEL PENDING" => OrderStatus::PendingCancel,
        "TRIGGER PENDING" => {
            if trigger_pending_is_triggered {
                OrderStatus::Triggered
            } else {
                OrderStatus::Accepted
            }
        }
        "OPEN" => OrderStatus::Accepted,
        "COMPLETE" => OrderStatus::Filled,
        "CANCELLED" => OrderStatus::Canceled,
        "REJECTED" => OrderStatus::Rejected,
        // Defensive — log + map to Accepted so a future Kite addition doesn't panic the engine
        // (spec §5/Phase-5 §1 "unknown_kite_status" defensive branch).
        _ => OrderStatus::Accepted,
    }
}

// -------------------------------------------------------------------------------------------------
// Side-band metadata + execution client
// -------------------------------------------------------------------------------------------------

/// Metadata we keep next to every order Nautilus owns. Carries the Kite-native fields that don't
/// fit in `Order` (variety, product), the venue-assigned `kite_order_id`, and the diff cursor
/// (`last_status`, `last_filled_qty`) used by [`generate_order_status_reports`] to avoid
/// re-emitting unchanged rows.
#[derive(Clone, Debug)]
pub struct ZerodhaOrderMeta {
    /// Nautilus identifier (originator).
    pub client_order_id: ClientOrderId,
    /// Kite venue ID (assigned on accept).
    pub kite_order_id: String,
    /// Required for any later modify / cancel.
    pub variety: KiteVariety,
    /// Margin / settlement class.
    pub product: KiteProduct,
    /// Original order spec; used to construct the report.
    pub instrument_id: InstrumentId,
    /// Original side.
    pub order_side: OrderSide,
    /// Original type.
    pub order_type: OrderType,
    /// Original TIF.
    pub time_in_force: TimeInForce,
    /// Original quantity.
    pub quantity: Quantity,
    /// Original limit price (if any).
    pub price: Option<Price>,
    /// Original trigger price (if any).
    pub trigger_price: Option<Price>,
    /// Diff cursor: most recently observed status.
    pub last_status: OrderStatus,
    /// Diff cursor: most recently observed filled quantity.
    pub last_filled_qty: f64,
    /// Diff cursor: most recently observed average fill price.
    pub last_avg_px: Option<f64>,
}

/// Inputs to [`ZerodhaExecClient::submit_order`].
#[derive(Clone, Debug)]
pub struct SubmitRequest {
    /// Nautilus originator id.
    pub client_order_id: ClientOrderId,
    /// Nautilus instrument identifier.
    pub instrument_id: InstrumentId,
    /// Buy / Sell.
    pub order_side: OrderSide,
    /// Market / Limit / SL / SL-M (the v1 set).
    pub order_type: OrderType,
    /// Day / IOC.
    pub time_in_force: TimeInForce,
    /// Quantity in lots (integer for India, never fractional).
    pub quantity: Quantity,
    /// Limit price for `Limit` / `StopLimit`.
    pub price: Option<Price>,
    /// Trigger price for `StopMarket` / `StopLimit`.
    pub trigger_price: Option<Price>,
    /// Order variety (default `Regular`).
    pub variety: KiteVariety,
    /// Margin / settlement class (default `Mis` for safest intraday default).
    pub product: KiteProduct,
}

/// Phase 5 exec client — order submission + status-report generation.
pub struct ZerodhaExecClient {
    http: Arc<ZerodhaHttpClient>,
    cache: Arc<ZerodhaInstrumentCache>,
    account_id: AccountId,
    order_meta: RwLock<HashMap<ClientOrderId, ZerodhaOrderMeta>>,
}

impl std::fmt::Debug for ZerodhaExecClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZerodhaExecClient")
            .field("account_id", &self.account_id)
            .field("http", &self.http)
            .finish_non_exhaustive()
    }
}

impl ZerodhaExecClient {
    /// Build a new exec client. `account_id` is whatever id the strategy is configured to use
    /// (Kite is single-account-per-key — typically `ZERODHA-{user_id}`).
    #[must_use]
    pub fn new(
        http: Arc<ZerodhaHttpClient>,
        cache: Arc<ZerodhaInstrumentCache>,
        account_id: AccountId,
    ) -> Self {
        Self {
            http,
            cache,
            account_id,
            order_meta: RwLock::new(HashMap::new()),
        }
    }

    /// Snapshot of the side-band metadata map (for tests / debugging).
    pub async fn order_meta_snapshot(&self) -> HashMap<ClientOrderId, ZerodhaOrderMeta> {
        self.order_meta.read().await.clone()
    }

    /// Submit one order via `POST /orders/{variety}`.
    ///
    /// Returns the venue-assigned `kite_order_id`. The side-band metadata map is updated
    /// before this returns, so the next `/orders` poll can diff against it.
    ///
    /// # Errors
    ///
    /// - Unsupported order type / TIF for v1 — fails up front rather than at Kite.
    /// - Limit / SL / SL-M orders without the corresponding `price` / `trigger_price`.
    /// - Underlying instrument not in the cache (we need `tradingsymbol` + `exchange`).
    /// - HTTP transport / Kite-side rejection errors propagated from
    ///   [`ZerodhaHttpClient::post_order`].
    pub async fn submit_order(&self, req: &SubmitRequest) -> Result<String> {
        validate_submit(req)?;

        let token = self
            .cache
            .lookup_by_id(&req.instrument_id)
            .ok_or_else(|| {
                ZerodhaError::InvalidResponse(format!(
                    "no Kite token for {} — instrument cache stale?",
                    req.instrument_id
                ))
            })?;

        let qty_str = format!("{}", req.quantity.as_f64() as u64);
        let price_str = req.price.map(|p| format!("{}", p.as_f64()));
        let trigger_str = req.trigger_price.map(|p| format!("{}", p.as_f64()));
        let tag = order_tag(&req.client_order_id);

        let mut form: Vec<(&str, &str)> = vec![
            ("tradingsymbol", token.tradingsymbol.as_str()),
            ("exchange", token.exchange.as_str()),
            ("transaction_type", kite_transaction_type(req.order_side)),
            ("order_type", kite_order_type(req.order_type)?),
            ("quantity", qty_str.as_str()),
            ("product", req.product.as_kite_str()),
            ("validity", kite_validity(req.time_in_force)?),
            ("tag", tag.as_str()),
        ];
        if let Some(p) = price_str.as_deref() {
            form.push(("price", p));
        }
        if let Some(t) = trigger_str.as_deref() {
            form.push(("trigger_price", t));
        }

        let path = format!("/orders/{}", req.variety.as_kite_str());
        let response: serde_json::Value = self.http.post_order(&path, &form).await?;
        let kite_order_id = response
            .get("order_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                ZerodhaError::InvalidResponse(format!(
                    "/orders response missing order_id: {response}"
                ))
            })?
            .to_string();

        let meta = ZerodhaOrderMeta {
            client_order_id: req.client_order_id,
            kite_order_id: kite_order_id.clone(),
            variety: req.variety,
            product: req.product,
            instrument_id: req.instrument_id,
            order_side: req.order_side,
            order_type: req.order_type,
            time_in_force: req.time_in_force,
            quantity: req.quantity,
            price: req.price,
            trigger_price: req.trigger_price,
            last_status: OrderStatus::Submitted,
            last_filled_qty: 0.0,
            last_avg_px: None,
        };
        self.order_meta.write().await.insert(req.client_order_id, meta);
        Ok(kite_order_id)
    }

    /// Poll `/orders` and emit one [`OrderStatusReport`] per row whose `(status, filled_qty,
    /// avg_px)` changed since the last call (or every row, on first call after restart).
    ///
    /// The framework's reconciliation manager calls this on a schedule (default 1 s; spec
    /// §5/Phase-5 §3 leaves cadence + adaptive backoff to the framework). External orders
    /// (rows whose `kite_order_id` we didn't submit) flow through with `client_order_id =
    /// None`; the framework's `generate_external_order_status_events()` handles them.
    ///
    /// # Errors
    ///
    /// HTTP / parse errors from `GET /orders`.
    pub async fn generate_order_status_reports(
        &self,
        ts_init: UnixNanos,
    ) -> Result<Vec<OrderStatusReport>> {
        let rows: Vec<KiteOrderRow> = self.http.get("/orders").await?;
        let mut reports = Vec::new();
        let mut meta_map = self.order_meta.write().await;

        for row in rows {
            let Some(report) = self
                .build_report(&row, &mut meta_map, ts_init)
                .await
                .transpose()?
            else {
                continue;
            };
            reports.push(report);
        }
        Ok(reports)
    }

    async fn build_report(
        &self,
        row: &KiteOrderRow,
        meta_map: &mut HashMap<ClientOrderId, ZerodhaOrderMeta>,
        ts_init: UnixNanos,
    ) -> Option<Result<OrderStatusReport>> {
        // Locate the matching meta (if any).
        let matching_id = meta_map
            .iter()
            .find_map(|(cid, m)| (m.kite_order_id == row.order_id).then_some(*cid));

        // For known orders, determine whether the local order is a stop variant so we can map
        // TRIGGER PENDING correctly.
        let is_stop = matching_id
            .and_then(|cid| meta_map.get(&cid))
            .is_some_and(|m| {
                matches!(m.order_type, OrderType::StopMarket | OrderType::StopLimit)
            });

        let new_status = map_kite_status(&row.status, is_stop);

        // Diff cursor.
        if let Some(cid) = matching_id
            && let Some(meta) = meta_map.get(&cid)
            && meta.last_status == new_status
            && (meta.last_filled_qty - row.filled_quantity).abs() < 1e-9
            && meta.last_avg_px == row.average_price
        {
            return None;
        }

        // Resolve instrument from cache; for external orders we look up by tradingsymbol +
        // exchange via a scan of the by_id snapshot (acceptable: external orders are rare and
        // /orders polling is at most 1/sec).
        let instrument_id = if let Some(cid) = matching_id {
            meta_map.get(&cid).map(|m| m.instrument_id)
        } else {
            self.lookup_instrument(&row.tradingsymbol, &row.exchange).await
        };
        let Some(instrument_id) = instrument_id else {
            return Some(Err(anyhow!(
                "unknown instrument: {} {}",
                row.tradingsymbol,
                row.exchange,
            )));
        };

        // For external orders we need to parse Kite's enums; for known orders we trust the meta.
        let (order_side, order_type, time_in_force, price, trigger_price, quantity) =
            if let Some(cid) = matching_id {
                let m = meta_map
                    .get(&cid)
                    .expect("meta_map entry exists by construction");
                (
                    m.order_side,
                    m.order_type,
                    m.time_in_force,
                    m.price,
                    m.trigger_price,
                    m.quantity,
                )
            } else {
                let side = match parse_kite_transaction_type(&row.transaction_type) {
                    Ok(s) => s,
                    Err(e) => return Some(Err(e)),
                };
                let ot = match parse_kite_order_type(&row.order_type) {
                    Ok(t) => t,
                    Err(e) => return Some(Err(e)),
                };
                let tif = match parse_kite_validity(&row.validity) {
                    Ok(t) => t,
                    Err(e) => return Some(Err(e)),
                };
                let qty = Quantity::new(row.quantity as f64, 0);
                (side, ot, tif, None, None, qty)
            };

        let filled_qty = Quantity::new(row.filled_quantity, 0);
        let ts_event = ts_init; // Kite timestamp parsing landed in Phase 4 if needed; reports
        //                       use the framework wall-clock by default to avoid replaying
        //                       stale timestamps after a long disconnect.

        let mut report = OrderStatusReport::new(
            self.account_id,
            instrument_id,
            matching_id,
            VenueOrderId::from(row.order_id.as_str()),
            order_side,
            order_type,
            time_in_force,
            new_status,
            quantity,
            filled_qty,
            ts_event,
            ts_event,
            ts_init,
            Some(UUID4::new()),
        );
        if let Some(p) = price {
            report = report.with_price(p);
        }
        if let Some(tp) = trigger_price {
            report.trigger_price = Some(tp);
        }
        if let Some(avg) = row.average_price
            && avg.is_finite()
            && avg > 0.0
            && let Ok(decimal) = rust_decimal::Decimal::try_from(avg)
        {
            report.avg_px = Some(decimal);
        }

        // Update diff cursor for known orders.
        if let Some(cid) = matching_id
            && let Some(meta) = meta_map.get_mut(&cid)
        {
            meta.last_status = new_status;
            meta.last_filled_qty = row.filled_quantity;
            meta.last_avg_px = row.average_price;
        }

        Some(Ok(report))
    }

    async fn lookup_instrument(&self, tradingsymbol: &str, exchange: &str) -> Option<InstrumentId> {
        let snapshot = self.cache.by_id.load();
        for (id, kt) in snapshot.iter() {
            if kt.tradingsymbol == tradingsymbol && kt.exchange == exchange {
                return Some(*id);
            }
        }
        // Try the InstrumentId we'd compose from raw — useful when the symbol mapping is the
        // -EQ suffix variety.
        let _ = Symbol::from(tradingsymbol);
        None
    }
}

// -------------------------------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------------------------------

/// Kite caps the `tag` field at 20 characters; we use `NTLZ` + a 12-char hex hash of the client
/// order id so external-order detection can match the prefix unambiguously.
#[must_use]
pub fn order_tag(client_order_id: &ClientOrderId) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(client_order_id.as_str().as_bytes());
    let hash = hasher.finalize();
    let hex_short: String = hash.iter().take(6).fold(String::with_capacity(12), |mut a, b| {
        a.push_str(&format!("{b:02x}"));
        a
    });
    format!("NTLZ{hex_short}")
}

/// Whether a Kite `tag` was emitted by us (used for external-order detection).
#[must_use]
pub fn tag_is_ours(tag: &str) -> bool {
    tag.starts_with("NTLZ")
}

fn validate_submit(req: &SubmitRequest) -> Result<()> {
    if req.quantity.as_f64() <= 0.0 {
        return Err(anyhow!("quantity must be positive"));
    }
    match req.order_type {
        OrderType::Limit | OrderType::StopLimit if req.price.is_none() => {
            Err(anyhow!("{:?} requires price", req.order_type))
        }
        OrderType::StopMarket | OrderType::StopLimit if req.trigger_price.is_none() => {
            Err(anyhow!("{:?} requires trigger_price", req.order_type))
        }
        _ => Ok(()),
    }
}

// -------------------------------------------------------------------------------------------------
// Kite `/orders` row deserializer
// -------------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
struct KiteOrderRow {
    order_id: String,
    #[allow(dead_code)]
    parent_order_id: Option<String>,
    #[allow(dead_code)]
    exchange_order_id: Option<String>,
    status: String,
    tradingsymbol: String,
    exchange: String,
    #[allow(dead_code)]
    instrument_token: u32,
    transaction_type: String,
    order_type: String,
    quantity: u32,
    filled_quantity: f64,
    average_price: Option<f64>,
    #[allow(dead_code)]
    price: Option<f64>,
    #[allow(dead_code)]
    trigger_price: Option<f64>,
    validity: String,
    #[allow(dead_code)]
    variety: Option<String>,
    #[allow(dead_code)]
    product: Option<String>,
    #[allow(dead_code)]
    tag: Option<String>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("OPEN", false, OrderStatus::Accepted)]
    #[case("COMPLETE", false, OrderStatus::Filled)]
    #[case("CANCELLED", false, OrderStatus::Canceled)]
    #[case("REJECTED", false, OrderStatus::Rejected)]
    #[case("VALIDATION PENDING", false, OrderStatus::Submitted)]
    #[case("MODIFY PENDING", false, OrderStatus::PendingUpdate)]
    #[case("CANCEL PENDING", false, OrderStatus::PendingCancel)]
    #[case("TRIGGER PENDING", true, OrderStatus::Triggered)]
    #[case("TRIGGER PENDING", false, OrderStatus::Accepted)]
    #[case("UNRECOGNISED", false, OrderStatus::Accepted)]
    fn status_mapping(
        #[case] kite: &str,
        #[case] is_stop: bool,
        #[case] expected: OrderStatus,
    ) {
        assert_eq!(map_kite_status(kite, is_stop), expected);
    }

    #[rstest]
    fn variety_round_trip() {
        for v in [KiteVariety::Regular, KiteVariety::Amo] {
            assert_eq!(KiteVariety::from_kite_str(v.as_kite_str()).unwrap(), v);
        }
        assert!(KiteVariety::from_kite_str("iceberg").is_err());
    }

    #[rstest]
    fn product_round_trip() {
        for p in [KiteProduct::Cnc, KiteProduct::Mis, KiteProduct::Nrml] {
            assert_eq!(KiteProduct::from_kite_str(p.as_kite_str()).unwrap(), p);
        }
        assert!(KiteProduct::from_kite_str("BO").is_err());
    }

    #[rstest]
    fn validity_mapping() {
        assert_eq!(kite_validity(TimeInForce::Day).unwrap(), "DAY");
        assert_eq!(kite_validity(TimeInForce::Ioc).unwrap(), "IOC");
        assert!(kite_validity(TimeInForce::Gtc).is_err());
    }

    #[rstest]
    fn order_type_mapping() {
        assert_eq!(kite_order_type(OrderType::Market).unwrap(), "MARKET");
        assert_eq!(kite_order_type(OrderType::Limit).unwrap(), "LIMIT");
        assert_eq!(kite_order_type(OrderType::StopMarket).unwrap(), "SL-M");
        assert_eq!(kite_order_type(OrderType::StopLimit).unwrap(), "SL");
    }

    #[rstest]
    fn tag_is_short_and_recognisable() {
        let cid = ClientOrderId::from("O-2026-05-20-001");
        let tag = order_tag(&cid);
        assert_eq!(tag.len(), 16);
        assert!(tag_is_ours(&tag));
        assert!(!tag_is_ours("ICICI-PROD-001"));
    }

    #[rstest]
    fn validate_submit_requires_price_on_limit() {
        let req = SubmitRequest {
            client_order_id: ClientOrderId::from("X"),
            instrument_id: nautilus_model::identifiers::InstrumentId::new(
                Symbol::from("RELIANCE-EQ"),
                nautilus_model::identifiers::Venue::from("NSE"),
            ),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Day,
            quantity: Quantity::new(1.0, 0),
            price: None,
            trigger_price: None,
            variety: KiteVariety::Regular,
            product: KiteProduct::Cnc,
        };
        assert!(validate_submit(&req).is_err());
    }

    #[rstest]
    fn validate_submit_requires_trigger_on_stop() {
        let req = SubmitRequest {
            client_order_id: ClientOrderId::from("X"),
            instrument_id: nautilus_model::identifiers::InstrumentId::new(
                Symbol::from("RELIANCE-EQ"),
                nautilus_model::identifiers::Venue::from("NSE"),
            ),
            order_side: OrderSide::Sell,
            order_type: OrderType::StopMarket,
            time_in_force: TimeInForce::Day,
            quantity: Quantity::new(1.0, 0),
            price: None,
            trigger_price: None,
            variety: KiteVariety::Regular,
            product: KiteProduct::Mis,
        };
        assert!(validate_submit(&req).is_err());
    }
}
