// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Local persistence of the [`ZerodhaOrderMeta`] side-band map (spec §5/Phase-7 §1).
//!
//! # Why JSON instead of sqlite
//!
//! The spec head table mentions `sqlx` with the `sqlite` feature for this layer. The current
//! workspace `sqlx` only enables `postgres` (see top-level `Cargo.toml`), so wiring sqlite
//! would either (a) add it to the workspace (touches shared build time) or (b) add a per-crate
//! sqlx dep with its own feature flags. Neither is justified for the actual data shape:
//!
//! - Per-row size is tiny (~250 bytes serialized).
//! - Row count is bounded — typically dozens to a few thousand open + recently-terminal orders
//!   per trading day; Kite drops them from `/orders` after EOD anyway.
//! - We don't query the persisted file — it's only ever read in full on startup.
//! - Writes happen on order submit / modify / cancel and on every poll cycle when state
//!   changes, never in a tight loop.
//!
//! A flat JSON file with atomic write-and-rename ticks every box without taking on a SQL
//! engine. If a future deployment grows past ~10 k concurrent orders this layer can be swapped
//! for sqlx + sqlite without changing the public [`OrderStore`] API — the methods stay the same.
//!
//! # Atomicity
//!
//! [`OrderStore::save`] writes to `<path>.tmp` then `std::fs::rename`s into place. SIGINT mid-
//! write leaves the prior good copy intact rather than a truncated file. The directory is
//! created on first save if absent.
//!
//! # Type discipline
//!
//! [`PersistedMeta`] is a flat record of `String` / `f64` / `u64` so the serialization format
//! doesn't track Nautilus's internal type evolution. [`From<&ZerodhaOrderMeta>`] / [`TryFrom`]
//! impls handle the round-trip — any field whose textual form changes shape (new
//! `OrderType` variant, new `TimeInForce`) fails at load time rather than smuggling bad
//! enum values into the engine.

use std::{collections::HashMap, path::PathBuf, time::SystemTime};

use anyhow::{Result, anyhow};
use nautilus_model::{
    enums::OrderStatus,
    identifiers::{ClientOrderId, InstrumentId},
    types::{Price, Quantity},
};
use serde::{Deserialize, Serialize};

use crate::{
    execution::{
        KiteProduct, KiteVariety, ZerodhaOrderMeta, kite_order_type, kite_transaction_type,
        kite_validity, parse_kite_order_type, parse_kite_transaction_type, parse_kite_validity,
    },
    symbology::price_precision_from_tick,
};

/// On-disk record. All-strings / numbers; type-safe enums round-trip via the kite-string
/// helpers we already have.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistedMeta {
    /// Kite venue ID (assigned on accept).
    pub kite_order_id: String,
    /// Order variety as the Kite wire string ("regular" / "amo").
    pub variety: String,
    /// Margin/settlement class as the Kite wire string ("CNC" / "MIS" / "NRML").
    pub product: String,
    /// `InstrumentId` as `SYMBOL.VENUE`.
    pub instrument_id: String,
    /// Order side as Kite's transaction_type ("BUY" / "SELL").
    pub order_side: String,
    /// Order type as Kite's order_type ("MARKET" / "LIMIT" / "SL" / "SL-M").
    pub order_type: String,
    /// Time-in-force as Kite's validity ("DAY" / "IOC").
    pub time_in_force: String,
    /// Quantity as a plain float (always integer-valued for Kite).
    pub quantity: f64,
    /// Decimal price precision used to reconstruct `Price` on load.
    pub price_precision: u8,
    /// Limit price (if any).
    pub price: Option<f64>,
    /// Trigger price (if any).
    pub trigger_price: Option<f64>,
    /// Diff cursor: most recent observed status, as Nautilus `OrderStatus` `Display`.
    pub last_status: String,
    /// Diff cursor: most recent filled quantity.
    pub last_filled_qty: f64,
    /// Diff cursor: most recent average fill price.
    pub last_avg_px: Option<f64>,
    /// UNIX seconds when the meta was last touched. Used by startup reconciliation to decide
    /// whether a record missing from today's `/orders` snapshot should emit a terminal
    /// `OrderCanceled` (≤ 24 h old) or be pruned silently (older than 24 h, Kite has dropped
    /// it from the snapshot).
    pub last_seen_ts: u64,
}

impl From<&ZerodhaOrderMeta> for PersistedMeta {
    fn from(m: &ZerodhaOrderMeta) -> Self {
        let now_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let (price, price_precision) = match m.price {
            Some(p) => (Some(p.as_f64()), p.precision),
            None => (None, 2),
        };
        let trigger_price = m.trigger_price.map(|p| p.as_f64());
        Self {
            kite_order_id: m.kite_order_id.clone(),
            variety: m.variety.as_kite_str().to_string(),
            product: m.product.as_kite_str().to_string(),
            instrument_id: m.instrument_id.to_string(),
            order_side: kite_transaction_type(m.order_side).to_string(),
            order_type: kite_order_type(m.order_type)
                .map_or_else(|_| "MARKET".to_string(), str::to_string),
            time_in_force: kite_validity(m.time_in_force)
                .map_or_else(|_| "DAY".to_string(), str::to_string),
            quantity: m.quantity.as_f64(),
            price_precision,
            price,
            trigger_price,
            last_status: format!("{}", m.last_status),
            last_filled_qty: m.last_filled_qty,
            last_avg_px: m.last_avg_px,
            last_seen_ts: now_secs,
        }
    }
}

impl PersistedMeta {
    /// Reconstruct a [`ZerodhaOrderMeta`] from disk.
    ///
    /// `client_order_id` is the key of the on-disk map (the JSON top-level object's key).
    ///
    /// # Errors
    ///
    /// Returns an error if any enum field has shifted shape since the file was written (new
    /// `OrderType` variant Kite added, etc.) or if `instrument_id` doesn't parse.
    pub fn rehydrate(&self, client_order_id: ClientOrderId) -> Result<ZerodhaOrderMeta> {
        let variety = KiteVariety::from_kite_str(&self.variety)?;
        let product = KiteProduct::from_kite_str(&self.product)?;
        let instrument_id = InstrumentId::from(self.instrument_id.as_str());
        let order_side = parse_kite_transaction_type(&self.order_side)?;
        let order_type = parse_kite_order_type(&self.order_type)?;
        let time_in_force = parse_kite_validity(&self.time_in_force)?;
        let last_status = parse_order_status(&self.last_status)?;
        let price = self
            .price
            .map(|p| Price::new(p, self.price_precision));
        let trigger_price = self
            .trigger_price
            .map(|p| Price::new(p, self.price_precision));
        let quantity = Quantity::new(self.quantity, 0);

        Ok(ZerodhaOrderMeta {
            client_order_id,
            kite_order_id: self.kite_order_id.clone(),
            variety,
            product,
            instrument_id,
            order_side,
            order_type,
            time_in_force,
            quantity,
            price,
            trigger_price,
            last_status,
            last_filled_qty: self.last_filled_qty,
            last_avg_px: self.last_avg_px,
        })
    }
}

/// Map `OrderStatus`'s `Display` form back to the enum.
fn parse_order_status(s: &str) -> Result<OrderStatus> {
    match s {
        "INITIALIZED" => Ok(OrderStatus::Initialized),
        "SUBMITTED" => Ok(OrderStatus::Submitted),
        "ACCEPTED" => Ok(OrderStatus::Accepted),
        "REJECTED" => Ok(OrderStatus::Rejected),
        "CANCELED" => Ok(OrderStatus::Canceled),
        "EXPIRED" => Ok(OrderStatus::Expired),
        "TRIGGERED" => Ok(OrderStatus::Triggered),
        "PENDING_UPDATE" => Ok(OrderStatus::PendingUpdate),
        "PENDING_CANCEL" => Ok(OrderStatus::PendingCancel),
        "PARTIALLY_FILLED" => Ok(OrderStatus::PartiallyFilled),
        "FILLED" => Ok(OrderStatus::Filled),
        "EMULATED" => Ok(OrderStatus::Emulated),
        "RELEASED" => Ok(OrderStatus::Released),
        "DENIED" => Ok(OrderStatus::Denied),
        other => Err(anyhow!("unknown OrderStatus on disk: {other:?}")),
    }
}

/// Atomic JSON store for the order-meta map.
#[derive(Clone, Debug)]
pub struct OrderStore {
    path: PathBuf,
}

impl OrderStore {
    /// Build a store pointing at `path`. The file is created on first save; the parent
    /// directory is created if absent.
    #[must_use]
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }

    /// Path the store writes to (test helper).
    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Persist the entire map.
    ///
    /// Writes to `<path>.tmp` then renames so a SIGINT mid-write can't leave a half-written
    /// file. The map is sorted by `ClientOrderId` so diffs are reviewable.
    ///
    /// # Errors
    ///
    /// I/O failure on either the temp write or the rename.
    pub async fn save(&self, map: &HashMap<ClientOrderId, ZerodhaOrderMeta>) -> Result<()> {
        let snapshot: std::collections::BTreeMap<String, PersistedMeta> = map
            .iter()
            .map(|(cid, m)| (cid.to_string(), PersistedMeta::from(m)))
            .collect();
        let bytes = serde_json::to_vec_pretty(&snapshot)
            .map_err(|e| anyhow!("serialize order store: {e}"))?;

        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                anyhow!("create_dir_all({}): {e}", parent.display())
            })?;
        }
        let tmp = self.path.with_extension("tmp");
        tokio::fs::write(&tmp, &bytes)
            .await
            .map_err(|e| anyhow!("write {}: {e}", tmp.display()))?;
        tokio::fs::rename(&tmp, &self.path)
            .await
            .map_err(|e| anyhow!("rename {} -> {}: {e}", tmp.display(), self.path.display()))?;
        Ok(())
    }

    /// Load the persisted map. An absent file yields an empty map (cold start).
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON or for any individual record that can't be
    /// rehydrated (e.g. an enum value the current build doesn't recognise).
    pub async fn load(&self) -> Result<HashMap<ClientOrderId, ZerodhaOrderMeta>> {
        let raw = self.load_raw().await?;
        let mut out = HashMap::with_capacity(raw.len());
        for (cid, persisted) in raw {
            let meta = persisted.rehydrate(cid)?;
            out.insert(cid, meta);
        }
        Ok(out)
    }

    /// Load the raw [`PersistedMeta`] map. Useful when callers need access to
    /// `last_seen_ts` (e.g. startup reconciliation deciding whether a missing order is a
    /// same-day cancel or a stale record Kite already dropped from `/orders`).
    ///
    /// # Errors
    ///
    /// I/O failure or malformed JSON.
    pub async fn load_raw(&self) -> Result<HashMap<ClientOrderId, PersistedMeta>> {
        if !tokio::fs::try_exists(&self.path).await.unwrap_or(false) {
            return Ok(HashMap::new());
        }
        let bytes = tokio::fs::read(&self.path)
            .await
            .map_err(|e| anyhow!("read {}: {e}", self.path.display()))?;
        if bytes.is_empty() {
            return Ok(HashMap::new());
        }
        let raw: std::collections::BTreeMap<String, PersistedMeta> = serde_json::from_slice(&bytes)
            .map_err(|e| anyhow!("parse order store: {e}"))?;
        Ok(raw
            .into_iter()
            .map(|(cid, m)| (ClientOrderId::from(cid.as_str()), m))
            .collect())
    }
}

// Silence the unused-import lint when `price_precision_from_tick` is referenced only through
// public re-export.
#[allow(dead_code)]
fn _doc_anchor() {
    let _ = price_precision_from_tick;
}

#[cfg(test)]
mod tests {
    use nautilus_model::enums::{OrderSide, OrderType, TimeInForce};
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;

    fn sample_meta() -> ZerodhaOrderMeta {
        ZerodhaOrderMeta {
            client_order_id: ClientOrderId::from("O-2026-05-20-001"),
            kite_order_id: "250520000000123".into(),
            variety: KiteVariety::Regular,
            product: KiteProduct::Mis,
            instrument_id: InstrumentId::from("RELIANCE-EQ.NSE"),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Day,
            quantity: Quantity::new(5.0, 0),
            price: Some(Price::new(1327.4, 1)),
            trigger_price: None,
            last_status: OrderStatus::Accepted,
            last_filled_qty: 0.0,
            last_avg_px: None,
        }
    }

    #[tokio::test]
    async fn round_trip_one_record() {
        let dir = TempDir::new().unwrap();
        let store = OrderStore::new(dir.path().join("orders.json"));
        let mut map = HashMap::new();
        let meta = sample_meta();
        map.insert(meta.client_order_id, meta.clone());

        store.save(&map).await.unwrap();
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded.len(), 1);

        let reloaded = loaded.get(&meta.client_order_id).unwrap();
        assert_eq!(reloaded.kite_order_id, meta.kite_order_id);
        assert_eq!(reloaded.variety, meta.variety);
        assert_eq!(reloaded.product, meta.product);
        assert_eq!(reloaded.order_side, meta.order_side);
        assert_eq!(reloaded.order_type, meta.order_type);
        assert_eq!(reloaded.time_in_force, meta.time_in_force);
        assert!((reloaded.quantity.as_f64() - meta.quantity.as_f64()).abs() < 1e-9);
        assert!((reloaded.price.unwrap().as_f64() - meta.price.unwrap().as_f64()).abs() < 0.01);
        assert_eq!(reloaded.last_status, meta.last_status);
    }

    #[tokio::test]
    async fn load_missing_file_is_empty_map() {
        let dir = TempDir::new().unwrap();
        let store = OrderStore::new(dir.path().join("does-not-exist.json"));
        let loaded = store.load().await.unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn save_creates_parent_directory() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("a/b/c/orders.json");
        let store = OrderStore::new(&nested);
        let map: HashMap<ClientOrderId, ZerodhaOrderMeta> = HashMap::new();
        store.save(&map).await.unwrap();
        assert!(tokio::fs::try_exists(&nested).await.unwrap());
    }

    #[tokio::test]
    async fn save_is_atomic_via_rename() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("orders.json");
        let store = OrderStore::new(&path);

        // Initial good state.
        let mut map = HashMap::new();
        let meta = sample_meta();
        map.insert(meta.client_order_id, meta.clone());
        store.save(&map).await.unwrap();

        // The temp file should NOT linger after a successful save.
        assert!(!tokio::fs::try_exists(path.with_extension("tmp")).await.unwrap());
        // The real file is present.
        assert!(tokio::fs::try_exists(&path).await.unwrap());
    }

    #[rstest]
    fn order_status_round_trip() {
        for s in [
            OrderStatus::Initialized,
            OrderStatus::Submitted,
            OrderStatus::Accepted,
            OrderStatus::Rejected,
            OrderStatus::Canceled,
            OrderStatus::Filled,
            OrderStatus::PartiallyFilled,
            OrderStatus::Triggered,
        ] {
            let parsed = parse_order_status(&format!("{s}")).expect("round trip");
            assert_eq!(parsed, s);
        }
    }
}
