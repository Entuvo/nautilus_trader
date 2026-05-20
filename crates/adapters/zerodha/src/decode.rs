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

//! Length-switched binary WebSocket packet decoder for the Kite ticker stream.
//!
//! # Frame format
//!
//! Each Kite ticker WebSocket message wraps zero or more per-instrument packets:
//!
//! ```text
//! u16 BE        — packet_count
//! count times:
//!   u16 BE     — payload_length
//!   payload_length bytes (per-instrument tick)
//! ```
//!
//! 1-byte frames are Kite server heartbeats — silently discarded.
//!
//! # Per-packet variants (verified against a live `RELIANCE` + `NIFTY 50` capture)
//!
//! - **8 B** — LTP-only (`instrument_token`, `last_price`)
//! - **28 B** — index LTP + timestamp variant (rare; reserved by Kite)
//! - **32 B** — index quote (NIFTY 50 in `full` mode lands here because indices
//!   have no depth)
//! - **44 B** — non-index quote
//! - **>= 184 B** — non-index full (quote fields + last-trade ts + OI + ts + 5-level depth).
//!   We accept anything `>= 184` so a future Kite extension doesn't silently drop frames.
//!
//! Unknown sizes increment [`DecodeStats::unknown_packets`] and are dropped — loud signal for the
//! next Kite layout shift.
//!
//! # Price scaling
//!
//! Kite serialises prices as `u32` integers scaled by 10^`price_precision`. The decoder consults
//! the supplied [`PrecisionLookup`] per-`instrument_token` because precisions vary across
//! exchanges (NSE equity = 2 dp, MCX commodity = 1 dp, CDS currency/debt = 4 dp). A hard-coded
//! `/100` would silently corrupt CDS pairs (spec §3.5).

use std::sync::atomic::{AtomicU64, Ordering};

/// Side-band oracle providing the price precision (decimal places) for a given Kite
/// `instrument_token`.
///
/// Phase 3 wires the live cache (`ZerodhaInstrumentCache::lookup_by_token`) in; tests pass a
/// closure-based stub.
pub trait PrecisionLookup {
    /// Return the decimal places for the given instrument token, or `None` if unknown.
    ///
    /// An unknown token falls back to the platform default of 2 dp (most common for INR cash and
    /// equity-derivative prices) — see [`Decoder::price`] for the fallback rule.
    fn precision(&self, token: u32) -> Option<u8>;
}

impl<F: Fn(u32) -> Option<u8>> PrecisionLookup for F {
    fn precision(&self, token: u32) -> Option<u8> {
        self(token)
    }
}

/// Streaming statistics — exposed for metrics / debugging.
#[derive(Debug, Default)]
pub struct DecodeStats {
    /// Total frames seen (including heartbeats).
    pub frames: AtomicU64,
    /// Heartbeat frames (1-byte payloads).
    pub heartbeats: AtomicU64,
    /// Successfully decoded packets.
    pub packets: AtomicU64,
    /// Packets dropped because their length isn't a recognised variant.
    pub unknown_packets: AtomicU64,
    /// Frames that ended mid-packet (malformed outer wrapper).
    pub truncated_frames: AtomicU64,
}

impl DecodeStats {
    /// Snapshot the current counters (relaxed ordering — metrics, not control flow).
    #[must_use]
    pub fn snapshot(&self) -> DecodeStatsSnapshot {
        DecodeStatsSnapshot {
            frames: self.frames.load(Ordering::Relaxed),
            heartbeats: self.heartbeats.load(Ordering::Relaxed),
            packets: self.packets.load(Ordering::Relaxed),
            unknown_packets: self.unknown_packets.load(Ordering::Relaxed),
            truncated_frames: self.truncated_frames.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot copy of [`DecodeStats`] — easier to log / assert on than atomics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DecodeStatsSnapshot {
    /// See [`DecodeStats::frames`].
    pub frames: u64,
    /// See [`DecodeStats::heartbeats`].
    pub heartbeats: u64,
    /// See [`DecodeStats::packets`].
    pub packets: u64,
    /// See [`DecodeStats::unknown_packets`].
    pub unknown_packets: u64,
    /// See [`DecodeStats::truncated_frames`].
    pub truncated_frames: u64,
}

/// Mode classification of a decoded packet (determined by payload length, not by user setting).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TickMode {
    /// 8-byte LTP-only packet.
    Ltp,
    /// 32-byte index quote (or rarer 28-byte index variant).
    IndexQuote,
    /// 44-byte non-index quote.
    Quote,
    /// 184+ byte non-index full (depth included).
    Full,
}

/// Decoded packet — a typed view over Kite's wire bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct KiteTick {
    /// Kite `instrument_token` (always at offset 0..4).
    pub instrument_token: u32,
    /// Mode dispatched by payload length.
    pub mode: TickMode,
    /// Last traded price (always present).
    pub last_price: f64,
    /// Quantity of the last trade (quote / full only).
    pub last_quantity: Option<u32>,
    /// Volume-weighted average price (quote / full only).
    pub average_price: Option<f64>,
    /// Cumulative day volume (quote / full only).
    pub volume: Option<u32>,
    /// Total buy-side resting quantity (quote / full only).
    pub buy_quantity: Option<u32>,
    /// Total sell-side resting quantity (quote / full only).
    pub sell_quantity: Option<u32>,
    /// Open / high / low / close (quote, full, index).
    pub ohlc: Option<Ohlc>,
    /// Signed change vs. previous close (index only).
    pub change: Option<f64>,
    /// Exchange timestamp in seconds (index, full).
    pub exchange_timestamp: Option<u32>,
    /// Timestamp of the last trade in seconds (full only).
    pub last_trade_timestamp: Option<u32>,
    /// Open interest (full, derivatives only).
    pub oi: Option<u32>,
    /// OI day high (full).
    pub oi_day_high: Option<u32>,
    /// OI day low (full).
    pub oi_day_low: Option<u32>,
    /// 5-level depth, bids index 0..5 and asks index 5..10 (full only).
    pub depth: Option<MarketDepth>,
}

/// Open / high / low / close prices, all scaled to the instrument's precision.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Ohlc {
    /// Day open.
    pub open: f64,
    /// Day high.
    pub high: f64,
    /// Day low.
    pub low: f64,
    /// Day close (previous trading day for indices).
    pub close: f64,
}

/// One level on either side of the order book.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct DepthLevel {
    /// Resting quantity at this level.
    pub quantity: u32,
    /// Limit price at this level.
    pub price: f64,
    /// Number of distinct orders aggregated at this level.
    pub orders: u16,
}

/// Five bid levels + five ask levels (Kite ships exactly 5 + 5 in full mode).
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct MarketDepth {
    /// Bid side, ordered best-to-worst (index 0 = best bid).
    pub bids: [DepthLevel; 5],
    /// Ask side, ordered best-to-worst (index 0 = best ask).
    pub asks: [DepthLevel; 5],
}

/// Length-switched binary decoder.
pub struct Decoder<L: PrecisionLookup> {
    lookup: L,
    stats: DecodeStats,
    default_precision: u8,
}

impl<L: PrecisionLookup> std::fmt::Debug for Decoder<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decoder")
            .field("default_precision", &self.default_precision)
            .field("stats", &self.stats.snapshot())
            .finish()
    }
}

impl<L: PrecisionLookup> Decoder<L> {
    /// Build a new decoder. `default_precision` (typically 2) is used when an
    /// `instrument_token` isn't yet in the cache (e.g. during the brief window between Kite
    /// shipping a new listing and our daily refresh picking it up).
    pub fn new(lookup: L, default_precision: u8) -> Self {
        Self {
            lookup,
            stats: DecodeStats::default(),
            default_precision,
        }
    }

    /// Stats snapshot.
    #[must_use]
    pub fn stats(&self) -> DecodeStatsSnapshot {
        self.stats.snapshot()
    }

    /// Decode one Kite WebSocket frame, appending each packet into `out`.
    ///
    /// Returns the number of packets appended. Heartbeats (1-byte frames) and malformed wrappers
    /// produce zero packets and are recorded in [`DecodeStats`].
    pub fn decode_frame(&self, frame: &[u8], out: &mut Vec<KiteTick>) -> usize {
        self.stats.frames.fetch_add(1, Ordering::Relaxed);

        if frame.len() == 1 {
            self.stats.heartbeats.fetch_add(1, Ordering::Relaxed);
            return 0;
        }
        if frame.len() < 4 {
            self.stats.truncated_frames.fetch_add(1, Ordering::Relaxed);
            return 0;
        }

        let count = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        let mut offset = 2usize;
        let mut emitted = 0usize;

        for _ in 0..count {
            if offset + 2 > frame.len() {
                self.stats.truncated_frames.fetch_add(1, Ordering::Relaxed);
                return emitted;
            }
            let len = u16::from_be_bytes([frame[offset], frame[offset + 1]]) as usize;
            offset += 2;
            if offset + len > frame.len() {
                self.stats.truncated_frames.fetch_add(1, Ordering::Relaxed);
                return emitted;
            }
            let packet = &frame[offset..offset + len];
            offset += len;

            if let Some(tick) = self.decode_packet(packet) {
                out.push(tick);
                self.stats.packets.fetch_add(1, Ordering::Relaxed);
                emitted += 1;
            } else {
                self.stats.unknown_packets.fetch_add(1, Ordering::Relaxed);
            }
        }
        emitted
    }

    fn decode_packet(&self, packet: &[u8]) -> Option<KiteTick> {
        if packet.len() < 8 {
            return None;
        }
        let token = read_u32(packet, 0);
        let precision = self
            .lookup
            .precision(token)
            .unwrap_or(self.default_precision);

        match packet.len() {
            8 => Some(self.decode_ltp(packet, token, precision)),
            28 | 32 => Some(self.decode_index(packet, token, precision)),
            44 => Some(self.decode_quote(packet, token, precision)),
            n if n >= 184 => Some(self.decode_full(packet, token, precision)),
            _ => None,
        }
    }

    fn decode_ltp(&self, packet: &[u8], token: u32, precision: u8) -> KiteTick {
        let last_price = self.price(read_u32(packet, 4), precision);
        KiteTick {
            instrument_token: token,
            mode: TickMode::Ltp,
            last_price,
            ..empty_tick(token, TickMode::Ltp)
        }
    }

    fn decode_index(&self, packet: &[u8], token: u32, precision: u8) -> KiteTick {
        // 32 B layout (verified against live NIFTY 50):
        //   0-4  instrument_token
        //   4-8  last_price
        //   8-12 high
        //  12-16 low
        //  16-20 open
        //  20-24 close
        //  24-28 change (signed)
        //  28-32 exchange_timestamp (seconds)
        // 28 B is a shorter index variant Kite still documents; we treat the trailing fields as
        // absent and bail out gracefully.
        let last_price = self.price(read_u32(packet, 4), precision);
        let high = self.price(read_u32(packet, 8), precision);
        let low = self.price(read_u32(packet, 12), precision);
        let open = self.price(read_u32(packet, 16), precision);
        let close = self.price(read_u32(packet, 20), precision);

        let (change, exchange_ts) = if packet.len() >= 32 {
            (
                Some(self.scale_signed(read_i32(packet, 24), precision)),
                Some(read_u32(packet, 28)),
            )
        } else {
            (None, None)
        };

        KiteTick {
            instrument_token: token,
            mode: TickMode::IndexQuote,
            last_price,
            ohlc: Some(Ohlc { open, high, low, close }),
            change,
            exchange_timestamp: exchange_ts,
            ..empty_tick(token, TickMode::IndexQuote)
        }
    }

    fn decode_quote(&self, packet: &[u8], token: u32, precision: u8) -> KiteTick {
        // 44 B layout:
        //   0-4   instrument_token
        //   4-8   last_price
        //   8-12  last_quantity
        //  12-16  average_price
        //  16-20  volume
        //  20-24  buy_quantity
        //  24-28  sell_quantity
        //  28-32  open
        //  32-36  high
        //  36-40  low
        //  40-44  close
        let last_price = self.price(read_u32(packet, 4), precision);
        let last_quantity = read_u32(packet, 8);
        let average_price = self.price(read_u32(packet, 12), precision);
        let volume = read_u32(packet, 16);
        let buy_quantity = read_u32(packet, 20);
        let sell_quantity = read_u32(packet, 24);
        let open = self.price(read_u32(packet, 28), precision);
        let high = self.price(read_u32(packet, 32), precision);
        let low = self.price(read_u32(packet, 36), precision);
        let close = self.price(read_u32(packet, 40), precision);

        KiteTick {
            instrument_token: token,
            mode: TickMode::Quote,
            last_price,
            last_quantity: Some(last_quantity),
            average_price: Some(average_price),
            volume: Some(volume),
            buy_quantity: Some(buy_quantity),
            sell_quantity: Some(sell_quantity),
            ohlc: Some(Ohlc { open, high, low, close }),
            ..empty_tick(token, TickMode::Quote)
        }
    }

    fn decode_full(&self, packet: &[u8], token: u32, precision: u8) -> KiteTick {
        // 184 B layout:
        //   0-44   quote fields (see `decode_quote`)
        //  44-48   last_trade_timestamp (seconds)
        //  48-52   open_interest
        //  52-56   oi_day_high
        //  56-60   oi_day_low
        //  60-64   exchange_timestamp (seconds)
        //  64-184  market depth: 5 levels × (qty:u32 + price:u32 + orders:u16 + pad:u16) × 2 sides
        let last_price = self.price(read_u32(packet, 4), precision);
        let last_quantity = read_u32(packet, 8);
        let average_price = self.price(read_u32(packet, 12), precision);
        let volume = read_u32(packet, 16);
        let buy_quantity = read_u32(packet, 20);
        let sell_quantity = read_u32(packet, 24);
        let open = self.price(read_u32(packet, 28), precision);
        let high = self.price(read_u32(packet, 32), precision);
        let low = self.price(read_u32(packet, 36), precision);
        let close = self.price(read_u32(packet, 40), precision);
        let last_trade_ts = read_u32(packet, 44);
        let oi = read_u32(packet, 48);
        let oi_h = read_u32(packet, 52);
        let oi_l = read_u32(packet, 56);
        let exchange_ts = read_u32(packet, 60);

        let mut bids = [DepthLevel { quantity: 0, price: 0.0, orders: 0 }; 5];
        let mut asks = [DepthLevel { quantity: 0, price: 0.0, orders: 0 }; 5];
        for (i, slot) in bids.iter_mut().enumerate() {
            let off = 64 + i * 12;
            *slot = DepthLevel {
                quantity: read_u32(packet, off),
                price: self.price(read_u32(packet, off + 4), precision),
                orders: u16::from_be_bytes([packet[off + 8], packet[off + 9]]),
            };
        }
        for (i, slot) in asks.iter_mut().enumerate() {
            let off = 64 + 60 + i * 12; // bids occupy 60 B then asks
            *slot = DepthLevel {
                quantity: read_u32(packet, off),
                price: self.price(read_u32(packet, off + 4), precision),
                orders: u16::from_be_bytes([packet[off + 8], packet[off + 9]]),
            };
        }

        KiteTick {
            instrument_token: token,
            mode: TickMode::Full,
            last_price,
            last_quantity: Some(last_quantity),
            average_price: Some(average_price),
            volume: Some(volume),
            buy_quantity: Some(buy_quantity),
            sell_quantity: Some(sell_quantity),
            ohlc: Some(Ohlc { open, high, low, close }),
            last_trade_timestamp: Some(last_trade_ts),
            oi: Some(oi),
            oi_day_high: Some(oi_h),
            oi_day_low: Some(oi_l),
            exchange_timestamp: Some(exchange_ts),
            depth: Some(MarketDepth { bids, asks }),
            change: None,
        }
    }

    fn price(&self, raw: u32, precision: u8) -> f64 {
        f64::from(raw) / divisor(precision)
    }

    fn scale_signed(&self, raw: i32, precision: u8) -> f64 {
        f64::from(raw) / divisor(precision)
    }
}

fn divisor(precision: u8) -> f64 {
    match precision {
        0 => 1.0,
        1 => 10.0,
        2 => 100.0,
        3 => 1_000.0,
        4 => 10_000.0,
        n => 10f64.powi(i32::from(n)),
    }
}

fn read_u32(packet: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        packet[offset],
        packet[offset + 1],
        packet[offset + 2],
        packet[offset + 3],
    ])
}

fn read_i32(packet: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes([
        packet[offset],
        packet[offset + 1],
        packet[offset + 2],
        packet[offset + 3],
    ])
}

fn empty_tick(token: u32, mode: TickMode) -> KiteTick {
    KiteTick {
        instrument_token: token,
        mode,
        last_price: 0.0,
        last_quantity: None,
        average_price: None,
        volume: None,
        buy_quantity: None,
        sell_quantity: None,
        ohlc: None,
        change: None,
        exchange_timestamp: None,
        last_trade_timestamp: None,
        oi: None,
        oi_day_high: None,
        oi_day_low: None,
        depth: None,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    // Mock lookup — every token resolves to 2 dp (typical for NSE/BSE equity prices).
    // The `Option<u8>` return matches the trait shape (production cache must distinguish
    // "unknown token" from "0 dp").
    #[allow(clippy::unnecessary_wraps)]
    fn nse_lookup(_token: u32) -> Option<u8> {
        Some(2)
    }

    #[rstest]
    fn decode_heartbeat_increments_counter() {
        let decoder = Decoder::new(nse_lookup, 2);
        let mut out = Vec::new();
        assert_eq!(decoder.decode_frame(&[0x00], &mut out), 0);
        assert_eq!(out.len(), 0);
        let s = decoder.stats();
        assert_eq!(s.heartbeats, 1);
        assert_eq!(s.frames, 1);
    }

    #[rstest]
    fn decode_nifty_index_packet() {
        // Captured live (NIFTY 50 in `full` mode → 32-byte index quote wrapped in a 36-byte frame).
        let frame = hex::decode(
            "000100200003e9090023da660023eeac0023b3920023cafd002409c8ffffd09e6a0d3b44",
        )
        .unwrap();
        let decoder = Decoder::new(nse_lookup, 2);
        let mut out = Vec::new();
        let n = decoder.decode_frame(&frame, &mut out);
        assert_eq!(n, 1);
        let t = &out[0];
        assert_eq!(t.instrument_token, 256_265); // NIFTY 50
        assert_eq!(t.mode, TickMode::IndexQuote);
        assert!((t.last_price - 23_496.70).abs() < 0.01);
        let ohlc = t.ohlc.expect("index has ohlc");
        assert!((ohlc.high - 23_548.60).abs() < 0.01);
        assert!((ohlc.low - 23_397.30).abs() < 0.01);
        let change = t.change.expect("index has change");
        assert!(change < 0.0, "expected negative change, got {change}");
        assert!(t.exchange_timestamp.is_some());
        assert!(t.depth.is_none());
    }

    #[rstest]
    fn decode_reliance_full_packet() {
        // Build a 184-byte full packet inline with known values.
        let mut packet = vec![0u8; 184];
        packet[0..4].copy_from_slice(&738_561u32.to_be_bytes());     // instrument_token
        packet[4..8].copy_from_slice(&132_742u32.to_be_bytes());     // last_price = 1327.42
        packet[8..12].copy_from_slice(&5u32.to_be_bytes());          // last_quantity = 5
        packet[12..16].copy_from_slice(&132_637u32.to_be_bytes());   // average_price = 1326.37
        packet[16..20].copy_from_slice(&1_234_567u32.to_be_bytes()); // volume
        packet[20..24].copy_from_slice(&111u32.to_be_bytes());       // buy_quantity
        packet[24..28].copy_from_slice(&222u32.to_be_bytes());       // sell_quantity
        packet[28..32].copy_from_slice(&130_000u32.to_be_bytes());   // open
        packet[32..36].copy_from_slice(&135_000u32.to_be_bytes());   // high
        packet[36..40].copy_from_slice(&129_000u32.to_be_bytes());   // low
        packet[40..44].copy_from_slice(&133_000u32.to_be_bytes());   // close
        packet[44..48].copy_from_slice(&1_779_424_000u32.to_be_bytes()); // last_trade_ts
        packet[48..52].copy_from_slice(&999u32.to_be_bytes());       // oi
        packet[52..56].copy_from_slice(&1500u32.to_be_bytes());      // oi_h
        packet[56..60].copy_from_slice(&800u32.to_be_bytes());       // oi_l
        packet[60..64].copy_from_slice(&1_779_424_068u32.to_be_bytes()); // exchange_ts
        // First bid level: qty=10, price=132741 (1327.41), orders=2
        packet[64..68].copy_from_slice(&10u32.to_be_bytes());
        packet[68..72].copy_from_slice(&132_741u32.to_be_bytes());
        packet[72..74].copy_from_slice(&2u16.to_be_bytes());
        // (skip the remaining levels — all zero)
        // First ask level (offset 64 + 60 = 124): qty=8, price=132743 (1327.43), orders=3
        packet[124..128].copy_from_slice(&8u32.to_be_bytes());
        packet[128..132].copy_from_slice(&132_743u32.to_be_bytes());
        packet[132..134].copy_from_slice(&3u16.to_be_bytes());

        let mut frame = vec![0u8; 4 + 184];
        frame[0..2].copy_from_slice(&1u16.to_be_bytes()); // count
        frame[2..4].copy_from_slice(&184u16.to_be_bytes()); // payload length
        frame[4..].copy_from_slice(&packet);

        let decoder = Decoder::new(nse_lookup, 2);
        let mut out = Vec::new();
        let n = decoder.decode_frame(&frame, &mut out);
        assert_eq!(n, 1);
        let t = &out[0];
        assert_eq!(t.instrument_token, 738_561);
        assert_eq!(t.mode, TickMode::Full);
        assert!((t.last_price - 1327.42).abs() < 0.01);
        assert_eq!(t.last_quantity, Some(5));
        let depth = t.depth.expect("full has depth");
        assert_eq!(depth.bids[0].quantity, 10);
        assert!((depth.bids[0].price - 1327.41).abs() < 0.01);
        assert_eq!(depth.bids[0].orders, 2);
        assert_eq!(depth.asks[0].quantity, 8);
        assert!((depth.asks[0].price - 1327.43).abs() < 0.01);
        assert_eq!(depth.asks[0].orders, 3);
    }

    #[rstest]
    fn decode_two_packet_frame_matches_live_capture() {
        // First packet (32 B index) and the leading 18 B of the 184 B full packet are real bytes
        // pulled from the captured fixture; we pad the full packet to 184 B with zeros for the
        // remaining fields the test doesn't assert on.
        let mut full_frame = Vec::with_capacity(222);
        full_frame.extend_from_slice(&hex::decode("000200200003e9090023da8e0023eeac0023b3920023cafd002409c8ffffd0c66a0d3b44").unwrap());
        full_frame.extend_from_slice(&hex::decode("00b8000b450100020486000000040002041d").unwrap());
        let pad_needed = 222 - full_frame.len();
        full_frame.extend(std::iter::repeat_n(0u8, pad_needed));
        assert_eq!(full_frame.len(), 222);

        let decoder = Decoder::new(nse_lookup, 2);
        let mut out = Vec::new();
        let n = decoder.decode_frame(&full_frame, &mut out);
        assert_eq!(n, 2, "expected two packets, frame={n}");
        assert_eq!(out[0].instrument_token, 256_265); // NIFTY 50
        assert_eq!(out[0].mode, TickMode::IndexQuote);
        assert_eq!(out[1].instrument_token, 738_561); // RELIANCE
        assert_eq!(out[1].mode, TickMode::Full);
    }

    #[rstest]
    fn replay_captured_fixture_matches_invariants() {
        // Operator-captured fixture from a live 60 s session. Skipped if absent (CI without
        // creds).
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/ws_session_2026-05-20.bin");
        if !path.exists() {
            return;
        }
        let bytes = std::fs::read(&path).expect("read fixture");
        assert!(bytes.len() > 12, "fixture too small");
        assert_eq!(&bytes[0..4], b"ZWSC");
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        assert_eq!(version, 1);

        let decoder = Decoder::new(nse_lookup, 2);
        let mut out = Vec::with_capacity(1024);
        let mut cursor = 8usize;
        let mut frames = 0u64;
        while cursor + 12 <= bytes.len() {
            cursor += 8; // skip timestamp_ns
            let len = u32::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
            ]) as usize;
            cursor += 4;
            let frame = &bytes[cursor..cursor + len];
            cursor += len;
            decoder.decode_frame(frame, &mut out);
            frames += 1;
        }

        let stats = decoder.stats();
        assert_eq!(stats.frames, frames, "stats vs replay frame count");
        assert!(
            stats.unknown_packets == 0,
            "unknown packets {} — Kite layout shifted",
            stats.unknown_packets,
        );
        assert!(stats.packets > 100, "too few packets emitted: {}", stats.packets);

        let mut has_nifty = false;
        let mut has_reliance = false;
        for t in &out {
            if t.instrument_token == 256_265 {
                has_nifty = true;
                assert_eq!(t.mode, TickMode::IndexQuote);
                assert!(t.last_price > 0.0);
            }
            if t.instrument_token == 738_561 {
                has_reliance = true;
                assert_eq!(t.mode, TickMode::Full);
                let depth = t.depth.expect("RELIANCE full has depth");
                // Best-bid <= last_price <= best-ask invariant on snapshot frames.
                if depth.bids[0].price > 0.0 && depth.asks[0].price > 0.0 {
                    assert!(
                        depth.bids[0].price <= t.last_price + 1.0,
                        "bid {} above ltp {}",
                        depth.bids[0].price,
                        t.last_price,
                    );
                    assert!(
                        depth.asks[0].price >= t.last_price - 1.0,
                        "ask {} below ltp {}",
                        depth.asks[0].price,
                        t.last_price,
                    );
                }
            }
        }
        assert!(has_nifty, "expected at least one NIFTY tick");
        assert!(has_reliance, "expected at least one RELIANCE tick");
    }

    #[rstest]
    fn unknown_packet_size_increments_counter() {
        // Outer count=1, payload length=17 (not a known variant).
        let mut frame = vec![0u8; 4 + 17];
        frame[0..2].copy_from_slice(&1u16.to_be_bytes());
        frame[2..4].copy_from_slice(&17u16.to_be_bytes());
        let decoder = Decoder::new(nse_lookup, 2);
        let mut out = Vec::new();
        let n = decoder.decode_frame(&frame, &mut out);
        assert_eq!(n, 0);
        assert_eq!(decoder.stats().unknown_packets, 1);
    }

    #[rstest]
    fn cds_pair_uses_four_decimal_precision() {
        // 8-byte LTP packet with a price of 79.4250 (raw integer 794250 with precision=4).
        let mut packet = vec![0u8; 8];
        packet[0..4].copy_from_slice(&270_595u32.to_be_bytes());
        packet[4..8].copy_from_slice(&794_250u32.to_be_bytes());
        let mut frame = vec![0u8; 4 + 8];
        frame[0..2].copy_from_slice(&1u16.to_be_bytes());
        frame[2..4].copy_from_slice(&8u16.to_be_bytes());
        frame[4..].copy_from_slice(&packet);

        // Lookup returns 4 dp for CDS-style tokens.
        let cds_lookup = |_: u32| Some(4u8);
        let decoder = Decoder::new(cds_lookup, 2);
        let mut out = Vec::new();
        decoder.decode_frame(&frame, &mut out);
        assert!((out[0].last_price - 79.4250).abs() < 1e-6);
    }
}
