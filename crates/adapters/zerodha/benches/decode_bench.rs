// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! Phase 3 decoder microbench (spec §5/Phase-3 §2).
//!
//! Targets (M-series Mac, --release):
//!
//! - single full-mode packet decode:  < 5 µs
//! - 100-packet frame decode + emit:  < 500 µs
//! - sustained throughput:            >= 50 k events/sec
//!
//! Regressions > 10 % vs. a previous baseline should fail review. Criterion saves baselines
//! under `target/criterion/` by default; gate via `cargo bench -p nautilus-zerodha --
//! --baseline main --noise-threshold 0.10` in CI.

use criterion::{Criterion, criterion_group, criterion_main};
use nautilus_zerodha::decode::Decoder;

fn build_full_packet(token: u32) -> Vec<u8> {
    let mut packet = vec![0u8; 184];
    packet[0..4].copy_from_slice(&token.to_be_bytes());
    packet[4..8].copy_from_slice(&132_742u32.to_be_bytes()); // ltp
    packet[8..12].copy_from_slice(&5u32.to_be_bytes()); // last_qty
    packet[12..16].copy_from_slice(&132_637u32.to_be_bytes()); // avg
    packet[16..20].copy_from_slice(&1_234_567u32.to_be_bytes()); // volume
    packet[20..24].copy_from_slice(&111u32.to_be_bytes()); // buy_qty
    packet[24..28].copy_from_slice(&222u32.to_be_bytes()); // sell_qty
    packet[28..32].copy_from_slice(&130_000u32.to_be_bytes()); // open
    packet[32..36].copy_from_slice(&135_000u32.to_be_bytes()); // high
    packet[36..40].copy_from_slice(&129_000u32.to_be_bytes()); // low
    packet[40..44].copy_from_slice(&133_000u32.to_be_bytes()); // close
    packet[44..48].copy_from_slice(&1_779_424_068u32.to_be_bytes()); // last_trade_ts
    packet[48..52].copy_from_slice(&999u32.to_be_bytes()); // oi
    packet[52..56].copy_from_slice(&1500u32.to_be_bytes()); // oi_h
    packet[56..60].copy_from_slice(&800u32.to_be_bytes()); // oi_l
    packet[60..64].copy_from_slice(&1_779_424_068u32.to_be_bytes()); // exchange_ts
    // Populate the 5 levels per side with non-zero data so the decoder traverses everything.
    for level in 0..5 {
        let off_bid = 64 + level * 12;
        packet[off_bid..off_bid + 4].copy_from_slice(&10u32.to_be_bytes());
        packet[off_bid + 4..off_bid + 8]
            .copy_from_slice(&(132_741u32 - level as u32 * 10).to_be_bytes());
        packet[off_bid + 8..off_bid + 10].copy_from_slice(&2u16.to_be_bytes());
        let off_ask = 64 + 60 + level * 12;
        packet[off_ask..off_ask + 4].copy_from_slice(&8u32.to_be_bytes());
        packet[off_ask + 4..off_ask + 8]
            .copy_from_slice(&(132_743u32 + level as u32 * 10).to_be_bytes());
        packet[off_ask + 8..off_ask + 10].copy_from_slice(&3u16.to_be_bytes());
    }
    packet
}

fn wrap_frame(packets: &[Vec<u8>]) -> Vec<u8> {
    let total_payload: usize = packets.iter().map(|p| p.len()).sum();
    let mut frame = Vec::with_capacity(2 + packets.len() * 2 + total_payload);
    frame.extend_from_slice(&(packets.len() as u16).to_be_bytes());
    for p in packets {
        frame.extend_from_slice(&(p.len() as u16).to_be_bytes());
        frame.extend_from_slice(p);
    }
    frame
}

fn decode_single_full(c: &mut Criterion) {
    let packet = build_full_packet(738_561);
    let frame = wrap_frame(&[packet]);
    let decoder = Decoder::new(|_: u32| Some(2u8), 2);

    c.bench_function("decode_single_full_packet", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(1);
            let n = decoder.decode_frame(std::hint::black_box(&frame), &mut out);
            assert_eq!(n, 1);
        });
    });
}

fn decode_100_packet_frame(c: &mut Criterion) {
    let packets: Vec<Vec<u8>> = (0..100u32)
        .map(|i| build_full_packet(700_000 + i))
        .collect();
    let frame = wrap_frame(&packets);
    let decoder = Decoder::new(|_: u32| Some(2u8), 2);

    c.bench_function("decode_100_packet_frame", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(100);
            let n = decoder.decode_frame(std::hint::black_box(&frame), &mut out);
            assert_eq!(n, 100);
        });
    });
}

fn decode_32b_index(c: &mut Criterion) {
    // The 36 B index frame captured live — most common shape during index-heavy market hours.
    let mut packet = vec![0u8; 32];
    packet[0..4].copy_from_slice(&256_265u32.to_be_bytes());
    packet[4..8].copy_from_slice(&2_349_670u32.to_be_bytes()); // ltp
    packet[8..12].copy_from_slice(&2_354_860u32.to_be_bytes()); // high
    packet[12..16].copy_from_slice(&2_339_730u32.to_be_bytes()); // low
    packet[16..20].copy_from_slice(&2_345_469u32.to_be_bytes()); // open
    packet[20..24].copy_from_slice(&2_362_312u32.to_be_bytes()); // close
    packet[24..28].copy_from_slice(&(-12_130i32).to_be_bytes()); // change
    packet[28..32].copy_from_slice(&1_779_424_068u32.to_be_bytes()); // ts
    let frame = wrap_frame(&[packet]);
    let decoder = Decoder::new(|_: u32| Some(2u8), 2);

    c.bench_function("decode_32b_index_packet", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(1);
            let n = decoder.decode_frame(std::hint::black_box(&frame), &mut out);
            assert_eq!(n, 1);
        });
    });
}

criterion_group!(benches, decode_single_full, decode_100_packet_frame, decode_32b_index);
criterion_main!(benches);
