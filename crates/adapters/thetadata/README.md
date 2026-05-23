# nautilus-thetadata

[ThetaData](https://www.thetadata.net) integration adapter for the Nautilus trading engine.

## Overview

ThetaData is a market-data-only vendor specializing in US options, stocks, and indices. This
adapter is **data-only** — no execution client.

The adapter connects to a locally-running ThetaTerminal (Java JAR) which proxies:

- **HTTP REST** at `http://127.0.0.1:25503/v3` — historical, snapshot, and reference data.
- **WebSocket** at `ws://127.0.0.1:25520/v1/events` — streaming quotes, trades, and OHLC
  summaries.

The Terminal owns authentication; the adapter never sees credentials directly.

## Authentication

ThetaTerminal authenticates against your ThetaData account using a `creds.txt` file in its
working directory:

```text
your.email@example.com
your-password
```

Alternative locations:

- `--creds-file <path>` flag passed to `ThetaTerminalv3.jar`.
- `THETADATA_CREDENTIALS_FILE` environment variable.

## Tier-gated features

| Tier | Price | Bar resolution | History | Live streaming |
|---|---|---|---|---|
| Value | $40/mo | 1-minute | 4 years | ✅ single-contract |
| Standard | $80/mo | tick (OPRA NBBO) | 8 years | ✅ 10K quote + 15K trade caps |
| Pro | $160/mo | tick | 12 years | ✅ + `STREAM_BULK` (full OPRA tape) |

The `tier` config knob drives client-side stream-count enforcement.

## Wire-format gotchas (handled transparently by the adapter)

1. **WS strike scale is `×1 000` (OCC thousandths), not `×10 000`** — the v3 docs example
   `$480 → 4_800_000` is documented incorrectly. The actual scale is `$480 → 480_000`.
   Confirmed by ThetaData support.
2. **OPRA sequence numbers can be negative** — `RestTradeRow.sequence` is `i64`, not `u64`.
3. **Timestamps are America/New_York wall clock** — REST returns naive ISO 8601, WS sends
   split `date`/`ms_of_day`. Both are converted to UTC `UnixNanos` via DST-aware `chrono-tz`.
4. **`/v3/option/list/contracts` doesn't exist on the v3 Terminal** — the adapter composes
   the equivalent from `list_expirations` + `list_strikes`.
5. **OHLC piggyback frames** — TRADE-stream subscriptions auto-push session-cumulative OHLC
   summaries. The adapter parses them and logs at debug; they are not bar-interval-aligned.
6. **STATE frames** — session-state notifications (e.g. `{"state":"START"}` at session open).
   Parsed and logged at debug.

## Examples

| Example | Language | Purpose |
|---|---|---|
| `thetadata-data-tester` | Rust | Live `LiveNode` smoke test — subscribes to quote+trade streams |
| `thetadata-hist-tester` | Rust | One-day historical-quote pull validating the REST → decode path |
| `thetadata-backfill-trades` | Rust | Multi-day catalog backfill (SPX/SPXW, ATM ± 20 strikes) |
| `examples/sandbox/thetadata_backfill_trades.py` | Python | Same backfill as above via the `nautilus_pyo3.ThetaDataHttpClient` surface |

```bash
# Live (requires market hours and Standard+ subscription):
java -jar ~/thetadata/ThetaTerminalv3.jar &
THETADATA_INSTRUMENT_ID="SPXW260520C07400000.THETADATA" \
  cargo run --release --example thetadata-data-tester -p nautilus-thetadata

# 30-day catalog backfill — Rust (writes per-instrument Parquet under ./data/thetadata-catalog):
THETADATA_DAYS_BACK=30 THETADATA_CATALOG_DIR=./data/thetadata-catalog \
  cargo run --release --example thetadata-backfill-trades -p nautilus-thetadata

# 30-day catalog backfill — Python (must be invoked as `-m` from the project root so the
# editable dev tree wins over any site-packages copy of `nautilus_trader`):
THETADATA_DAYS_BACK=30 THETADATA_CATALOG_DIR=./data/thetadata-catalog THETADATA_CONCURRENCY=4 \
  python -u -m examples.sandbox.thetadata_backfill_trades
```

## Entry points (all live-verified or test-covered)

| Path | Status |
|---|---|
| Rust `LiveNode::builder().add_data_client(...)` | ✅ live-verified — 50K+ QuoteTicks streamed |
| Rust standalone (`ThetaDataHistoricalClient` direct) | ✅ live-verified — 41.9M-trade catalog |
| Python `TradingNode.add_data_client_factory("THETADATA", ThetaDataLiveDataClientFactory)` | ✅ |
| Python `ImportableConfig` (YAML/JSON node configs) | ✅ |

The Python orchestrator wraps two pyo3 primitives (`nautilus_pyo3.ThetaDataHttpClient`
and `nautilus_pyo3.ThetaDataWsClient`) and routes engine commands through them.
See `docs/integrations/thetadata.md` for the TradingNode example. The Rust
`ThetaDataDataClient` remains for `LiveNode` consumers — both paths coexist.

## See also

- Full integration guide: `docs/integrations/thetadata.md`
- Python integration plan (now executed): [`PYTHON_INTEGRATION_PLAN.md`](./PYTHON_INTEGRATION_PLAN.md)
- Adapter architecture rules: `.claude/skills/nautilus-expert/rules/adapter-architecture.md`
