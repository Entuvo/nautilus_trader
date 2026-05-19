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

| Example | Purpose |
|---|---|
| `thetadata-data-tester` | Live `LiveNode` smoke test — subscribes to quote+trade streams |
| `thetadata-hist-tester` | One-day historical-quote pull validating the REST → decode path |
| `thetadata-backfill-trades` | Multi-day catalog backfill (SPY/SPX/SPXW, ATM ± 20 strikes) |

```bash
# Live (requires market hours and Standard+ subscription):
java -jar ~/thetadata/ThetaTerminalv3.jar &
THETADATA_INSTRUMENT_ID="SPXW260520C07400000.THETADATA" \
  cargo run --release --example thetadata-data-tester -p nautilus-thetadata

# 30-day catalog backfill (writes per-instrument Parquet under ./data/thetadata-catalog):
THETADATA_DAYS_BACK=30 THETADATA_CATALOG_DIR=./data/thetadata-catalog \
  cargo run --release --example thetadata-backfill-trades -p nautilus-thetadata
```

## Known gap: Python TradingNode integration

The Rust `LiveNode` path is complete and live-verified. The Python `TradingNode` path is
**not yet wired** — `factories.py` currently re-exports the pyo3 class which does not pass
the `issubclass(factory, LiveDataClientFactory)` check in `nautilus_trader/live/node_builder.py`.
Closure is fully planned in [`PYTHON_INTEGRATION_PLAN.md`](./PYTHON_INTEGRATION_PLAN.md)
(9 atomic steps, ~6–10 hours of focused work, mirrors the bitmex pattern).

## See also

- Full integration guide: `docs/integrations/thetadata.md`
- Python integration gap + plan: [`PYTHON_INTEGRATION_PLAN.md`](./PYTHON_INTEGRATION_PLAN.md)
- Adapter architecture rules: `.claude/skills/nautilus-expert/rules/adapter-architecture.md`
