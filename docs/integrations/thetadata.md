# ThetaData

NautilusTrader includes an adapter for [ThetaData](https://www.thetadata.net/) — an
institutional-grade US options, stocks, and indices data vendor. The adapter is
**data-only** (no execution client); pair it with a sandbox or another adapter
(e.g. Interactive Brokers) for execution.

The adapter supports:

- Subscribing to real-time NBBO quotes and tape trades via WebSocket.
- Requesting historical quotes, trades, and OHLC bars via REST.
- Bulk-backfilling a Nautilus `ParquetDataCatalog` from REST.
- Instrument enumeration via the option chain (`list_expirations` + `list_strikes`).
- Both **Rust `LiveNode`** and **Python `TradingNode`** entry points.

## Architecture

ThetaData ships a Java application — **ThetaTerminal** — that runs locally and
proxies both the REST and streaming APIs. The adapter talks to the local Terminal,
**not** ThetaData's cloud directly; the Terminal owns authentication and upstream
connection management.

```text
┌─────────────────┐  REST (HTTP)   ┌──────────────┐  FPSS (TCP)  ┌──────────────────┐
│ Nautilus        │ ──────────────▶│ ThetaTerminal│ ─────────────▶ ThetaData servers│
│ adapter         │  WS (streaming)│  (Java JAR)  │              │  (nj-a/nj-b)     │
└─────────────────┘ ──────────────▶└──────────────┘              └──────────────────┘
   ↑ 25503/v3                       ↑ creds.txt
   ↑ 25520/v1/events                ↑ Standard / Pro subscription
```

The following adapter classes are available:

### Python (TradingNode)

- `ThetaDataDataClient` — `LiveMarketDataClient` orchestrator; routes engine commands.
- `ThetaDataDataClientConfig` — msgspec-backed config; serializable via `ImportableConfig`.
- `ThetaDataLiveDataClientFactory` — `LiveDataClientFactory` subclass for `TradingNode`.
- `ThetaDataInstrumentProvider` — builds Nautilus `OptionContract` instruments from OCC IDs.

### Rust (LiveNode) and pyo3 primitives

- `ThetaDataDataClient` (Rust) — `DataClient` implementation for `LiveNode`.
- `ThetaDataDataClientFactory` (Rust + pyo3 re-export) — wires into `LiveNode::builder`.
- `nautilus_pyo3.ThetaDataHttpClient` — REST primitive (`hist_quotes`, `hist_trades`,
  `hist_ohlc`, `list_expirations`, `list_strikes`, `list_contracts`, `hist_stock_eod`,
  `hist_index_eod`, `option_contract_from_id`).
- `nautilus_pyo3.ThetaDataWsClient` — WebSocket primitive with reconnect-replay.

## Prerequisites

1. **Java 21+** — `java -version` to confirm.
2. **ThetaTerminal v3** — download `ThetaTerminalv3.jar` from
   [download-unstable.thetadata.us](https://download-unstable.thetadata.us/ThetaTerminalv3.jar).
3. **A ThetaData subscription** — see
   [pricing](https://www.thetadata.net/pricing). The adapter targets Options
   tiers; live streaming requires Standard ($80/mo) at minimum.

## Authentication

The local Terminal owns authentication. The adapter passes credentials **only by
filesystem**, never on the wire. Create `creds.txt` next to the JAR:

```text
your.email@example.com
your-password
```

Then start the Terminal:

```bash
java -jar /path/to/ThetaTerminalv3.jar
```

Alternative credential locations:

- `--creds-file <path>` CLI flag.
- `THETADATA_CREDENTIALS_FILE` environment variable pointing at the file.

The adapter **does not** read `THETADATA_API_KEY` or `THETADATA_API_SECRET` env
vars; the upstream Terminal handles auth.

## Subscription tiers

ThetaData's Options tiers (verified against live Terminal 2026-05 build):

| Tier | Price | History | Live single-contract streams | Bulk OPRA stream |
|---|---|---|---|---|
| Value | $40/mo | 4 years | ✅ 1-minute resolution | ❌ |
| Standard | $80/mo | 8 years | ✅ tick NBBO + trades (10K quote / 15K trade caps) | ❌ |
| Pro | $160/mo | 12 years | ✅ tick | ✅ STREAM_BULK |

The adapter's `ThetaDataTier` enum drives stream-count enforcement client-side
(returns `Some(15_000)` from `max_concurrent_streams()` on Standard).

## Configuration

```python
from nautilus_trader.adapters.thetadata import (
    ThetaDataDataClientConfig,
    ThetaDataLiveDataClientFactory,
)

config = ThetaDataDataClientConfig(
    http_url="http://127.0.0.1:25503/v3",          # ThetaTerminal HTTP
    ws_url="ws://127.0.0.1:25520/v1/events",       # ThetaTerminal WebSocket
    tier="standard",                                # "value" | "standard" | "pro"
    http_timeout_secs=30,
    max_reconnects=10,
)
```

## Python TradingNode example

```python
from nautilus_trader.adapters.thetadata import (
    ThetaDataDataClientConfig,
    ThetaDataLiveDataClientFactory,
)
from nautilus_trader.live.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId

config = TradingNodeConfig(
    trader_id="TESTER-001",
    data_clients={
        "THETADATA": ThetaDataDataClientConfig(
            tier="standard",
            instrument_ids=[
                InstrumentId.from_str("SPXW260520C07400000.THETADATA"),
            ],
        ),
    },
)
node = TradingNode(config=config)
node.add_data_client_factory("THETADATA", ThetaDataLiveDataClientFactory)
node.build()
node.run()
```

This routes engine subscribe/request commands through the Python
`ThetaDataDataClient`, which delegates the actual REST/WS work to the pyo3
primitives (`nautilus_pyo3.ThetaDataHttpClient` and
`nautilus_pyo3.ThetaDataWsClient`). The same configuration is YAML/JSON
serializable through `ImportableConfig`.

## Rust LiveNode example

`LiveNode::builder().add_data_client(...)` continues to work with the Rust
`ThetaDataDataClient`. See `crates/adapters/thetadata/examples/node_data_tester.rs`.

## Examples

The crate ships three runnable examples under
`crates/adapters/thetadata/examples/`:

| Example | Purpose | Tier needed |
|---|---|---|
| `thetadata-hist-tester` | One-day historical quote pull, validates decode pipeline | Value+ |
| `thetadata-backfill-trades` | Multi-day catalog backfill for SPY/SPX/SPXW (ATM ± 20 strikes) | Standard+ |
| `thetadata-data-tester` | Live `LiveNode` smoke test, subscribes to quote+trade streams | Standard+ |

Run with `cargo run --release --example <name> -p nautilus-thetadata`.
Override the target instrument with `THETADATA_INSTRUMENT_ID="SPXW260520C07400000.THETADATA"`.

## Symbology

The adapter uses an OCC-style InstrumentId encoded as
`{ROOT}{YY}{MM}{DD}{C|P}{STRIKE_THOUSANDTHS}`:

| Wire format | Convention | Example |
|---|---|---|
| Internal canonical | `strike_thousandths: u64` (OCC ×1 000) | `$480 → 480_000` |
| REST `strike` query | Decimal dollars, 3 places | `"480.000"` |
| REST response `strike` | Float dollars | `480.0` |
| WS `contract.strike` | Integer ×1 000 (OCC thousandths) | `480000` |

:::warning
The v3 streaming docs example states the WS strike scale is `×10 000`
(`$480 → 4_800_000`). **This is documented incorrectly** — confirmed by
ThetaData support. The actual scale is `×1 000` (OCC convention). The adapter
uses the correct scale; do not be misled by the docs page.
:::

## REST/streaming wire format gotchas

The adapter handles several non-obvious wire-format details transparently:

1. **`right` capitalization** — REST returns `"CALL"` / `"PUT"` (uppercase);
   the decoder accepts `"call"|"put"|"C"|"P"|"CALL"|"PUT"`.
2. **Sequence numbers** — OPRA sequence numbers in `RestTradeRow` are encoded
   as 32-bit-cast-to-64 signed integers; values can come back negative. The
   `sequence` field is `i64`. Earlier versions (`u64`) silently dropped roughly
   half of all SPY trades on days when sequences wrapped.
3. **Timestamps** — REST timestamps are ISO 8601 millisecond strings without
   timezone offset (e.g. `"2026-05-15T09:30:22.025"`); WS timestamps are split
   into `date` (`YYYYMMDD`) + `ms_of_day`. Both are
   **America/New_York wall-clock** and converted to UTC via `chrono-tz` (handles
   DST correctly).
4. **`list_contracts` doesn't exist on v3 Terminal** — despite the docs page.
   The adapter implements `list_contracts` as a client-side composite of
   `list_expirations` + `list_strikes` + cross-product of `[call, put]`.
5. **OHLC piggyback frames** — TRADE-stream subscriptions auto-push session-
   cumulative OHLC summaries (`{"header":{"type":"OHLC",...}, "ohlc":{...}}`).
   These are not bar-interval aligned; the adapter parses them and logs at
   debug. For real bars, use historical requests or aggregate from ticks.
6. **STATE frames** — session-state notifications (`{"state":"START"}` at
   session open). Parsed and logged at debug.

## TC-D conformance matrix

The adapter targets the following subset of the data-conformance matrix
(see `nautilus-expert/rules/tc-data-matrix.md`):

| TC | Path | Status |
|---|---|---|
| TC-D01 | Request instruments | ✅ via composite list_contracts |
| TC-D03 | Load specific instrument | ✅ |
| TC-D20 | Subscribe quotes | ✅ live-verified |
| TC-D21 | Request historical quotes | ✅ |
| TC-D30 | Subscribe trades | ✅ |
| TC-D31 | Request historical trades | ✅ — 41.9M trades captured in 30-day backfill |
| TC-D40 | Subscribe bars | ⏭ (use ticks + aggregator; OHLC frames are session-cumulative) |
| TC-D41 | Request historical bars | ✅ |
| TC-D70 | Unsubscribe on stop | ✅ |
| TC-D71 | Custom subscribe params | ✅ |
| TC-D72 | Custom request params | ✅ |
| TC-D10–D15 | Order book | ⏭ skip — no L2 from ThetaData |
| TC-D50–D53 | Mark/index/funding | ⏭ skip — equities/options, not perps |
| TC-D60/61 | Instrument status/close | ⏭ skip — synthesizable from calendar |
| TC-D62/63 | Option greeks | ⏭ skip — REST-only, no streaming |

## Known limitations

- `AssetClass::Equity` is hardcoded for all options; SPX/NDX index options
  technically want `AssetClass::Index`.
- `OptionContract.expiration_ns` uses fixed 21:00 UTC (close-of-session in
  EST); during EDT this is 1 hour late.
- Default instrument metadata: `multiplier=100`, `tick=$0.01`, `lot=1` — won't
  fit sub-$3 nickel-tick contracts.
- WebSocket allows only one connection per Terminal — the adapter multiplexes
  every subscription through it.
- The streaming docs example for WS strike scale (`×10 000`) is wrong; the
  adapter uses the correct `×1 000` scale.

## Backfill the catalog

The backfill example writes directly to a Nautilus `ParquetDataCatalog`:

```bash
# Capture 30 days of trade ticks for SPY/SPX/SPXW (ATM ± 20 strikes):
THETADATA_DAYS_BACK=30 \
THETADATA_CATALOG_DIR=./data/thetadata-catalog \
cargo run --release --example thetadata-backfill-trades -p nautilus-thetadata
```

The resulting catalog uses the canonical Nautilus layout:

```text
./data/thetadata-catalog/
└── data/trades/
    ├── SPY260626P00750000.THETADATA/2026-05-15T13-30-00Z_2026-05-15T19-59-51Z.parquet
    ├── SPXW260604P07360000.THETADATA/...
    └── ...
```

Load via:

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog

cat = ParquetDataCatalog("./data/thetadata-catalog")
cat.list_instruments("trade_tick")
# → ['SPY260522C00740000.THETADATA', ...]
```

## Troubleshooting

**Subscribe returns `SUBSCRIBED` but no QUOTE/TRADE frames arrive.**
Almost always a strike-scale issue or a subscription tier issue. Run the
diagnostic script at
`crates/adapters/thetadata/examples/` (or attach `~/thetadata/terminal_standalone.py`
if you set it up during initial integration) to compare ack vs delivery. If
`STREAM_BULK` returns `INVALID_PERMS` but single-contract `STREAM` returns
`SUBSCRIBED` with zero data, double-check the strike encoding (must be
×1 000, not ×10 000) and that you're testing during US market hours
(9:30–16:00 ET regular; 16:15–04:00 ET extended for SPX).

**Live frame counts are zero but REST snapshot returns live updates.**
Markets may be in regular-session-closed but extended-session-open. SPX and
SPXW have an overnight session (16:15 ET – 04:00 ET next day); SPY does not
unless paired with the Globex extended session for futures-on-options.

**`Got NOT_FOUND error: No data found for your request` warnings during
backfill.** Normal — many far-OTM contracts have no trades on a given day.
The backfill skips these silently.

**`Got PERMISSION_DENIED` errors.** Some endpoints (e.g. greeks streaming,
bulk OPRA tape) require Pro tier. Check the
[pricing tier table](https://www.thetadata.net/pricing).
