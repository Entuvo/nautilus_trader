# ThetaData adapter (data-only)

Pure-Python `LiveMarketDataClient` for [ThetaData](https://thetadata.us) options
and equities, talking to the local **ThetaTerminal** proxy over HTTP+WebSocket.

> **Status:** divergence from upstream — there is no ThetaData adapter at
> `nautilus_trader@ef69aaf2a6` (neither Rust crate nor Python module). This
> adapter ships under `nautilus_trader/adapters/thetadata/` with a patch
> ledger row in `.sync-upstream/PATCHES.md` and a retirement criterion of
> "upstream merges a ThetaData adapter."

---

## 1. Authentication

The adapter **never handles credentials**. Authentication is fully owned by
ThetaTerminal (a Java daemon) via its `creds.txt`. There is no `credential.rs`
analog and no env var to set in the Nautilus process.

| OS | `creds.txt` path |
|----|------------------|
| macOS | `~/ThetaData/ThetaTerminal/creds.txt` |
| Linux | `~/ThetaData/ThetaTerminal/creds.txt` |
| Windows | `%USERPROFILE%\ThetaData\ThetaTerminal\creds.txt` |

Format (per ThetaData docs):

```
username=<your-thetadata-username>
password=<your-thetadata-password>
```

Tier changes (e.g., `value` → `standard` → `pro`) are entered in the
ThetaTerminal UI on first login, not in adapter config.

**Start the terminal before starting Nautilus:**

```bash
java -jar ThetaTerminal.jar
# verify it's listening:
curl http://127.0.0.1:25510/v2/system/version
```

---

## 2. Tier-gated features

ThetaData tiers gate both subscription types and historical depth. The exact
matrix is account-specific and lives on
[thetadata.us pricing](https://www.thetadata.us/subscribe); this adapter
**does not enforce tiers** (the terminal refuses unauthorized subscriptions).
Document your tier in `config.tier` so downstream tooling can reason about
expected availability.

| Tier | Live quote stream | Live trade stream | Historical depth | Notes |
|---|---|---|---|---|
| `value` | options (limited) | options | shallow | Sufficient for development/research |
| `standard` | options + equities | options + equities | medium | Most strategies |
| `pro` | full OPRA quote depth | full OPRA trade tape | deep | Production / institutional |

Confirm against the official pricing page before relying on a tier-specific
feature; the matrix shifts over time. The wire-format note at
`docs/architecture/notes/thetadata-wire-format.md` flags features that need
live-terminal verification.

---

## 3. Wire-format gotchas

Behaviors that will silently produce wrong data if you don't account for them:

- **Strike scale** — wire strikes are in **1/10ᵗʰ of a cent** (`×10000`).
  OCC encoding uses **thousandths** (`×1000`). The adapter converts at the
  symbology boundary; never pass a wire strike to an OCC-aware path or vice
  versa. Example: $480.00 → wire `4800000` → OCC `00480000`.
- **REST response format** — JSON envelope by default
  (`{"header": {...}, "response": [...]}`). Each historical row is a
  positional-aligned array; field names come from `header.format`. The
  adapter uses fixed column indices documented in
  `decode.py` (`_Q_*` / `_T_*` / `_O_*` constants). If a future API
  revision reorders fields, the conformance test is the canary.
- **DST handling**:
  - Spring-forward gap times (e.g., 2025-03-09 02:00–03:00 ET) **raise
    `DecodeError`** — they don't exist in local time.
  - Fall-back ambiguous times (e.g., 2025-11-02 01:00–02:00 ET) resolve
    to `fold=0` (earlier UTC, EDT side). Verify ThetaData's convention
    if mid-fall-back data ever looks an hour off.
- **Condition codes** — only 145 (Buyer) and 146 (Seller) emit an
  `AggressorSide`; everything else emits `AggressorSide.NO_AGGRESSOR`.
  The R3 inferer in `nautilus-formulas` downstream-fills from the
  contemporaneous NBBO. The adapter **never invents an aggressor.**
- **OHLC WS frames are session-cumulative** — dropped at the adapter.
  Use `_request_bars` (REST `/v2/hist/stock/ohlc`) for bar-aligned data.
- **30-day historical chunk limit** — `hist_quotes` / `hist_trades` /
  `hist_ohlc` split longer ranges automatically; a failed chunk after
  retries propagates the error (no partial returns — that would mask
  silent gaps).
- **Default ports** — HTTP `25510`, WS `25520`. Earlier adapter drafts
  used `25503` — that was wrong. Verify against the running terminal if
  in doubt.
- **Negative `sequence` values on trade rows** — OPRA quirk; treat as
  opaque identifiers, don't compare ordinally.

---

## 4. Node config example

```python
from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.factories import ThetaDataLiveDataClientFactory
from nautilus_trader.model.identifiers import InstrumentId

config = ThetaDataDataClientConfig(
    http_url="http://127.0.0.1:25510",
    ws_url="ws://127.0.0.1:25520/v1/events",
    tier="standard",
    http_rate_limit_per_sec=5.0,
    instrument_ids=[
        InstrumentId.from_str("SPXW250620C00480000.THETADATA"),
    ],
)

# In TradingNodeConfig:
#   data_clients = {"THETADATA": config}
#   data_client_factories = {"THETADATA": ThetaDataLiveDataClientFactory}
```

---

## 5. Testing

Unit + conformance tests live under
`tests/integration_tests/adapters/thetadata/` and run unconditionally in CI
against `MockThetaTerminal` fixtures. The manual smoke
(`nautilus-ops catalog backfill thetadata`) is gated by
`THETADATA_TERMINAL_AVAILABLE=1` and runs against a real local terminal.

Run the full adapter suite:

```bash
pytest tests/integration_tests/adapters/thetadata/ -v
```

DST + throughput coverage:

```bash
pytest tests/integration_tests/adapters/thetadata/test_decode.py::TestDateMsToUnixNanos
pytest tests/integration_tests/adapters/thetadata/test_decode.py -m slow
```
