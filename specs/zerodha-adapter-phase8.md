# Zerodha adapter — Phase 8 design

**Status**: draft for review
**Goal**: make the Zerodha adapter usable from a Python `LiveNode` strategy
**Rust side**: ✅ feature-complete through Phase 7 (steps 1–12 landed)
**This doc**: surfaces the design questions before any PyO3 / Python code lands

---

## 1. What "usable from Python" actually means

`LiveNode.builder()`-driven strategies expect to register an adapter via standard factories:

```python
from nautilus_trader.adapters.zerodha import (
    ZERODHA,
    ZerodhaDataClientConfig,
    ZerodhaExecClientConfig,
    ZerodhaLiveDataClientFactory,
    ZerodhaLiveExecClientFactory,
)

node = (
    TradingNode.builder()
    .add_data_client(ZerodhaLiveDataClientFactory, ZerodhaDataClientConfig(...))
    .add_exec_client(ZerodhaLiveExecClientFactory, ZerodhaExecClientConfig(...))
    .build()
)
```

That single user-facing line of code drives every Phase 8 decision below.

---

## 2. Decisions to settle (with concrete options + recommendation)

### 2.1 LiveMarketDataClient + LiveExecutionClient — full trait impl vs. primitives

Nautilus's `LiveMarketDataClient` (`nautilus_trader/live/data_client.py:82`) and
`LiveExecutionClient` (`.../execution_client.py:66`) are Python classes with a large surface:
`_subscribe_quote_ticks`, `_subscribe_trade_ticks`, `_subscribe_order_book_deltas`,
`_request_bars`, `_submit_order`, `_modify_order`, `_cancel_order`, `_query_account`, etc.
LiveNode talks to these traits — not our Rust types directly.

**Option A — Full trait impl**
Write Python subclasses `ZerodhaDataClient(LiveMarketDataClient)` and
`ZerodhaExecutionClient(LiveExecutionClient)` that delegate to the Rust types via PyO3.

- ✅ Drop-in for `LiveNode.add_data_client(...)` — strategies need zero glue.
- ✅ Matches every other adapter in the workspace (Binance, IBKR, ThetaData all do this).
- ❌ More Python code (~300–500 LOC per client class).
- ❌ Some Nautilus methods don't map cleanly to Kite (`query_account_state`, `request_order_status_report` from arbitrary ids, etc.) — need stubs that no-op or return defaults.

**Option B — PyO3 primitives only**
Expose `ZerodhaDataDispatcher` and `ZerodhaExecClient` as `#[pyclass]` types directly. User
writes their own thin `LiveDataClient` / `LiveExecutionClient` subclass in their strategy
repo if they want LiveNode integration.

- ✅ Less code now — maybe 1 day saved.
- ❌ Every consumer rewrites the same glue. Not how any other adapter works.
- ❌ Strategies can't be moved between adapters without rewriting their wiring.

**Recommendation: A.** The spec's §4 architecture and every reference adapter use this
shape — divergence would create maintenance debt. The "no clean mapping" Nautilus methods
just become no-ops with a warning log; that's normal.

---

### 2.2 Orchestrator vs. independent clients

Both data and exec need the **same** `ZerodhaSessionManager`, `ZerodhaInstrumentCache`, and
`ZerodhaHttpClient`. If two `add_*_client(...)` calls construct them independently, we'd
end up with two parallel token-rotation paths, two daily-refresh tasks, etc.

**Option A — Hidden singleton-by-config**
Factories share-by-`config.api_key`: first call constructs the session+cache+http, the
second call's factory looks up and reuses.

- ✅ The strategy code looks natural — two `add_*_client` calls, no orchestrator concept.
- ❌ Magic global state. Two configs with the same key but different `base_url` would
  silently conflict.

**Option B — Explicit `ZerodhaClient` orchestrator**
A single `ZerodhaClient::new(config)` owns the shared deps; data + exec are accessors.
Factory uses an `Arc<ZerodhaClient>` shared by both factories' configs.

- ✅ Clear ownership; tests can construct subsets.
- ❌ Slightly weirder API — strategy code holds a `client` object to pass into both factories.

**Option C — Combined factory**
Single factory call adds both data + exec, returning paired clients.

- ✅ Atomic, no possible mismatch.
- ❌ Not how `LiveNode.builder()` works; would require a custom builder method.

**Recommendation: A (singleton-by-config) with an explicit key.**
The shared-resources key is `(api_key, base_url)`. A `static OnceCell<Mutex<HashMap<Key,
Arc<SharedDeps>>>>` lets both factories find the same `SharedDeps`. Documentation calls out
that two configs with the same `api_key` MUST agree on `base_url`. This mirrors how
Binance/Bybit handle the same "data + exec sharing one auth" problem.

---

### 2.3 `pyo3-stub-gen` annotations

The `nautilus-expert/pyo3-bindings.md` rule says every `#[pyclass]` needs `gen_stub_pyclass`,
every `#[pymethods]` block needs `gen_stub_pymethods`. This is a workspace invariant —
non-negotiable per the rules. The cost is annotation boilerplate (~3 lines per class).
ThetaData's `python/mod.rs` is the canonical pattern.

**No option here — adopt the workspace rule.** Adds ~50 LOC of `#[gen_stub_pyclass]`
attributes across ~10 classes.

---

### 2.4 Config DTOs

**Spec contract**: `bon::Builder` Rust DTOs + `msgspec.Struct` Python mirrors. Credentials
NEVER in DTOs — they resolve from env vars in `credential.rs` (already done).

**Shape we'll need**:
- `ZerodhaDataClientConfig`: instrument-provider config, WS / HTTP base URL overrides
- `ZerodhaExecClientConfig`: order-store path, account_id, polling cadence, default product

No real decision to make here — the spec sets it. Estimated ~80 LOC across both Rust
configs + both Python mirrors.

---

### 2.5 TC matrix CI coverage — full vs. minimum

Spec calls out TC-D01..D45 + D50..D55 + D90 (~50 data tests) and TC-E01..E55 + E91..E101
(~70 exec tests). These are mock-server tests against canned Kite responses.

**Option A — Full TC matrix**
Build out a full `axum`-based mock server (binance/bitmex pattern: `crates/adapters/binance/tests/`),
write JSON fixtures, implement every TC line.

- ✅ Production-grade regression coverage.
- ❌ ~1 day of work on its own. Most of it is fixture wrangling.

**Option B — TC smoke only**
A handful of the most load-bearing TCs (D04 instruments, D05 quote subscribe, E01 market
buy, E15 cancel, E91 reconnect). ~10 tests instead of ~120.

- ✅ Catches the obvious regressions.
- ❌ A future Kite layout shift in a less-trodden path lands in production unnoticed.

**Recommendation: B for v1, A as v1.1 work.**
The captured fixture in `tests/fixtures/ws_session_2026-05-20.bin` already catches WS-layout
shifts (per the Phase 3 fixture replay test). Order REST is the gap; ~10 mock-server tests
cover the load-bearing flows. The remaining ~110 TCs are insurance against unlikely paths
and can be a follow-up.

---

### 2.6 Mock-server harness choice

If we go with TC option B (above), we still need the mock harness. The Binance / BitMEX /
Coinbase / OKX adapters all use `axum` for this (see their `Cargo.toml` dev-deps).

**No decision — adopt `axum` (workspace pattern).** Adds ~30 LOC of harness setup.

---

### 2.7 Python factory wrappers

`nautilus_trader/adapters/zerodha/factories.py` will mirror ThetaData's pattern:
`ZerodhaLiveDataClientFactory.create(loop, name, config, msgbus, cache, clock) -> ZerodhaDataClient`.

No real decision — boilerplate. ~80 LOC across both factories. The interesting bit is the
shared-deps lookup (§2.2).

---

### 2.8 Example breadth

Spec lists three examples:
- `examples/live/zerodha/login.py` — ✅ already shipped (step 3)
- `examples/live/zerodha/trading_node_quotes.py` — pending
- `examples/live/zerodha/trading_node_orders.py` — pending

**Recommendation: ship both.** Together they're ~150 LOC and they're the only end-to-end
verification a user has that the wiring works.

---

### 2.9 Docs scope

`docs/integrations/zerodha.md` per spec §8.3. Topics:
- Auth setup + daily login workflow + no-sandbox caveat
- `ZerodhaOrderMeta` side-band map for product (CNC/MIS/NRML)
- Bar-open timestamp convention (IST anchoring)
- Single-account-per-key limitation
- `ZerodhaAccountSnapshot` event (v1.x — segment-level breakdown)
- Known limitations: TradeTick aggregation loss within one WS frame; `OrderBookSnapshots`
  not deltas; PnL excludes Indian charges (STT/GST/exchange fees)
- v1.x roadmap (GTT, bracket/cover, postback webhook, automated TOTP login)

**Recommendation: ship this.** ~300 LOC of Markdown. Operators will read it once and refer
back; not shipping it means every consumer asks the same questions.

---

## 3. Estimated effort under each path

| Path | Scope | Effort |
|---|---|---|
| **Minimum** | §2.1 A + §2.2 A + §2.4 + §2.7 + §2.8 trading_node_quotes only | ~3 hours |
| **Recommended** | + §2.3 stub-gen + §2.5 B mock TCs + §2.8 both examples + §2.9 docs | ~6 hours |
| **Full spec** | + §2.5 A full TC matrix | ~12 hours |

---

## 4. Sequencing

Whichever path is chosen, the order is forced:

1. **stub-gen + PyO3 primitives** (`ZerodhaInstrumentCache`, `ZerodhaWsClient`,
   `ZerodhaExecClient`, `ZerodhaDataDispatcher`, `ZerodhaSessionManager`, `KiteResolution`).
   No decisions inside — mechanical.
2. **Config DTOs** (Rust + Python). Depends on (1) only.
3. **Shared-deps registry** (§2.2 singleton-by-config). Depends on (1).
4. **Python `ZerodhaDataClient` + `ZerodhaExecutionClient` subclasses**. Depend on (1, 2, 3).
5. **Python factories**. Depend on (4).
6. **Examples**. Depend on (5).
7. **Mock-server TCs** (if chosen). Depend on (1) only — runs against Rust primitives.
8. **Docs**. Last, but the outline can be drafted in parallel.

---

## 5. Open question for the user

**Which path do you want me to implement?**

- (a) **Minimum** — fastest path to a Python-driveable adapter; smoke test only
- (b) **Recommended** — adds stub-gen, basic TC coverage, both examples, docs
- (c) **Full spec** — closes every Phase 8 spec item

And — **any of the §2 sub-decisions you want to override?** Defaults baked in if you don't
specify: §2.1 A, §2.2 A, §2.3 yes, §2.5 B, §2.7 ThetaData pattern, §2.9 ship.
