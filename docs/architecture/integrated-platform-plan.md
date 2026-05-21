# Integrated Platform Plan

**Date:** 2026-05-21
**Supersedes:** `integrated-platform-plan.archived-2026-05-21-phase-organized.md` (kept for history; this is canonical)

## Intent

Build on top of nautilus. Use what's already there. Design every requirement so it depends on a small number of nautilus capabilities through **one named file** — so when an upstream capability changes, one file changes.

Two-sided constraint:
1. **Leverage upstream** — benefit from continuing nautilus development.
2. **Modularity** — our additions don't break when upstream churns.

Resolved by: thin wrappers, single seam per requirement, ABI surface test naming exactly which upstream symbol drifted.

---

## 1. Capabilities Inventory

What nautilus already provides that we build on.

### Event / data model — `crates/model/src/data/`

- `TradeTick`, `QuoteTick`, `Bar`, `OrderBookDelta` / `OrderBookDeltas` / `OrderBookDepth10`.
- `OptionGreeks`, `GreeksData`, `PortfolioGreeks`.
- `MarkPriceUpdate`, `IndexPriceUpdate`, `FundingRateUpdate`, `InstrumentClose`.
- **`CustomData` + `register_custom_data_json` + `register_python_data_class`** — first-class extension point for our own typed payloads. Custom types persist via `register_arrow`.

### Indicator framework

- Base: `nautilus_trader/indicators/base.pyx` (`Indicator`); Rust trait at `crates/indicators/src/indicator.rs`.
- Registration on `Strategy` / `Actor` in `nautilus_trader/common/actor.pyx`:
  - `register_indicator_for_quote_ticks` (line 825)
  - `register_indicator_for_trade_ticks` (line 853)
  - `register_indicator_for_bars` (line 881)
- 38 Rust-core indicators in `crates/indicators/src/{momentum,average,volatility,ratio,book}/`.

### Options / Greeks

- `subscribe_option_greeks(instrument_id, interval)` — `actor.pyx:2055`.
- `on_option_greeks` handler — `actor.pyx:543`.
- `GreeksCalculator` already instantiated on every Strategy as `self.greeks` — `actor.pyx:766`.
- Black-Scholes + `imply_vol_and_greeks` / `refine_vol_and_greeks` — `crates/model/src/data/black_scholes.rs`.

### Adapters (3-component: `InstrumentProvider` + `LiveMarketDataClient` + `LiveExecutionClient`)

- ThetaData — `crates/adapters/thetadata/` — data-only, OPRA-consolidated.
- Zerodha — `crates/adapters/zerodha/` — data + execution, NSE/BSE.
- IB — full data + execution; RTVolume tick quality degraded (per options-formulas FINDINGS).
- Deribit / Bybit / OKX / Binance — full incl. venue-streamed Greeks.

### Execution

- `submit_order` / `modify_order` / `cancel_order` / `cancel_all_orders` on Strategy.
- `OrderFactory` for typed construction.
- Account / position state via `Cache`.

### Backtest / replay

- `BacktestNode` / `BacktestEngine` — `nautilus_trader/backtest/`.
- `ParquetDataCatalog` — `nautilus_trader/persistence/catalog/parquet.py`.
- `TestClock` vs `LiveClock` auto-injected.
- Replay of `subscribe_option_greeks` already supported in `backtest/data_client.pyx:343`.

### MessageBus outbound seam for derived data

- **`publish_data(DataType, Data)` — `actor.pyx:2925`**
- **`publish_signal(name, value, ts_event)` — `actor.pyx:2944`**
- `register_serializable_type` — `serialization/base.pyx:296`.

### CLI

- Rust admin CLI: `crates/cli/`.
- Python entrypoints: `python -m nautilus_trader.backtest`, `python -m nautilus_trader.live`.
- No general operator-verb CLI exists yet.

### Stable surfaces to depend on

`Strategy` event hooks, `register_indicator_for_*`, `subscribe_*` family, `publish_data` / `publish_signal`, `register_serializable_type`, `register_custom_data_json`, `OrderFactory`, `Cache`, `MessageBus`, `ParquetDataCatalog`.

### Volatile surfaces to avoid

Cython `.pxd` private members, `_msgbus.publish_c`, Rust `actor/indicators.rs` private registration internals, adapter implementation details.

---

## 2. Per-Requirement Plans

Each requirement has the same three-bullet shape: capabilities used, build-on-top, update-proofing seam.

### R1 — Chart UI live subscriptions + order commands

- **Capabilities used:** `Strategy` event hooks (`on_quote_tick`, `on_trade_tick`, `on_bar`, `on_order_book_deltas`, `on_event`), `subscribe_quote_ticks` / `subscribe_trade_ticks` / `subscribe_bars` / `subscribe_order_book_deltas`, `OrderFactory` + `submit_order` / `modify_order` / `cancel_order`, `ParquetDataCatalog` for warmup.
- **Build-on-top:** keep current `ChartBridgeStrategy` (`nautilus-chart/bridge/nautilus_chart_bridge/chart_bridge_strategy.py`). Event hooks already map to `envelope.py` schemas; inbound `SubmitOrder` / `CancelOrder` / `ModifyOrder` dispatch through Strategy methods. No new abstraction.
- **Update-proofing seam:** `nautilus-chart/bridge/nautilus_chart_bridge/envelope.py` (engine → wire) and `catalog.py` (engine → envelope conversion). Upstream rename of a field on `TradeTick` / `QuoteTick` / `Bar` → only `catalog.py` changes.

### R2 — Live NDF / FEP / Black-76 over tick streams

- **Capabilities used:** `Indicator` base, `register_indicator_for_trade_ticks` and `register_indicator_for_quote_ticks`, `subscribe_option_greeks` + `on_option_greeks`, `GreeksCalculator`, `publish_signal`.
- **Build-on-top:** new `nautilus-formulas` Python package. Each locked formula is a subclass of `Indicator`:
  - `NDFIndicator(Indicator)` registered against trade ticks.
  - `FEPIndicator(Indicator)` registered against trade ticks + sibling quote-tick indicator for NBBO.
  - `Black76Greeks(Indicator)` consuming trade ticks + Greeks events.
  Each indicator calls into existing `options-formulas` Python (`import options_formulas.ndf`, etc.) — no formula re-implementation. Strategy holds a small `FormulasMixin` that registers them and republishes via `publish_signal("ndf", value)`.
- **Update-proofing seam:** `nautilus-formulas/src/nautilus_formulas/adapters.py` — the only file touching `Indicator.handle_trade_tick` / `handle_quote_tick` signatures.

### R3 — Online HMM / BOCPD / composites

- **Capabilities used:** same `Indicator` registration as R2; `register_serializable_type` for posterior-distribution payloads; `register_custom_data_json` so changepoint events survive catalog round-trip; `publish_data(DataType, Data)` for the typed stream.
- **Build-on-top:** in same `nautilus-formulas` package add `HMMRegimeIndicator` and `BOCPDIndicator` subclasses fed by R2 vol-surface outputs; emit `RegimePosterior` and `ChangepointEvent` as `CustomData` subclasses registered once at strategy startup. Composite signal is a third indicator that subscribes to the others' published values via `self.subscribe_data(DataType(RegimePosterior))` — no orchestrator class. The BOCPD pitfalls (level-vs-velocity; `P(r=0) ≡ H`; propagate joint not conditional) ship as invariant tests in `nautilus-formulas/tests/`.
- **Update-proofing seam:** `nautilus-formulas/src/nautilus_formulas/types.py` (custom data type definitions + arrow registration).

### R4 — Multi-broker tape-quality awareness

- **Capabilities used:** existing adapter clients in `crates/adapters/thetadata/`, `crates/adapters/zerodha/`, `nautilus_trader/adapters/interactive_brokers/`; `InstrumentId.venue` already on every tick.
- **Build-on-top:** a small `TapeQuality` enum (`OPRA_CONSOLIDATED` | `VENUE_BEST` | `RTVOLUME_DEGRADED`) keyed by `Venue` in `nautilus-formulas/src/nautilus_formulas/quality.py`. Each formula indicator declares `REQUIRED_QUALITY`; refuses (logs + skips publish) below threshold unless strategy passes `allow_degraded=True`. **No adapter changes** — venue identity already in the data.
- **Update-proofing seam:** `quality.py` — only file referencing venue strings; one map entry per new adapter.

### R5 — Indicator catalog the chart reads back

- **Capabilities used:** `publish_signal` emits to `signals.{name}.{strategy_id}` on the MessageBus.
- **Build-on-top:** in `chart_bridge_strategy.py`, in `on_start` add `self._msgbus.subscribe("signals.*", handler=self._on_signal)` and fan out as a new `SignalEnvelope` in `envelope.py`. Chart receives the same numeric the engine sees — single source of truth — for every R2/R3 indicator and any stock `nautilus_trader.indicators.*`. No registry, no separate indicator service.
- **Update-proofing seam:** `envelope.py` `SignalEnvelope` + `_on_signal` method on the bridge strategy.

### R6 — Same code in live, backtest, replay

- **Capabilities used:** `BacktestNode` / `BacktestEngine`, `ParquetDataCatalog`, `backtest/data_client.pyx:343 subscribe_option_greeks` (already implemented for replay), `TestClock` vs `LiveClock` auto-injection.
- **Build-on-top:** nothing. `Strategy` subclasses run unchanged across modes. Discipline: `nautilus-formulas` ships with zero wall-clock dependencies (use `self.clock.timestamp_ns()`, never `time.time()`); `register_arrow` called once at module import so custom signal types persist in the catalog.
- **Update-proofing seam:** `nautilus_formulas/__init__.py` `register_all()` function called from `Strategy.on_start`. If upstream changes registry signatures, fix one function.

### R7 — Operator surface (CLI, MCP, NL panel)

- **Capabilities used:** `python -m nautilus_trader.backtest` / `live`, `TradingNode` / `BacktestNode` public APIs, chart bridge's existing FastAPI app on `ChartBridgeStrategy`.
- **Build-on-top:** one `nautilus` click CLI in a new sibling repo `nautilus-ops/` with verbs `run-live`, `run-backtest`, `replay`, `catalog ls`, `catalog warmup`, `chart` — each shells into existing `__main__` entry points or constructs `TradingNode` / `BacktestNode` directly. MCP server is a thin wrapper exposing the same verbs as tools. NL panel later — adds `/chat` route to existing FastAPI app routing to the same verbs.
- **Update-proofing seam:** `nautilus-ops/src/nautilus_ops/verbs.py` — every operator surface (CLI / MCP / chat) calls verbs from this one module.

---

## 3. Sync Regime

How upstream changes flow through without breaking what's built on top.

### Remote topology

- `origin` → our private fork (entuvo-labs)
- `upstream` → `nautechsystems/nautilus_trader`
- `develop` tracks upstream; `master` is integration.

### Pinning

`nautilus-chart`, `nautilus-formulas`, `nautilus-ops` pin `nautilus_trader` by git SHA in `pyproject.toml`. Bumps are explicit PRs.

### ABI surface test

One `tests/test_abi_surface.py` in `nautilus-formulas` (copied in `nautilus-chart`). Asserts via `inspect.signature` / `importlib`:

- `nautilus_trader.indicators.base.Indicator` — class exists with `handle_quote_tick(QuoteTick)`, `handle_trade_tick(TradeTick)`, `handle_bar(Bar)`, `initialized` property.
- `nautilus_trader.trading.strategy.Strategy` — methods `register_indicator_for_quote_ticks`, `register_indicator_for_trade_ticks`, `register_indicator_for_bars`, `subscribe_quote_ticks`, `subscribe_trade_ticks`, `subscribe_bars`, `subscribe_order_book_deltas`, `subscribe_option_greeks`, `publish_data`, `publish_signal`, `submit_order`, `modify_order`, `cancel_order`, `cancel_all_orders`; properties `cache`, `clock`, `msgbus`, `order_factory`, `greeks`.
- `nautilus_trader.model.data` — `TradeTick`, `QuoteTick`, `Bar`, `BarType`, `OrderBookDeltas`, `OptionGreeks`, `MarkPriceUpdate`, `FundingRateUpdate`, `InstrumentClose` importable with expected field set.
- `nautilus_trader.serialization.base.register_serializable_type` callable with `(cls, encoder, decoder)`.
- `nautilus_trader.persistence.catalog.parquet.ParquetDataCatalog` — `query`, `write_data`.
- `nautilus_trader.backtest.node.BacktestNode` and `nautilus_trader.live.node.TradingNode` constructors take config object.

When red, names exactly which symbol drifted; the seam to fix is in the corresponding R1–R7 entry above.

### Patch ledger

`.sync-upstream/PATCHES.md` — one table row per local divergence:

| file | reason | upstream PR | retirement criterion |
|---|---|---|---|

Examples to seed it:
- `build.py` feature flag (local builds without optional features) — retire when upstream gates these by default.
- `Cargo.lock` axum entry — local-only.
- IB delayed-tick types fix (commit `ab42a11e65`) — retire when merged upstream.
- `ImportableConfig.create` monkey-patch in `nautilus-chart/engine/main.py` — retire when upstream passes `dec_hook` through.

Reviewed every sync.

### Sync workflow

```
git fetch upstream
git rebase upstream/develop                       (on a sync/<date> branch)
re-apply ledger patches that haven't retired
pytest tests/test_abi_surface.py                  ABI test names broken symbols
nautilus-chart + nautilus-formulas smoke tests    replay 60s fixture parquet
                                                  assert NDF/FEP publish
                                                  assert chart WS receives bars
green  → merge to master
red    → ABI failure names the upstream symbol + the seam file from R1–R7
```

---

## Package layout (what gets built)

```
nautilus_trader/      upstream fork; minimal local diff, ledger-tracked
nautilus-chart/       existing; seams = envelope.py + catalog.py
nautilus-formulas/    new; seams = adapters.py + types.py + quality.py + __init__.py
nautilus-ops/         new; seam   = verbs.py
options-formulas/     existing calibration lab; nautilus-formulas imports locked formulas from it
```

Five seams, named. ABI test catches drift; the named seam is where the fix lands.

---

## What this plan does NOT do (and why)

- **No new abstraction primitive** ("Node", "DerivedStream", "SignalPack"). `Indicator` + `register_indicator_for_*` + `publish_signal` / `publish_data` already cover every requirement.
- **No multi-tier promotion gates, no content-addressed signal packs.** Locked formulas are versioned through normal Python package versioning of `nautilus-formulas` + `options-formulas`.
- **No fidelity-tag protocol added to upstream events.** Tape quality is inferred from `InstrumentId.venue` via a small map; no new fields on `TradeTick`.
- **No five-layer architecture.** Two added repos (`nautilus-formulas`, `nautilus-ops`), one existing repo extended (`nautilus-chart`), and the upstream fork. Done.

Anything beyond this becomes a candidate for a follow-up plan only when a concrete requirement demands it.
