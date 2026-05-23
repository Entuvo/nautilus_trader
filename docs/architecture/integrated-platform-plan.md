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

- `subscribe_option_greeks(instrument_id, interval)` — `actor.pyx:2048`.
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

- **`publish_data(DataType, Data)` — `actor.pyx:2925`** — topic `data.<TypeName>.*`. Use for any non-scalar payload (vectors, posteriors, surfaces).
- **`publish_signal(name, value, ts_event)` — `actor.pyx:2944`** — topic `signals.<Name>.*`. **Scalar-only**: enforces `type(value) in (int, float, str)` at runtime.
- `register_serializable_type(cls, to_dict, from_dict)` — `serialization/base.pyx:296` — msgbus external publishability + JSON round-trip.
- `register_arrow(data_cls, schema, encoder, decoder)` — `serialization/arrow/serializer.py:87` — parquet catalog round-trip; required for any custom type to survive `ParquetDataCatalog` replay.
- `register_python_data_class(type_name, data_class)` — `crates/model/src/python/data/mod.rs:597` — Python-typed `CustomData` visibility in the Rust core.

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
- **Shape constraint:** `Indicator` is **not** an `Actor`. It has no `_msgbus`, no `clock`, no `subscribe_*` / `publish_*`. The framework only calls `handle_quote_tick` / `handle_trade_tick` / `handle_bar`. Therefore: **republish happens in the Strategy, not the indicator.** Time inside an indicator must come from `tick.ts_event`, never wall-clock.
- **Build-on-top:** new `nautilus-formulas` Python package. Each locked formula is a subclass of `Indicator` exposing two accessors after every `handle_*`:
  - `latest_value() -> float | None` — scalar suitable for `publish_signal`.
  - `latest_payload() -> Data | None` — optional structured payload for `publish_data`.
  Formulas:
  - `NDFIndicator(Indicator)` registered against trade ticks; `latest_value()` returns scalar NDF.
  - `FEPIndicator(Indicator)` registered against trade ticks + sibling quote-tick indicator for NBBO.
  - `Black76Greeks(Indicator)` consuming trade ticks + Greeks events; `latest_payload()` returns a typed Greeks snapshot.
  Each indicator calls into existing `options-formulas` Python (`import options_formulas.ndf`, etc.) — no formula re-implementation. Strategy holds a small `FormulasMixin` that registers them, and in `on_trade_tick` / `on_quote_tick` (after the framework has driven the indicator) calls `publish_signal(name, ind.latest_value())` and/or `publish_data(DataType(cls), ind.latest_payload())`.
- **Update-proofing seam:** `nautilus-formulas/src/nautilus_formulas/adapters.py` — the only file touching `Indicator.handle_trade_tick` / `handle_quote_tick` signatures **and** the only file that owns the indicator→msgbus republish bridge.

### R3 — Online HMM / BOCPD / composites

- **Capabilities used:** same `Indicator` registration as R2; `register_serializable_type(cls, to_dict, from_dict)` for msgbus JSON publishability; **`register_arrow(data_cls, schema, encoder, decoder)`** for parquet catalog round-trip (without this, replay drops these payloads); `register_python_data_class` for Rust-core visibility; `publish_data(DataType, Data)` for the typed stream on topic `data.<TypeName>.*`.
- **Build-on-top:** in same `nautilus-formulas` package add `HMMRegimeIndicator` and `BOCPDIndicator` subclasses fed by R2 vol-surface outputs; expose `latest_payload()` returning `RegimePosterior` / `ChangepointEvent` `CustomData` instances. Strategy republishes both:
  - a scalar summary via `publish_signal("regime_id", int)` / `publish_signal("p_changepoint", float)` so the chart's `signals.*` subscription receives a numeric (see R5),
  - the full payload via `publish_data(DataType(RegimePosterior), payload)`.
  Composite is a **`CompositeActor`** (not an indicator — `Indicator` has no msgbus / `subscribe_data`) living in `nautilus-formulas`. It subscribes via `self.subscribe_data(DataType(RegimePosterior))` and `subscribe_data(DataType(ChangepointEvent))`, applies the join rule, and republishes its own `CompositeSignal` `CustomData`. The BOCPD pitfalls (level-vs-velocity; `P(r=0) ≡ H`; propagate joint not conditional) ship as invariant tests in `nautilus-formulas/tests/`.
- **Update-proofing seam:** `nautilus-formulas/src/nautilus_formulas/types.py` — custom data type definitions + **all three registrations** (`register_python_data_class`, `register_serializable_type`, `register_arrow` with explicit `pa.Schema`) — called once via `register_all()` (see R6).

### R4 — Multi-broker tape-quality awareness

- **Capabilities used:** existing adapter clients in `crates/adapters/thetadata/`, `crates/adapters/zerodha/`, `nautilus_trader/adapters/interactive_brokers/`; `InstrumentId.venue`, `instrument.asset_class`, and subscription type already on every tick / cache entry. RTVolume degradation for IB is documented in `../options-formulas/FINDINGS.md` and was fixed in commit `ab42a11e65` — discriminator is **subscription mode**, not venue.
- **Build-on-top:** a small `TapeQuality` enum (`OPRA_CONSOLIDATED` | `VENUE_BEST` | `RTVOLUME_DEGRADED`) **keyed by `(Venue, asset_class, subscription_mode)`** (not Venue alone — IB serves both clean equity ticks and degraded options/delayed ticks under the same `Venue("IB")`). Lives in `nautilus-formulas/src/nautilus_formulas/quality.py`. Each formula declares `REQUIRED_QUALITY`; the **Strategy republish bridge** (R2) early-returns and skips `publish_signal` / `publish_data` when the bound instrument's tape quality is below threshold unless `allow_degraded=True`. (The indicator itself still receives `handle_*` calls — framework registration cannot be refused after the fact; the gate is in the republish, not in registration.) **No adapter changes** — venue + asset class + subscription mode already in the cache.
- **Update-proofing seam:** `quality.py` — only file referencing venue strings and the discriminator tuple; one map entry per new (venue, asset class, mode) combination.

### R5 — Indicator catalog the chart reads back

- **Capabilities used:** `publish_signal` emits to `signals.<Name>.*` on the MessageBus (scalars only); `publish_data` emits to `data.<TypeName>.*` (custom payloads).
- **Build-on-top:** in `chart_bridge_strategy.py`, in `on_start` add **two** subscriptions:
  1. `self._msgbus.subscribe("signals.*", handler=self._on_signal)` — picks up every R2 scalar formula and the R3 scalar summaries (`regime_id`, `p_changepoint`).
  2. `self._msgbus.subscribe("data.*", handler=self._on_custom_data)` filtered (in handler) to the R3 `CustomData` type names — picks up `RegimePosterior` / `ChangepointEvent` / `CompositeSignal` payloads.
  Fan out as `SignalEnvelope` (scalars) and `CustomDataEnvelope` (structured) in `envelope.py`. Chart receives the same numerics and payloads the engine sees — single source of truth — for every R2/R3 indicator and any stock `nautilus_trader.indicators.*`. No registry, no separate indicator service.
- **Update-proofing seam:** `envelope.py` `SignalEnvelope` + `CustomDataEnvelope` + `_on_signal` / `_on_custom_data` methods on the bridge strategy.
- **Frontend rendering protocol:** scalar signals auto-render — config-only overrides (pane grouping, color, scale, threshold bands) live in `nautilus-chart/frontend/config/signals.json`, no frontend code change to add a new scalar indicator. Structured payloads require one renderer file in `nautilus-chart/frontend/src/renderers/<TypeName>.tsx` declaring `type` (must equal the name passed to `register_python_data_class`), `pane`, `draw`, and `tooltip`. Bridge routes `signals.*` → scalar panes and `data.*` → typed renderers by `TypeName`. Aggregation scope is inferred from the `FormulasMixin` publish helper:
  - `publish_signal(name, v)` → `instrument` scope (sub-pane on the active instrument).
  - `publish_signal_aggregated(name, v)` → `chain-aggregate` scope (sub-pane on the underlying, value is the across-chain reduction).
  - `publish_signal_per_contract(name, iid, v)` → `chain-surface` scope (heatmap pane keyed by strike × expiry, bridge bins by parsing `iid`).
  Backtest and replay use the identical pipeline; a `ts_event`-keyed time slider scrubs every pane in lockstep. New indicator = zero or one renderer file; new visualization mode (e.g. 3D vol surface) = one pane class in `nautilus-chart/frontend/src/panes/`. No bridge changes for either.

### R6 — Same code in live, backtest, replay

- **Capabilities used:** `BacktestNode` / `BacktestEngine`, `ParquetDataCatalog`, `backtest/data_client.pyx:343 subscribe_option_greeks` (acknowledges the subscription; actual Greeks playback requires either Greeks rows in the catalog via `register_arrow` *or* `GreeksCalculator` reconstructing from option-book replay), `TestClock` vs `LiveClock` auto-injection.
- **Build-on-top:** nothing on the strategy side. `Strategy` subclasses run unchanged across modes. Disciplines:
  - **Strategy/Actor code** uses `self.clock.timestamp_ns()`; never `time.time()`.
  - **Indicator code** (`nautilus-formulas`) has no clock at all — time comes from `tick.ts_event` passed through `handle_*`. Any indicator that imports `time`, `datetime.now`, or `Date.now` is a bug.
  - `register_all()` (R3 seam) calls `register_python_data_class`, `register_serializable_type`, and `register_arrow` once at module import so custom signal types survive both msgbus serialization and catalog persistence.
- **Update-proofing seam:** `nautilus_formulas/__init__.py` `register_all()` function called from `Strategy.on_start`. If upstream changes any of the three registry signatures, fix one function.

### R7 — Operator surface (CLI, MCP, NL panel)

- **Capabilities used:** `python -m nautilus_trader.backtest` / `live`, `TradingNode` / `BacktestNode` public APIs, chart bridge's existing FastAPI app on `ChartBridgeStrategy`. The existing Rust admin CLI in `crates/cli/` covers admin-side concerns only — it is **not** the home for these operator verbs.
- **CLI home decision:** Python click CLI in a new sibling repo `nautilus-ops/`. Rationale: operator verbs bind to **Python** public APIs (`TradingNode`, `BacktestNode`, `ParquetDataCatalog`, `ChartBridgeStrategy` FastAPI) and to `nautilus-formulas` / `nautilus-chart` — extending `crates/cli/` would force re-exposing all of these through PyO3 just for the CLI. Keep `crates/cli/` as the Rust admin surface; keep `nautilus-ops` as the Python operator surface. Two surfaces, two scopes — documented.
- **Build-on-top:** one `nautilus` click CLI in `nautilus-ops/` with verbs `run-live`, `run-backtest`, `replay`, `catalog ls`, `catalog warmup`, `chart` — each constructs `TradingNode` / `BacktestNode` directly via their config-object constructors (the same constructors the ABI test pins). MCP server is a thin wrapper exposing the same verbs as tools. NL panel later — adds `/chat` route to existing FastAPI app routing to the same verbs.
- **Update-proofing seam:** `nautilus-ops/src/nautilus_ops/verbs.py` — every operator surface (CLI / MCP / chat) calls verbs from this one module. The ABI test (below) pins the constructor surface that `verbs.py` depends on.

### R8 — Composition surfaces (non-restrictive strategy authoring)

The five surfaces below absorb the realities of multi-source, multi-venue, multi-rule strategy authoring (aggressor inference, catalog plurality, dynamic chain membership, coverage gating, exec routing) so individual strategies don't re-implement them. Each is a small pluggable interface with concrete implementations shipped in `nautilus-formulas`. They share an **informal `Node` protocol** documented in `nautilus_formulas/__init__.py`:

```
- constructor takes config + dependencies (no I/O)
- on_start subscribes; on_stop unsubscribes
- consumes events by ts_event order; emits via publish_data / publish_signal
- state is fully reconstructable from event replay
```

No new framework class — the protocol is contract-only. This is the deliberate, bottom-up replacement for the archived plan's top-down `Node` primitive: same five properties, zero upstream coupling.

#### R8.1 — `AggressorInferer` (Lee-Ready / EMO / tick rule / BVC, swappable)

- **Capabilities used:** `TradeTick`, `QuoteTick`, `AggressorSide` from `nautilus_trader.model`.
- **Build-on-top:** `AggressorInferer` Protocol; concrete `ConditionFirstInferer(fallback)`, `LeeReadyInferer`, `TickRuleInferer`, `EMOInferer`, `BulkVolumeClassifier`. Every flow-style indicator (R2) takes `inferer: AggressorInferer` in `__init__` and never knows which rule is active. ThetaData's `aggressor_from_condition` (`crates/adapters/thetadata/src/enums.rs:70`) supplies the marked subset; the inferer fills in `NoAggressor` prints from the contemporaneous NBBO the indicator already holds.
- **Update-proofing seam:** `nautilus-formulas/src/nautilus_formulas/inference.py`.

#### R8.2 — `DataPlane` (catalog union, quality-aware source selection)

- **Capabilities used:** `ParquetDataCatalog.query` / `write_data`; R4's `(Venue, asset_class, subscription_mode) → TapeQuality` map.
- **Build-on-top:** `DataPlane(sources: list[ParquetDataCatalog], policy: ResolutionPolicy)` that resolves by `InstrumentId`, not by source. Policy implementations: `PreferHighestQuality`, `PreferLowestLatency`, explicit `PinSourcePerInstrument`. R4's tape-quality table moves from a runtime gate inside indicators to a **source-selection input** at the catalog layer where it belongs. `BacktestNode` takes the `DataPlane` in place of a single catalog. New data vendor = one `add_source()` call; zero strategy edits.
- **Update-proofing seam:** `nautilus-formulas/src/nautilus_formulas/data_plane.py`.

#### R8.3 — `ChainSelector` actor + `ChainSelectionRule` (dynamic membership)

- **Capabilities used:** `Actor` lifecycle (`on_start`/`on_stop`), `subscribe_quote_ticks` / `subscribe_trade_ticks` / `unsubscribe_*`, `publish_data` on a new `ChainMembershipChanged` `CustomData` type.
- **Build-on-top:** `ChainSelector(Actor)` consumes underlying `QuoteTick`s, runs a `ChainSelectionRule` (`ATMBandRule`, `DeltaBandRule`, `VolumeRule`, `CompositeRule`) on each event, diffs the membership set, and subscribes / unsubscribes accordingly. Emits `ChainMembershipChanged` (added, removed, ts_event) on every transition. Strategies subscribe to that custom data and add / tear down per-contract indicators in response. **Same code in backtest** — driven by event-time `QuoteTick`s, no wall-clock. Going wider (full chain) or narrower (ATM only) is one rule swap.
- **Update-proofing seam:** `nautilus-formulas/src/nautilus_formulas/chain.py`.

#### R8.4 — `CatalogManifest` (declared requirements + pre-flight gap report)

- **Capabilities used:** `ParquetDataCatalog` introspection, `InstrumentId`, `BarType`, the R4 tape-quality table.
- **Build-on-top:** `nautilus-ops catalog scan` (R7 verb) walks each source and writes a `manifest.json` containing `coverage[InstrumentId] -> list[DateRange]`, `types_present[InstrumentId] -> set[type]`, `quality[InstrumentId] -> TapeQuality`, `resolution[InstrumentId] -> Resolution`. Strategies declare `requires: list[DataRequirement]` in config; a pre-flight check intersects requirements with the active `DataPlane`'s union of manifests and fails fast with a precise gap report (which instrument, which range, which type, which source could supply it). Eliminates mid-backtest discovery of missing data.
- **Update-proofing seam:** `nautilus-formulas/src/nautilus_formulas/manifest.py`.

#### R8.5 — `Router` actor + `RoutingRule` (exec pluralism, one strategy across venues)

- **Capabilities used:** `OrderFactory`, `submit_order` / `modify_order` / `cancel_order`, `ExecClientId`, `Cache` for account / position state.
- **Build-on-top:** strategies publish `OrderIntent` `CustomData` (instrument, side, qty, TIF, parent strategy id, optional limit/stop). `Router(Actor)` owns `InstrumentId → ExecClientId` mapping via a `list[RoutingRule]` evaluated in order — `VenueClassRoute(asset_class, venue)`, `InstrumentPinRoute`, `FallbackRoute`. Translates intent → `OrderFactory` → `submit_order` on the right client. Backtest = all rules route to the sim venue. Live = options to IB, India equities to Zerodha, etc. — keyed by the same `(Venue, asset_class)` tuple R4 establishes.
- **Update-proofing seam:** `nautilus-formulas/src/nautilus_formulas/router.py`.

#### Event-time discipline (cross-cutting hardening)

Beyond the existing R6 rule that indicators use `tick.ts_event` only:

- CI lint that fails the `nautilus-formulas` build if any file in `src/` contains `time.time(`, `datetime.now(`, `time.monotonic(`, or `time.perf_counter(`.
- ABI test asserts no indicator subclass exposes a `clock` attribute — indicators must remain clock-free; time arrives through `handle_*` payloads.
- Optional typed wrapper `EventTime(int)` in `nautilus-formulas/timing.py` for handler signatures that want compile-time-grade enforcement.

This makes "same code live/backtest" a property of the type system, not a habit.

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
- `nautilus_trader.serialization.base.register_serializable_type` callable with parameters named `(cls, to_dict, from_dict)` (arity check only — names checked best-effort to tolerate upstream renames; arity drift is the hard fail).
- `nautilus_trader.serialization.arrow.serializer.register_arrow` callable with `(data_cls, schema, encoder, decoder, batch_encoder)`.
- `nautilus_trader.model.data` exports `register_python_data_class` (or its PyO3-bound equivalent) callable from Python.
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

## 4. Adding a new adapter or broker

Same shape as adding an indicator: fixed steps, one named file per concern, conformance tests as the gate. Two paths — data-only (ThetaData-shaped) and full broker (Zerodha-shaped) — share steps 0–2 and 5–6, diverge only on the exec client.

### Step 0 — Scaffold via `/adapter-new`

```
/adapter-new <venue>
```

Stamps the standard layout:

```
crates/adapters/<venue>/
    src/
        config.rs        # bon::Builder DTOs (never credentials)
        credential.rs    # env-var resolution: VENUE_API_KEY etc.
        http.rs          # signed REST client (hmac, rate limits, retries)
        ws.rs            # two-layer WS (outer state + inner handler)
        decode.rs        # venue payload → TradeTick / QuoteTick / OrderFilled
        instruments.rs   # InstrumentProvider
        data.rs          # LiveMarketDataClient
        exec.rs          # LiveExecutionClient        (omit if data-only)
        symbology.rs     # venue symbol ↔ InstrumentId
        enums.rs         # venue enums → nautilus enums
        factories.rs     # factory functions for the node
        python/          # PyO3 bindings (mod.rs, data.rs, exec.rs)
    tests/               # mock-server integration tests
    examples/            # data-tester, backfill-trades, hist-tester
nautilus_trader/adapters/<venue>/
    factories.py         # data_client_factory + exec_client_factory
    config.py            # Python-side config DTOs
    constants.py         # VENUE = Venue("<VENUE>")
```

Generator also stamps the TC-D / TC-E conformance test scaffolds.

### Step 1 — Implement the Rust core

Non-negotiables from `adapter-architecture.md`:

- `bon::Builder` + `Default` on every config struct; **never** credentials in DTOs.
- `credential.rs` resolves `{VENUE}_API_KEY`, `{VENUE}_API_SECRET`, optional `{VENUE}_PASSPHRASE`, sandbox flavor `_TESTNET`.
- WS two-layer: outer client holds `Arc<ArcSwap<AtomicU8>>` state + `mpsc::UnboundedSender<Msg>`; inner handler is stateless, runs in a Tokio task started via `get_runtime().spawn()` (Python threads have no Tokio context).
- REST: signed via `hmac`, token-bucket per endpoint family, jittered exponential backoff for 5xx / 429 / network only, cursor pagination iterator.
- WS reconnect **must** replay the last subscription set — the client owns it, not the strategy.
- Decoder emits `nautilus_model::data::*` types directly; **no mutation** of upstream payloads (design-principle invariant #1).
- `PyObject` for callbacks, **never** `Arc<PyObject>` (PyO3 cycle hazard).

### Step 2 — `InstrumentProvider` + TC-D instrument-shape rows

`load_all_async()`, `load_ids_async(ids)`, `load_async(id)`. `DataTester` runs TC-D01…TC-D10 (IDs, tick size, contract size, lot size, expiry, strike, asset class, base/quote).

### Step 3 — `LiveMarketDataClient` + the rest of the TC-D matrix

Every subscription path the venue offers:

- TC-D11–D24: `subscribe_quote_ticks`, `subscribe_trade_ticks`, `subscribe_order_book_deltas`, `subscribe_order_book_depth10`.
- TC-D31–D40: bar subscriptions per resolution.
- TC-D51–D60: option-specific (`subscribe_instrument_close`, `subscribe_index_prices`, `subscribe_mark_prices`, `subscribe_funding_rates` where applicable).
- TC-D71+: `subscribe_option_greeks` if streamed; otherwise skip (engine reconstructs via `GreeksCalculator`).

Each matrix row records `pass`, `fail`, or `skip`. Skip is acceptable when the venue genuinely doesn't offer the type. Historical-request paths (`request_quote_ticks(from, to)` etc.) live here too — they power `nautilus-ops catalog backfill`.

### Step 4 — (Exec only) `LiveExecutionClient` + the TC-E matrix

Skip for data-only.

TC-E01…TC-E101 covers order lifecycle, TIFs, brackets, reduce-only / post-only flags, IOC/FOK, modifies, cancels, partial fills, commission attribution, account state, position reconciliation. Beyond the matrix:

- **Order state machine** (`order-state-machine.md`): submit → accept → fill / reject / cancel / modify / expire. Every venue message decodes to exactly one `Order*` event; no synthetic transitions.
- **Startup reconciliation**: `generate_order_status_reports()` + `generate_position_status_reports()` reconstruct the venue's view; engine reconciles against `Cache`.
- **Idempotent cancels**: cancel of a terminal order returns success silently.
- **Commission attribution**: `OrderFilled.commission` populated correctly (venue field or derived from a fee schedule in `credential.rs`).

### Step 5 — Factories + Python config + node wiring

```python
def <venue>_data_client_factory(loop, name, config, msgbus, cache, clock) -> ...
def <venue>_exec_client_factory(loop, name, config, msgbus, cache, clock) -> ...   # exec only

node = (LiveNode.builder("TRADER-001", trader_id, Environment.LIVE)
    .add_data_client(None, <venue>_data_client_factory, data_cfg)
    .add_exec_client(None, <venue>_exec_client_factory, exec_cfg)                  # exec only
    .build())
```

End of upstream-shaped work. Steps 6+ integrate the venue into R1–R8.

### Step 6 — Declare tape quality (R4)

File: `nautilus-formulas/src/nautilus_formulas/quality.py`

```python
QUALITY_MAP[(Venue("THETADATA"), AssetClass.OPTION, SubscriptionMode.LIVE_TICK)] = TapeQuality.OPRA_CONSOLIDATED
QUALITY_MAP[(Venue("THETADATA"), AssetClass.EQUITY, SubscriptionMode.LIVE_TICK)] = TapeQuality.VENUE_BEST
QUALITY_MAP[(Venue("ZERODHA"),   AssetClass.EQUITY, SubscriptionMode.LIVE_TICK)] = TapeQuality.VENUE_BEST
QUALITY_MAP[(Venue("IB"),        AssetClass.OPTION, SubscriptionMode.RTVOLUME)]   = TapeQuality.RTVOLUME_DEGRADED
```

Indicator `REQUIRED_QUALITY` checks gate against this table inside the R2 republish bridge — adding a venue never touches indicator code.

### Step 7 — Register with `DataPlane` (R8.2)

User node config:

```python
data_plane = DataPlane(
    sources=[
        ParquetDataCatalog("./data/thetadata-catalog"),
        ParquetDataCatalog("./data/zerodha-catalog"),
        ParquetDataCatalog("./data/ib-catalog"),
    ],
    policy=PreferHighestQuality(QUALITY_MAP),
)
```

New source = one line. `DataPlane` consults `QUALITY_MAP` to pick the best source per instrument when multiple cover the same data.

### Step 8 — Backfill examples + manifest support (R8.4)

For data adapters, ship two examples in `crates/adapters/<venue>/examples/`:

- `<venue>-backfill-trades` — multi-day catalog write under `./data/<venue>-catalog`.
- `<venue>-hist-tester` — REST → decode → parquet round-trip smoke test.

These feed `nautilus-ops catalog backfill <venue>` (R7). The `CatalogManifest` scanner is generic over `ParquetDataCatalog`; per-adapter manifest walkers only needed if the catalog uses a non-standard directory layout (`nautilus-formulas/src/nautilus_formulas/manifest_walkers/<venue>.py`).

### Step 9 — (Exec only) Register with `Router` (R8.5)

File: `nautilus-formulas/src/nautilus_formulas/router.py`

```python
router = Router(rules=[
    VenueClassRoute(asset_class=AssetClass.OPTION, venue=Venue("IB")),
    VenueClassRoute(asset_class=AssetClass.EQUITY, venue=Venue("ZERODHA"), market=Market.NSE_BSE),
    VenueClassRoute(asset_class=AssetClass.EQUITY, venue=Venue("IB"),      market=Market.US),
    FallbackRoute(venue=Venue("SIM")),
])
```

Adding a broker = appending one rule. Strategies publish `OrderIntent` payloads and never name the venue.

### Step 10 — Conformance + smoke gate

```
pytest tests/integration_tests/adapters/<venue>/test_data_conformance.py     # TC-D matrix
pytest tests/integration_tests/adapters/<venue>/test_exec_conformance.py     # TC-E matrix (exec only)
pytest tests/integration_tests/adapters/<venue>/test_reconciliation.py       # exec only
cargo test -p nautilus-<venue>                                                # Rust unit + integration
nautilus-ops catalog scan ./data/<venue>-catalog                              # manifest produces non-empty coverage
nautilus-ops run-backtest --strategy smoke_<venue> --catalog ./data/<venue>-catalog
```

All green = adapter is live, backtestable, and visible to every strategy without strategy edits. CI runs this gate on every PR.

### Step 11 — Patch ledger entry (only if upstream divergence)

`.sync-upstream/PATCHES.md` — one row per divergence with explicit retirement criterion.

### Step 12 — Documentation (manual, three sections)

Adapter `README.md` (stamped by `/adapter-new`) must be filled in:

- **Authentication** — env vars `credential.rs` reads, where the venue issues them.
- **Tier-gated features** — what subscriptions / resolutions each account level unlocks.
- **Wire-format gotchas** — anomalies the adapter handles transparently (e.g. ThetaData's `×1000` strike scale, OPRA negative sequence numbers). The things that bite the next person.

### Files touched in the R8 layer

| Concern | Data-only (ThetaData-shaped) | Full broker (Zerodha-shaped) |
|---|---|---|
| `exec.rs` + Python exec client | skip | implement |
| TC-E conformance matrix | skip | required |
| Startup reconciliation | skip | required |
| `Router` rule | skip | append one rule |
| `DataPlane` source | append one source | append one source |
| `QUALITY_MAP` entry | required | required (data side) |
| Backfill example | required | optional but useful |
| **Total R8 integration files** | **2** (`quality.py`, node config) | **3** (`quality.py`, `router.py`, node config) |

### Non-restrictiveness payoffs

- Strategy code unchanged — strategies subscribe by `InstrumentId`, route via intents, read quality off the table.
- Chart code unchanged — tape-quality hints pull from the same `QUALITY_MAP`.
- Backtest unchanged — `DataPlane` resolves; `CatalogManifest` reports any coverage gaps pre-flight.
- Adapter is isolated — everything venue-specific in `crates/adapters/<venue>/` and `nautilus_trader/adapters/<venue>/`; three integration points are tiny and explicit.
- `/adapter-new` automates the boilerplate; conformance scaffolds make the gate explicit from minute one.

Adding the 10th broker = same work as adding the 4th.

---

## Package layout (what gets built)

All four are **sibling git repos** of `nautilus_trader/`; each pins `nautilus_trader` by SHA in its own `pyproject.toml`. Paths below are relative to the parent directory holding all five repos.

```
nautilus_trader/      upstream fork; minimal local diff, ledger-tracked
nautilus-chart/       existing sibling repo; seams = envelope.py + catalog.py
nautilus-formulas/    new sibling repo; seams =
                        adapters.py    (R2  indicator→msgbus republish bridge)
                        types.py       (R3  custom data + register_all)
                        quality.py     (R4  tape-quality table, keyed by venue/class/mode)
                        inference.py   (R8.1 aggressor inferers)
                        data_plane.py  (R8.2 catalog union + selection policy)
                        chain.py       (R8.3 chain selector + selection rules)
                        manifest.py    (R8.4 catalog manifest + pre-flight check)
                        router.py      (R8.5 exec routing rules)
                        timing.py      (R8   optional EventTime wrapper)
                        __init__.py    (register_all + Node-protocol docstring)
nautilus-ops/         new sibling repo; seam  = verbs.py
options-formulas/     existing sibling repo (calibration lab); nautilus-formulas imports locked formulas from it
```

Ten seams in `nautilus-formulas`, two in `nautilus-chart`, one in `nautilus-ops`. ABI test catches drift; the named seam is where the fix lands.

---

## What this plan does NOT do (and why)

- **No new upstream abstraction primitive** ("Node", "DerivedStream", "SignalPack"). `Indicator` + `register_indicator_for_*` + `publish_signal` / `publish_data` + `CustomData` + (where cross-component subscription is needed) a thin `Actor` subclass already cover every requirement. **This is a deliberate departure from the prior "Node primitive + event-sourced computation graph" stance** documented in the archived plan — but only for upstream. Inside `nautilus-formulas`, R8.1–R8.5 are five composable Node-shaped components (constructor + dependencies, `on_start` / `on_stop`, event-time consumption, replay-deterministic state, single named seam). The protocol is documented in `nautilus_formulas/__init__.py` as a contract, not a base class. If a sixth or seventh composable shows up with the same shape, formalizing the protocol into a class is a one-file change — built bottom-up from real requirements rather than declared top-down.
- **No multi-tier promotion gates, no content-addressed signal packs.** Locked formulas are versioned through normal Python package versioning of `nautilus-formulas` + `options-formulas`.
- **No fidelity-tag protocol added to upstream events.** Tape quality is inferred from `InstrumentId.venue` via a small map; no new fields on `TradeTick`.
- **No five-layer architecture.** Two added repos (`nautilus-formulas`, `nautilus-ops`), one existing repo extended (`nautilus-chart`), and the upstream fork. Done.

Anything beyond this becomes a candidate for a follow-up plan only when a concrete requirement demands it.
