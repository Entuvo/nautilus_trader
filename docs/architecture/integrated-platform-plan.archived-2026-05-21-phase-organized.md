# Integrated Platform Implementation Plan

> **For agentic workers:** This is a **program plan** spanning eight phases (A–H). It is not a single bite-sized task list. Each phase below names its own implementation sub-plan path; sub-plans are written at the start of each phase using `superpowers:writing-plans` and executed via `superpowers:subagent-driven-development` or `superpowers:executing-plans`. Phase-A sub-plan is the first deliverable after this document.

**Goal:** Build a single capability layer (`nautilus_trader.agent_surface`) that serves as the canonical contract between the `nautilus` CLI, the `nautilus-chart` web UI, in-process autonomous Strategies, coding agents (Claude Code/Cursor/Codex via MCP), and a human-in-the-loop NL chat panel — backtest and live symmetrically — while insulating all clients from upstream nautilus_trader API churn through a single sync seam.

**Architecture:** A Python module at `nautilus_trader/agent_surface/` owns the protocol (frozen msgspec envelopes, decorator-discoverable verbs, one in-process dispatcher). Five transports (`stdin`, `ws`, `inproc`, `mcp`, future `rest`) wrap the same dispatcher with thin (de)serializers. Clients depend only on `agent_surface`; weekly upstream syncs are insulated by a patch ledger + ABI surface test in the surface module. Chart bridge shrinks from 1443 lines to ~200 (embed the surface, serve WS).

**Tech Stack:** Python 3.12, msgspec, Click, FastAPI/uvicorn (existing transport), websockets, asyncio, pyo3-bound Rust core (unchanged), nautilus-chart React/TS frontend (consumer only), MCP protocol for agent shells, NDJSON event journal.

---

## 0. North Star

> *Charts, web shells, autonomous strategies, and agent tools all wrap or co-mount the same capability module. The `nautilus` binary IS the canonical surface. Backtest and live are symmetric — same verbs, same envelopes; mode is context, not branching.*

Verbatim from `docs/architecture/agent-operable.md §1`. Every workstream below justifies itself against this sentence. Anything that doesn't serve it gets cut.

## 1. Locked decisions

These are immovable for the duration of this plan. Changing one re-opens the architecture.

| # | Decision | Choice | Source |
|---|---|---|---|
| D1 | Who is "agent"? | Coding agents (Claude Code, Cursor, Codex) + in-platform autonomous trading agents + human-in-the-loop NL operators | agent-operable.md §1 |
| D2 | Execution modes | Backtest and live **symmetric** — same verbs, same envelopes; mode is context, not branching control flow | agent-operable.md §1, §6 |
| D3 | Protocol shape | CLI-first. The `nautilus` binary IS the canonical surface; every other shell wraps it | agent-operable.md §1, §4 |
| D4 | Surface location | `nautilus_trader/agent_surface/` (Python module inside the engine repo) | agent-operable.md §4.2 |
| D5 | Envelope encoding | Frozen `msgspec.Struct`, JSONL on the wire | agent-operable.md §5 |
| D6 | Auth | Opaque bearer tokens with scopes (`read` / `trade` / `admin`), sqlite-persisted, `--token` flag or `NAUTILUS_TOKEN` env | This plan, default to agent-operable §13.1 |
| D7 | Dispatch architecture | One in-process dispatcher; transports are thin (de)serializers; no parallel implementations | This plan, lock of agent-operable §13.5 |
| D8 | CLI implementation | Python `nautilus` entrypoint for operator verbs; existing Rust `crates/cli/` keeps DB admin as `nautilus db ...` sub-namespace | This plan, resolution of agent-operable §16 ambiguity |
| D9 | Event journal | NDJSON file v1; sqlite migration deferred until `event_query` verbs land | agent-operable.md §13.2 |
| D10 | No Redis stream gateway | Settled by prior memory; surface IS the gateway | memory `feedback_simple_strategy_bridge` |
| D11 | No second indicator implementation in TS | Chart deletes `src/utils/indicators/*.ts` in Phase F | memory `feedback_trading_systems_data_architect`, agent-operable §8 |
| D12 | No license caveats anywhere | Private fork, no NOTICE-rewrite work | memory `feedback_no_license_concerns` |
| D13 | Never discard work during consolidation | Every uncommitted/untracked file gets committed or tracked, not dropped | memory `feedback_never_discard_work` |

## 2. Trading-systems-data-architect invariants (baked in, not added later)

Every envelope and verb is designed to honor these axes from `memory/feedback_trading_systems_data_architect.md`. They are constraints on the design, not features to add.

| Axis | How the platform honors it | Where enforced |
|---|---|---|
| Event-sourced | Envelopes are immutable; derived state recomputable from journal | `envelopes.py` frozen=True; event_journal.py |
| Plural time semantics | Every event carries `ts_exchange`, `ts_ingestion`, `ts_processing` distinctly | Envelope schema (Phase A) |
| Provenance / lineage | `data_source` + `formula_version` tags per derived value | Envelope schema (Phase A) |
| Fidelity as first-class | `fidelity: {is_synthetic, estimated_side, broker_greek_vs_local}` block | Envelope schema (Phase A) |
| Schema as versioned contract | `capabilities.payload.version`; additive evolution; replay journals keep parsing | Phase A + Phase G |
| Determinism | No wall-clock in surface; all time via `node.kernel.clock`; replay journal | Phase A invariants + Phase E |
| Two-phase indicator lifecycle | Warmup and steady-state use one code path, differing only in clock source | Phase F (indicator service) |
| Out-of-order handling explicit | Documented drop / reorder policy per data type | Phase B (subscription envelopes) |
| Decoupling via message bus | Surface subscribes to bus; producers don't know it exists | Inherent — Phase B |
| Raw → normalized seam testable | Adapter normalizers + golden-file tests | Phase H (AdapterCapabilities) + adapter work |

## 3. Current state inventory

Anchored on what is in the tree on 2026-05-21. Every row is verified.

### 3.1 Already shipped (do not redo)

| Artifact | Location | Status |
|---|---|---|
| Strategy-as-bridge architecture decision | `nautilus-chart/docs/ROUND5_DESIGN.md` | Done |
| ChartBridgeStrategy implementation | `nautilus-chart/bridge/nautilus_chart_bridge/chart_bridge_strategy.py` (1442 lines) | Done; shrinks in Phase B |
| Engine custom entrypoint | `nautilus-chart/engine/main.py` (279 lines) | Done; reduces in Phase A as `register_serializable_type` moves into surface |
| Indicator catalog + extractors (33 nautilus indicators) | `nautilus-chart/bridge/nautilus_chart_bridge/indicators/{catalog,extractors}.py` | Done; absorbed into surface in Phase F |
| Wire envelope schema (initial) | `nautilus-chart/bridge/nautilus_chart_bridge/envelope.py` | Done; promoted to surface in Phase A |
| Agent-operable proposal | `nautilus_trader/docs/architecture/agent-operable.md` (580 lines) | Proposal, awaiting status flip to Accepted (Phase 0 below) |
| Zerodha adapter Phase 6 (Rust core + Python wrappers) | `crates/adapters/zerodha/` + `nautilus_trader/adapters/zerodha/` | Done; receives `AdapterCapabilities` in Phase H |
| ThetaData adapter | `crates/adapters/thetadata/` + `nautilus_trader/adapters/thetadata/` | Done |
| IB delayed-tick fix | `nautilus_trader/adapters/interactive_brokers/client/market_data.py` | Done (commit `ab42a11e65`) |
| 20 chart bugs catalogued | `nautilus-chart/docs/CHART_AUDIT_2026-05-19.md` | Audit complete; reconciled into phases in §5 below |
| 3 deferred items | `nautilus-chart/TODOS.md` | Reconciled into Phases C (WS auth → D6), A (subscribe-on-demand), G (E2E) |

### 3.2 Uncommitted / untracked (decide track-or-ignore in Phase 0)

| Path | Status | Default disposition |
|---|---|---|
| `.sync-upstream/worktree-280ae1762d/` | Untracked | `.gitignore` it; the *sync workflow* gets tracked (Phase 0) |
| `.sync-upstream/baseline-280ae1762d.log` | Untracked | `.gitignore` |
| `.sync-upstream/baseline-280ae1762d.results` | Untracked | `.gitignore` |
| `.sync-upstream/latest-baseline` | Untracked | `.gitignore` |

Per memory `feedback_never_discard_work`: present these to user explicitly during Phase 0 commit before gitignoring.

### 3.3 Active divergence from upstream (commits on `master` not in `develop`)

These become rows in the patch ledger (`patches/MANIFEST.md`) with retirement criteria.

| Commit | Subject | Patch class | Retirement criterion |
|---|---|---|---|
| `cefebbc38d` | Local build tweaks: build.py feature flag + Cargo.lock axum entry | Build/private | Never (private feature) |
| `ab42a11e65` | Fix IB delayed tick types to live for non-realtime accounts | Upstream-able | Upstream `trade_type` field accepts nullable / explicit delayed enum |
| `fb566bbf26` | Add `.gastown-ignore` marker file | Private | Never |
| `4fe195b27e` | Add Zerodha adapter specs and agent-operable architecture proposal | Private | Never (this IS our roadmap) |
| `805f84f5fe` | Merge develop into master: Zerodha + ThetaData adapters, IB delayed-tick fix, architecture docs | Merge commit | n/a |

### 3.4 In-tree monkey-patches (Phase 0 inventory)

| Location | Patch | Retirement criterion |
|---|---|---|
| `nautilus-chart/engine/main.py:_patched_importable_config_create` | `ImportableConfig.create` doesn't pass `dec_hook` in 1.226/1.227 | Upstream passes `dec_hook` through |

## 4. Target architecture (delta-only)

Refer to `agent-operable.md §4–§11` for the full picture. **This document does not re-specify it.** The deltas this plan locks in beyond the proposal:

- **D6, D7, D8** above (auth scopes, single dispatcher, Python CLI + Rust db sub-namespace)
- **Chart bridge end state**: ~200 lines, embeds `CapabilitySurface`, serves WS — per agent-operable §9.1
- **Indicator service location**: `nautilus_trader/agent_surface/indicator_service.py` — chart drops `src/utils/indicators/*.ts` in Phase F
- **Adapter capability advertisement**: starts with Zerodha + IB in Phase H; expands to one adapter per cycle thereafter

## 5. The phases (A–H)

Each phase is independently shippable. Each ships with its own bite-sized TDD sub-plan written at the *start* of the phase (so later-phase plans benefit from what's learned earlier). Sub-plan paths are listed; they are written when the phase begins, not now.

For each phase: motivation, scope, acceptance criteria, dependencies, owner placeholder, chart-audit items absorbed (if any), risks.

### Phase 0 — Foundation (1–3 days)

**Sub-plan:** `docs/superpowers/plans/2026-05-21-phase-0-foundation.md` (write before starting)

**Motivation:** Close the three blockers that prevent the rest of the plan from executing cleanly: tracked-vs-ignored ambiguity in working tree, missing remote topology for sync, proposal status not yet "Accepted."

**Scope:**

1. Commit or `.gitignore` every uncommitted / untracked file per inventory §3.2; present each to user before deciding (per memory `feedback_never_discard_work`).
2. Rename `origin → upstream`; create new private fork; add as `origin`; push `master` and `develop`.
3. Flip `agent-operable.md` status from "Proposal" to "Accepted" with date and the three locked decisions (D6, D7, D8) appended.
4. Create `patches/MANIFEST.md` with rows from inventory §3.3 and §3.4.

**Acceptance criteria:**

- `git status` is clean.
- `git remote -v` shows `upstream → nautechsystems/...` and `origin → <your-fork>/...`.
- `agent-operable.md` header says "Accepted 2026-05-21" and includes D6/D7/D8.
- `patches/MANIFEST.md` exists with five+ rows, each having `retirement_criterion` and `retirement_test_path`.

**Dependencies:** None.

**Owner:** _user to assign_ (default: shergill).

**Chart audit items absorbed:** None.

**Risks:**

- Pushing to a new private origin requires GitHub repo creation outside the workflow. Document loud in the sub-plan.

---

### Phase A — Surface scaffolding (1–2 weeks)

**Sub-plan:** `docs/superpowers/plans/2026-05-21-phase-a-surface-scaffold.md`

**Motivation:** Stand up the empty container so every later phase has a place to put work. Read-only verbs only.

**Scope:**

1. Create `nautilus_trader/agent_surface/` package with: `__init__.py`, `envelopes.py`, `surface.py`, `errors.py`, `auth.py`, `event_journal.py`, `transports/__init__.py`, `transports/inproc.py`, `transports/stdin.py`.
2. Port the chart bridge's envelope schema verbatim into `envelopes.py` (chart still uses it via re-export — `nautilus-chart` adds a `from nautilus_trader.agent_surface import envelopes` shim).
3. Add the architect-mindset triple-timestamp + fidelity block to every event envelope (`ts_exchange`, `ts_ingestion`, `ts_processing`, `data_source`, `fidelity{}`).
4. Implement `@verb` decorator with msgspec-derived JSON Schema generation.
5. Implement read-only verbs:
   - `capabilities`
   - `catalog_list`, `catalog_query`, `catalog_info`
   - `instruments_list`, `instruments_search`, `instruments_show`
   - `adapter_list`, `adapter_status`
   - `event_tail` (read-only over the bus, filter only)
6. Implement `transports/stdin.py` (JSONL over stdin/stdout, one envelope per line).
7. Implement `nautilus` Click subcommand tree under `nautilus_trader/agent_surface/cli/` exposing every read-only verb. Rust `crates/cli/` keeps its current commands under `nautilus db ...` namespace (D8).
8. Implement opaque bearer token scaffold in `auth.py` (sqlite at `~/.config/nautilus/tokens.db`; scopes `read`/`trade`/`admin`).
9. Tests: `tests/integration_tests/agent_surface/test_smoke.py` exercises the read-only surface against a static catalog fixture.

**Acceptance criteria:**

- `nautilus capabilities | jq '.payload.verbs[].name'` lists the 10+ read-only verbs.
- `nautilus catalog list` returns JSONL of catalog entries against a real `ParquetDataCatalog` fixture.
- `nautilus adapter list` returns all registered adapter factories.
- Every event envelope carries `ts_exchange`, `ts_ingestion`, `ts_processing`, `data_source`, `fidelity{}`.
- `pytest tests/integration_tests/agent_surface/` is green.
- A `read` scope token can issue `capabilities` and `catalog_*`; `trade` scope is required for the order verbs that don't exist yet (negative test ready).

**Dependencies:** Phase 0 complete.

**Owner:** _user to assign_.

**Chart audit items absorbed:**

- **CHART-005** root-causes here when `catalog_bars` lands (FX `whatToShow` plumbing)
- **CHART-006** EXTERNAL/INTERNAL mismatch — `catalog_bars` verb canonicalizes
- **CHART-020** SymbolSearch empty-state — `instruments_search` returns introspectable state (`adapter_initializing`, `no_results`, `adapter_disconnected`)

**Risks:**

- Importing the chart envelope schema verbatim risks circular dependency if `nautilus-chart`'s `pyproject.toml` already requires `nautilus_trader`. Mitigation: the surface import happens at runtime, chart's setup.cfg unchanged.
- Auth scope decisions ramify across all later phases; lock D6 firmly before starting.

---

### Phase B — Live data subscriptions + chart cutover (1–2 weeks)

**Sub-plan:** `docs/superpowers/plans/2026-XX-XX-phase-b-live-subscriptions.md`

**Motivation:** Move the load-bearing chart bridge code (subscription dispatch, envelope emission) into the surface; flip the chart to consume the surface. This is the phase where the chart shrinks from 1443 → ~200 lines.

**Scope:**

1. Port subscribe/unsubscribe machinery from `chart_bridge_strategy.py:_on_subscribe` etc. into `surface.py:data_subscribe`.
2. Implement envelope emitters lifted from `on_bar`, `on_quote_tick`, `on_trade_tick`, `on_order_book_deltas`, `on_mark_price`, `on_index_price`, `on_funding_rate`, `on_instrument`.
3. Implement `transports/ws.py` — runs `CapabilitySurface` over WebSocket JSONL with the same protocol as stdin.
4. Update `nautilus-chart/bridge/nautilus_chart_bridge/chart_bridge_strategy.py` to embed `CapabilitySurface` and serve via `transports/ws.py`. Delete the inline subscription/dispatch code that moved.
5. Update `nautilus-chart/src/services/nautilus.ts` to handle the surface's envelope tags (verify identical given verbatim import).
6. Implement `quote` envelope additions for prev-close / open-24h (resolves CHART-002).
7. Add `error` envelope routing — every adapter subscribe error reaches the originating client via `error` envelope (resolves CHART-003).
8. Tests: chart bridge contract test confirms WS protocol is byte-for-byte unchanged for existing envelope types.

**Acceptance criteria:**

- `chart_bridge_strategy.py` ≤ 250 lines (excluding imports and docstrings).
- `nautilus data subscribe bars BTCUSDT.BINANCE 1-MINUTE-LAST --follow` works end-to-end via CLI.
- Existing chart UI works without changes — WS protocol parity verified by chart e2e smoke.
- CHART-002 (watchlist chg/chgP) and CHART-003 (IB subscribe errors) closed.
- Order-book delta subscription (`book_delta`, `book_deltas`) reaches WS clients (resolves CHART-009).

**Dependencies:** Phase A.

**Owner:** _user to assign_.

**Chart audit items absorbed:**

- **CHART-002** chg/chgP — `quote` envelope gets prev-close
- **CHART-003** IB subscribe errors propagate
- **CHART-009** DepthOfMarket empty — `book_delta` envelope now in surface; chart subscribes

**Risks:**

- Chart e2e parity regression. Mitigation: byte-for-byte envelope diffs in a contract test before merging.
- Surface import path triggers nautilus-chart wheel rebuild on every `pyproject.toml` bump. Mitigation: pin nautilus-trader by exact version (per upstream-sync regime, §7 below).

---

### Phase C — Order / position / account verbs (1 week)

**Sub-plan:** `docs/superpowers/plans/2026-XX-XX-phase-c-trading-verbs.md`

**Motivation:** Make the surface tradeable. Today's chart bridge has `submit_order`/`modify_order`/`cancel_order`; promote those + `position_*` + `account_*` into the surface.

**Scope:**

1. Surface verbs: `order_submit`, `order_modify`, `order_cancel`, `order_cancel_all`, `order_list`, `order_show`, `position_list`, `position_close`, `account_list`, `account_show`.
2. Promote `_panel_for_order[cid]` routing into surface-level `subscription_id` ↔ event routing (works for any client, not just chart).
3. CLI subcommands for each verb.
4. Gate trading verbs behind `trade` scope token (D6).
5. Generic `error` envelope → chart toast pipe (resolves CHART-012).
6. Account-state envelope wired into surface; chart adds `accountStore` (Zustand) handler (resolves CHART-011).
7. WS auth landing: `--token` query param required when `NAUTILUS_REQUIRE_AUTH=true` (closes TODOS.md item 1).

**Acceptance criteria:**

- `nautilus order submit --venue ZERODHA --instrument RELIANCE.NSE --side BUY --qty 100 --type LIMIT --price 2810.50` succeeds against a real venue and the fill streams back as `event` envelopes.
- A `read`-scoped token issuing `order_submit` returns `error{code:"scope_denied"}`.
- Chart's AccountPanel updates live on fills.
- CHART-011 and CHART-012 closed.

**Dependencies:** Phase B.

**Owner:** _user to assign_.

**Chart audit items absorbed:**

- **CHART-011** account_state envelope ignored
- **CHART-012** order gate silent block
- **TODOS.md WS auth** — D6 lands here

**Risks:**

- Cross-session order routing semantics (agent A places order, agent B reconnects, who gets the fill?) — design decision needed: route by client_order_id namespace, fall back to broadcast on unknown. Document in sub-plan.

---

### Phase D — Backtest as a streaming verb (1–2 weeks)

**Sub-plan:** `docs/superpowers/plans/2026-XX-XX-phase-d-backtest-stream.md`

**Motivation:** Unlock D2 (backtest = live symmetric). Today `BacktestNode.run()` is blocking and opaque; the surface must stream per-step events.

**Scope:**

1. `backtest_run` verb — accepts a `BacktestRunConfig`, returns `subscription_id` immediately, streams `bar` / `quote` / `trade` / `event` / `backtest_run.progress` / `backtest_run.completed` envelopes.
2. Implementation: wire `BacktestEngine` to publish to its own `MessageBus`; surface subscribes; forwards through serializer. Same pattern as Phase B for live mode.
3. CLI: `nautilus backtest run --config bt.json --follow` emits NDJSON to stdout.
4. Chart can attach a panel to a running backtest by subscribing to its `subscription_id`.

**Acceptance criteria:**

- `nautilus backtest run --config tests/fixtures/bt-1min-1day.json --follow` emits ~1440 bar envelopes + a completion stat envelope.
- A chart panel pointed at the same `subscription_id` renders the backtest's bars and any strategy-emitted fills.
- Two parallel backtests with different `subscription_id`s do not cross-talk.

**Dependencies:** Phase B (subscription machinery) + Phase C (event routing).

**Owner:** _user to assign_.

**Chart audit items absorbed:** None directly; enables future chart "scrub backtest" workflow.

**Risks:**

- `BacktestNode` doesn't natively emit a per-step stream — implementation detail per agent-operable §6.2. Implementation effort may exceed 2 weeks if internal bus contention surfaces.

---

### Phase E — Replay + ScrubberClock generalization (1 week)

**Sub-plan:** `docs/superpowers/plans/2026-XX-XX-phase-e-replay.md`

**Motivation:** Move replay machinery out of the chart bridge and into the surface so agents can use it too. Resolves CHART-010 structurally.

**Scope:**

1. Move `ReplayFeed` / `ScrubberClock` from `nautilus-chart/bridge/nautilus_chart_bridge/replay_feed.py` to `nautilus_trader/agent_surface/replay.py`.
2. Verbs: `replay_start`, `replay_seek`, `replay_speed`, `replay_stop`.
3. `/health` endpoint reports `catalog_configured: bool` (resolves CHART-010a).
4. Chart bridge re-imports from surface; chart UI unchanged.
5. Determinism test: same catalog inputs + same seek pattern → byte-for-byte identical envelope stream (modulo `ts_processing`).

**Acceptance criteria:**

- `nautilus replay start --catalog ./catalog --from 2024-01-01 --to 2024-01-02 --speed 4x --follow` works.
- Chart replay scrubber still works (regression test against e2e).
- Determinism test passes.

**Dependencies:** Phase B (subscription) + Phase D (BacktestEngine wiring patterns).

**Owner:** _user to assign_.

**Chart audit items absorbed:**

- **CHART-010** replay silently broken

**Risks:**

- `ScrubberClock` and `TestClock` semantics overlap; clarify in sub-plan which is canonical for replay vs which for backtest.

---

### Phase F — Server-side indicators (1–2 weeks) — the real "tight integration"

**Sub-plan:** `docs/superpowers/plans/2026-XX-XX-phase-f-indicators.md`

**Motivation:** Honor D11 (no second indicator implementation). Resolves CHART-008 structurally. This is the phase that delivers "chart shows exactly the value a strategy sees."

**Scope:**

1. `nautilus_trader/agent_surface/indicator_service.py` — `dict[content_hash(name, params, bar_type)] → IndicatorInstance`. One instance, many subscribers.
2. Verbs: `indicator_list`, `indicator_compute` (one-shot historical), `indicator_subscribe` (live stream).
3. Port the 33-entry indicator catalog from `nautilus-chart/bridge/nautilus_chart_bridge/indicators/catalog.py` to surface, expanding to all 38 Rust-core indicators per agent-operable §2.3.
4. Two-phase lifecycle: warmup from catalog → `indicator_subscribe.ok` with `state:"warming"` → caught up → `state:"ready"` → live stream on each new bar.
5. **Delete `nautilus-chart/src/utils/indicators/*.ts`** (all ~20 files). Chart imports/usages get repointed to `indicator_subscribe`.
6. Update chart's `INDICATOR_REGISTRY` (`src/components/Chart/utils/indicatorMetadata.ts`) to be auto-generated from `nautilus capabilities`.
7. Custom indicators: drop a Python class with `@register_indicator("name")` decorator; `indicator_list` picks it up.

**Acceptance criteria:**

- `nautilus indicator list` returns all 38+ catalog entries with parameter schemas.
- `nautilus indicator compute --name ema --params '{"period":20}' --instrument BTCUSDT.BINANCE --bar 1-MINUTE --from 2024-01-01 --to 2024-01-02` emits NDJSON of `{ts_ns, value}` points.
- Chart EMA(20) value equals strategy EMA(20) value bit-for-bit (regression test).
- `grep -r "calculateSMA\|calculateEMA\|calculateRSI" nautilus-chart/src/` returns zero hits.
- `engine /clients` reports `indicators: <non-zero>` when chart is open.

**Dependencies:** Phase A (capabilities discovery) + Phase B (subscription machinery).

**Owner:** _user to assign_.

**Chart audit items absorbed:**

- **CHART-008** indicators never reach engine — the marquee fix

**Risks:**

- Some chart TS indicators (TPO, PriceActionRange, RangeBreakout, HilengaMilenga, FirstCandle, IchimokuCloud) are not in nautilus's catalog. Decision needed: port to Rust core, register as Python custom, or accept as chart-rendering-only (drawing-tool tier). Default: register as Python custom indicators in `nautilus_trader/agent_surface/custom_indicators/`. Document in sub-plan.

---

### Phase G — Capabilities introspection + MCP server (1 week)

**Sub-plan:** `docs/superpowers/plans/2026-XX-XX-phase-g-mcp.md`

**Motivation:** Unlock the coding-agent client class (D1) by exposing the surface as MCP tools. Auto-generated; one piece of work, all agents benefit.

**Scope:**

1. `nautilus_trader/agent_surface/mcp.py` — MCP server exposing every `@verb` as an MCP tool with auto-generated input schema.
2. Decorator-driven verb registry: `@verb(modes=["live","backtest"], scope="trade", description="...")` is the single source of truth; JSON Schema for capabilities discovery + MCP tool definition both derive from it.
3. Document agent onboarding in `docs/integrations/agents.md`.
4. E2E test: spawn MCP server, connect with the MCP test client, verify every surface verb is callable and returns expected schema.
5. Add CI matrix entry for the MCP integration test.

**Acceptance criteria:**

- Claude Code with MCP wired to `nautilus_trader.agent_surface.mcp` lists every verb as a tool.
- An agent calling `adapter_list` via MCP returns the same envelope as the CLI's `nautilus adapter list`.
- `docs/integrations/agents.md` documents the setup in ≤ 30 lines.

**Dependencies:** Phases A through F (so the agent has a full surface to use, not a partial one).

**Owner:** _user to assign_.

**Chart audit items absorbed:** None directly.

**Risks:**

- MCP transport semantics for streaming subscriptions need verification; one-shot tools are well-supported but subscription lifecycle may need custom handling.

---

### Phase H — Adapter capability advertisement (rolling, starts ~Phase F)

**Sub-plan:** `docs/superpowers/plans/2026-XX-XX-phase-h-adapter-caps.md` (per adapter, rolling)

**Motivation:** Stop forcing agents and UIs to reverse-engineer venue constraints. Pre-block impossible orders at the surface boundary.

**Scope (per adapter, starting with Zerodha):**

1. Add `AdapterCapabilities` struct to the adapter (Rust struct → PyO3-bound, or Python constant).
2. Fields: `venue`, `asset_classes`, `order_types`, `time_in_force`, flags, `book_support`, `historical_bar_aggregations`, `auth_modes`, `market_hours`, `constraints`.
3. Register in `nautilus_trader/agent_surface/adapter_caps.py` registry.
4. `capabilities` envelope exposes per-adapter caps.
5. Pre-trade validation in `order_submit`: refuse impossible orders before the wire (e.g., Zerodha LIMIT-after-15:20-MIS → `error{code:"adapter_constraint_violation"}`).

**Acceptance criteria (per adapter):**

- `nautilus adapter status zerodha` returns the full `AdapterCapabilities` payload.
- A `order_submit` with `order_type:"OCO"` to Zerodha (unsupported) returns `error{code:"order_type_unsupported"}` without touching the wire.
- Per-adapter golden test asserts the capability payload matches a fixture.

**Sequence:** Zerodha first (active build), IB second (well-understood baseline), then one adapter per cycle — Binance, ThetaData, then the rest of the crypto suite.

**Dependencies:** Phase A (capabilities discovery) — but can start earlier per-adapter once Phase A's discovery is up.

**Owner:** _user to assign per adapter_.

**Chart audit items absorbed:**

- **CHART-007** RTH/ETH per-instrument — `AdapterCapabilities.market_hours` advertises
- **CHART-013** OptionChain bogus venue — pre-trade refusal

**Risks:**

- Capability advertisement diverges from actual venue behavior; mitigation: per-adapter golden tests run against venue (or fixture) regularly.

---

## 6. Chart audit reconciliation (consolidated)

Every CHART-XXX item assigned to a phase or marked chart-only. Chart-only items run in parallel with phases; do not block the surface roadmap.

| ID | Severity | Disposition | Phase / track |
|---|---|---|---|
| CHART-001 | P0 | Chart-only | Hot-fix, Week 1 alongside Phase 0 |
| CHART-002 | P1 | Phase B | Quote envelope additions |
| CHART-003 | P1 | Phase B | Error envelope routing |
| CHART-004 | P1 | Chart-only | FE dual subscribe trade+quote |
| CHART-005 | P1 | Phase A + Phase H | `catalog_bars` verb canonicalizes; IB FX whatToShow gets adapter cap |
| CHART-006 | P2 | Phase A | `catalog_bars` canonicalizes EXTERNAL/INTERNAL |
| CHART-007 | P1 | Phase H | Market hours in `AdapterCapabilities` |
| **CHART-008** | **P0** | **Phase F** | **Server-side indicators** |
| CHART-009 | P0 | Phase B | Book delta envelopes |
| CHART-010 | P1 | Phase E | Replay verbs |
| CHART-011 | P1 | Phase C | Account_state verb |
| CHART-012 | P1 | Phase C | Error envelope + scope concept |
| CHART-013 | P1 | Phase H | Adapter capability pre-trade check |
| CHART-014 | P2 | Chart-only | FE backoff policy |
| CHART-015 | P1 | CLOSED | No-repro per audit |
| CHART-016 | P2 | Chart-only | FE state machine |
| CHART-017 | P2 | Chart-only + Phase A | Session-break needs `catalog_calendar` verb |
| CHART-018 | P2 | Chart-only | FE logging level |
| CHART-019 | P2 | Chart-only | FE perf (memo + throttle) |
| CHART-020 | P2 | Chart-only + Phase A | `instruments_search` introspectable state |

**Chart-only track** runs throughout. Recommended sub-plan: `nautilus-chart/docs/superpowers/plans/2026-05-21-chart-only-fixes.md` covering CHART-001/004/014/016/017a/018/019/020. Estimated ~1 week of FE work, parallelizable with Phase A and B.

## 7. Upstream-sync regime

This is the meta-deliverable that makes the platform sustainable.

### 7.1 Remote topology

```
upstream → https://github.com/nautechsystems/nautilus_trader.git    (read-only)
origin   → git@github.com:<you>/nautilus_trader-fork.git           (private)
```

Established in Phase 0.

### 7.2 Patch ledger

`patches/MANIFEST.md` rows have:

| Field | Example |
|---|---|
| `id` | P-001 |
| `subject` | IB delayed-tick normalize |
| `commit` | `ab42a11e65` |
| `patch_file` | `patches/0001-ib-delayed-tick-normalize.patch` |
| `class` | `upstream-able` / `private` |
| `upstream_issue` | https://github.com/nautechsystems/nautilus_trader/issues/NNNN |
| `retirement_criterion` | "Upstream `trade_type` field accepts nullable" |
| `retirement_test_path` | `nautilus_trader/agent_surface/tests/test_patches/test_p_001_still_needed.py` |

Retirement tests assert the upstream bug STILL EXISTS. When upstream fixes it, the test turns red → delete the patch + the test.

### 7.3 ABI surface test

`nautilus_trader/agent_surface/tests/test_abi.py` (created in Phase A). For every internal nautilus symbol the surface depends on:

- Assert symbol exists at expected path.
- Assert signature unchanged (use `inspect.signature`).
- Assert behavior contract where load-bearing (e.g., `MessageBus._publishable_types` snapshot at init).

Run as the **first** test in `make sync-upstream`. Red here = upstream changed something load-bearing; fix in the surface; clients are untouched.

### 7.4 Sync workflow (`make sync-upstream`)

```
1. git fetch upstream
2. git checkout -b sync/$(date +%Y%m%d) upstream/develop
3. git am patches/*.patch                                  # replay tracked divergences
4. uv sync                                                  # rebuild env
5. pytest nautilus_trader/agent_surface/tests/test_abi.py  # ABI surface test
6. pytest nautilus_trader/agent_surface/tests/             # surface unit + integration
7. pytest tests/integration_tests/agent_surface/           # surface against real adapters
8. (optional) cd ~/projects/nautilus-chart && uv run pytest && npm test
9. Verdict: green ≥ 7 days → fast-forward master ; red → triage
```

Promote to `nautilus_trader/.claude/skills/sync-upstream/SKILL.md` as a `/sync-upstream` slash command in Phase 0 or Phase A.

### 7.5 Pinning

`bridge/pyproject.toml` and any other downstream `pyproject.toml`: `nautilus-trader == X.Y.Z`, never `^X.Y` or `~X.Y`. Version bumps are deliberate sync events.

## 8. Cross-cutting workstreams

These run alongside the phases.

### 8.1 Chart-only fixes

Track in `nautilus-chart/docs/superpowers/plans/2026-05-21-chart-only-fixes.md`. ~1 week of FE work, parallelizable. Items: CHART-001 (NSE seed), CHART-004 (dual subscribe), CHART-014 (greeks backoff), CHART-016 (RECONNECTING state), CHART-017 partial, CHART-018 (log levels), CHART-019 (perf), CHART-020 (empty state).

### 8.2 Zerodha Phase 7+

Already in flight per `4fe195b27e`. First to receive `AdapterCapabilities` in Phase H. Coordinate with the Zerodha track owner to land caps + webhook + bracket-order support together.

### 8.3 Event journal evolution

Phase A ships NDJSON. Phase G or later (when `event_query` verbs are needed) migrates to sqlite. Schema is forward-compatible by design.

## 9. Risks register

| # | Risk | Mitigation | Owner |
|---|---|---|---|
| R1 | Upstream renames a load-bearing internal symbol (`MessageBus._publishable_types`, `ImportableConfig.create`) | ABI surface test catches before deploy; patch ledger documents | Surface owner |
| R2 | Chart e2e parity regression during Phase B cutover | Byte-for-byte envelope diff contract test; gradual rollout via feature flag | Chart owner |
| R3 | BacktestEngine event-stream wiring exceeds 2 weeks | Time-box Phase D; fall back to "completion-event-only" shape if streaming is intractable | Surface owner |
| R4 | Chart custom TS indicators (TPO, PriceActionRange) have no nautilus equivalent | Port as Python custom indicators; document migration per indicator in Phase F sub-plan | Surface owner |
| R5 | Auth scope retrofit costs ramify | D6 locked in Phase 0; scopes baked into every verb decorator from Phase A | Surface owner |
| R6 | MCP subscription lifecycle awkward | Phase G prototype against MCP test client before committing protocol shape | Surface owner |
| R7 | Adapter capability claims drift from actual behavior | Per-adapter golden tests in Phase H; CI runs against fixtures | Adapter owners |
| R8 | Plan ownership unassigned | Phase 0 requires owners; document blocks Phase A start | User |
| R9 | Upstream `develop` lands a breaking change between syncs | Weekly sync cadence + ABI test catches early | Surface owner |
| R10 | Chart NL chat panel scope creep | NL chat is Phase ≥ G; explicitly out of scope for A–F | Chart owner |

## 10. Open questions (status)

From agent-operable.md §13. Each is now decided, deferred, or open.

| # | Question | Status |
|---|---|---|
| Q1 | Auth | **DECIDED** — D6 (bearer + scopes + sqlite) |
| Q2 | Event journal storage | **DECIDED** — D9 (NDJSON v1, sqlite later) |
| Q3 | Long-running CLI subprocess for streams | **DEFERRED** — Phase G addresses via `--output-file` flag |
| Q4 | Backtest progress fidelity | **OPEN** — Phase D sub-plan resolves |
| Q5 | In-process vs subprocess client | **DECIDED** — D7 (one dispatcher) |
| Q6 | Schema versioning | **DECIDED** — `capabilities.payload.version="1"`; advisory `min_client_version` per verb (Phase A) |
| Q7 | Order routing by subscription | **DEFERRED** — Phase C sub-plan resolves |
| Q8 | `nautilus serve` default bind | **DEFERRED** — Phase A sub-plan resolves; default proposal: Unix domain socket + opt-in TCP with auth |
| Q9 | Token scope granularity | **DECIDED** — D6 (`read` / `trade` / `admin`) |
| Q10 | Indicator service warmup cost | **DECIDED** — `state: warming` → `state: ready` per Phase F sub-plan |

## 11. Definition of done

The platform is **done** when **all** of the following pass:

1. `nautilus capabilities | jq '.payload.verbs[].name' | wc -l` is ≥ 28.
2. A clean machine running `make install && nautilus serve --env live --config example.json` plus a separate chart container brings up a working chart UI inside 60 seconds.
3. `nautilus order submit --venue ZERODHA …` works against a real venue and the fill is observable from (a) the chart, (b) `nautilus event tail`, (c) an MCP-connected agent — within one second on each.
4. Chart shows EMA(20) value identical (bit-for-bit) to the value a Strategy `register_indicator_for_bars` consumer sees.
5. `nautilus backtest run --config sweep.json --follow` streams ≥ 1000 bar events per second to stdout on the reference fixture.
6. `nautilus replay start --catalog ./catalog --from … --to … --speed 4x` works end-to-end and the chart's scrubber drives it.
7. `make sync-upstream` runs cleanly on `upstream/develop` HEAD with the current patch ledger applied.
8. `grep -r openalgo nautilus-chart/src/` returns ≤ 1 hit (in `NOTICE.md`).
9. `chart_bridge_strategy.py` is ≤ 250 lines.
10. Every adapter in `crates/adapters/*` has an `AdapterCapabilities` entry, or is documented as not-yet-advertised in `adapter_caps.py`.

When all 10 pass, the integrated platform is delivered.

## 12. References

- `nautilus_trader/docs/architecture/agent-operable.md` — the spine; this plan is its execution
- `nautilus-chart/docs/ROUND5_DESIGN.md` — Strategy-as-bridge architecture (consumed by Phase B)
- `nautilus-chart/docs/CHART_AUDIT_2026-05-19.md` — 20-bug audit (reconciled in §6)
- `nautilus-chart/TODOS.md` — 3 deferred items (reconciled into Phases A, C, G)
- `nautilus_trader/.claude/skills/nautilus-expert/rules/design-principles.md` — invariants
- `nautilus_trader/.claude/skills/nautilus-expert/rules/adapter-architecture.md` — three-component pattern, credential discipline
- `~/.claude/projects/-Users-shergill-projects-nautilus-trader/memory/feedback_trading_systems_data_architect.md` — the 10 architect axes (§2 of this plan)
- `~/.claude/projects/-Users-shergill-projects-nautilus-trader/memory/feedback_simple_strategy_bridge.md` — no Redis, no parallel gateways
- `~/.claude/projects/-Users-shergill-projects-nautilus-trader/memory/feedback_never_discard_work.md` — Phase 0 inventory discipline
- `~/.claude/projects/-Users-shergill-projects-nautilus-trader/memory/feedback_no_license_concerns.md` — no NOTICE-rewrite work

---

## Self-review (run after writing this document)

**1. Spec coverage** — every locked decision and every CHART-XXX has a phase assigned in §5 or §6. Every memory invariant maps to §2. Every agent-operable §1 decision is in §1 here. Pass.

**2. Placeholder scan** — no TBD / TODO / "fill in later" / "similar to" in the body. Owner fields say "_user to assign_" which is a deliberate placeholder (this plan can't pick owners). Pass.

**3. Type consistency** — verb names match across §1 / §3 / §5 / §11 (`capabilities`, `catalog_*`, `instruments_*`, `adapter_*`, `data_subscribe`, `order_*`, `position_*`, `account_*`, `backtest_run`, `replay_*`, `indicator_*`, `event_tail`). Phase letters match across all sections. Pass.

---

## Execution handoff

Plan complete and saved to `docs/architecture/integrated-platform-plan.md`.

**Per-phase sub-plans are not yet written** — they get drafted at the start of each phase via `superpowers:writing-plans`, with each phase's sub-plan informed by what the previous phase learned. This is intentional.

**Recommended next steps, in order:**

1. **Confirm owners** for Phases 0 and A so work can start.
2. **Write Phase 0 sub-plan** (`docs/superpowers/plans/2026-05-21-phase-0-foundation.md`) — small, ~1-3 days, unblocks everything else.
3. **Write the chart-only-fixes sub-plan** in parallel (`nautilus-chart/docs/superpowers/plans/2026-05-21-chart-only-fixes.md`) so FE work runs alongside Phase 0/A.
4. **Begin Phase 0** — execute via `superpowers:subagent-driven-development` (recommended; isolated subagent per task) or `superpowers:executing-plans` (inline with batch checkpoints).

When you're ready, tell me which sub-plan to draft next and which execution mode (subagent-driven or inline) to use.
