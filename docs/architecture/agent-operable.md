# Agent-operable Nautilus — architecture for unifying CLI, charts, and agents

**Status:** Proposal — 2026-05-20
**Author:** drafted with `/nautilus-expert` review
**Audience:** maintainers extending the operator surface; agent builders writing tooling against Nautilus
**Scope:** the cross-cutting design that lets CLI, charts (`nautilus-chart`), and AI agents drive the same engine through one coherent capability layer.

---

## 1. Decisions locked

These answers fix the architecture's shape before any of the analysis below.

| Decision | Choice |
|---|---|
| Who is "agent"? | All three — coding agents (Claude Code et al.), in-platform autonomous trading agents, and human-in-the-loop natural-language operators. |
| Execution modes | **Both backtest and live, symmetrically.** Same verbs, same envelopes; mode is context, not branching control flow. |
| Protocol shape | **CLI-first.** The `nautilus` binary IS the canonical surface. Charts, web shells, autonomous strategies, and agent tools all wrap or co-mount the same capability module. |
| Report location | `docs/architecture/agent-operable.md` (this file). |

The CLI-first choice is the single most consequential decision. It forces a textual, scriptable, JSON-shaped surface as the *primary* contract — every other shell (chart WebSocket, in-process Python client, future REST proxy) is a wrapper over that contract, not a sibling reinvention.

---

## 2. Current state — what exists today

### 2.1 Operator surfaces

| Surface | What it does | What it can't do |
|---|---|---|
| `crates/cli/` (Rust `nautilus` binary) | Database admin only — Postgres init/teardown; gated `defi` blockchain commands. | No trading, data, backtest, or replay verbs. Not currently agent-relevant. |
| `python -m nautilus_trader.backtest` | Click CLI that takes `--raw` JSON or `--fsspec-url` and runs a `list[BacktestRunConfig]` via `BacktestNode.run()` (`nautilus_trader/backtest/__main__.py:28`). | One-shot, opaque to agents — you cannot subscribe, inspect, or steer; you submit a config blob and read whatever the strategy logs. |
| `python -m nautilus_trader.live` | Same shape for `TradingNodeConfig` (`nautilus_trader/live/__main__.py:28`). | Same limitations as the backtest entry point. |
| `nautilus-chart` ChartBridgeStrategy | FastAPI + uvicorn embedded inside a Nautilus `Strategy` (`bridge/nautilus_chart_bridge/chart_bridge_strategy.py:174`). Exposes a WS protocol with bars/quotes/trades/book/mark/index/funding envelopes (`bridge/.../envelope.py:124–264`), order submit/modify/cancel commands (`chart_bridge_strategy.py:1137–1224`), replay seek/speed (`chart_bridge_strategy.py:1109–1130`), and HTTP routes for catalog/instruments/positions/orders/accounts (`chart_bridge_strategy.py:557–730`). | Tightly coupled to the chart use case; no CLI exposure of the same verbs; no formal capabilities-discovery endpoint; auth deferred (`D4`); no JSON audit trail. |

### 2.2 Adapters (capabilities to expose)

From the inventory of `crates/adapters/*` + `nautilus_trader/adapters/*`:

- **Zerodha** — active build, Phase 6 (modify/cancel/positions/funds) just shipped. Full Rust core (`crates/adapters/zerodha/src/`): auth, session, http, ws_handler, data/decode/live, execution, instruments/symbology/persistence, historical. Python wrappers in `nautilus_trader/adapters/zerodha/`. Spec at `specs/zerodha-adapter.md`.
- **Interactive Brokers** — production-mature, 49 Rust modules + 27 Python files, maintenance-only churn (last edit on `client/market_data.py`).
- **Mature crypto suite** — binance, coinbase, bitmex, kraken, bybit, deribit, hyperliquid, dydx, okx, betfair, thetadata, tardis, databento, polymarket. All follow the three-component pattern (`rules/adapter-architecture.md`).
- **Exploratory / research** — architect_ax, sandbox, blockchain.

Every adapter exposes the same surface: `InstrumentProvider`, `LiveMarketDataClient`, `LiveExecutionClient`, factory functions for `LiveNode.add_data_client` / `add_exec_client`. *Already* uniform — the operator layer just needs to project it.

### 2.3 Indicators

38 production indicators in `crates/indicators/` (momentum, average, volatility), PyO3-bound and re-exported from `nautilus_trader/indicators/__init__.py`. All Rust-core. No custom Python indicator tree; no "indicator service" yet — they are pure stateful objects strategies own.

### 2.4 In-flight specs

`specs/zerodha-adapter.md` is the only spec in the directory (untracked, 514 lines). All other adapter work is direct-to-code.

---

## 3. The architecture gap

Two surfaces exist today (`python -m nautilus_trader.backtest` + ChartBridgeStrategy). They share **nothing**:

- Different transports (Click + stdin blob vs FastAPI/WS).
- Different schemas (Click flags vs msgspec envelopes).
- Different verbs (none vs ~15 chart-flavoured commands).
- Different mode coverage (backtest-only vs live-only).
- Different discovery story (none vs nothing).

A coding agent that wants to "run a backtest, then watch its fills stream into the chart, then start a live strategy" must traverse three different protocols, none of which are introspectable.

The Strategy-embedded chart bridge happens to have already evolved the *right shape* of a capability protocol — tagged-union envelopes, bidirectional commands, subscription lifecycle, replay control. But it's locked inside the chart project and chart-specific.

**The architectural move is to extract that envelope + dispatch core into Nautilus itself, give it a CLI front door, keep the chart as one client of it, and add the verbs that complete the picture.**

---

## 4. The capability surface

### 4.1 Layered architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Shells                                                              │
│  ┌────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │ `nautilus` CLI │  │  Chart WS shell  │  │ In-proc Py client    │  │
│  │ (canonical)    │  │  (nautilus-chart)│  │ (autonomous agents)  │  │
│  └───────┬────────┘  └────────┬─────────┘  └──────────┬───────────┘  │
│          │ stdin/stdout JSONL │ WebSocket JSONL       │ direct calls │
└──────────┼────────────────────┼───────────────────────┼──────────────┘
           ▼                    ▼                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  CapabilitySurface  (nautilus_trader/agent_surface/)                 │
│                                                                      │
│  • capabilities()      — discovery (schema, verbs, schemas)          │
│  • catalog.*           — list / query / info / export / ingest       │
│  • instruments.*       — list / search / show                        │
│  • adapter.*           — list / status / connect / disconnect        │
│  • data.subscribe/*    — bars / quotes / trades / book / derivatives │
│  • order.*             — submit / modify / cancel / list / show      │
│  • position.* / account.*                                            │
│  • strategy.*          — list / start / stop / pause / status        │
│  • backtest.run        — execute a config, stream events             │
│  • replay.*            — start / seek / speed / stop                 │
│  • indicator.*         — list / compute / subscribe                  │
│  • event.tail          — stream message-bus events with filters      │
└─────────────────────────────┬────────────────────────────────────────┘
                              │  uses Strategy / TradingNode /
                              │  BacktestNode / Cache / MessageBus
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Nautilus core (Rust + Python, unchanged)                            │
└──────────────────────────────────────────────────────────────────────┘
```

### 4.2 Where the surface lives

A new Python package: **`nautilus_trader/agent_surface/`**.

- `envelopes.py` — tagged-union request/response/event structs (msgspec, frozen). Imports the chart bridge's existing tags verbatim (`bar`, `quote`, `trade`, `book_delta`, `book_deltas`, `mark_price`, `index_price`, `funding_rate`, `instrument`, `event`, `account_state`, `error`) and adds the missing operational ones (`capabilities`, `catalog_*`, `adapter_*`, `strategy_*`, `backtest_*`, `replay_*`, `indicator_*`).
- `surface.py` — the `CapabilitySurface` class. Pure Python. Takes a *node* on construction (`BacktestNode` or `LiveNode`/`TradingNode`) plus a `MessageBus` reference. All verbs are async methods returning either a single envelope or an async iterator of envelopes.
- `cli/` — Click-based `nautilus` subcommand tree that wires every surface verb to a CLI entry point.
- `transports/stdin.py` — runs a `CapabilitySurface` over stdin JSONL ⇄ stdout JSONL.
- `transports/ws.py` — runs a `CapabilitySurface` over WebSocket JSONL (consumed by `nautilus-chart`).
- `client.py` — a Python `CapabilityClient` that speaks the same protocol so in-process strategies and notebooks can introspect the surface without spawning a subprocess.

This package is **the** vendor-neutral protocol module. The chart project consumes it; the CLI consumes it; autonomous strategies consume it; future REST/gRPC fronts wrap it.

### 4.3 Why this honors Nautilus's invariants

| Invariant | How the surface honors it |
|---|---|
| Message immutability (`rules/design-principles.md`) | Envelopes are `msgspec.Struct(frozen=True)`. The surface never mutates inbound events; it serializes and forwards. |
| Rust core, Python control plane | The surface is entirely Python control-plane. Hot paths (book deltas, ticks, decoder) stay in Rust. The surface marshals events that already exist on the bus. |
| `get_runtime().spawn()` discipline | The stdin and WS transports spawn their I/O loops via the same Tokio runtime adapters that the chart bridge already uses. Python threads never touch raw Tokio handles. |
| Clock abstraction | The surface reads `node.kernel.clock` to expose timestamps; it never calls `time.time()` itself. Replay verbs delegate to the existing `ScrubberClock` machinery. |
| Determinism / replay | Every command and event is journalled to an optional event log (NDJSON file or sqlite); replay reads commands back in order. This *adds* a determinism guarantee that the current chart bridge lacks. |
| Credentials never in DTOs (`rules/adapter-architecture.md`) | Adapter connect verbs take a `venue` name only; credentials still resolve from env vars in `{venue}/credential.rs`. |

---

## 5. Wire protocol

### 5.1 Envelope shape (request / response / event)

Single tagged-union schema, served as one JSON object per line (NDJSON for streams, single object for one-shot calls).

```jsonc
// Command (client → surface)
{ "op": "order_submit",
  "req_id": "01HV...",            // ULID, client-generated
  "env": "live",                  // "live" | "backtest" — required, see §6
  "payload": {
    "venue": "ZERODHA",
    "instrument_id": "RELIANCE.NSE",
    "side": "BUY",
    "quantity": "100",
    "order_type": "LIMIT",
    "price": "2810.50",
    "time_in_force": "DAY",
    "client_order_id": "panel-a:42",
    "tags": ["panel-a"] } }

// Response (surface → client, one-shot)
{ "op": "order_submit.ok",
  "req_id": "01HV...",
  "payload": { "venue_order_id": "...", "status": "ACCEPTED", "ts_ns": 1716170400000000000 } }

// Stream event (surface → client, after subscribe)
{ "op": "event",
  "kind": "order_filled",
  "subscription_id": "sub-7",
  "payload": { ... } }

// Error
{ "op": "error",
  "req_id": "01HV...",
  "code": "instrument_unknown",
  "message": "...",
  "retryable": false }
```

### 5.2 Verb-naming convention

`<resource>_<verb>`. Snake-case, no slashes. Avoids both CLI-flag confusion and HTTP-path coupling. Examples: `catalog_list`, `order_submit`, `replay_seek`, `indicator_compute`.

### 5.3 Discovery: `capabilities`

```jsonc
// Request
{ "op": "capabilities", "req_id": "01HV..." }

// Response
{ "op": "capabilities.ok",
  "req_id": "01HV...",
  "payload": {
    "version": "1",
    "build": "nautilus-1.226+agent-surface.0.1",
    "verbs": [
      { "name": "order_submit",
        "request_schema": { /* JSON schema */ },
        "response_schema": { /* JSON schema */ },
        "modes": ["live", "backtest"],
        "auth_required": true },
      ...
    ],
    "events": [
      { "name": "order_filled", "schema": { ... } },
      ...
    ],
    "adapters": ["zerodha", "interactive_brokers", "binance", ...],
    "indicators": ["ema", "sma", "rsi", "macd", "bollinger_bands", ...]
  } }
```

This is the single endpoint AI agents key off. They auto-generate tools from it. Adding a new verb only requires updating the envelope schema + surface method; capability advertising follows automatically (the surface introspects its own decorated methods).

### 5.4 Subscription lifecycle

Adopt the chart bridge's existing model verbatim (`chart_bridge_strategy.py:_on_subscribe`):

- `data_subscribe` → returns `subscription_id`, starts streaming envelopes tagged with that id.
- `data_unsubscribe` → cancels.
- Subscription IDs are per-connection (CLI session, WS connection, in-proc client).
- The surface preserves subscriptions across reconnects when an idempotency token is supplied (`req.session_token`); otherwise they die with the transport.

### 5.5 Streaming over stdout

For one-shot CLI invocations:

```
$ nautilus order submit --venue ZERODHA --instrument RELIANCE.NSE --side BUY --qty 100 --type LIMIT --price 2810.50
{"op":"order_submit.ok","req_id":"01HV...","payload":{...}}
$
```

For streaming verbs (`--follow`, default for `subscribe` / `tail`):

```
$ nautilus data subscribe bars BTCUSDT.BINANCE 1-MINUTE-LAST --follow
{"op":"data_subscribe.ok","req_id":"01HV...","payload":{"subscription_id":"sub-1"}}
{"op":"bar","subscription_id":"sub-1","payload":{...}}
{"op":"bar","subscription_id":"sub-1","payload":{...}}
^C
{"op":"data_unsubscribe.ok","payload":{"subscription_id":"sub-1"}}
```

The CLI binds `Ctrl-C` to send a clean `data_unsubscribe` before exiting. Agents that own the subprocess can write commands to its stdin, mixing subscriptions and ad-hoc requests on the same channel.

### 5.6 Interactive shell mode

`nautilus shell` opens a long-running REPL where each line is one envelope (or one human-friendly verb that the shell translates to an envelope). This is the **agent-friendly** mode — Claude Code spawns this, pipes JSON, reads NDJSON, and can orchestrate multi-step flows without re-paying TradingNode startup cost.

---

## 6. Mode parity (backtest = live)

The user's directive: same verbs, same envelopes, same observation shape, in both modes.

### 6.1 Mode is connection-scoped

A CLI session, WS connection, or Python client binds to one mode at startup:

```
$ nautilus serve --env backtest --config bt.json   # serves CapabilitySurface bound to BacktestNode
$ nautilus serve --env live --config live.json     # bound to TradingNode/LiveNode
```

Inside the surface, identical Python interfaces are used:

| Surface need | Live | Backtest |
|---|---|---|
| Submit an order | `strategy.submit_order(...)` | `strategy.submit_order(...)` (identical API) |
| Subscribe to bars | bus subscription via `subscribe_bars` | bus subscription via `subscribe_bars` |
| Replay scrubbing | n/a — `replay_*` returns `unsupported_in_mode` error | `ScrubberClock` from chart bridge, generalized |
| Adapter connect | adapter factory + venue env vars | adapter factory wired against synthetic exchange |
| Catalog read | always available | always available |

Only **replay** verbs are mode-restricted. Every other verb behaves identically. This is the discipline that lets agents write strategies once and exercise them both ways.

### 6.2 Backtest as a streaming verb

Today's `BacktestNode.run()` is blocking and produces results only at the end. The surface must expose backtest as a streaming verb:

```
$ nautilus backtest run --config sweep.json --follow
{"op":"backtest_run.started","req_id":"01HV...","payload":{"runs":[{"id":"run-0","total_steps":1440},...]}}
{"op":"event","kind":"order_filled","payload":{...,"run_id":"run-0"}}
{"op":"event","kind":"bar","payload":{...,"run_id":"run-0"}}
{"op":"backtest_run.progress","payload":{"run_id":"run-0","steps_done":720}}
{"op":"backtest_run.completed","payload":{"run_id":"run-0","stats":{...}}}
{"op":"backtest_run.ok","req_id":"01HV..."}
```

This composes with chart panels (a panel can subscribe to a backtest-run's bar/event stream just like a live feed) and with agents (they observe progress incrementally).

`BacktestNode` doesn't natively emit a per-step stream — implementation involves wiring an in-process `BacktestEngine` and forwarding `MessageBus` events through the surface's envelope serializer, the same way the chart bridge does for live mode.

---

## 7. Adapter exposure

Adapters keep their existing three-component shape (`rules/adapter-architecture.md`). The surface adds two things:

### 7.1 Adapter verbs

```jsonc
{ "op": "adapter_list" }
// → { "ok": [{"name":"zerodha","state":"disconnected","supports":["data","execution"]}, ...] }

{ "op": "adapter_status", "payload": {"venue": "zerodha"} }
// → { "ok": {"venue":"zerodha","data":{"state":"connected","subscriptions":12,"latency_ms_p50":4.1,...},
//             "execution":{"state":"connected","orders_open":3,"reconciliation_ts_ns":...}} }

{ "op": "adapter_connect",    "payload": {"venue": "zerodha", "components": ["data","execution"]} }
{ "op": "adapter_disconnect", "payload": {"venue": "zerodha"} }
{ "op": "adapter_reload_instruments", "payload": {"venue": "zerodha"} }
```

These call into the existing factory-returned clients (`venue_data_client_factory`, `venue_exec_client_factory`) — the surface holds references the same way `LiveNode` does and exposes lifecycle.

### 7.2 Adapter capability advertisement

Every adapter declares (in a static `AdapterCapabilities` Rust struct surfaced via PyO3 or a Python `factories.py` constant):

- Asset classes (`equity`, `fx`, `crypto`, `futures`, `options`, ...).
- Supported order types, TIFs, post-only/reduce-only/iceberg flags.
- Supports book deltas vs snapshots vs top-of-book only.
- Historical data: bar aggregations, depth, time range limits.
- Auth modes (`api_key`, `oauth`, `daily_token` for Zerodha, ...).

`capabilities` returns this list per adapter. Agents key off it to refuse impossible orders before they touch the wire (e.g., "Zerodha doesn't accept LIMIT on MIS positions after 3:20pm" → `adapter.zerodha.order_constraints` advertises the rule).

This finally gives **agents a single place** to learn what's possible per venue without reading 19 different README files.

### 7.3 Zerodha-specific notes

Given the active build (Phase 6 just shipped):

- Phase 7 work should add `AdapterCapabilities` for Zerodha alongside webhook + bracket-order support.
- The agent surface in turn surfaces those capabilities from day one — there's no chance of the chart and CLI drifting out of sync on what Zerodha can do.

---

## 8. Indicator exposure

Today's 38 Rust-core indicators are bound to Python (`nautilus_trader/indicators/__init__.py`) and used by strategies as objects with `update_raw(...)` / `.value`. The chart computes indicators *client-side* in TypeScript (`nautilus-chart/src/components/Chart/utils/indicators`). Two parallel codebases of the same math — that's a fidelity hazard.

### 8.1 Promote indicators to first-class server-side primitives

The surface exposes:

```jsonc
// One-shot compute on a historical range
{ "op": "indicator_compute",
  "payload": {
    "name": "ema",
    "params": {"period": 20, "price_type": "LAST"},
    "instrument_id": "BTCUSDT.BINANCE",
    "bar_type": "BTCUSDT.BINANCE-1-MINUTE-LAST-EXTERNAL",
    "start_ts_ns": ..., "end_ts_ns": ... } }
// → ndjson of {ts_ns, value} points + final ok envelope

// Subscribe to a live indicator stream (server computes on each new bar)
{ "op": "indicator_subscribe",
  "payload": { "name": "ema", "params": {...}, "bar_type": "...", "warmup_from_catalog": true } }
```

### 8.2 Why this matters for charts and agents

- The chart drops its TypeScript indicator implementations and renders whatever the server streams. **One source of truth.**
- Agents can ask "compute RSI(14) on RELIANCE.NSE-1-DAY-LAST since 2024-01-01" without spinning up a Strategy.
- The chart bridge's `ROUND5_DESIGN.md §4` already planned this; the agent surface makes it the *primary* delivery mechanism, not a chart afterthought.

### 8.3 Stateful indicator service

A `nautilus_trader/agent_surface/indicator_service.py` holds `dict[(name, params, bar_type)] → indicator` keyed by a content hash. Multiple subscribers share one indicator instance. The service warms it from the catalog on first creation (so chart panels and agent queries see consistent history) and forwards each new bar's `update_raw(...)` output to all subscribers.

The math implementation never leaves Rust.

---

## 9. Chart integration — `nautilus-chart` as one client

### 9.1 What changes in the chart project

The chart bridge today owns:

1. **An envelope schema** — moves to `nautilus_trader/agent_surface/envelopes.py`. Chart imports it.
2. **Command dispatch** — moves into `CapabilitySurface`. Chart's WS handler becomes a thin transport that delegates to it.
3. **HTTP routes** for catalog/instruments/positions/orders/accounts — become wrappers over the corresponding `catalog_*`, `instruments_*`, `position_*`, `order_*`, `account_*` verbs. The HTTP layer survives for browser convenience but its semantics are the surface's, not chart-specific.
4. **Order routing back to originating panel** (`_panel_for_order[cid]`) — generalizes into "subscription id ↔ event routing". Same code, broader use case.
5. **ReplayFeed + ScrubberClock** — moves to `nautilus_trader/agent_surface/replay.py`. The chart uses it; agents use it too.

The end state: `chart_bridge_strategy.py` shrinks dramatically and becomes "embed a CapabilitySurface in this Strategy and serve it over WebSocket." Around 200 lines instead of 1443.

### 9.2 What stays unique to nautilus-chart

- The React + TradingView Lightweight Charts UI itself.
- Per-panel UI state (timeframe, indicators-overlaid, drawings).
- Drawing persistence (currently localStorage; could be promoted to a `drawing_*` verb later).
- The chat panel for the human-in-the-loop NL operator (see §10.3).

### 9.3 What the chart gains

- Symmetric backtest support — drop a panel onto a backtest run, scrub through it, see fills materialize. Today that's stubbed.
- Server-side indicators (§8) — no more TypeScript indicator drift.
- A real audit log of operator commands (free byproduct of the event journal).
- Multiplex with CLI sessions and agent connections — the chart can show events from an ongoing CLI-driven backtest, because they share one bus.

---

## 10. Agent integration — the three classes

### 10.1 Coding agents (Claude Code, Cursor, Codex)

- They consume the surface via subprocess: spawn `nautilus shell --env backtest` or `--env live`, communicate over stdin/stdout JSONL.
- First call is always `{"op":"capabilities"}`. Returned JSON-Schema lets the agent auto-generate typed tools.
- For introspection-heavy work (catalog browsing, strategy inspection), the agent issues one-shot verbs; for "watch this happen" workflows (run a backtest, observe fills), it streams events.
- Shipping recommendation: a thin MCP server (`nautilus_trader/agent_surface/mcp.py`) that exposes the capability surface as MCP tools, so agents that prefer MCP wiring get the surface for free without subprocess management. This is the *only* deviation from the CLI-first stance — the MCP server is itself just an MCP-shaped client of the same surface, not a parallel implementation.

### 10.2 In-platform autonomous trading agents

- These run *inside* a `TradingNode` (or `BacktestNode`) as a Nautilus Strategy.
- They use the in-process `CapabilityClient` (no subprocess, no JSON serialization) to do reflection:
  - "What adapters are connected?" — `client.adapter_list()`.
  - "What indicators are available?" — `client.indicator_list()`.
  - "Compute EMA(20) on this instrument's last 500 bars" — `client.indicator_compute(...)`.
- Their *trading* actions still use the direct `self.submit_order(...)` Strategy API for the hot path. The surface is for *meta* operations — discovery, observability, and any LLM-tool-shaped call where ergonomics beat microseconds.
- Critical: the surface exposes nothing that bypasses the engine's risk / pretrade checks. `order_submit` goes through the same `submit_order` path as a hand-coded strategy.

### 10.3 Human-in-the-loop natural language

- A chat panel inside `nautilus-chart` runs a small LLM client.
- It receives the chart's currently-selected instrument/timeframe/panel context as a system prefix.
- The LLM has the same capability schema as a coding agent (auto-derived from `capabilities`).
- "Buy 100 RELIANCE at market" → the LLM emits an `order_submit` envelope → the chart's UI shows the proposed envelope before sending → user confirms → goes through the surface like any other client.
- All replies are grounded in surface responses; the LLM never invents a status.

The three modes converge on one protocol. None of them is a special case in the surface — they're all just clients with different ergonomics.

---

## 11. Extension story — how new pieces plug in

| Adding... | Steps |
|---|---|
| A new adapter (say, `mock_venue`) | 1. Build the adapter crate per `rules/adapter-architecture.md`. 2. Implement `AdapterCapabilities`. 3. Register its factory in `nautilus_trader/adapters/mock_venue/factories.py`. 4. The surface auto-detects it via the factory registry — no surface code changes. |
| A new indicator | 1. Add to `crates/indicators/src/...`. 2. PyO3-bind it. 3. Register in `nautilus_trader/agent_surface/indicator_registry.py` (one line — name + python class). 4. `indicator_list` reflects it; `indicator_compute` / `indicator_subscribe` accept it. |
| A new operational verb (e.g., `risk_summary`) | 1. Add envelope structs in `envelopes.py`. 2. Add `CapabilitySurface.risk_summary(...)` method, decorated with `@verb(modes=["live","backtest"])`. 3. Add a Click subcommand in `cli/`. 4. Capability advertisement picks it up automatically. |
| A new chart panel type | 1. Define a new subscription envelope tag if needed. 2. Wire the new tag in the surface (most reuse existing verbs). 3. The chart consumes via its existing WS dispatcher. |
| A new transport (REST, gRPC) | 1. Build the transport module under `transports/`. 2. Translate verbs ⇄ HTTP/gRPC shapes; reuse envelopes. No surface code changes. |

The discipline: **envelopes are the contract.** Anything else is a transport or a shell.

---

## 12. Migration plan — phased rollout

Each phase is independently shippable. No big bang.

### Phase A — Surface scaffolding (1–2 weeks of focused work)

- Create `nautilus_trader/agent_surface/` package.
- Move chart bridge envelope schema verbatim into `envelopes.py` (chart still uses it via re-export; no chart change required yet).
- Implement `CapabilitySurface` skeleton: `capabilities`, `catalog_list`, `instruments_search`, `adapter_list`, `adapter_status`, `event_tail`. Read-only at first.
- Implement `transports/stdin.py` and the `nautilus` Click subcommand tree for the read-only verbs.
- Add `tests/integration_tests/agent_surface/test_surface_smoke.py` exercising the read-only surface against a static catalog fixture.

**Deliverable:** an agent can run `nautilus capabilities` and `nautilus catalog list` and `nautilus adapter list` against a real installation.

### Phase B — Subscription + live data (1–2 weeks)

- Port the chart bridge's subscribe/unsubscribe machinery to `surface.py`.
- Implement the bars/quotes/trades/book envelope-emitters (lift directly from `chart_bridge_strategy.py:on_bar` etc.).
- `nautilus data subscribe bars BTCUSDT.BINANCE 1-MINUTE-LAST --follow` works end-to-end.
- Add `transports/ws.py` and have `nautilus-chart` flip its WS handler to use it. Verify chart parity.

**Deliverable:** the chart is now a thin client of the surface. Same UX, less code.

### Phase C — Order + position + account verbs (1 week)

- Surface `order_submit`, `order_modify`, `order_cancel`, `order_cancel_all`, `order_list`, `order_show`, `position_list`, `position_close`, `account_list`, `account_show`.
- Promote chart bridge's `_panel_for_order` routing to surface-level subscription-id routing.
- CLI: `nautilus order submit ...`, `nautilus position list`, etc.
- Gate `order_*` behind `--enable-trading` (mirrors chart's `enable_order_submission`).

**Deliverable:** an agent (CLI or otherwise) can submit, modify, cancel orders against a live `TradingNode` and observe fills as events.

### Phase D — Backtest as a streaming verb (1–2 weeks)

- Wire `BacktestNode` + per-step event forwarding into the surface.
- `nautilus backtest run --config bt.json --follow` emits ndjson of bars + events + completion stats.
- Chart can subscribe to a backtest run's stream like a live feed.

**Deliverable:** agents and humans can run backtests through the same protocol as live.

### Phase E — Replay verbs + ScrubberClock generalization (1 week)

- Move `ReplayFeed` / `ScrubberClock` from chart bridge into `agent_surface/replay.py`.
- `replay_start`, `replay_seek`, `replay_speed`, `replay_stop` verbs.
- Chart bridge re-uses them through the surface (no change in chart behavior).

**Deliverable:** the chart's replay scrubber works through the surface; agents can drive replays for offline analysis.

### Phase F — Indicators server-side (1–2 weeks)

- `indicator_service.py` + `indicator_*` verbs.
- Chart project deletes its TypeScript indicator math; chart panels subscribe to server-side streams instead.
- Document the migration in `docs/integrations/nautilus-chart.md`.

**Deliverable:** one source of truth for every indicator's value.

### Phase G — Capabilities introspection + MCP server (1 week)

- Decorator-driven `@verb` registration with auto-generated JSON Schema (use `msgspec.json.schema_components`).
- MCP server module exposing every verb as an MCP tool, with the schema generated from the registry.
- Document agent onboarding in `docs/integrations/agents.md`.

**Deliverable:** any MCP-aware agent can use the full surface without writing tool definitions by hand.

### Phase H — Adapter capability advertisement (alongside future adapter work)

- Add `AdapterCapabilities` to each adapter, starting with Zerodha (since it's hot) and IB (well-understood baseline).
- Roll forward to one adapter per cycle.

**Deliverable:** agents know per-venue what's possible without reading source.

---

## 13. Open questions / risks

1. **Auth.** Today's chart bridge defers auth (`D4`, localhost-only). The agent surface must address it before any non-localhost deployment. Recommended: opaque bearer tokens issued by a `nautilus_trader/agent_surface/auth.py`, persisted in a sqlite file the operator chmod-protects. CLI subcommands accept `--token` or `NAUTILUS_TOKEN` env var; WS shell accepts an `Authorization: Bearer ...` header. Out-of-band token rotation is fine for v1.
2. **Event journal storage.** NDJSON file is simplest; sqlite is queryable; both are fine for v1. Recommend NDJSON now, sqlite later if `event_query` verbs need indexing.
3. **Long-running CLI subprocess for streams.** Agents have to keep a subprocess alive to consume a subscription. For Claude Code this is fine (the harness owns processes). For headless CI it's awkward — consider a `--output-file` mode that writes ndjson to disk while the subprocess detaches.
4. **Backtest progress fidelity.** `BacktestNode` doesn't natively emit step-level events. Wiring is straightforward (subscribe to the engine's bus before `run()`) but needs care to avoid double-buffering in long sweeps.
5. **In-process client vs subprocess client.** Don't ship two parallel implementations. The Python `CapabilityClient` and the stdin transport must share their dispatcher; the transport is a thin adapter that serializes/deserializes around an in-proc client.
6. **Versioning.** Envelope schema needs a version field. `capabilities.payload.version = "1"` and a `min_client_version` advisory on each verb. Add a compatibility test suite before phase G.
7. **Order routing back to subscription-id.** The chart's `_panel_for_order[cid]` works because the chart owns client_order_id minting. For agents that re-use cid across sessions or omit it, the surface needs a "subscription token" field on the order envelope and must route fills by that, not by cid.
8. **What does `nautilus serve` listen on by default?** Recommend a Unix domain socket for local trust + an opt-in TCP bind with auth required. Document loud.
9. **Permissioning beyond auth.** A token might be read-only (data + observation) or read-write (submit orders). Bake a `--scope` concept into the token now to avoid retrofitting.
10. **Indicator service warmup cost.** Warming an indicator from catalog on first subscribe blocks the response. For multi-second warmups, return `indicator_subscribe.ok` immediately with `state: "warming"`, then emit `indicator_subscribe.ready` when caught up. Same pattern any time the surface has to do nontrivial async work before streaming.

---

## 14. Self-check against `nautilus-expert` invariants

Before declaring this design ready, re-verify against the load-bearing rules:

| Invariant | Check |
|---|---|
| Message immutability | ✓ Envelopes are frozen. Surface never mutates inbound. |
| Rust core, Python control | ✓ Surface is Python; all math/IO stays where it lives. |
| Actor model | ✓ Surface is a Python-side coordinator; doesn't cross actor thread boundaries. Each transport runs in its own asyncio loop / Tokio task. |
| `get_runtime().spawn()` | ✓ WS transport reuses existing Tokio-backed primitives the chart bridge already uses. |
| Clock abstraction | ✓ Surface delegates time queries to `node.kernel.clock`. |
| `PyObject` not `Arc<PyObject>` | n/a — no new Rust bindings, but if/when transports get a Rust core, follow `rules/pyo3-bindings.md`. |
| Credentials never in DTOs | ✓ Adapter verbs take venue names; credentials resolve in `credential.rs` per adapter. |
| Determinism / replay | ✓ Event journal + replay verbs ensure operator actions are reproducible. |

---

## 15. Glossary

- **CapabilitySurface** — the Python module at `nautilus_trader/agent_surface/` that exposes every operator verb as an async method.
- **Verb** — a single method on the surface (e.g., `order_submit`).
- **Envelope** — a frozen `msgspec.Struct` representing a command, response, or event.
- **Shell** — a transport+UX layer over the surface (CLI, chart WS, in-process client, MCP server).
- **Mode** — `live` or `backtest`. Bound at surface construction time per session.
- **Subscription** — a persistent stream of events keyed by `subscription_id`, valid for the lifetime of a connection.
- **Capability** — a verb name plus its request/response schema plus mode applicability, advertised by `capabilities`.

---

## 16. References

- `rules/design-principles.md` — message immutability, layered architecture, clock abstraction.
- `rules/adapter-architecture.md` — three-component pattern, factories, credentials.
- `nautilus-chart/bridge/nautilus_chart_bridge/envelope.py` — current envelope schema, to be promoted.
- `nautilus-chart/bridge/nautilus_chart_bridge/chart_bridge_strategy.py` — current command dispatch + subscription bookkeeping, to be promoted.
- `nautilus_trader/backtest/__main__.py` / `nautilus_trader/live/__main__.py` — current Python CLI entries (will be subsumed by `nautilus backtest run` / `nautilus serve --env live`).
- `crates/cli/` — existing Rust `nautilus` binary; extends with the subcommand tree, or, if simpler, the new CLI is a Python module also exposed as `nautilus` and the Rust binary takes a subcommand-namespace (`nautilus db ...`, `nautilus blockchain ...`).
- `specs/zerodha-adapter.md` — current in-flight adapter; first candidate for `AdapterCapabilities`.
