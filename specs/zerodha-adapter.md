# Zerodha (Kite Connect) Adapter for NautilusTrader

**Status**: planning (3 review passes complete: critical / sub-agent-adversarial / repo-verified)
**Owner**: shergillbs
**Target**: live data + execution against production Kite (`api.kite.trade`, `ws.kite.trade`)
**Reference adapters**: ThetaData (binary-WS + REST shape), Interactive Brokers (multi-segment equities broker, polling reconciliation)

## Verified Nautilus model API contracts (anchor for implementation)

All Phase 5-7 work depends on these being right. Verified against the workspace at this commit; **do not implement against memory of what these types look like — read the file first**:

| Type / event | File | Contract |
|---|---|---|
| `Order.tags` | `crates/model/src/orders/mod.rs:604` | `Option<Vec<Ustr>>` — flat list of strings, NOT a dict. To carry product (CNC/MIS/NRML) we hold a side-band `HashMap<ClientOrderId, ZerodhaOrderMeta>` inside the exec client; we do NOT use `tags["product"]`. |
| `OrderStatusReport` | `crates/model/src/reports/order.rs` | This is the DTO. There is **no `ExternalOrderStatusReport` event**. The framework's reconciliation manager (`crates/execution/src/reconciliation/orders.rs:199`, used at `crates/live/src/manager.rs:2502`) converts unclaimed reports into external-order events via `generate_external_order_status_events()`. Adapter implements `generate_order_status_reports()` and lets the framework do the rest. |
| `Money` | `crates/model/src/types/money.rs:80+` | `Money { raw, currency }` — no `label` field. |
| `AccountState` balances | `crates/model/src/events/account/state.rs:107-130` | Keyed by `Currency`; two INR rows would collide. Equity + commodity must be summed into one INR balance; segment breakdown carried out-of-band. |
| `OrderStatus::Triggered` | `crates/model/src/enums.rs:1436`, `crates/model/src/orders/mod.rs:218-264` | Legal only for stop/conditional orders. Mapping a Kite `TRIGGER PENDING` to `Triggered` on a plain LIMIT panics on event apply. |
| `pyo3-stub-gen` pattern | `crates/adapters/thetadata/src/python/mod.rs` | Use `gen_stub_pyclass` macro per the ThetaData pattern, not freeform "annotations". |
| Workspace SQL dep | `Cargo.toml:265` | `sqlx = "0.8.6"` with the `sqlite` feature. `rusqlite` is NOT in the workspace. |
| Rate limiter | `crates/network/src/ratelimiter/mod.rs:144` | Use `nautilus-network::RateLimiter` with a `10/sec` quota; do NOT build a bespoke semaphore. |
| Connection-state holder | `crates/adapters/thetadata/src/live.rs` (and family) | `Arc<AtomicU8>` is the canonical pattern. `Arc<ArcSwap<AtomicU8>>` is pointless double-indirection. |
| `PyObject` ownership | `nautilus-expert/rules/pyo3-bindings.md` | Plain `PyObject`, clone via `clone_py_object()` under the GIL. `Arc<PyObject>` is banned. |
| Channel bounding | `nautilus-expert/rules/adapter-architecture.md` | Use bounded `mpsc` with drop-oldest + dropped-event metric; unbounded channels OOM under WS storm. |

---

## 1. Scope

### In scope (v1)

- Live market data + execution against production Kite Connect v3
- Exchanges: **NSE, BSE, NFO, BFO, CDS, MCX, NCO** plus their `_INDEX` variants
- Instrument types: **Equity, FuturesContract, OptionContract, IndexInstrument**
- Order types: **MARKET, LIMIT, SL, SL-M**
- Products: **CNC, NRML, MIS** (carried on `Order.tags`)
- TIF: **DAY, IOC**
- Historical bars via `/instruments/historical`

### Out of scope (v1)

| Item | Reason | Disposition |
|---|---|---|
| GTT orders | Separate API surface, niche | v1.1 |
| Bracket / Cover orders | Kite deprecating BO; CO thin demand | v1.2 if asked |
| Kite sandbox | No public sandbox exists | n/a |
| Smart-order delta logic | Strategy concern, not adapter | n/a |
| Postback webhook receiver | Polling sufficient; webhook needs HTTP server | v1.2 |

---

## 2. Reference inventory

| Source | Path | Use |
|---|---|---|
| openalgo Zerodha | `/Users/shergill/projects/openalgo/broker/zerodha/` | Auth, instrument CSV parsing, binary WS decoder, order REST shapes, error codes, rate limits |
| Nautilus ThetaData | `crates/adapters/thetadata/`, `nautilus_trader/adapters/thetadata/` | Fresh Rust+PyO3 layout, binary-WS+REST pattern |
| Nautilus IBKR | `crates/adapters/interactive_brokers/`, `nautilus_trader/adapters/interactive_brokers/` | Multi-exchange equities broker, polling reconciliation, gateway pattern |
| Adapter rules | `.claude/skills/nautilus-expert/rules/{adapter-architecture,tc-data-matrix,tc-exec-matrix}.md` | Architecture and conformance criteria |

---

## 3. Cross-cutting constraints

### 3.1 Authentication model (mirrors openalgo)

openalgo's pattern:

1. Long-lived secrets in env: `BROKER_API_KEY`, `BROKER_API_SECRET`.
2. Daily interactive login: user completes Kite browser flow → host receives `request_token` (single-use, ~5-min TTL) → `authenticate_broker(request_token)` POSTs `/session/token` with `sha256(api_key + request_token + api_secret)` → returns `access_token`. Reference: `openalgo/broker/zerodha/api/auth_api.py:8-63`.
3. `access_token` is then **passed per-call** as an `auth` parameter to every REST function (`order_api.py:35`, `35`, `163`, etc.) and to the WS adapter (`zerodha_websocket.py:69-73`).
4. **No refresh** — tokens expire ~06:00 IST next trading day; host re-logs in daily. `request_token` is single-use; failed exchange = user must re-login.

Nautilus v1 mapping:

- Env (long-lived): `ZERODHA_API_KEY`, `ZERODHA_API_SECRET`.
- Session token resolution order, checked at client startup:
  1. Explicit `access_token` field on `ZerodhaDataClientConfig` / `ZerodhaExecClientConfig` (set by caller after login).
  2. `ZERODHA_ACCESS_TOKEN` env var.
  3. Optional `access_token_provider: Option<PyObject>` callable that returns a fresh token (used on startup if 1+2 absent, and on token-death recovery).
  4. If all three fail → hard error with instructions to run the login helper.
- Ship `nautilus_trader.adapters.zerodha.auth.exchange_request_token(request_token) -> str` — 1:1 port of `authenticate_broker`.
- Ship `examples/live/zerodha/login.py` — a small CLI that opens the Kite login URL, prompts for `request_token`, prints the `access_token`, and optionally writes it to a chmod-600 `.env` file. **Write atomically**: write to `.env.new` then `os.rename` (POSIX-atomic) — SIGINT mid-write doesn't leave a half-truncated `.env` with no token.
- **Credentials never live in DTOs.** `credential.rs` resolves them. (`adapter-architecture` rule.)

#### 3.1.1 `ZerodhaSessionManager` — shared token across clients

`ZerodhaDataClient` and `ZerodhaExecClient` both need the same `access_token` and both can independently detect token death. We hoist the token into a process-singleton:

```rust
pub struct ZerodhaSessionManager {
    api_key: Arc<String>,
    access_token: ArcSwap<String>,               // hot-swapped on rotation
    provider: Mutex<Option<PyObject>>,           // callable for refresh — plain PyObject, no Arc (per pyo3-bindings rule)
    last_rotation_ns: AtomicU64,                 // throttles concurrent rotations
    rotation_notify: tokio::sync::Notify,        // wakes WS client to reconnect after a successful rotation
}
```

Both clients hold an `Arc<ZerodhaSessionManager>` (the outer `Arc` is fine; what's banned is `Arc<PyObject>`). They read the token on every request via `manager.access_token.load()`. On `TokenException`:

1. **CAS guard** — `last_rotation_ns` checked atomically; concurrent callers within a 5 s window await an in-flight rotation rather than re-invoking the provider.
2. **Provider invocation off-runtime** — the Python callable is invoked via `tokio::task::spawn_blocking` that acquires the GIL inside the blocking thread. This prevents an interactive `input()` or slow HTTP call inside the provider from parking the executor.
3. **Atomic token swap** — `access_token.store(Arc::new(new_token))`.
4. **WS handler barrier** — `rotation_notify.notify_waiters()`; the WS handler's `select!` wakes, drains pending sends, then reconnects with the new URL. Without the barrier, a write already serialized with the old URL races the swap and lands on a dead socket; without a wake signal, the handler sits idle until ping timeout (30 s).

If rotation fails (no provider, or provider raised), retry with exponential backoff (1 s → 60 s) up to N=3 attempts before emitting `ComponentStateChange(disconnected, reason="auth_dead")` and stopping reconnect loops. This avoids dying permanently on a transient provider failure (e.g. user's login site briefly down).

Without this manager, a token rotation by the data client leaves the exec client using a dead token until its next request fails — easy to lose orders during the gap.

### 3.2 No push for order events

Kite has no order-update stream. We build a **polling reconciler** in the exec client: `tokio::spawn` loop hitting `/orders` (and `/trades` when a row's `filled_quantity` changes) at a configurable cadence (default 1 s, adaptive 1→5 s under 429). Reconciler diffs against tracked state and emits `OrderAccepted / Rejected / Working / Filled / Canceled / Modified`.

### 3.3 IST timezone seams

All Nautilus instants are nanos-UTC. Kite quirks:

- Daily candles arrive as date strings; we anchor them to **session-open 09:15 IST = 03:45 UTC** (see Phase 4 for the rationale).
- Intraday candles arrive IST-stamped — parse via `chrono-tz Asia/Kolkata` and convert to UTC.
- Pin `chrono-tz` to a workspace version in `Cargo.toml` (don't blanket-trust the host `tzdata`); add a unit test asserting `Asia/Kolkata` resolves to `+05:30` for a known timestamp.

### 3.4 Kite Connect limits (consolidated)

> All values must be re-verified against `https://kite.trade/docs/connect/v3/` at implementation start. The openalgo citations are 2024-vintage; Kite has shifted these (notably the WS sub limit, which was 200 then raised to 3000) and may shift again.

| Limit | Value | Source | Enforcement |
|---|---|---|---|
| Order submit | **10 / sec / user** | Kite docs | `nautilus-network::RateLimiter` with `quota = 10/sec` (NOT a bespoke `Semaphore + interval` — that's burst-prone) |
| Quote batch REST | 500 symbols / request, **1 QPS** | `data.py:255-256` | Phase 3 startup-snapshot helper only; not on the WS hot path |
| WS subscriptions per connection | **3000** (verify current) | `zerodha_websocket.py:50` | Phase 3 sub batcher; error before exceeding |
| WS subscribe batch | **200 / batch**, **500 ms gap** | `zerodha_websocket.py:51-58` | Phase 3 sub batcher |
| WS connections per API key | **3** (verify current — may be 5) | Kite docs | Phase 1 logs connection count; warn on multi-instance |
| Historical chunk (resolution-dependent) | minute=60d, 3-15min=100d, 30-60min=200d, day=2000d | Kite docs | Phase 4 chunker; off-by-one causes silent gaps |
| `/orders` polling | not published | inferred | Adaptive 1→5 s under 429 (Phase 5) |

### 3.5 Architectural non-negotiables (from `adapter-architecture`)

1. `bon::Builder + Default` for every config.
2. Configs are DTOs — no I/O, no credential resolution.
3. All Python-callable async work through `get_runtime().spawn()`.
4. WS client two-layered (outer/inner). Connection state in `Arc<AtomicU8>`. (Use `ArcSwap` only for things that genuinely need atomic pointer swap — instrument cache, access token. Wrapping an atomic in `ArcSwap` is pointless double-indirection.)
5. Events emit through **bounded `mpsc::channel(N)`** with drop-oldest policy + a `dropped_event` metric. Unbounded channels OOM under WS storm (open auction, tick burst on expiry).
6. `anyhow::Result<T>` for fallible fns; `thiserror` for domain errors.
7. PyO3 boundary uses `py_` prefix in Rust, `#[pyo3(name = "...")]` for public Python name. `gen_stub_pyclass` macro from `pyo3-stub-gen` on every exposed class/method (mirror `crates/adapters/thetadata/src/python/mod.rs`).
8. No `Arc<PyObject>`. Plain `PyObject`; clone via `clone_py_object()` under the GIL.

---

## 4. Architecture

Layout mirrors ThetaData (canonical flat structure), not IBKR (which is deeply nested because it's an order of magnitude bigger):

```
crates/adapters/zerodha/
├── Cargo.toml                          (workspace inheritance, sqlx feature flag)
├── README.md
├── src/
│   ├── lib.rs                          (module re-exports + feature gating)
│   ├── common.rs                       (consts, enums, parse helpers)
│   ├── credential.rs                   (ZerodhaCredentials, zeroize)
│   ├── session.rs                      (ZerodhaSessionManager — §3.1.1)
│   ├── auth.rs                         (request_token → access_token helper)
│   ├── config.rs                       (DataClientConfig + ExecClientConfig via bon::Builder)
│   ├── error.rs                        (thiserror domain errors)
│   ├── http.rs                         (Kite v3 REST client; auth header reads session manager per request)
│   ├── decode.rs                       (binary WS packet decoder, length-switched, per-instrument precision)
│   ├── live.rs                         (outer WS client + bounded mpsc; resubscribe-on-reconnect)
│   ├── ws_handler.rs                   (inner stateless I/O loop + rotation_notify barrier)
│   ├── instruments.rs                  (master CSV fetch + Nautilus mapping + daily refresh task)
│   ├── symbology.rs                    (InstrumentId ↔ Kite trading symbol)
│   ├── data.rs                         (market data subscription dispatch)
│   ├── historical.rs                   (candle fetch + resolution-dependent chunking)
│   ├── execution.rs                    (submit/modify/cancel + reconciler, generate_order_status_reports)
│   ├── persistence.rs                  (sqlx async persistence of KiteOrderRecord map)
│   ├── factories.rs                    (Rust factory entry points)
│   └── python/                         (PyO3 wrappers, gen_stub_pyclass annotations)
├── examples/
│   ├── node_data_tester.rs
│   ├── node_exec_tester.rs
│   └── smoke_connect.rs
└── tests/
    ├── integration.rs
    └── fixtures/
        └── ws_session_<date>.bin       (recorded binary WS frames for decoder tests)

nautilus_trader/adapters/zerodha/
├── __init__.py
├── config.py                           (mirrors Rust config; msgspec Struct)
├── constants.py                        (ZERODHA = Venue("ZERODHA"), NSE = Venue("NSE"), ...)
├── factories.py                        (data_client_factory + exec_client_factory)
├── providers.py                        (InstrumentProvider wrapper)
├── data.py                             (LiveMarketDataClient wrapper)
├── execution.py                        (LiveExecutionClient wrapper)
├── auth.py                             (exchange_request_token helper)
└── README.md

tests/integration_tests/adapters/zerodha/
├── conftest.py
├── test_data_conformance.py
└── test_exec_conformance.py

examples/live/zerodha/
├── login.py                            (one-shot CLI: request_token → access_token)
├── trading_node_quotes.py
└── trading_node_orders.py

docs/integrations/zerodha.md
```

---

## 5. Phased delivery

Realistic single-engineer timeline: **~9 working days**. Phase 4 (historical) can parallelise with Phase 5 (execution) once Phases 1-3 are done → ~8 days compressed. Phases 1+2 do **not** parallelise: Phase 2 depends on the HTTP client + credential resolution from Phase 1.

### Phase 0 — Scaffolding (½ day)

**DOD**: `cargo check -p nautilus-zerodha --all-features` clean; `pytest tests/integration_tests/adapters/zerodha/ -k imports` green.

1. Generate skeleton matching the tree above (clone ThetaData structure, rename).
2. Add to workspace `Cargo.toml` (`version.workspace = true` — match workspace version) and to `build.py` (mirror current ThetaData entry — `build.py` is dirty in `git status` precisely for ThetaData; do the same here).
3. Empty `__init__.py`s, placeholder PyO3 module export.
4. CI: ensure the `extension-module` feature builds; ensure the python wheel includes the module.

### Phase 1 — Rust infrastructure (1.5 days) — auth, HTTP, WS connect

**DOD**: `cargo run -p nautilus-zerodha --example smoke_connect` prints WS frames for `RELIANCE` for ≥30 s without disconnects; `/user/profile` round-trips (and demonstrates the colon-joined auth header works).

1. **`credential.rs`**: `ZerodhaCredentials { api_key, api_secret, access_token }`, `from_env()` reads three vars, `from_config(...)` accepts explicit overrides, `zeroize::ZeroizeOnDrop`. No `_TESTNET` variants — Kite has no sandbox.
2. **`auth.rs`**: `pub fn exchange_request_token(api_key, api_secret, request_token) -> Result<String>` — port of `authenticate_broker` (`auth_api.py:8-63`). Pure function, no I/O on env.
3. **`session.rs`**: `ZerodhaSessionManager` per §3.1.1. `ArcSwap<String>` for token (no extra `Arc` wrapper on the swap), `rotate()` method with CAS guard, provider invocation via `spawn_blocking` (GIL acquired inside the blocking thread), `tokio::sync::Notify` as WS reconnect barrier.
4. **`common.rs`**: `REST_BASE = "https://api.kite.trade"`, `WS_BASE = "wss://ws.kite.trade"`, `KITE_VERSION = "3"`, rate-limit defaults.
5. **`http.rs`**: `reqwest::Client` with default header `X-Kite-Version: 3`. **`Authorization: token {api_key}:{access_token}` (colon-joined; the openalgo single-token form is wrong for direct API use — openalgo proxies through its own server)** — resolved from `ZerodhaSessionManager` on every request, not stamped at client build time. `get/post/put/delete` helpers; rate-limit via `nautilus-network::RateLimiter`. Detects `TokenException` from response body and triggers `manager.rotate()`.
6. **`error.rs`**: thiserror enum — `Network`, `KiteError { status, error_type, message }`, `TokenException`, `RateLimited`, `MarketClosed` (parsed from `error_type=NetworkException` with the documented message). Detect via response shape (`{"status":"error","error_type":"TokenException",...}`).
7. **`live.rs` + `ws_handler.rs`**: two-layer per `adapter-architecture`; connection state `Arc<AtomicU8>` (not `Arc<ArcSwap<AtomicU8>>` — that was a misread of the rule); URL `wss://ws.kite.trade?api_key={k}&access_token={t}` rebuilt on every (re)connect from the session manager (token-in-URL is documented Kite v3 behaviour — accept the proxy-log exposure; an HTTPS proxy with logging will see this and any access token); ping/pong 30 s; backoff 1→60 s ×1.5 (matches `zerodha_websocket.py:209`); detect token death on `403/401`/`tokenexception` → call `manager.rotate()`, then await `rotation_notify`, then reconnect; if rotation fails three times → emit fatal `auth_dead` event and stop.
8. **Outbound WS messages**: `{"a":"subscribe","v":[token,…]}`, `{"a":"mode","v":["full",[token,…]]}`, `{"a":"unsubscribe","v":[…]}`. Inbound mux: binary frames → decoder; text JSON → error/postback handler.
9. **`examples/smoke_connect.rs`**: fetches `/instruments`, filters for `tradingsymbol=RELIANCE, exchange=NSE`, uses the discovered `instrument_token` for the smoke subscribe (don't hardcode `738561` — NSE has stable equity tokens but the test that auto-discovers protects against the day this stops being true).

### Phase 2 — Instruments (1 day) — TC-D01..D04

**DOD**: `InstrumentProvider.load_all_async()` populates Nautilus cache with all NSE+NFO instruments; round-trip parse property test green on the full CSV; daily-refresh task spawns, logs cycle count, and a corrupt-CSV test confirms the old cache is retained.

1. **`KiteInstrument`** row deserializer via `csv` crate. Realistic size: **150-180k rows / 12-15 MB** (Kite's dump grows quarterly). Memory budget for `HashMap<u32, Instrument>` + `HashMap<InstrumentId, KiteToken>` reverse map + `ArcSwap` double-buffering during refresh = **~80-120 MB peak**, doubling briefly during the atomic swap. Budget for this in node-sizing docs.
2. **`instruments.rs`**: `fetch_instruments()` GETs `/instruments`; parse → `Vec<KiteInstrument>`; per-segment mapping:
   - `EQ` → `Equity` (NSE/BSE)
   - `FUT` → `FuturesContract` (underlying, expiry, multiplier = lot_size)
   - `CE`/`PE` → `OptionContract` (strike, expiry, kind = Call/Put)
   - segment `INDICES` (or composite-token suffix `NSEIX`/`GLOBAL`) → `IndexInstrument`
3. **Per-instrument `price_precision`**: derive from `tick_size` (e.g. `0.05` → 2, `0.0025` → 4, `0.10` → 1). Stored on the `Instrument` and consumed by the WS decoder (Phase 3). Critical: hard-coded `/100` will break CDS pairs.
4. **`tick_size = 0` handling**: log a warning and skip the row (data error in Kite CSV — happens rarely but happens).
5. **Symbology choice (resolves §7 open question)**: preserve Kite-native trading symbols; normalize whitespace only (`"NIFTY 50"` → `"NIFTY-50"`). Do **not** apply openalgo's `NIFTY 50→NIFTY` semantic rename — divergence from Kite docs is worse than divergence from openalgo. Document this in the README's symbology section.
6. **`common/parse.rs`**: minimal symbol normalization (whitespace + uppercase), expiry `YYYY-MM-DD`→nanos, IST→UTC.
7. **`symbology.rs`**: `InstrumentId` shapes:
   - Equity: `RELIANCE-EQ.NSE`
   - Future: `NIFTY25MAYFUT.NFO` (Kite-native, no rewrite)
   - Option: `NIFTY25MAY25000CE.NFO`
   - Index: `NIFTY-50.NSE_INDEX`
   Round-trip helpers (`to_native`, `from_native`).
8. **In-memory composite-token map**: cache `instrument_token::::exchange_token` keyed by `InstrumentId`. Phase 3 needs the integer `instrument_token` for WS subs; phase 5 needs the trading symbol for orders. Cache lives behind `Arc<RwLock<HashMap<InstrumentId, KiteToken>>>`.
9. **Daily refresh task**: `tokio::spawn` cron at **07:45 IST** (Kite publishes the new dump ~07:30; 06:00 fetches yesterday's file) re-fetches `/instruments`, validates (≥10 k rows, has a `RELIANCE-EQ` row, no instruments with `expiry < today`), and only then atomically swaps the cache (`ArcSwap<HashMap<...>>`). On validation failure, keep the old cache and emit a `refresh_failed` metric — never replace a known-good cache with a corrupt one.
10. **NSE corporate-action handling**: bonus/split/dividend events on T+1 ex-date silently change the `instrument_token` for the affected symbol. The 07:45 refresh picks up the new token, but live positions / open orders keyed by the old token become orphaned. Phase 7 startup reconciliation handles this by reconciling on `tradingsymbol+exchange`, not `instrument_token`, when the latter is no longer in the cache.
11. **`providers.py`** + PyO3 wrapper: standard `load_all_async / load_ids_async / load_async`.
12. **Scope clarification**: drop `NCO` (commodity options on NSE — never tested in openalgo, low volume); leave the segment enum in place but route to a "log + ignore" branch in the CSV parser.

### Phase 3 — Live market data (2 days) — TC-D05..D45

**DOD**: `DataTester` groups 1–5 pass on NSE equity, NFO option, NSE index; decoder microbench under budget; recorded-fixture unit tests green.

1. **`decode.rs`**: binary packet decoder.
   - Outer: `u16 BE count`, then `count × (u16 BE length, bytes)`.
   - Per-packet variants by length (re-derive from current Kite docs at implementation; openalgo's table is 2024-vintage):
     - **8 B** — LTP only (indices and non-indices)
     - **28 B** — indices full (no depth) OR LTP+ts variants
     - **32 B** — indices quote variant
     - **44 B** — non-indices quote (ltp, last_qty, avg, vol, buy_qty, sell_qty, ohlc)
     - **184 B** — non-indices **full** (quote fields + last_trade_ts + oi + oi_day_h/l + ts + 5-level depth × (qty:u32, price:u32, orders:u16, pad:u16) = 5 × 12 B per side × 2 sides)
     - The earlier draft's "492 B" was fictional — there is no 492 B mode in current Kite docs.
   - Price scaling uses **per-instrument `price_precision`** from the instrument cache, NOT a hardcoded constant. (CDS/BCD = 4 dp, NSE equity = 2 dp, MCX bullion = 1 dp.) Decoder receives an `Arc<ArcSwap<HashMap<u32, Instrument>>>` keyed by `instrument_token`.
   - Unknown-size frames → `metrics::counter!("zerodha_ws_unknown_packet", "size" => n)` + drop. Loud signal catches Kite layout changes. (Confirm the workspace uses `metrics` crate vs `prometheus`/`opentelemetry`; ThetaData currently uses `tracing` + `metrics`.)
   - **Allocation-free hot path**: decode into a `tick_buffer: SmallVec<[Tick; 64]>` per frame; recycle. No `Vec::new()` per packet.
2. **Performance budget** (gated in CI via `criterion`):
   - Single-packet decode: **< 5 µs**
   - 100-packet frame decode + emit: **< 500 µs**
   - Sustained throughput: **≥ 50 k events/sec** on M-series Mac
   - Regressions > 10 % fail the benchmark.
3. **Unit-test fixtures**: capture a 5-minute prod WS session as `tests/fixtures/ws_session_<date>.bin`; decoder tests replay it and assert tick counts, price ranges, and depth-level invariants. The 184 B layout has shifted in Kite history — fixtures catch silent regressions.
4. **`data.rs`**: subscription dispatch.
   - `subscribe_quote_ticks` → mode `quote`.
   - `subscribe_trade_ticks` → mode `full`; **synthesize** `TradeTick` from `last_trade_qty + last_price + last_trade_ts` deltas (Kite has no separate trade stream); skip emission when `last_trade_ts` is unchanged. **Known limitation**: if two trades hit the same instrument inside one WS frame, Kite reports only the final `last_trade_qty`; we synthesize one fake TradeTick with the last quantity rather than two. Document in README. For accurate per-trade data, use `/trades` REST poll on filled orders only — not a general per-tick stream.
   - `subscribe_order_book` → mode `full`; emit **`OrderBookSnapshots`** (not `OrderBookDeltas with is_snapshot=true` — that overwhelms subscribers expecting actual deltas; snapshot is the correct primitive for snapshot venues).
   - Sub batcher: 200/batch, 500 ms gap; track sub set in `Arc<RwLock<HashSet<u32>>>` for resubscribe-on-reconnect.
   - Connection-count guard: if `subs.len() > 3000` → `Err(SubscriptionLimitExceeded)`; don't silently truncate.
5. **Index handling**: indices ride the same WS but only LTP mode is meaningful; route NSE_INDEX/BSE_INDEX subs through the same connection but tag emitted ticks with the index venue (matches `zerodha_adapter.py:298`).
6. **`data.py`**: `ZerodhaLiveMarketDataClient` PyO3 wrapper — mirror ThetaData's structure.

### Phase 4 — Historical bars (½ day) — TC-D50..D55

**DOD**: `request_bars(NIFTY25MAYFUT, 5-MINUTE, last 5 days)` returns sorted `BarList`; bar timestamp convention asserted in tests; daily and intraday boundary tests pass.

1. **`historical.rs`**: `fetch_candles(token, resolution, from, to, oi)`.
   - Resolution map: `1-MINUTE`→`minute`, `3-MINUTE`→`3minute`, …, `1-HOUR`→`60minute`, `1-DAY`→`day`.
   - **Resolution-dependent chunking** (Kite limits, not uniform): `minute` = 60 days/request, `3-15minute` = 100 days, `30-60minute` = 200 days, `day` = 2000 days. Stitch responses, dedupe on timestamp. Wrong chunk size produces silent gaps on backfill.
   - Daily bars: Kite returns date strings (e.g. `"2025-05-15"`). Anchor to **IST session open** (09:15 IST = 03:45 UTC) and store as UTC nanos. The "+5:30 conversion" in §3.3 refers to intraday minute bars; daily bars need anchor selection, not arithmetic conversion.
   - Intraday bars: arrive IST-stamped (e.g. `"2025-05-15 09:15:00"` interpreted as IST). Parse with `chrono-tz Asia/Kolkata` then convert to UTC.
   - OI: on by default for derivatives, off for equity.
2. **Bar timestamp convention**: Kite returns each candle stamped at **bar-open**. We use **`ts_event` = bar-open** (matches Kite directly, matches IBKR Indian-equities adapter, matches strategy intuition for IST sessions). Add unit tests asserting (a) a known 09:15-09:20 5-minute intraday bar has `ts_event` corresponding to 09:15 IST exactly, and (b) a daily bar for `2025-05-15` has `ts_event` corresponding to 2025-05-15 09:15 IST (= 03:45 UTC).
3. Emit `Bar` with `bar_type` matching the request.

### Phase 5 — Order submit + reconciler (1 day) — TC-E01..E15

**DOD**: place a MARKET buy of 1 RELIANCE (variety=`regular`), see `OrderAccepted` then `OrderFilled` via the framework's reconciliation manager (driven by our `generate_order_status_reports()`); reject reason surfaces; account currency = INR; submit rate-limit caps at 10 QPS via `nautilus-network::RateLimiter`.

1. **Enum mappers** (`common.rs`):
   - `OrderType`: MARKET/LIMIT/SL/SL-M ↔ Nautilus. **Mapping rule for `Triggered`**: Kite's `TRIGGER PENDING` status maps to Nautilus `OrderStatus::Triggered` **only when the local order is a Stop/Conditional variant** (`StopMarketOrder`, `StopLimitOrder`, `MarketIfTouchedOrder`, etc.). For plain LIMIT or MARKET orders, `TRIGGER PENDING` is illegal in the Nautilus state machine and would panic on event apply — map to `Accepted` instead.
   - `Validity`: DAY/IOC ↔ TimeInForce.
   - `Variety`: `regular | amo | iceberg | bo | co | auction` — **first-class field** on the submit DTO and persisted per-order. v1 supports `regular` and `amo`; others rejected.
   - `Product`: CNC/MIS/NRML — **`Order.tags` is `Option<Vec<Ustr>>`, not a dict** (verified: `crates/model/src/orders/mod.rs:604`). Carry product in a side-band `Arc<RwLock<HashMap<ClientOrderId, ZerodhaOrderMeta>>>` inside the exec client (we already maintain that map for `kite_order_id`+`variety`). Optionally also push a prefix-encoded `"product:NRML"` into `Order.tags` for read-only visibility. Default product configurable on `ZerodhaExecClientConfig.default_product`.
   - **Kite status mapping table** (full table; revised):

     | Kite status | Nautilus `OrderStatus` |
     |---|---|
     | `PUTORDER REQ RECEIVED`, `VALIDATION PENDING`, `OPEN PENDING`, `AMO REQ RECEIVED` | `Submitted` |
     | `MODIFY VALIDATION PENDING`, `MODIFY PENDING`, `MODIFY REQ RECEIVED` | `PendingUpdate` |
     | `CANCEL PENDING` | `PendingCancel` |
     | `TRIGGER PENDING` | `Triggered` **if local order is stop/conditional**, else `Accepted` |
     | `OPEN` | `Accepted` |
     | `COMPLETE` | `Filled` |
     | `CANCELLED` | `Canceled` |
     | `REJECTED` | `Rejected` |

     Any status not in the table → log `unknown_kite_status` metric and map to `Accepted` (defensive — catches future Kite additions without panicking).

2. **`execution.rs`** — `submit_order`:
   - Form-encoded POST to `/orders/{variety}` (variety from the DTO).
   - Required keys per `order_api.py:202-204`: `tradingsymbol, exchange, transaction_type, order_type, quantity, product, price?, trigger_price?, disclosed_quantity?, validity, market_protection?, tag?`.
   - **Submit rate-limit**: `nautilus-network::RateLimiter` with `quota = 10/sec` (verified: `crates/network/src/ratelimiter/mod.rs:144`). Replaces the earlier "bespoke `Semaphore + interval(100ms)`" design — that's burst-prone and not actually 10 QPS.
   - **Order tag length**: Kite caps `tag` at **20 chars**. We tag own orders with a 12-char hash of `ClientOrderId` prefixed by `NTLZ` (4 chars) — total 16, fits with margin. Full `ClientOrderId` lives in the side-band map keyed by `kite_order_id`.
   - **Order tracking map**: `Arc<RwLock<HashMap<ClientOrderId, ZerodhaOrderMeta>>>` where `ZerodhaOrderMeta = { kite_order_id, variety, product, tag_hash, last_status, last_filled_qty, fill_sequence: u32, seen_trade_ids: HashSet<String> }`. The `fill_sequence` monotonic counter fixes the synthetic-`TradeId` collision on reverse-position fills (100 → 200 → 100 reuses `filled_quantity=100` otherwise).
   - **Persistence**: `persistence.rs` writes to `<cache_dir>/zerodha_orders.sqlite` via **`sqlx` with the sqlite feature** (verified: `Cargo.toml:265`; rusqlite is NOT in the workspace). All writes are async via `sqlx::Pool`; reconciler awaits the future rather than calling `spawn_blocking`. Bounded writes per second via a coalescer (debounce 50 ms) so a 1k-order burst doesn't drown the sqlite executor.
   - **Source-of-truth precedence**: when sqlite and the in-memory map disagree on (variety, product), the in-memory map wins for the lifetime of the process; sqlite is overwritten on next state transition. Sqlite is authoritative only on cold-start.

3. **Reconciliation contract** (`generate_order_status_reports`):
   - We implement `generate_order_status_reports()` returning a `Vec<OrderStatusReport>` (verified: `crates/model/src/reports/order.rs`); the framework's reconciliation manager (`crates/live/src/manager.rs:2502`) calls this on a schedule and converts unclaimed reports into external-order events via `generate_external_order_status_events()`. We do **not** emit our own `ExternalOrderStatusReport` (that event does not exist).
   - **`tokio::spawn` reconciler loop** drives GET `/orders` at configurable `poll_interval_ms` (default 1000) and **fills the report stream**. Crash safety: wrap in `loop { match reconcile_once() { Err(e) => log+backoff } }` so a panic in one iteration doesn't abort the task. Plus a supervisor that restarts the task with exponential backoff if the `JoinHandle` ever resolves.
   - Per tick: GET `/orders` → diff against tracked state by `(kite_order_id, status, filled_quantity, average_price)` → emit reports for all changed rows.
   - **Fill emission policy (handles `/trades` lag)**:
     1. On `filled_quantity` delta Δ → emit a fill in the report stream using order-level `average_price` and Δ quantity, with synthetic `TradeId = "ZER-{kite_order_id}-{fill_sequence}"` (where `fill_sequence` increments per emission — fixes the reverse-position collision).
     2. Asynchronously fetch `/trades?order_id={id}` for real `trade_id`s; store in `seen_trade_ids` for audit (no re-emit).
   - Adaptive: on 429 back off (`poll_interval *= 2`, max 5 s); recover after success.
   - **External-order detection**: rows with an unrecognised `kite_order_id` AND `tag` not starting with `NTLZ` flow through `generate_order_status_reports()` unchanged; the framework's reconciliation manager handles them. Critical for the 15:20 IST MIS auto-square-off and Kite-mobile orders while the node is running.

4. **`execution.py`**: `ZerodhaLiveExecutionClient` PyO3 wrapper.

### Phase 6 — Modify, cancel, advanced exec (1 day) — TC-E16..E55

1. **Modify** → PUT `/orders/{variety}/{id}` where `variety` comes from the persisted `ZerodhaOrderMeta`. Kite sometimes returns a new `order_id`; track both in `history`; update the record's `kite_order_id`.
2. **Cancel** → DELETE `/orders/{variety}/{id}` (variety from the record).
3. **Stop / SL-M** → `trigger_price` field; map Nautilus `StopMarketOrder`/`StopLimitOrder` to SL-M/SL.
4. **Partial fills** — reconciler emits fills per quantity-delta from `/orders` (see Phase 5 fill policy with `fill_sequence`).
5. **Positions** — Kite gives a **net-position snapshot**, not per-trade deltas. Each fill triggers a position-snapshot fetch; we diff against the cached snapshot and emit `PositionChanged` accordingly. If the user closes a position via Kite mobile, the next snapshot reveals the change and we emit the corresponding `PositionClosed` — ties to the external-order path in Phase 5.
6. **Holdings** (`/portfolio/holdings`) → separate getter (delivery holdings; emit as cached snapshot).
7. **Funds** (`/user/margins`) → `AccountState` snapshots. **Single INR balance** (verified: `AccountState` keys balances by `Currency` at `crates/model/src/events/account/state.rs:107-130`, and `Money` has no `label` field — two INR rows would collide). Sum `equity.net + commodity.net` into one `AccountBalance { currency: INR, total, locked, free }`. Carry the per-segment breakdown out-of-band via a custom `ZerodhaAccountSnapshot` event (published on the same bus, subscribers can listen for segment-level detail). Reconcile both whenever `/user/margins` is fetched.

### Phase 7 — Lifecycle, reconcile, errors (1 day) — TC-D90, TC-E91..E101

1. **Startup reconciliation**:
   - Load persisted `ZerodhaOrderMeta` map from `<cache_dir>/zerodha_orders.sqlite` (via `sqlx`).
   - Fetch `/orders` snapshot.
   - For each persisted record:
     - Matched + terminal in snapshot → emit final `OrderStatusReport` then prune.
     - Matched + live → reattach (re-arm the reconciler).
     - **Not in snapshot** → Kite drops orders from `/orders` after EOD. Either filter by `last_seen_date` (if today, treat as missing → emit `OrderCanceled` as terminal-unknown; if yesterday, prune silently) or query `/orders/{order_id}/history` to confirm. Default: prune silently for records older than 24 h, emit `OrderCanceled` for same-day misses.
   - For each snapshot row not in our map: hand to `generate_order_status_reports()` and let the framework's reconciliation manager handle as external order.
   - Snapshot `/positions` and emit `PositionChanged` for any drift.
   - **NSE corporate-action match**: if an `instrument_token` from sqlite is no longer in today's instrument cache, look up the symbol by `tradingsymbol+exchange` and rebind to the new token before issuing the report.
2. **Token death**: HTTP 403 / `TokenException` from either client → call `ZerodhaSessionManager::rotate()`.
   - Success → `ArcSwap` swap propagates; `rotation_notify` wakes the WS handler which drains pending sends and reconnects.
   - Transient provider failure → bounded retry (1 s → 60 s × 1.5, max 3 attempts) before declaring `auth_dead`. Avoids killing the node on a 30-second login-site blip.
   - Permanent failure → emit `ComponentStateChange(disconnected, reason="auth_dead")` on both clients; stop reconnecting; log clear instruction (`Run examples/live/zerodha/login.py to refresh ZERODHA_ACCESS_TOKEN, then restart the node`).
3. **WS reconnect**: covered in Phase 3; here we verify the session-manager bridge end-to-end: kill the token mid-session and confirm both clients recover within one rotation cycle, with no orders lost and no duplicate fills emitted across the gap.
4. **Rate-limit handling**: 429 → exponential backoff; surface as `RateLimited` event after N retries.
5. **Market-closed rejection clarity**: Kite returns `error_type=NetworkException` with messages like `"Trading is not allowed at this time"` outside market hours. Map these to a clear `RejectedOrder` cause (`MarketClosed`) rather than the generic network bucket — the difference matters for strategy retry logic.
6. **NSE/BSE trading-holiday awareness**: fetch the NSE/BSE holiday calendar once daily (Kite doesn't expose it via API — read from a checked-in JSON updated annually, or scrape NSE's public calendar). When the next market day is a holiday, the reconciler pauses `/orders` polling between 16:00 IST (post-close) and 08:00 IST next *trading* day. Stops 429-storms on Diwali / Holi / Republic Day.

### Phase 8 — Polish (1 day)

- Examples: copy ThetaData's `node_data_tester.rs`, `node_exec_tester.rs`; adapt.
- Python examples: `examples/live/zerodha/{login.py, trading_node_quotes.py, trading_node_orders.py}` mirroring `examples/live/thetadata/*`.
- `login.py` enhancements: open Kite login URL via `webbrowser.open`; prompt for `request_token` from the redirect URL; write to a chmod-600 `.env` file by default.
- README + `docs/integrations/zerodha.md` (auth setup, env vars, daily login workflow, no-sandbox caveat, the `Order.tags["product"]` convention, the bar-open timestamp convention).
- `pyo3-stub-gen` annotations on every exposed class.
- Run full TC-D + TC-E matrix (dry-run mode in CI; manual prod smoke per §8 below).
- **v1.x enhancement notes** (in README's "Future work" section): TOTP-based automated daily login via `pyotp`; postback webhook receiver; GTT support; brokerage/STT computation.

---

## 6. Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| Access-token expires mid-session and no rotation hook supplied | High | `ZerodhaSessionManager.rotate()` + bounded retry + provider callable; loud `auth_dead` after retries |
| Kite WS packet sizes shift | Med | Length-switch decoder + `unknown_packet` metric + fixture-based decoder tests so a layout shift fails CI |
| `/orders` poll at 1 s trips rate limit at scale | Med | Adaptive cadence (1→5 s under 429), configurable |
| Symbol normalization edge cases (BFO weeklies, MCX commodities) | Med | Property test the round-trip on the full master CSV in Phase 2 |
| IST/UTC daily-bar off-by-one | Low | Pin `chrono-tz Asia/Kolkata` in `Cargo.toml`; unit test boundary |
| Product (CNC/MIS/NRML) has no Nautilus-model field; `Order.tags` is `Vec<Ustr>` not a dict | Cert | Side-band `HashMap<ClientOrderId, ZerodhaOrderMeta>` inside exec client; optional `"product:NRML"` tag for read-only visibility |
| Order map lost on node restart → orphaned live orders | High | Sqlite persistence via `sqlx`; reload in Phase 7 startup reconciliation |
| 15:20 IST MIS auto-square emits orders we didn't submit | Cert | Reports stream through `generate_order_status_reports()`; framework's `generate_external_order_status_events()` handles them |
| Mismatched price precision (e.g. CDS 4 dp into 2 dp `Price`) crashes decoder | Med | Per-instrument `price_precision` derived from `tick_size` in Phase 2; decoder reads from instrument cache |
| `/trades` lag causes stuck-in-Working orders that are actually filled | Cert | Phase 5 fill policy: emit on `/orders` quantity delta with synthetic `TradeId` keyed by `fill_sequence`; backfill real `trade_id` async |
| Submit rate-limit (10 QPS) hit during burst | Med | `nautilus-network::RateLimiter` (proper sliding-window quota); over-budget → `RateLimited` event |
| Long-running node misses new listings / contract rollovers | Med | Daily 07:45 IST cache refresh task (after Kite publishes ~07:30) |
| `request_token` single-use; failed exchange leaves user without access | Low | Documented in §3.1; `login.py` exits with a clear retry instruction |
| Mapping `TRIGGER PENDING` to `OrderStatus::Triggered` on non-stop order panics | Cert if naive | Phase 5 mapping checks local order is Stop/Conditional variant; otherwise maps to `Accepted` |
| Two INR balances (equity + commodity) collide in `AccountState` | Cert | Sum into single INR balance; segment breakdown via custom `ZerodhaAccountSnapshot` event |
| Sync sqlite writes inside reconciler park the tokio runtime | Cert with rusqlite | Use `sqlx` async + 50 ms write coalescer; never `spawn_blocking` inside the hot loop |
| Token rotation races in-flight WS sends | Med | `tokio::sync::Notify` barrier in `ZerodhaSessionManager`; WS handler drains pending sends before reconnect |
| `Arc<PyObject>` in session manager → segfault on GIL-less drop | Cert if used | Plain `PyObject`, `clone_py_object()` under GIL, per pyo3-bindings rule |
| Reconciler task panic swallowed by `tokio::spawn` | Med | Supervised loop: panic → log → backoff → restart; expose restart count metric |
| Synthetic `TradeId` collides on reverse-position fills | Cert | `fill_sequence: u32` monotonic counter in `ZerodhaOrderMeta`, not raw `filled_quantity` |
| Kite-tag length (20 chars) overflows `NAUTILUS-{UUID}` and Kite rejects submit | Cert | Tag = `NTLZ` + 12-char hash of `ClientOrderId`; full id in side-band map |
| Reconciler hammers closed exchange on trading holidays (Holi/Diwali) | Med | NSE/BSE holiday calendar (checked-in JSON or scraped); pause `/orders` polling between 16:00 IST close and 08:00 IST next trading day |
| Daily refresh fetches corrupt or empty CSV and clobbers cache | Low | Validation gate (≥10 k rows, has RELIANCE-EQ, no past-expiry rows) before atomic swap; keep old on failure |
| Daily refresh at 06:00 IST hits Kite *before* new dump publishes (~07:30) | Cert | Cron moved to 07:45 IST |
| NSE corporate action (bonus/split/dividend) silently re-tokens an instrument; positions/orders orphan | Cert on ex-date | Phase 7 reconciliation rebinds by `tradingsymbol+exchange` when old `instrument_token` missing from today's cache |
| `OrderBookDeltas` snapshot-every-frame floods subscribers | Med | Emit `OrderBookSnapshots` (correct primitive); document in README |
| `TradeTick` synthesis loses interleaved trades within one WS frame | Low | Documented known limitation; for accurate per-trade data, use `/trades` REST on filled orders |
| Daily-bar timestamp anchoring contradicts intraday (off-by-one for the 09:15 boundary) | Low | Explicit anchor + boundary tests in Phase 4 |
| Instrument CSV in-memory size larger than budgeted | Low | Documented memory budget (~80-120 MB peak with double-buffer); node-sizing docs reflect |
| Token-in-URL exposed via HTTPS proxy logs / tcpdump | Low | Documented; recommend users avoid running through inspectable proxies; alternative is the IBKR-style auth-header mode but Kite WS doesn't support it |
| Unbounded mpsc channels OOM under WS storm | Med | Bounded channels (Phase 3 §3) with drop-oldest + metric |
| Reconciler missing case "in sqlite, gone from Kite EOD snapshot" | Cert | Phase 7 startup adds prune-or-emit-Canceled branch keyed on `last_seen_date` |

---

## 7. Resolved decisions + remaining open questions

**Resolved during review passes** (folded into the plan above):
- Symbology: Kite-native trading symbols, whitespace-normalized only. (Phase 2 §5)
- Multi-segment account: single INR balance summing equity+commodity; segment breakdown via custom `ZerodhaAccountSnapshot` event. (Phase 6 §7) **Revised** from earlier "labeled Money" idea after verifying `Money` has no `label` field.
- Phase parallelisation: only Phase 4+5 can overlap; Phases 1+2 cannot. (§5 timeline)
- Token sharing across clients: `ZerodhaSessionManager` singleton with `tokio::sync::Notify` reconnect barrier and `spawn_blocking` provider invocation. (§3.1.1)
- Fill emission policy: emit on `/orders` quantity delta with synthetic `TradeId = "ZER-{kite_order_id}-{fill_sequence}"`, backfill real `trade_id` async. (Phase 5 §3)
- External orders: stream through `generate_order_status_reports()`; the framework's reconciliation manager handles via `generate_external_order_status_events()`. **Revised** from earlier non-existent `ExternalOrderStatusReport` event.
- Product (CNC/MIS/NRML): side-band `HashMap<ClientOrderId, ZerodhaOrderMeta>`. **Revised** from `Order.tags["product"]` after verifying `Order.tags` is `Option<Vec<Ustr>>`.
- Kite REST auth header: `Authorization: token {api_key}:{access_token}` (colon-joined). **Revised** from openalgo's single-token form (openalgo uses an auth proxy).
- Workspace SQL: `sqlx` async with sqlite feature; no `rusqlite`. (§4 architecture)
- WS state holder: `Arc<AtomicU8>`, not `Arc<ArcSwap<AtomicU8>>`. (§3.5 #4)
- WS packet sizes: re-derived from current Kite docs (no 492 B mode; 184 B is full+depth). (Phase 3 §1)

**Still open** (decide during build, won't block):
1. Postback URL receiver as v1.x — would replace `/orders` polling. Default: defer to v1.2.
2. `disclosed_quantity` and `market_protection` — Nautilus `Order` extras vs `tags`. Provisional: tags.
3. Brokerage / STT / GST computation — Kite doesn't push these; strategy or adapter responsibility? Provisional: skip for v1; document that PnL excludes charges.
4. Multi-account-per-API-key (rare, special Kite agreement) — out of scope; document.
5. `iceberg`, `bo`, `co`, `auction` order varieties — wired in the enum, but submit/modify/cancel intentionally error for v1. Phase 8 readme makes this explicit.

---

## 8. Acceptance gate

### 8.1 CI gates (mandatory, all green before merge)

- TC-D matrix: D01..D45 + D50..D55 + D90 green (replayed against recorded WS fixtures + mocked REST responses; **no live network in CI**).
- TC-E matrix: E01..E55 + E91..E101 green **in dry-run mode** — HTTP layer intercepted by an in-process mock server that returns canned Kite responses; reconciler exercised against scripted state transitions.
- Decoder microbench (Phase 3 §2): within budget on the CI runner; gate on **percentage regression** (>10 % vs baseline fails), not absolute number — Linux x86 CI and M-series dev numbers diverge.
- **Failure-mode tests** (in addition to TC matrix):
  - Token rotation while WS sending: kill token mid-frame, verify session manager rotates + WS reconnects + no orders lost.
  - WS packet layout shift: replay a fixture with synthetic 200 B unknown frame, verify metric increments and decoder doesn't crash.
  - Sqlite corruption: truncate `zerodha_orders.sqlite` mid-write, restart, verify graceful recovery (empty cache, normal startup reconciliation) rather than panic.
  - External order: inject an unknown `kite_order_id` into mock `/orders` response, verify framework emits external-order events.
  - MIS auto-square simulation: at virtual 15:20 IST, mock injects close orders for our positions; verify reconciler closes the matching positions correctly.
  - Token rotation transient failure: provider raises once, succeeds on retry — verify bounded retry kicks in and node doesn't `auth_dead`.
- `cargo clippy -p nautilus-zerodha --all-features -- -D warnings` clean.
- `mypy nautilus_trader/adapters/zerodha` clean.

### 8.2 Manual prod-smoke checklist (run by maintainer before tagging v1.0)

Kite has no sandbox, so live execution checks are manual and must be done with low-vol instruments during normal hours.

1. Run `examples/live/zerodha/login.py` → fresh `access_token`.
2. Subscribe to RELIANCE-EQ.NSE quotes for 60 s; assert ≥1 tick/sec arrives.
3. Subscribe to NIFTY-50.NSE_INDEX LTP for 60 s; assert ticks arrive.
4. Fetch 5-day 5-minute bars for RELIANCE-EQ.NSE; eyeball OHLC matches Kite Web.
5. Place 1-share MARKET BUY on a liquid stock; verify `OrderAccepted` → `OrderFilled` within 5 s; verify position appears.
6. Place 1-share LIMIT BUY at 0.5× market price (won't fill); verify `OrderAccepted` + `Working`; cancel; verify `Canceled`.
7. Kill the node mid-session; restart; verify the resting limit order from step 6 is rediscovered via startup reconciliation (skip this and step 6 ordering if step 6 cleaned up).
8. Manually expire the `access_token` (use a clearly stale value); verify `auth_dead` event surfaces with the login-script instruction.

### 8.3 Documentation

- Docs page in `docs/integrations/zerodha.md` published covering:
  - Auth setup, env vars, daily login workflow, no-sandbox caveat.
  - `ZerodhaOrderMeta` side-band map for product (CNC/MIS/NRML).
  - Bar-open timestamp convention.
  - Single-account-per-key limitation.
  - `ZerodhaAccountSnapshot` event for segment-level balance breakdown.
  - Known limitations: TradeTick aggregation loss within one WS frame; `OrderBookSnapshots` not deltas; PnL excludes Indian charges (STT/GST/exchange fees) — strategy must compute these.
  - v1.x roadmap.

---

## 9. India-market caveats (must read before strategy goes live)

1. **STT / GST / exchange fees materially distort F&O PnL.** Kite doesn't push charges via API; the adapter does not compute them. A strategy that takes `OrderFilled.amount - prior_amount` as PnL is wrong by 0.025-0.10 % per fill on F&O. Document loudly; provide a `compute_indian_charges()` helper as v1.x.
2. **T+1 settlement** (equity holdings): a delivery sell on day T cannot be re-bought on day T (intraday). The adapter doesn't enforce this — strategy responsibility. Document.
3. **NSE corporate actions** (bonus issue, split, dividend) change `instrument_token` on the T+1 ex-date. The daily 07:45 IST refresh picks up new tokens; Phase 7 reconciliation rebinds positions/orders by `tradingsymbol+exchange`. Strategies holding positions across an ex-date should still verify counts (a 1:1 bonus doubles the position quantity).
4. **NSE/BSE trading holidays** (Holi, Diwali, Independence Day, etc.) — adapter consults a checked-in holiday calendar and pauses `/orders` polling. Strategies should not place orders for holidays.
5. **MIS auto-square at 15:20 IST** — Kite's risk engine closes open MIS positions automatically. Adapter detects via external-order path (Phase 5 §3); strategy sees `PositionClosed` for any MIS positions it didn't close itself.
6. **Pre-open auction segment** (AUC variety) — enum-listed but not implemented in v1; submitting an auction order errors.
7. **Circuit limits / freeze quantity** — Kite returns specific reject reasons (`InsufficientFunds`, `OrderQuantityExceedsFreeze`, etc.). Map to clear Nautilus reject causes; document in README's "Common reject reasons" section.
8. **SEBI position limits** — index option position limits enforced by exchanges; adapter does not track. Strategy must.
9. **GTT regulatory limit**: 50 active GTTs per account at any time (Kite limit, SEBI-derived). v1.1 implementer note when GTT lands.
