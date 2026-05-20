# nautilus-zerodha

Zerodha (Kite Connect v3) integration adapter for [NautilusTrader](https://nautilustrader.io).

## Status

**Phases 0–8 implemented and committed.** All scaffolding, infrastructure, data,
historical, execution, persistence, holiday calendar, Python wrapper layer, and
mock-server tests are in place. See `specs/zerodha-adapter.md` for the original
phased delivery plan.

User-facing documentation lives at `docs/integrations/zerodha.md`. This README is
the implementation-side companion.

## What's verified

| Surface | Verification |
|---|---|
| Auth (`exchange_request_token`) | Live Kite, daily browser-redirect flow via `examples/live/zerodha/login.py` |
| HTTP `/instruments` | Live: 105,399 rows parsed, 37,807 dropped (NCO + zero-tick), 0 errors |
| HTTP `/orders/historical` | Live: 225 candles for `RELIANCE-EQ.NSE` |
| HTTP `/portfolio/positions` | Live: empty list (no positions on unfunded account) |
| HTTP `/user/margins` | Live: 0.00 INR balance returned cleanly |
| HTTP `/orders` POST | Live error path (`InputException` on price < circuit limit); **happy path mock-tested only** |
| HTTP `/orders/{variety}/{id}` PUT, DELETE | Mock-server tested (TC-MS04/05) |
| WS handshake + subscribe | Live: Kite ticker WS connected, subscribed RELIANCE-EQ.NSE |
| WS binary decoder | 60s real capture replayed in unit tests; 75 ns/packet, 5.9 µs/100-packet frame |
| Subscription cap (3000) | Unit-tested (`subscribe_above_cap_errors`) |
| Token rotation | Unit-tested (`rotate_installs_new_token_and_notifies`) |
| Persistence (JSON OrderStore) | Unit-tested round-trip |
| 24h cancel synthesis (reconcile) | Unit-tested |
| Python TradingNode end-to-end | Live: factory builds → connect → subscribe → clean shutdown |

**Tests:** 134 unit + 10 mock-server (TC-MS01..MS10), all passing. Clippy clean.

## Known gaps

- **Live happy-path order placement has NOT been verified** — the test account
  is unfunded. The submit/modify code paths are mock-tested only; the actual
  Kite acceptance of a real order has not been observed.
- **No reconnect-under-load test.** WS reconnect + resubscribe replay is coded
  but only briefly smoke-tested.
- **TC-D / TC-E conformance matrices** are not yet run against this adapter.
- **Only `Regular` variety orders** are wired. AMO, CO (cover order), ICEBERG,
  BO (bracket order) variants are not implemented.
- **Burst-load benchmarks not run** — the PyO3 polling forwarder (three
  `next_quote` / `next_trade` / `next_depth` async drains) is fine for Indian
  equity volumes but unbenchmarked under open-auction or expiry-day bursts.

## Architecture

3-component pattern, mirroring other Nautilus adapters:

```text
crates/adapters/zerodha/src/
├── auth.rs            SHA-256 checksum + /session/token exchange
├── credential.rs      Env-var resolution (ZeroizeOnDrop)
├── session.rs         ZerodhaSessionManager (ArcSwap<access_token>, CAS-guarded rotate)
├── http.rs            ZerodhaHttpClient (rate-limited submit, token-death retry)
├── ws_handler.rs      Inner stateless WS loop (backoff, ping, reconnect)
├── live.rs            Outer ZerodhaWsClient (Python-facing, subscription replay)
├── decode.rs          Length-switched binary tick decoder (8/28/32/44/184 byte variants)
├── instruments.rs     ZerodhaInstrumentCache (ArcSwap<HashMap>) + daily refresh (07:45 IST)
├── symbology.rs       InstrumentKind classification + KiteToken side-band
├── data.rs            ZerodhaDataDispatcher (Kite WS → QuoteTick/TradeTick/Depth10)
├── historical.rs      KiteResolution + chunked /historical fetch
├── execution.rs       ZerodhaExecClient (submit/modify/cancel + status reports + reconcile)
├── persistence.rs     JSON OrderStore (atomic write-and-rename)
├── holidays.rs        NSE/BSE 2026 calendar + should_poll_orders
├── config.rs          ZerodhaDataClientConfig / ZerodhaExecClientConfig (bon::Builder)
├── factories.rs       SharedDeps singleton + ZerodhaDataClientFactory / ExecutionClientFactory
├── data_client.rs     Rust DataClient trait impl for LiveNode
├── execution_client.rs Rust ExecutionClient trait impl
└── python/
    ├── mod.rs         PyO3 module registration
    ├── client.rs      PyZerodhaClient façade (Python ↔ Rust bridge)
    └── factories.rs   PyO3 #[pymethods] for configs + factories
```

Python wrappers at `nautilus_trader/adapters/zerodha/`:

```text
__init__.py     Re-exports
config.py       msgspec.Struct subclasses of LiveDataClientConfig / LiveExecClientConfig
factories.py    LiveDataClientFactory / LiveExecClientFactory subclasses
data.py         ZerodhaDataClient(LiveMarketDataClient)
execution.py    ZerodhaExecutionClient(LiveExecutionClient)
providers.py    ZerodhaInstrumentProvider stub
```

## Examples

| File | Purpose |
|---|---|
| `examples/live/zerodha/login.py` | Daily browser-redirect token refresh |
| `examples/live/zerodha/zerodha_subscribe_quotes.py` | TradingNode quote-tick smoke |
| `examples/live/zerodha/trading_node_orders.py` | TradingNode order-submit smoke (LIMIT @ ₹1, expects rejection) |
| `examples/smoke_connect.rs` | Rust auth + HTTP + WS connect smoke |
| `examples/ws_capture.rs` | Capture 60s of live WS to `tests/fixtures/ws_session_*.bin` |
| `examples/exec_smoke.rs` | Rust live verification of historical + positions + margins |

## Before going live

1. Fund the test account and run `examples/live/zerodha/trading_node_orders.py`
   with a real symbol and a price that will fill within seconds. Cancel
   immediately. Confirm:
   - `OrderAccepted` → `OrderFilled` events arrive on the strategy.
   - Position update reaches `Strategy.on_position_*`.
   - `cancel_order` after a partial fill produces the right status report.
2. Run `zerodha_subscribe_quotes.py` during NSE hours (09:15–15:30 IST) and
   confirm `Strategy.on_quote_tick` fires. Watch `ws.dropped_events()` — any
   non-zero count means the forwarder isn't keeping up.
3. Kill the WS mid-session (`pkill -f ws.kite.trade`) and confirm the handler
   reconnects and replays subscriptions automatically.
4. Run the adapter past midnight IST to confirm the daily instrument-refresh
   task at 07:45 IST fires without leaving subscriptions in a bad state.

Until these four are done, treat the adapter as **beta** — code-complete and
test-covered, but not field-proven.

## Spec

Full design and decision log in `specs/zerodha-adapter.md` and
`specs/zerodha-adapter-phase8.md`.
