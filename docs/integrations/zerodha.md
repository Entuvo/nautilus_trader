# Zerodha (Kite Connect v3)

NautilusTrader includes an adapter for [Zerodha Kite Connect v3](https://kite.trade/docs/connect/v3/),
the broker API for India's largest discount brokerage. The adapter is a **broker
connection** — it provides both a live market-data client (Kite ticker WebSocket)
and a live execution client (Kite REST `/orders` family).

The adapter supports:

- Real-time L1 quotes, trades, and L2 (10-deep) order-book snapshots via the Kite
  binary WebSocket protocol.
- Historical candles via `GET /instruments/historical` (minute/3min/5min/10min/15min/
  30min/60min/day).
- Order submission, modification, and cancellation for the Regular variety
  (MIS / CNC / NRML products; DAY / IOC validity).
- Daily instrument-dump refresh (105k+ rows: NSE/BSE/NFO/BFO/MCX/CDS).
- Both **Rust `LiveNode`** and **Python `TradingNode`** entry points.

## Architecture

Zerodha exposes a REST API at `https://api.kite.trade` and a binary WebSocket
ticker at `wss://ws.kite.trade`. Authentication is a daily `request_token` →
`access_token` exchange via browser redirect — there is no machine-credential
flow.

```text
                                            ┌──────────────┐
                ┌───── REST (HTTPS) ───────▶│  Kite REST   │
┌────────────────┐                          │  api.kite.   │
│ Nautilus       │                          │  trade       │
│ Zerodha        │                          └──────────────┘
│ adapter        │
└────────────────┘                          ┌──────────────┐
                └───── WS (binary) ────────▶│  Kite ticker │
                                            │  ws.kite.    │
                                            │  trade       │
                                            └──────────────┘
```

The adapter classes are:

### Python (TradingNode)

- `ZerodhaDataClient` — `LiveMarketDataClient` orchestrator; routes engine commands
  through the shared Rust `PyZerodhaClient`.
- `ZerodhaExecutionClient` — `LiveExecutionClient` orchestrator (submit / modify /
  cancel / status reports).
- `ZerodhaDataClientConfig` / `ZerodhaExecClientConfig` — msgspec configs.
- `ZerodhaLiveDataClientFactory` / `ZerodhaLiveExecClientFactory` —
  `TradingNode` factory entry points.

### Rust (LiveNode) and pyo3 primitives

- `ZerodhaDataClient` (Rust) — `DataClient` implementation.
- `ZerodhaExecutionClient` (Rust) — `ExecutionClient` implementation with
  startup reconciliation, JSON order-store persistence, and NSE/BSE holiday
  calendar.
- `nautilus_pyo3.zerodha.PyZerodhaClient` — composite façade
  (`ZerodhaWsClient` + `ZerodhaDataDispatcher` + `ZerodhaExecClient`)
  shared between the data and execution Python clients.
- `nautilus_pyo3.zerodha.ZerodhaDataClientFactory` /
  `ZerodhaExecutionClientFactory` — Rust factories for `LiveNode`.

## Prerequisites

1. **Kite Connect developer subscription**: ₹2,000/month per app at
   <https://developers.kite.trade/>.
2. **Daily access token**: Kite requires a manual browser login each trading day.
   Use the helper script:

   ```bash
   python examples/live/zerodha/login.py
   ```

   It opens the Kite Connect login URL, waits on `127.0.0.1:5000/zerodha/callback`,
   exchanges the captured `request_token` for an `access_token`, and rewrites
   `examples/live/zerodha/.env` atomically. Run it once a day before market
   open (09:00 IST).
3. **Environment variables** (resolved by `credential.rs` — never put these in
   config DTOs):
   - `ZERODHA_API_KEY`
   - `ZERODHA_API_SECRET`
   - `ZERODHA_ACCESS_TOKEN`
   - `ZERODHA_REDIRECT_URL` (only needed by `login.py` — defaults to
     `http://127.0.0.1:5000/zerodha/callback`)

## Symbology

The adapter preserves Kite's native `tradingsymbol` form (no remapping to
generic names). Examples:

| Kite | Nautilus `InstrumentId` |
|---|---|
| `RELIANCE` (NSE equity) | `RELIANCE-EQ.NSE` |
| `NIFTY 50` (index) | `NIFTY 50.NSE` |
| `NIFTY26JANFUT` (futures) | `NIFTY26JANFUT.NFO` |
| `BANKNIFTY26JAN45000CE` (call) | `BANKNIFTY26JAN45000CE.NFO` |

Equities get an `-EQ` suffix; indices route by `segment=INDICES`; the NCO segment
is dropped during the instrument dump load.

## Configuration

`TradingNode` setup needs both factories registered and `RoutingConfig` set to
the venues Zerodha covers (so the data engine resolves
`InstrumentId.venue == NSE` to the Zerodha client):

```python
from nautilus_trader.adapters.zerodha import (
    ZERODHA,
    ZerodhaDataClientConfig,
    ZerodhaExecClientConfig,
    ZerodhaLiveDataClientFactory,
    ZerodhaLiveExecClientFactory,
)
from nautilus_trader.config import RoutingConfig, TradingNodeConfig

INDIAN_VENUES = frozenset({"NSE", "BSE", "NFO", "BFO", "MCX", "CDS"})

config = TradingNodeConfig(
    trader_id=TraderId("MYTRADER-001"),
    data_clients={
        ZERODHA: ZerodhaDataClientConfig(routing=RoutingConfig(venues=INDIAN_VENUES)),
    },
    exec_clients={
        ZERODHA: ZerodhaExecClientConfig(
            routing=RoutingConfig(venues=INDIAN_VENUES),
            default_product="MIS",   # or "CNC" / "NRML"
        ),
    },
)
```

## Examples

The `examples/live/zerodha/` directory contains:

- `login.py` — daily browser-redirect token refresh.
- `zerodha_subscribe_quotes.py` — subscribe to RELIANCE-EQ.NSE quote ticks and
  print the first 10. Verifies the data path end-to-end.
- `trading_node_orders.py` — submit one far-from-market LIMIT order and cancel
  it. Verifies the execution path. **Run this only with `MIS` and quantity 1
  unless you've reviewed it carefully** — Kite is live by default.

## Operational notes

- **Trading hours**: 09:15–15:30 IST Mon–Fri. The adapter's holiday calendar
  (NSE/BSE 2026) is wired into `should_poll_orders` so the orders-poll loop
  sleeps overnight and on holidays.
- **Subscription cap**: Kite allows ≤ 3,000 instrument tokens per WS
  connection; the adapter enforces this and surfaces
  `ZerodhaError::SubscriptionLimitExceeded`.
- **Rate limit**: order submits are throttled to 10/sec via
  `nautilus_network::RateLimiter`.
- **Reconnect**: on TokenException (daily token rotation needed) the adapter
  rotates via `session.rotate()` and replays subscriptions automatically.
- **Persistence**: order-meta is JSON-persisted (`order_store_path` config
  field). On startup, `reconcile_startup` re-reads the file and synthesises
  cancel reports for orders older than 24 h that Kite no longer knows about.
