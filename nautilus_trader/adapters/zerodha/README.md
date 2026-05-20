# Zerodha (Kite Connect) adapter

Live market-data and execution integration for Indian equities, derivatives,
currencies, and commodities via the
[Kite Connect v3 API](https://kite.trade/docs/connect/v3/).

## Status

**Phases 0–8 complete.** Code-complete and test-covered (134 unit + 10
mock-server tests, all green). **Not yet field-proven with a funded account.**
See `crates/adapters/zerodha/README.md` for the readiness matrix and pre-live
checklist.

User-facing documentation: `docs/integrations/zerodha.md`.

## Quick start

```bash
# 1. Refresh the daily access token (browser redirect)
python examples/live/zerodha/login.py

# 2. Run the quote-tick smoke (during NSE hours: 09:15-15:30 IST Mon-Fri)
python examples/live/zerodha/zerodha_subscribe_quotes.py
```

## Python surface

```python
from nautilus_trader.adapters.zerodha import (
    ZERODHA,                          # ClientId
    ZERODHA_VENUE,                    # Venue
    ZerodhaDataClient,                # LiveMarketDataClient
    ZerodhaExecutionClient,           # LiveExecutionClient
    ZerodhaDataClientConfig,          # msgspec.Struct
    ZerodhaExecClientConfig,          # msgspec.Struct
    ZerodhaLiveDataClientFactory,     # for TradingNode.add_data_client_factory
    ZerodhaLiveExecClientFactory,     # for TradingNode.add_exec_client_factory
)
```

Credentials are resolved from environment variables — never from config DTOs:

- `ZERODHA_API_KEY`
- `ZERODHA_API_SECRET`
- `ZERODHA_ACCESS_TOKEN`
- `ZERODHA_REDIRECT_URL` (only needed by `login.py`)

## Routing

Zerodha covers six exchanges. Register them all via `RoutingConfig` so the
data/exec engines route `InstrumentId.venue == NSE` (or BSE, NFO, ...) to this
client:

```python
from nautilus_trader.config import RoutingConfig

INDIAN_VENUES = frozenset({"NSE", "BSE", "NFO", "BFO", "MCX", "CDS"})

data_clients={ZERODHA: ZerodhaDataClientConfig(routing=RoutingConfig(venues=INDIAN_VENUES))},
exec_clients={ZERODHA: ZerodhaExecClientConfig(routing=RoutingConfig(venues=INDIAN_VENUES))},
```

## Spec + design

- `specs/zerodha-adapter.md` — full phased plan + decision log
- `specs/zerodha-adapter-phase8.md` — Python wrapper layer design
