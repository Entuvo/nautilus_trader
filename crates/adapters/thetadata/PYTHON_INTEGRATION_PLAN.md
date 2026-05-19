# ThetaData adapter — Python TradingNode integration plan

**Status:** Planned (Rust path complete; Python `TradingNode` path incomplete)
**Owner:** TBD
**Estimated effort:** 6–10 hours focused work, sequenced into 9 atomic steps.
**Last updated:** 2026-05-19

---

## 1. Why this plan exists

The adapter currently has **two entry points**:

| Path | Status | Used by |
|---|---|---|
| Rust `LiveNode::builder().add_data_client(...)` | ✅ working | `examples/node_data_tester.rs` (verified live: 50K+ QuoteTicks) |
| Rust standalone (`ThetaDataHistoricalClient` direct) | ✅ working | `examples/thetadata-backfill-trades` (1.3 GB / 41.9M trades) |
| **Python `TradingNode` / `LiveNode`** | ❌ broken | most user code |
| **Python `ImportableConfig` (YAML/JSON node configs)** | ❌ broken | declarative deployments |

The Python paths fail because `nautilus_trader/live/node_builder.py:add_data_client_factory()` performs:

```python
if not issubclass(factory, LiveDataClientFactory):
    self._log.error(f"Factory was not of type `LiveDataClientFactory`, was {factory}")
    return
self._data_factories[name] = factory
```

Our current `nautilus_trader/adapters/thetadata/factories.py` re-exports the pyo3 class
`nautilus_pyo3.thetadata.ThetaDataDataClientFactory`. A pyo3 class is **not** a Python
`LiveDataClientFactory` subclass, so the `issubclass` check fails silently, the factory is
dropped, and at line 178 the engine logs `"No LiveDataClientFactory registered"` and skips
client creation.

This document is the precise plan to close that gap.

---

## 2. Verified facts (audited 2026-05-19)

| Claim | Verified |
|---|---|
| Bitmex (same pyo3-registry pattern as us) has 8 Python files; we have 3 | ✅ `ls nautilus_trader/adapters/{bitmex,thetadata}/` |
| Databento has full Python adapter (data.py + providers.py + factories.py + config.py + …) | ✅ |
| `LiveDataClientFactory.create(loop, name, config, msgbus, cache, clock)` is the required signature | ✅ `nautilus_trader/live/factories.py` |
| `LiveMarketDataClient` base class uses `_subscribe_*` / `_unsubscribe_*` / `_request_*` overrides (underscore-prefixed) | ✅ `nautilus_trader/live/data_client.py:562+` |
| The global pyo3 factory-extractor registry does **not** bypass the Python `issubclass` check | ✅ pyo3 registry exists for Rust-side downcasting only |
| Bitmex pattern: pyo3 HTTP client + Python `LiveMarketDataClient` subclass that orchestrates subscribe/dispatch | ✅ `nautilus_trader/adapters/bitmex/data.py` (496 lines) |

## 3. Architecture decision

Three approaches considered:

**Option A — Python orchestrates pyo3 primitives (bitmex pattern).** ← **Chosen**
- Pyo3 exposes `ThetaDataHttpClient` and `ThetaDataWsClient` as pyclasses.
- Python `data.py` (`ThetaDataDataClient(LiveMarketDataClient)`) holds both and routes engine commands.
- WS inbound frames flow through a Python callback the Rust client invokes.
- Pros: matches every other adapter; easier to debug; subscribe orchestration visible in Python.
- Cons: re-implements the orchestration we already have in Rust `data.rs`.

**Option B — Expose the entire Rust `ThetaDataDataClient` as a pyclass, Python is thin proxy.**
- Pros: less duplication.
- Cons: novel pattern; harder to debug because all logic is opaque to Python; lifecycle and msgbus integration are tricky over the FFI boundary.

**Option C — Reimplement WS in Python with `websockets` lib.**
- Pros: zero new pyo3 surface.
- Cons: throws away the proven Rust WS code (subscribe registry, reconnect-replay, OHLC/STATE handling, ContractKey lookup); double-maintenance.

Option A wins because it preserves the strong Rust WS implementation while satisfying the
`LiveDataClientFactory` contract Python expects.

The existing Rust `ThetaDataDataClient` (in `src/data.rs`) is retained for Rust `LiveNode`
consumers — both code paths coexist with no conflict.

---

## 4. Step-by-step plan

Each step is atomic, committable, and independently verifiable. Run `cargo test` / `pytest`
after each step.

### Step 1 — Expose `ThetaDataHttpClient` to Python (1.5 h)

**New file:** `crates/adapters/thetadata/src/python/historical.rs`

```rust
#[pyclass(module = "nautilus_trader.core.nautilus_pyo3.thetadata")]
pub struct ThetaDataHttpClient {
    inner: crate::historical::ThetaDataHistoricalClient,
}

#[pymethods]
impl ThetaDataHttpClient {
    #[new]
    #[pyo3(signature = (http_url = DEFAULT_HTTP_URL.to_string(), timeout_secs = 30))]
    fn py_new(http_url: String, timeout_secs: u64) -> PyResult<Self> { ... }

    fn py_list_expirations<'py>(&self, py: Python<'py>, symbol: String)
        -> PyResult<Bound<'py, PyAny>>;    // returns Python awaitable

    fn py_list_strikes<'py>(&self, py: Python<'py>, symbol: String, expiration: String)
        -> PyResult<Bound<'py, PyAny>>;

    fn py_list_contracts<'py>(&self, py: Python<'py>, symbol: String, date: String)
        -> PyResult<Bound<'py, PyAny>>;

    fn py_hist_quotes<'py>(&self, py: Python<'py>, ...) -> PyResult<Bound<'py, PyAny>>;
    fn py_hist_trades<'py>(&self, py: Python<'py>, ...) -> PyResult<Bound<'py, PyAny>>;
    fn py_hist_ohlc<'py>  (&self, py: Python<'py>, ...) -> PyResult<Bound<'py, PyAny>>;
    fn py_hist_stock_eod<'py>(...) -> PyResult<Bound<'py, PyAny>>;
    fn py_hist_index_eod<'py>(...) -> PyResult<Bound<'py, PyAny>>;
}
```

Bridge async via `pyo3_async_runtimes::tokio::future_into_py`. Each method returns Python
awaitables that yield `Vec<QuoteTick>` / `Vec<TradeTick>` / `Vec<Bar>` / `Vec<OptionContract>`
(all already pyo3-exposed by `nautilus-model`).

**Modify:** `crates/adapters/thetadata/src/python/mod.rs` — add `m.add_class::<ThetaDataHttpClient>()?;`.

**Verify:**
```bash
cargo +1.95.0 check -p nautilus-thetadata --all-features  # zero warnings
cargo +1.95.0 test  -p nautilus-thetadata --lib            # 110 tests still pass
```

### Step 1.5 — Expose `ThetaDataWsClient` to Python (1 h)

**New file:** `crates/adapters/thetadata/src/python/live.rs`

```rust
#[pyclass(module = "nautilus_trader.core.nautilus_pyo3.thetadata")]
pub struct ThetaDataWsClient {
    inner: crate::live::ThetaDataWsClient,
    handler: Arc<Mutex<Option<PyObject>>>,  // Python callback
}

#[pymethods]
impl ThetaDataWsClient {
    #[new]
    fn py_new(ws_url: String) -> PyResult<Self>;

    fn py_connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;

    fn py_subscribe_quotes<'py>(&self, py: Python<'py>, instrument_id: InstrumentId)
        -> PyResult<Bound<'py, PyAny>>;
    fn py_subscribe_trades<'py>(&self, py: Python<'py>, instrument_id: InstrumentId)
        -> PyResult<Bound<'py, PyAny>>;
    fn py_unsubscribe_quotes<'py>(...) -> PyResult<Bound<'py, PyAny>>;
    fn py_unsubscribe_trades<'py>(...) -> PyResult<Bound<'py, PyAny>>;
    fn py_close<'py>(...) -> PyResult<Bound<'py, PyAny>>;

    fn set_quote_handler(&self, handler: PyObject) -> PyResult<()>;  // takes (QuoteTick) -> None
    fn set_trade_handler(&self, handler: PyObject) -> PyResult<()>;  // takes (TradeTick) -> None
}
```

Inside the existing `run_forwarder` (Rust), call the registered Python handler when a
QuoteTick/TradeTick is decoded. Use `Python::with_gil(|py| handler.call1(py, (tick,)))`.

**Per `rules/pyo3-bindings.md`:** the handler is stored as `PyObject`, not `Arc<PyObject>`,
and cloned under the GIL via `clone_py_object()`.

### Step 2 — Python `config.py` (0.5 h)

**New file:** `nautilus_trader/adapters/thetadata/config.py`

```python
from typing import Literal
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.model.identifiers import InstrumentId


class ThetaDataDataClientConfig(LiveDataClientConfig, frozen=True):
    """msgspec-backed config for `ThetaDataDataClient`. Serializable for ImportableConfig."""

    http_url: str = "http://127.0.0.1:25503/v3"
    ws_url: str = "ws://127.0.0.1:25520/v1/events"
    tier: Literal["value", "standard", "pro"] = "standard"
    http_timeout_secs: float = 30.0
    max_reconnects: int = 10
    instrument_ids: list[InstrumentId] | None = None
```

Note: this **replaces** the pyo3 re-export. Users who want the pyo3 class can still import
`nautilus_pyo3.thetadata.ThetaDataDataClientConfig` directly.

**Verify:**
```python
from nautilus_trader.config import ImportableConfig
ImportableConfig("nautilus_trader.adapters.thetadata.config:ThetaDataDataClientConfig",
                  config={"tier": "standard"}).create()  # round-trip ok
```

### Step 3 — `providers.py` (0.5 h)

**New file:** `nautilus_trader/adapters/thetadata/providers.py`

```python
class ThetaDataInstrumentProvider(InstrumentProvider):
    def __init__(self, client: nautilus_pyo3.ThetaDataHttpClient,
                  config: InstrumentProviderConfig | None = None) -> None:
        super().__init__(config=config)
        self._client = client

    async def load_all_async(self, filters: dict | None = None) -> None:
        # No global "list all" for options — bail with a helpful message
        raise NotImplementedError(
            "ThetaData has no global instrument list. Use load_ids_async(...) with "
            "specific InstrumentIds, or call list_expirations + list_strikes via the "
            "HTTP client directly."
        )

    async def load_ids_async(self, instrument_ids: list[InstrumentId], filters: dict | None = None) -> None:
        for instrument_id in instrument_ids:
            await self.load_async(instrument_id)

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        # Parse OCC symbol, build OptionContract via pyo3 client metadata + symbology.
        # Insert via self.add(instrument).
        ...
```

### Step 4 — `data.py` (2.5–3 h, the bulk of the work)

**New file:** `nautilus_trader/adapters/thetadata/data.py`

```python
class ThetaDataDataClient(LiveMarketDataClient):
    def __init__(self, loop, http_client, ws_client, msgbus, cache, clock,
                  instrument_provider, config, name) -> None:
        super().__init__(loop=loop, client_id=ClientId(name or THETADATA),
                          venue=THETADATA_VENUE, msgbus=msgbus, cache=cache, clock=clock,
                          instrument_provider=instrument_provider)
        self._http_client = http_client
        self._ws_client = ws_client
        self._config = config
        # Register inbound-tick handlers on the WS client.
        self._ws_client.set_quote_handler(self._handle_quote)
        self._ws_client.set_trade_handler(self._handle_trade)

    async def _connect(self) -> None:
        await self._ws_client.connect()

    async def _disconnect(self) -> None:
        await self._ws_client.close()

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        await self._ws_client.subscribe_quotes(command.instrument_id)

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        await self._ws_client.subscribe_trades(command.instrument_id)

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        await self._ws_client.unsubscribe_quotes(command.instrument_id)

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        await self._ws_client.unsubscribe_trades(command.instrument_id)

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        # Build ThetaOptionContract from instrument_id, call hist_quotes, emit ticks.
        ticks = await self._http_client.hist_quotes(...)
        self._handle_quote_ticks_response(request, ticks)

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None: ...
    async def _request_bars(self, request: RequestBars) -> None: ...
    async def _request_instruments(self, request: RequestInstruments) -> None: ...
    async def _request_instrument(self, request: RequestInstrument) -> None: ...

    def _handle_quote(self, tick: QuoteTick) -> None:
        # Called from Rust WS forwarder under GIL. Push to engine.
        self._handle_data(tick)

    def _handle_trade(self, tick: TradeTick) -> None:
        self._handle_data(tick)
```

**Routing details to mirror from bitmex/data.py:**
- `self._handle_data(...)` is the inherited base-class method that publishes ticks onto the
  msgbus topic the engine subscribes to.
- For request responses, use `self._handle_*_response(request, ticks)` helpers from the base
  class (they format the `DataResponse` and publish on the correlation-id topic).
- Instrument metadata caching: pre-populate via the provider in `_connect()`, look up by
  `instrument_id` in `self._cache.instrument()` when decoding ticks (the Rust side will need
  to pass instrument metadata when invoking the handler, OR the Python handler looks it up).

### Step 5 — `factories.py` rewrite (0.5 h)

**Overwrite:** `nautilus_trader/adapters/thetadata/factories.py`

```python
import asyncio
from functools import lru_cache

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.data import ThetaDataDataClient
from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.live.factories import LiveDataClientFactory


@lru_cache(maxsize=1)
def get_cached_thetadata_http_client(http_url: str, timeout_secs: float) -> nautilus_pyo3.ThetaDataHttpClient:
    return nautilus_pyo3.ThetaDataHttpClient(http_url=http_url, timeout_secs=int(timeout_secs))


@lru_cache(maxsize=1)
def get_cached_thetadata_ws_client(ws_url: str) -> nautilus_pyo3.ThetaDataWsClient:
    return nautilus_pyo3.ThetaDataWsClient(ws_url=ws_url)


@lru_cache(maxsize=1)
def get_cached_thetadata_instrument_provider(
    http_client: nautilus_pyo3.ThetaDataHttpClient,
    config: InstrumentProviderConfig | None,
) -> ThetaDataInstrumentProvider:
    return ThetaDataInstrumentProvider(client=http_client, config=config)


class ThetaDataLiveDataClientFactory(LiveDataClientFactory):
    """Factory for ThetaData live data clients usable by Python TradingNode."""

    @staticmethod
    def create(  # type: ignore[override]
        loop: asyncio.AbstractEventLoop,
        name: str | None,
        config: ThetaDataDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> ThetaDataDataClient:
        http_client = get_cached_thetadata_http_client(config.http_url, config.http_timeout_secs)
        ws_client = get_cached_thetadata_ws_client(config.ws_url)
        provider = get_cached_thetadata_instrument_provider(http_client, config.instrument_provider)
        return ThetaDataDataClient(
            loop=loop, http_client=http_client, ws_client=ws_client,
            msgbus=msgbus, cache=cache, clock=clock,
            instrument_provider=provider, config=config, name=name,
        )
```

**Verify:** `issubclass(ThetaDataLiveDataClientFactory, LiveDataClientFactory) is True`.

### Step 6 — `__init__.py` updates (15 min)

```python
from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.constants import (THETADATA, THETADATA_CLIENT_ID, THETADATA_VENUE)
from nautilus_trader.adapters.thetadata.data import ThetaDataDataClient
from nautilus_trader.adapters.thetadata.factories import (
    ThetaDataLiveDataClientFactory,
    get_cached_thetadata_http_client,
    get_cached_thetadata_instrument_provider,
    get_cached_thetadata_ws_client,
)
from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider

__all__ = [
    "THETADATA", "THETADATA_CLIENT_ID", "THETADATA_VENUE",
    "ThetaDataDataClient", "ThetaDataDataClientConfig",
    "ThetaDataInstrumentProvider", "ThetaDataLiveDataClientFactory",
    "get_cached_thetadata_http_client",
    "get_cached_thetadata_instrument_provider",
    "get_cached_thetadata_ws_client",
]
```

### Step 7 — Tests (1.5 h)

**New directory:** `tests/integration_tests/adapters/thetadata/`

| File | Purpose |
|---|---|
| `conftest.py` | Fixtures: mocked `nautilus_pyo3.ThetaDataHttpClient` + `ThetaDataWsClient`, `MessageBus`, `Cache`, `LiveClock`. |
| `test_factory.py` | Assert `issubclass(...)`, `.create(...)` returns a `ThetaDataDataClient`, lifecycle works. |
| `test_config.py` | `ImportableConfig` round-trip. |
| `test_providers.py` | `load_async` builds correct `OptionContract` for known SPY symbol. |
| `test_data_client.py` | Subscribe → mock WS frame → `_handle_data` → msgbus topic delivers `QuoteTick`. |

### Step 8 — Documentation (30 min)

**Update:** `docs/integrations/thetadata.md`

Add a "Python TradingNode example" section:

```python
from nautilus_trader.adapters.thetadata import (
    ThetaDataDataClientConfig,
    ThetaDataLiveDataClientFactory,
)
from nautilus_trader.live.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode

config = TradingNodeConfig(
    trader_id="TESTER-001",
    data_clients={
        "THETADATA": ThetaDataDataClientConfig(tier="standard"),
    },
)
node = TradingNode(config=config)
node.add_data_client_factory("THETADATA", ThetaDataLiveDataClientFactory)
node.build()
node.run()
```

**Update:** `crates/adapters/thetadata/README.md` — mark the gap as closed; link to the
TradingNode example.

### Step 9 — Live end-to-end verification (1 h)

1. `make install-debug` or `maturin develop` — rebuild the extension.
2. Write `examples/live/thetadata_subscribe_quotes.py` — actual Python TradingNode that
   subscribes to a SPXW quote stream.
3. Run against a live Terminal during market hours.
4. Confirm `on_quote()` fires in the strategy.

---

## 5. Risks + mitigations

| Risk | Mitigation |
|---|---|
| Pyo3 async bridging (`async fn` → Python awaitable) edge cases | Use `pyo3_async_runtimes::tokio::future_into_py`; mirror the patterns already used in `nautilus-network`. |
| Rust `ThetaDataDataClient` (Rust-only path) becomes orphaned by the new Python-orchestrated path | Both code paths coexist; document that Rust `LiveNode` keeps using `src/data.rs`, Python `TradingNode` uses Python `data.py`. |
| Python WS handler callback Send/Sync issues across the FFI boundary | Store handler as `PyObject` (per `rules/pyo3-bindings.md`); invoke under GIL via `Python::with_gil`. |
| Cache-key consistency between Python provider and Rust WS contract lookup | Reuse `ContractKey` / `SubscriptionKey` from `data.rs` / `live.rs` — single source of truth in Rust. |
| Strike-scale regression sneaks back in via new pyo3 surface | Any new contract construction goes through `symbology.rs::ws_strike()` which now correctly uses ×1000. Add a dedicated test for `ThetaDataWsClient::py_subscribe_quotes` that asserts the on-wire strike. |
| `TradingNode` lifecycle differs from `LiveNode` for `disconnect()` ordering | Step 9 runs full `node.run() → node.stop()` cycle; assert no panics on shutdown. |
| `lru_cache(1)` collision when user constructs multiple clients with different configs | Document the cache-by-URL semantics; mirror bitmex's identical pattern. |

---

## 6. Definition of done

All four entry points work end-to-end during US market hours:

| Path | Status target |
|---|---|
| Rust `LiveNode::builder().add_data_client(...)` | ✅ already works |
| Rust standalone (backfill / hist tester) | ✅ already works |
| Python `TradingNode.add_data_client_factory("THETADATA", ThetaDataLiveDataClientFactory)` | **target ✅** |
| Python `ImportableConfig` (YAML/JSON-defined node configs) | **target ✅** |

Plus:
- `pytest tests/integration_tests/adapters/thetadata/` green
- `cargo +1.95.0 test -p nautilus-thetadata --lib` ≥ 110 passing (+ any new Rust tests for the pyo3 surface)
- `cargo +1.95.0 check -p nautilus-pyo3 --all-features` zero warnings
- Documentation updated in `docs/integrations/thetadata.md` and `crates/adapters/thetadata/README.md`

---

## 7. Out of scope for this plan

- Execution adapter (ThetaData is data-only by design — pair with IB or sandbox).
- Greeks streaming (REST-only on the vendor side).
- Order-book L2 data (ThetaData does not provide L2).
- Calendar / instrument-status streaming events (deferred to a follow-up if needed).
- Per-root `AssetClass::Index` override for SPX/NDX (currently all options use `Equity`).
- `expiration_ns` DST refinement (currently fixed 21:00 UTC; 1h skew during EDT).

These are documented as known limitations in `docs/integrations/thetadata.md` and aren't
required for closing the Python integration gap.
