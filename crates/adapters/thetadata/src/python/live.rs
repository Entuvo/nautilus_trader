// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Python bindings for the ThetaData WebSocket (live streaming) client.
//!
//! Exposes a `ThetaDataWsClient` pyclass that wraps the Rust
//! [`crate::live::ThetaDataWsClient`] and hands decoded `QuoteTick`/`TradeTick` instances to
//! Python callbacks. The Python `ThetaDataDataClient` (orchestrator) registers handlers via
//! [`ThetaDataWsClient::set_quote_handler`] / [`set_trade_handler`] before subscribing.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use nautilus_common::live::get_runtime;
use nautilus_core::{
    UnixNanos,
    python::{to_pyruntime_err, to_pyvalue_err},
};
use nautilus_model::identifiers::InstrumentId;
use nautilus_network::websocket::WebSocketConfig;
use pyo3::prelude::*;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    common::DEFAULT_WS_URL,
    live::ThetaDataWsClient as InnerWsClient,
    symbology::ThetaOptionContract,
    types::{WsFrame, WsQuoteFrame, WsTradeFrame},
};

const DEFAULT_PRICE_PRECISION: u8 = 2;
const DEFAULT_SIZE_PRECISION: u8 = 0;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ContractKey {
    root: String,
    expiration: u32,
    strike: u64,
    right: String,
}

impl ContractKey {
    fn from_contract(contract: &ThetaOptionContract) -> Self {
        Self {
            root: contract.root.clone(),
            expiration: contract.ws_expiration(),
            strike: contract.ws_strike(),
            right: contract.right.as_wire().to_string(),
        }
    }

    fn from_ws_quote(frame: &WsQuoteFrame) -> Self {
        Self {
            root: frame.contract.root.clone(),
            expiration: frame.contract.expiration,
            strike: frame.contract.strike,
            right: frame.contract.right.clone(),
        }
    }

    fn from_ws_trade(frame: &WsTradeFrame) -> Self {
        Self {
            root: frame.contract.root.clone(),
            expiration: frame.contract.expiration,
            strike: frame.contract.strike,
            right: frame.contract.right.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct InstrumentMetadata {
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
}

#[derive(Debug)]
struct Shared {
    instruments: Mutex<HashMap<ContractKey, InstrumentMetadata>>,
    quote_handler: Mutex<Option<Py<PyAny>>>,
    trade_handler: Mutex<Option<Py<PyAny>>>,
    is_connected: AtomicBool,
}

impl Shared {
    fn new() -> Self {
        Self {
            instruments: Mutex::new(HashMap::new()),
            quote_handler: Mutex::new(None),
            trade_handler: Mutex::new(None),
            is_connected: AtomicBool::new(false),
        }
    }
}

/// WebSocket client over the local ThetaTerminal streaming endpoint, callable from Python.
#[pyo3_stub_gen::derive::gen_stub_pyclass]
#[pyclass(module = "nautilus_trader.core.nautilus_pyo3.thetadata", from_py_object)]
#[derive(Clone)]
pub struct ThetaDataWsClient {
    ws_url: String,
    inner: Arc<InnerWsClient>,
    events_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<WsFrame>>>>,
    shared: Arc<Shared>,
    forwarder: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl std::fmt::Debug for ThetaDataWsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThetaDataWsClient")
            .field("ws_url", &self.ws_url)
            .field("is_connected", &self.shared.is_connected.load(Ordering::Acquire))
            .finish()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ThetaDataWsClient {
    /// Creates a new (unconnected) WebSocket client targeting the local ThetaTerminal endpoint.
    #[new]
    #[pyo3(signature = (ws_url = DEFAULT_WS_URL.to_string()))]
    fn py_new(ws_url: String) -> PyResult<Self> {
        let config = WebSocketConfig::builder().url(ws_url.clone()).build();
        let (inner, rx) = InnerWsClient::new(config);
        Ok(Self {
            ws_url,
            inner: Arc::new(inner),
            events_rx: Arc::new(Mutex::new(Some(rx))),
            shared: Arc::new(Shared::new()),
            forwarder: Arc::new(Mutex::new(None)),
        })
    }

    /// Returns the WebSocket URL the client targets.
    #[getter]
    #[pyo3(name = "url")]
    fn py_url(&self) -> &str {
        &self.ws_url
    }

    /// Returns `True` once `connect()` has succeeded and the inbound forwarder has started.
    #[pyo3(name = "is_connected")]
    fn py_is_connected(&self) -> bool {
        self.shared.is_connected.load(Ordering::Acquire)
    }

    fn __repr__(&self) -> String {
        format!(
            "ThetaDataWsClient(url={:?}, is_connected={})",
            self.ws_url,
            self.shared.is_connected.load(Ordering::Acquire),
        )
    }

    /// Registers instrument metadata so the inbound forwarder can decode ticks for this contract.
    ///
    /// Must be called for each instrument prior to subscribing. `instrument_id.symbol` must
    /// use the OCC-style symbology produced by `ThetaOptionContract::to_instrument_id`.
    #[pyo3(name = "cache_instrument")]
    #[pyo3(signature = (instrument_id, price_precision = DEFAULT_PRICE_PRECISION, size_precision = DEFAULT_SIZE_PRECISION))]
    fn py_cache_instrument(
        &self,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
    ) -> PyResult<()> {
        let contract = ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())
            .map_err(to_pyvalue_err)?;
        let key = ContractKey::from_contract(&contract);
        let meta = InstrumentMetadata {
            instrument_id,
            price_precision,
            size_precision,
        };
        self.shared
            .instruments
            .lock()
            .map_err(|_| to_pyruntime_err("instruments mutex poisoned"))?
            .insert(key, meta);
        Ok(())
    }

    /// Registers the Python callback invoked for each decoded inbound `QuoteTick`.
    ///
    /// The callback is invoked under the GIL on the Tokio runtime worker thread that drives the
    /// forwarder. Keep callbacks short — long-running work belongs on the asyncio event loop.
    #[pyo3(name = "set_quote_handler")]
    fn py_set_quote_handler(&self, handler: Py<PyAny>) -> PyResult<()> {
        *self
            .shared
            .quote_handler
            .lock()
            .map_err(|_| to_pyruntime_err("quote_handler mutex poisoned"))? = Some(handler);
        Ok(())
    }

    /// Registers the Python callback invoked for each decoded inbound `TradeTick`.
    #[pyo3(name = "set_trade_handler")]
    fn py_set_trade_handler(&self, handler: Py<PyAny>) -> PyResult<()> {
        *self
            .shared
            .trade_handler
            .lock()
            .map_err(|_| to_pyruntime_err("trade_handler mutex poisoned"))? = Some(handler);
        Ok(())
    }

    /// Opens the WebSocket connection and spawns the inbound-frame forwarder.
    ///
    /// Idempotent — calling on an already-connected client returns immediately.
    #[pyo3(name = "connect")]
    fn py_connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let events_rx = Arc::clone(&self.events_rx);
        let shared = Arc::clone(&self.shared);
        let forwarder = Arc::clone(&self.forwarder);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if shared.is_connected.load(Ordering::Acquire) {
                return Ok(Python::attach(|py| py.None()));
            }
            inner.connect().await.map_err(to_pyvalue_err)?;
            let rx = {
                let mut slot = events_rx
                    .lock()
                    .map_err(|_| to_pyruntime_err("events_rx mutex poisoned"))?;
                slot.take()
                    .ok_or_else(|| to_pyruntime_err("WebSocket forwarder already spawned"))?
            };
            let handle = get_runtime().spawn(run_forwarder(rx, Arc::clone(&shared)));
            *forwarder
                .lock()
                .map_err(|_| to_pyruntime_err("forwarder mutex poisoned"))? = Some(handle);
            shared.is_connected.store(true, Ordering::Release);
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Closes the WebSocket connection and aborts the inbound forwarder.
    ///
    /// Idempotent.
    #[pyo3(name = "close")]
    fn py_close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let shared = Arc::clone(&self.shared);
        let forwarder = Arc::clone(&self.forwarder);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _ = inner.close().await; // close is idempotent; errors are non-fatal
            if let Ok(mut slot) = forwarder.lock() {
                if let Some(handle) = slot.take() {
                    handle.abort();
                }
            }
            shared.is_connected.store(false, Ordering::Release);
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Subscribes to live quotes for the given instrument. The instrument must have been
    /// registered via [`cache_instrument`] first.
    #[pyo3(name = "subscribe_quotes")]
    fn py_subscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contract = ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())
                .map_err(to_pyvalue_err)?;
            inner.subscribe_quotes(&contract).await.map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Subscribes to live trades for the given instrument.
    #[pyo3(name = "subscribe_trades")]
    fn py_subscribe_trades<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contract = ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())
                .map_err(to_pyvalue_err)?;
            inner.subscribe_trades(&contract).await.map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Unsubscribes from live quotes for the given instrument. Idempotent.
    #[pyo3(name = "unsubscribe_quotes")]
    fn py_unsubscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contract = ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())
                .map_err(to_pyvalue_err)?;
            inner.unsubscribe_quotes(&contract).await.map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Unsubscribes from live trades for the given instrument. Idempotent.
    #[pyo3(name = "unsubscribe_trades")]
    fn py_unsubscribe_trades<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contract = ThetaOptionContract::from_symbol(instrument_id.symbol.as_str())
                .map_err(to_pyvalue_err)?;
            inner.unsubscribe_trades(&contract).await.map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }
}

async fn run_forwarder(mut rx: mpsc::UnboundedReceiver<WsFrame>, shared: Arc<Shared>) {
    while let Some(frame) = rx.recv().await {
        match frame {
            WsFrame::Status(_) => {}
            WsFrame::State(s) => log::debug!("session state: {:?}", s.state),
            WsFrame::Ohlc(o) => log::debug!(
                "ohlc {}/{}/{}/{} close={} count={}",
                o.contract.root,
                o.contract.expiration,
                o.contract.strike,
                o.contract.right,
                o.ohlc.close,
                o.ohlc.count,
            ),
            WsFrame::Quote(quote) => dispatch_quote(&shared, quote),
            WsFrame::Trade(trade) => dispatch_trade(&shared, trade),
        }
    }
}

fn dispatch_quote(shared: &Arc<Shared>, frame: WsQuoteFrame) {
    let key = ContractKey::from_ws_quote(&frame);
    let Some(meta) = lookup_metadata(shared, &key) else {
        log::debug!(
            "ignoring quote for unsubscribed contract {}/{}/{}/{}",
            frame.contract.root,
            frame.contract.expiration,
            frame.contract.strike,
            frame.contract.right,
        );
        return;
    };
    let ts_init = now_unix_nanos();
    let tick = match frame.to_quote_tick(
        meta.instrument_id,
        meta.price_precision,
        meta.size_precision,
        ts_init,
    ) {
        Ok(tick) => tick,
        Err(e) => {
            log::warn!("failed to decode quote tick: {e}");
            return;
        }
    };
    Python::attach(|py| {
        let handler = match shared.quote_handler.lock() {
            Ok(g) => match g.as_ref() {
                Some(h) => h.clone_ref(py),
                None => return,
            },
            Err(_) => {
                log::error!("quote_handler mutex poisoned");
                return;
            }
        };
        if let Err(e) = handler.call1(py, (tick,)) {
            log::warn!("quote handler raised: {e}");
        }
    });
}

fn dispatch_trade(shared: &Arc<Shared>, frame: WsTradeFrame) {
    let key = ContractKey::from_ws_trade(&frame);
    let Some(meta) = lookup_metadata(shared, &key) else {
        log::debug!(
            "ignoring trade for unsubscribed contract {}/{}/{}/{}",
            frame.contract.root,
            frame.contract.expiration,
            frame.contract.strike,
            frame.contract.right,
        );
        return;
    };
    let ts_init = now_unix_nanos();
    let tick = match frame.to_trade_tick(
        meta.instrument_id,
        meta.price_precision,
        meta.size_precision,
        ts_init,
    ) {
        Ok(tick) => tick,
        Err(e) => {
            log::warn!("failed to decode trade tick: {e}");
            return;
        }
    };
    Python::attach(|py| {
        let handler = match shared.trade_handler.lock() {
            Ok(g) => match g.as_ref() {
                Some(h) => h.clone_ref(py),
                None => return,
            },
            Err(_) => {
                log::error!("trade_handler mutex poisoned");
                return;
            }
        };
        if let Err(e) = handler.call1(py, (tick,)) {
            log::warn!("trade handler raised: {e}");
        }
    });
}

fn lookup_metadata(shared: &Arc<Shared>, key: &ContractKey) -> Option<InstrumentMetadata> {
    shared.instruments.lock().ok()?.get(key).cloned()
}

fn now_unix_nanos() -> UnixNanos {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    UnixNanos::from(nanos)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rstest::*;

    use super::*;
    use crate::enums::OptionRight;

    fn sample_contract() -> ThetaOptionContract {
        ThetaOptionContract::from_dollar_strike(
            "SPXW",
            NaiveDate::from_ymd_opt(2025, 3, 15).unwrap(),
            480.0,
            OptionRight::Call,
        )
        .unwrap()
    }

    #[rstest]
    fn test_contract_key_from_contract_matches_ws_encoding() {
        let key = ContractKey::from_contract(&sample_contract());
        assert_eq!(key.root, "SPXW");
        assert_eq!(key.expiration, 20_250_315);
        assert_eq!(key.strike, 480_000);
        assert_eq!(key.right, "C");
    }

    #[rstest]
    fn test_shared_starts_disconnected() {
        let shared = Shared::new();
        assert!(!shared.is_connected.load(Ordering::Acquire));
    }
}
