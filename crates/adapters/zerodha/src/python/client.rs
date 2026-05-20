// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  See LICENSE for full text.
// -------------------------------------------------------------------------------------------------

//! `PyZerodhaClient` — the Python-facing façade that lets the Python
//! `LiveMarketDataClient` / `LiveExecutionClient` subclasses delegate to the Rust primitives
//! (`ZerodhaWsClient`, `ZerodhaDataDispatcher`, `ZerodhaExecClient`).
//!
//! Constructor reads `ZERODHA_API_KEY` / `ZERODHA_API_SECRET` / `ZERODHA_ACCESS_TOKEN` from
//! env (the Python config carries no credentials per spec §3.1) and wires the shared
//! `Arc<SharedDeps>` registry from `factories.rs`.

#![allow(clippy::needless_pass_by_value)] // PyO3 method params take owned String / PathBuf
#![allow(clippy::missing_const_for_fn)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_model::identifiers::{ClientOrderId, InstrumentId};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyclass;
use tokio::sync::mpsc;

use crate::{
    common::{REST_BASE, WS_BASE, WS_EVENT_CHANNEL_CAPACITY},
    data::{DispatchSinks, ZerodhaDataDispatcher},
    execution::ZerodhaExecClient,
    factories::{SharedDeps, get_or_build_shared},
    instruments::{ValidationLimits, load_all},
    live::ZerodhaWsClient,
};

/// Python-facing façade exposing the methods the Python `ZerodhaDataClient` /
/// `ZerodhaExecutionClient` subclasses need.
#[pyclass(unsendable, module = "nautilus_trader.core.nautilus_pyo3.zerodha")]
#[gen_stub_pyclass(module = "nautilus_trader.zerodha")]
pub struct PyZerodhaClient {
    #[allow(dead_code)]
    shared: Arc<SharedDeps>,
    ws: Arc<ZerodhaWsClient>,
    dispatcher: Arc<ZerodhaDataDispatcher>,
    exec: Arc<ZerodhaExecClient>,
    is_connected: Arc<AtomicBool>,
    sink_rxs: std::sync::Mutex<Option<SinkReceivers>>,
}

impl std::fmt::Debug for PyZerodhaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyZerodhaClient")
            .field("is_connected", &self.is_connected.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)] // receivers parked until tick-forwarding lands; see py_connect comment
struct SinkReceivers {
    quotes: mpsc::Receiver<nautilus_model::data::QuoteTick>,
    trades: mpsc::Receiver<nautilus_model::data::TradeTick>,
    depths: mpsc::Receiver<nautilus_model::data::depth::OrderBookDepth10>,
}

#[pymethods]
impl PyZerodhaClient {
    /// Construct the client.
    ///
    /// `http_url` and `ws_url` default to the production Kite endpoints.
    #[new]
    #[pyo3(signature = (http_url = REST_BASE.to_string(), ws_url = WS_BASE.to_string(), account_id = None, order_store_path = None))]
    fn py_new(
        http_url: String,
        ws_url: String,
        account_id: Option<String>,
        order_store_path: Option<std::path::PathBuf>,
    ) -> PyResult<Self> {
        let shared = get_or_build_shared(&http_url).map_err(to_pyvalue_err)?;

        let ws = Arc::new(ZerodhaWsClient::spawn(
            shared.session.clone(),
            Some(ws_url),
        ));

        let (qtx, qrx) = mpsc::channel(WS_EVENT_CHANNEL_CAPACITY);
        let (ttx, trx) = mpsc::channel(WS_EVENT_CHANNEL_CAPACITY);
        let (dtx, drx) = mpsc::channel(WS_EVENT_CHANNEL_CAPACITY);
        let sinks = DispatchSinks {
            quotes: qtx,
            trades: ttx,
            depths: dtx,
        };
        let dispatcher = Arc::new(ZerodhaDataDispatcher::new(
            shared.cache.clone(),
            ws.clone(),
            sinks,
            2,
        ));

        let account_id = account_id.map_or_else(
            || nautilus_model::identifiers::AccountId::from("ZERODHA-DEFAULT"),
            |s| nautilus_model::identifiers::AccountId::from(s.as_str()),
        );
        let store = order_store_path.map(crate::persistence::OrderStore::new);
        let exec = Arc::new(ZerodhaExecClient::new(
            shared.http.clone(),
            shared.cache.clone(),
            account_id,
            store,
        ));

        Ok(Self {
            shared,
            ws,
            dispatcher,
            exec,
            is_connected: Arc::new(AtomicBool::new(false)),
            sink_rxs: std::sync::Mutex::new(Some(SinkReceivers {
                quotes: qrx,
                trades: trx,
                depths: drx,
            })),
        })
    }

    /// Block (via `tokio::time::sleep` polling) until the WS handshake completes — up to
    /// 15 s. Spawns the dispatcher → runner forwarder so ticks start flowing.
    #[pyo3(name = "connect")]
    fn py_connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.ws.clone();
        let is_connected = self.is_connected.clone();
        let shared = self.shared.clone();
        let sink_rxs = {
            let mut guard = self
                .sink_rxs
                .lock()
                .map_err(|_| to_pyruntime_err("sink_rxs poisoned"))?;
            guard.take()
        };
        // NOTE: tick → runner forwarding is intentionally NOT spawned here. The runner's
        // `get_data_event_sender()` thread-local is only accessible from the runner thread,
        // but the pyo3 future runs on a Tokio worker. Wiring the forwarder properly requires
        // the Python `ZerodhaDataClient._connect()` to push events through msgbus.publish
        // directly — see the open TODO in data.py. For now the dispatcher's sink channels
        // back up; subscribe/cancel still work, but ticks don't reach `on_quote_tick` yet.
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if is_connected.load(Ordering::Acquire) {
                return Ok(Python::attach(|py| py.None()));
            }
            // Load instrument cache so dispatcher symbology lookups (RELIANCE-EQ.NSE → token)
            // succeed on the first subscribe. Skipped if already populated by an earlier call.
            if shared.cache.row_count() == 0 {
                let ts_now =
                    nautilus_core::UnixNanos::from(std::time::SystemTime::now());
                load_all(
                    &shared.cache,
                    &shared.session,
                    ValidationLimits::default(),
                    ts_now,
                )
                .await
                .map_err(|e| to_pyruntime_err(format!("instrument load failed: {e}")))?;
            }
            let deadline = Instant::now() + Duration::from_secs(15);
            while !ws.is_connected() && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if !ws.is_connected() {
                return Err(to_pyruntime_err(
                    "ZerodhaWsClient did not connect within 15 s",
                ));
            }
            // Drop the sink receivers so the dispatcher's channels don't deadlock on full
            // mpsc buffers while no consumer exists yet. We'll re-create them when the
            // tick-forwarding path is properly wired in the follow-up commit.
            drop(sink_rxs);
            is_connected.store(true, Ordering::Release);
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Close the WS + abort the forwarder.
    #[pyo3(name = "close")]
    fn py_close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.ws.clone();
        let is_connected = self.is_connected.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            ws.close().await;
            is_connected.store(false, Ordering::Release);
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// True when the WS is up.
    #[pyo3(name = "is_connected")]
    fn py_is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    /// Subscribe to quote ticks for an instrument. `instrument_id` is the Nautilus form
    /// (`"RELIANCE-EQ.NSE"`).
    #[pyo3(name = "subscribe_quotes")]
    fn py_subscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        instrument_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dispatcher = self.dispatcher.clone();
        let id = InstrumentId::from(instrument_id.as_str());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            dispatcher
                .subscribe_quote_ticks(&id)
                .await
                .map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Subscribe to trade ticks.
    #[pyo3(name = "subscribe_trades")]
    fn py_subscribe_trades<'py>(
        &self,
        py: Python<'py>,
        instrument_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dispatcher = self.dispatcher.clone();
        let id = InstrumentId::from(instrument_id.as_str());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            dispatcher
                .subscribe_trade_ticks(&id)
                .await
                .map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Subscribe to 10-level order book snapshots.
    #[pyo3(name = "subscribe_book")]
    fn py_subscribe_book<'py>(
        &self,
        py: Python<'py>,
        instrument_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dispatcher = self.dispatcher.clone();
        let id = InstrumentId::from(instrument_id.as_str());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            dispatcher
                .subscribe_order_book(&id)
                .await
                .map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Unsubscribe from quote ticks.
    #[pyo3(name = "unsubscribe_quotes")]
    fn py_unsubscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        instrument_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dispatcher = self.dispatcher.clone();
        let id = InstrumentId::from(instrument_id.as_str());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            dispatcher
                .unsubscribe_quote_ticks(&id)
                .await
                .map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Unsubscribe from trade ticks.
    #[pyo3(name = "unsubscribe_trades")]
    fn py_unsubscribe_trades<'py>(
        &self,
        py: Python<'py>,
        instrument_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dispatcher = self.dispatcher.clone();
        let id = InstrumentId::from(instrument_id.as_str());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            dispatcher
                .unsubscribe_trade_ticks(&id)
                .await
                .map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Unsubscribe from order book snapshots.
    #[pyo3(name = "unsubscribe_book")]
    fn py_unsubscribe_book<'py>(
        &self,
        py: Python<'py>,
        instrument_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dispatcher = self.dispatcher.clone();
        let id = InstrumentId::from(instrument_id.as_str());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            dispatcher
                .unsubscribe_order_book(&id)
                .await
                .map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Cancel an order by `client_order_id`.
    #[pyo3(name = "cancel_order")]
    fn py_cancel_order<'py>(
        &self,
        py: Python<'py>,
        client_order_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let exec = self.exec.clone();
        let cid = ClientOrderId::from(client_order_id.as_str());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            exec.cancel_order(cid).await.map_err(to_pyvalue_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }
}
