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

use std::str::FromStr;

use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::{ClientOrderId, InstrumentId},
    types::{Price, Quantity},
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyclass;
use tokio::sync::{Mutex as TokioMutex, mpsc};

use crate::{
    common::{REST_BASE, WS_BASE, WS_EVENT_CHANNEL_CAPACITY},
    data::{DispatchSinks, ZerodhaDataDispatcher},
    execution::{KiteProduct, KiteVariety, SubmitRequest, ZerodhaExecClient},
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
    quote_rx: Arc<TokioMutex<mpsc::Receiver<nautilus_model::data::QuoteTick>>>,
    trade_rx: Arc<TokioMutex<mpsc::Receiver<nautilus_model::data::TradeTick>>>,
    depth_rx: Arc<TokioMutex<mpsc::Receiver<nautilus_model::data::depth::OrderBookDepth10>>>,
}

impl std::fmt::Debug for PyZerodhaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyZerodhaClient")
            .field("is_connected", &self.is_connected.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
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
            quote_rx: Arc::new(TokioMutex::new(qrx)),
            trade_rx: Arc::new(TokioMutex::new(trx)),
            depth_rx: Arc::new(TokioMutex::new(drx)),
        })
    }

    /// Block (via `tokio::time::sleep` polling) until the WS handshake completes — up to
    /// 15 s. Spawns the dispatcher → runner forwarder so ticks start flowing.
    #[pyo3(name = "connect")]
    fn py_connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ws = self.ws.clone();
        let is_connected = self.is_connected.clone();
        let shared = self.shared.clone();
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
            is_connected.store(true, Ordering::Release);
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Await the next quote tick from the dispatcher. Returns `None` once the dispatcher's
    /// sender is dropped (i.e. on shutdown). Python forwarder loops on this and calls
    /// `LiveMarketDataClient._handle_data` per tick.
    #[pyo3(name = "next_quote")]
    fn py_next_quote<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.quote_rx.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = rx.lock().await;
            Ok(guard.recv().await)
        })
    }

    /// Await the next trade tick. See [`py_next_quote`].
    #[pyo3(name = "next_trade")]
    fn py_next_trade<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.trade_rx.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = rx.lock().await;
            Ok(guard.recv().await)
        })
    }

    /// Await the next L2 depth snapshot. See [`py_next_quote`].
    #[pyo3(name = "next_depth")]
    fn py_next_depth<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.depth_rx.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = rx.lock().await;
            Ok(guard.recv().await)
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

    /// Submit an order. Enums are passed as their Nautilus string form
    /// (`OrderSide` = "BUY"/"SELL", `OrderType` = "MARKET"/"LIMIT"/"STOP_MARKET"/"STOP_LIMIT",
    /// `TimeInForce` = "DAY"/"IOC"). `product` is the Kite-native form ("MIS"/"CNC"/"NRML")
    /// — defaults to "MIS" when `None`.
    #[pyo3(name = "submit_order", signature = (
        client_order_id,
        instrument_id,
        order_side,
        order_type,
        time_in_force,
        quantity,
        price = None,
        trigger_price = None,
        product = None,
        price_precision = 2,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_submit_order<'py>(
        &self,
        py: Python<'py>,
        client_order_id: String,
        instrument_id: String,
        order_side: String,
        order_type: String,
        time_in_force: String,
        quantity: f64,
        price: Option<f64>,
        trigger_price: Option<f64>,
        product: Option<String>,
        price_precision: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        let exec = self.exec.clone();
        let side = OrderSide::from_str(&order_side).map_err(to_pyvalue_err)?;
        let otype = OrderType::from_str(&order_type).map_err(to_pyvalue_err)?;
        let tif = TimeInForce::from_str(&time_in_force).map_err(to_pyvalue_err)?;
        let product = match product.as_deref().unwrap_or("MIS") {
            "MIS" => KiteProduct::Mis,
            "CNC" => KiteProduct::Cnc,
            "NRML" => KiteProduct::Nrml,
            other => return Err(to_pyvalue_err(format!("unknown product {other}"))),
        };
        let qty = Quantity::new(quantity, 0);
        let price = price
            .map(|p| Price::new(p, price_precision))
            ;
        let trigger_price = trigger_price
            .map(|p| Price::new(p, price_precision))
            ;
        let req = SubmitRequest {
            client_order_id: ClientOrderId::from(client_order_id.as_str()),
            instrument_id: InstrumentId::from(instrument_id.as_str()),
            order_side: side,
            order_type: otype,
            time_in_force: tif,
            quantity: qty,
            price,
            trigger_price,
            variety: KiteVariety::Regular,
            product,
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let kite_order_id = exec.submit_order(&req).await.map_err(to_pyvalue_err)?;
            Ok(kite_order_id)
        })
    }

    /// Modify an order. At least one of `quantity`, `price`, `trigger_price` must be set.
    #[pyo3(name = "modify_order", signature = (
        client_order_id,
        quantity = None,
        price = None,
        trigger_price = None,
        price_precision = 2,
    ))]
    fn py_modify_order<'py>(
        &self,
        py: Python<'py>,
        client_order_id: String,
        quantity: Option<f64>,
        price: Option<f64>,
        trigger_price: Option<f64>,
        price_precision: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        let exec = self.exec.clone();
        let cid = ClientOrderId::from(client_order_id.as_str());
        let qty = quantity.map(|q| Quantity::new(q, 0));
        let price = price.map(|p| Price::new(p, price_precision));
        let trig = trigger_price.map(|p| Price::new(p, price_precision));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let new_id = exec
                .modify_order(cid, qty, price, trig)
                .await
                .map_err(to_pyvalue_err)?;
            Ok(new_id)
        })
    }
}
