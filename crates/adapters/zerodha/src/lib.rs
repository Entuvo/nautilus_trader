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

//! [NautilusTrader](https://nautilustrader.io) adapter for Zerodha [Kite Connect v3](https://kite.trade/docs/connect/v3/).
//!
//! The `nautilus-zerodha` crate provides live market-data and execution integrations against the
//! production Kite Connect API (`api.kite.trade` / `ws.kite.trade`) for Indian equities,
//! derivatives, currencies, and commodities.
//!
//! # NautilusTrader
//!
//! [NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
//! engine for multi-asset, multi-venue trading systems.
//!
//! # Feature Flags
//!
//! - `live` (default): Enables live data + execution functionality.
//! - `python`: Enables Python bindings from [PyO3](https://pyo3.rs).
//! - `extension-module`: Builds as a Python extension module.
//! - `high-precision`: Enables 128-bit value types.

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod auth;
pub mod common;
pub mod config;
pub mod credential;
pub mod decode;
pub mod error;
pub mod historical;
pub mod holidays;
pub mod instruments;
pub mod session;
pub mod symbology;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "live")]
pub mod data;

#[cfg(feature = "live")]
pub mod data_client;

#[cfg(feature = "live")]
pub mod execution;

#[cfg(feature = "live")]
pub mod execution_client;

#[cfg(feature = "live")]
pub mod factories;

#[cfg(feature = "live")]
pub mod http;

#[cfg(feature = "live")]
pub mod live;

#[cfg(feature = "live")]
pub mod persistence;

#[cfg(feature = "live")]
pub mod ws_handler;
