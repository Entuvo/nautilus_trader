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

//! [NautilusTrader](https://nautilustrader.io) adapter for [ThetaData](https://www.thetadata.net).
//!
//! The `nautilus-thetadata` crate provides a market-data integration with ThetaData's local
//! Terminal for US options, stocks, and indices. The adapter is data-only — there is no
//! execution client.
//!
//! # NautilusTrader
//!
//! [NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
//! engine for multi-asset, multi-venue trading systems.
//!
//! # Feature Flags
//!
//! - `live` (default): Enables live data functionality (`data`, `factories`, `live` modules).
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

pub mod common;
pub mod config;
pub mod decode;
pub mod enums;
pub mod historical;
pub mod instruments;
pub mod symbology;
pub mod types;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "live")]
pub mod data;

#[cfg(feature = "live")]
pub mod factories;

#[cfg(feature = "live")]
pub mod live;
