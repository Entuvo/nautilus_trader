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

//! Order submit / modify / cancel and the `/orders` polling reconciler that drives
//! `generate_order_status_reports()`.
//!
//! TODO(Phase 5): submit (`POST /orders/{variety}`), 10-QPS rate limit via
//! `nautilus-network::RateLimiter`, side-band `ZerodhaOrderMeta` map for product (CNC/MIS/NRML),
//! synthetic `TradeId = "ZER-{kite_order_id}-{fill_sequence}"` on quantity delta.
