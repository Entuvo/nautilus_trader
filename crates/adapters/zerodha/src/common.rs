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

//! Common constants for the Zerodha adapter (Kite Connect v3).

use std::time::Duration;

/// The Kite Connect REST base URL.
pub const REST_BASE: &str = "https://api.kite.trade";

/// The Kite ticker WebSocket base URL (token + api_key go in the query string).
pub const WS_BASE: &str = "wss://ws.kite.trade";

/// The Kite Connect API version. Sent as the `X-Kite-Version` header on every REST request.
pub const KITE_VERSION: &str = "3";

/// Adapter identifier for the venue.
pub const ZERODHA: &str = "ZERODHA";

/// Default order-submit rate limit (Kite v3: 10/sec/user). Applied via `nautilus-network::RateLimiter`.
pub const SUBMIT_RATE_PER_SEC: u32 = 10;

/// Default `/orders` reconciler polling cadence.
pub const ORDER_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Adaptive upper bound for `/orders` polling on 429 throttling.
pub const ORDER_POLL_MAX_INTERVAL: Duration = Duration::from_secs(5);

/// Maximum WebSocket subscriptions per Kite connection.
///
/// The Kite docs cap a single ticker connection at 3000 instrument tokens; emit a
/// `SubscriptionLimitExceeded` error rather than silently truncating beyond this.
pub const WS_MAX_SUBSCRIPTIONS: usize = 3000;

/// Maximum tokens per subscribe / unsubscribe / mode frame on the ticker.
pub const WS_SUB_BATCH: usize = 200;

/// Gap between consecutive subscribe batches when fanning out a large initial subscription set.
pub const WS_SUB_BATCH_GAP: Duration = Duration::from_millis(500);

/// WebSocket reconnect backoff floor.
pub const WS_BACKOFF_INITIAL: Duration = Duration::from_secs(1);

/// WebSocket reconnect backoff ceiling.
pub const WS_BACKOFF_MAX: Duration = Duration::from_secs(60);

/// WebSocket reconnect backoff multiplier (1→60s at ×1.5 matches openalgo's loop).
pub const WS_BACKOFF_MULTIPLIER: f64 = 1.5;

/// Kite ticker keepalive cadence (server sends a heartbeat byte every 30 s).
pub const WS_PING_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum consecutive `ZerodhaSessionManager::rotate()` failures before declaring `auth_dead`.
pub const SESSION_ROTATION_RETRIES: u32 = 3;

/// CAS-guard window during which a concurrent `rotate()` call awaits the in-flight rotation
/// instead of triggering a duplicate provider invocation.
pub const SESSION_ROTATION_WINDOW: Duration = Duration::from_secs(5);

/// Bounded WebSocket event channel capacity (drop-oldest on overflow).
pub const WS_EVENT_CHANNEL_CAPACITY: usize = 4096;
