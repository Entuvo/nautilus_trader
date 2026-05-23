from __future__ import annotations

import msgspec

from nautilus_trader.live.config import LiveDataClientConfig


class ThetaDataDataClientConfig(LiveDataClientConfig, frozen=True, kw_only=True):
    http_url: str = msgspec.field(default="http://127.0.0.1:25503")
    ws_url: str = msgspec.field(default="ws://127.0.0.1:25520/v1/events")
    tier: str = msgspec.field(default="value")
    http_timeout_secs: int = msgspec.field(default=60)
    http_max_retries: int = msgspec.field(default=3)
    http_rate_limit_per_sec: float = msgspec.field(default=5.0)
    max_reconnects: int = msgspec.field(default=10)
    reconnect_initial_backoff_secs: float = msgspec.field(default=1.0)
    reconnect_max_backoff_secs: float = msgspec.field(default=30.0)
    instrument_ids: list | None = msgspec.field(default=None)
