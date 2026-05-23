"""
Tests for nautilus_trader.adapters.thetadata.config.
"""

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.constants import (
    THETADATA_DEFAULT_HTTP_URL,
    THETADATA_DEFAULT_WS_URL,
)


class TestThetaDataDataClientConfig:
    def test_defaults(self):
        cfg = ThetaDataDataClientConfig()
        assert cfg.http_url == THETADATA_DEFAULT_HTTP_URL
        assert cfg.ws_url == THETADATA_DEFAULT_WS_URL
        assert cfg.tier == "value"
        assert cfg.http_timeout_secs == 60
        assert cfg.http_max_retries == 3
        assert cfg.http_rate_limit_per_sec == 5.0
        assert cfg.max_reconnects == 10
        assert cfg.reconnect_initial_backoff_secs == 1.0
        assert cfg.reconnect_max_backoff_secs == 30.0
        assert cfg.instrument_ids is None

    def test_override(self):
        cfg = ThetaDataDataClientConfig(
            http_url="http://localhost:9999",
            tier="pro",
            http_rate_limit_per_sec=10.0,
        )
        assert cfg.http_url == "http://localhost:9999"
        assert cfg.tier == "pro"
        assert cfg.http_rate_limit_per_sec == 10.0
        # untouched defaults preserved
        assert cfg.ws_url == THETADATA_DEFAULT_WS_URL

    def test_frozen(self):
        cfg = ThetaDataDataClientConfig()
        # msgspec.Struct frozen=True raises on attribute set
        import pytest
        with pytest.raises((AttributeError, Exception)):
            cfg.tier = "pro"  # type: ignore[misc]
