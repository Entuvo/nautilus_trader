import pytest

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig


def test_config_defaults():
    config = ThetaDataDataClientConfig()
    assert config.http_url == "http://127.0.0.1:25510"
    assert config.ws_url == "ws://127.0.0.1:25520/v1/events"
    assert config.tier == "value"
    assert config.http_timeout_secs == 60
    assert config.http_max_retries == 3
    assert config.http_rate_limit_per_sec == 5.0
    assert config.max_reconnects == 10
    assert config.reconnect_initial_backoff_secs == 1.0
    assert config.reconnect_max_backoff_secs == 30.0
    assert config.instrument_ids is None


def test_config_override():
    config = ThetaDataDataClientConfig(
        http_url="http://localhost:9999",
        tier="pro",
        http_timeout_secs=120,
    )
    assert config.http_url == "http://localhost:9999"
    assert config.tier == "pro"
    assert config.http_timeout_secs == 120
    # Defaults preserved
    assert config.ws_url == "ws://127.0.0.1:25520/v1/events"
