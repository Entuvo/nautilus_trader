"""
Tests for nautilus_trader.adapters.thetadata.factories.
"""

import asyncio

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.data import ThetaDataDataClient
from nautilus_trader.adapters.thetadata.factories import ThetaDataLiveDataClientFactory
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.model.identifiers import TraderId


def _msgbus_cache_clock():
    clock = LiveClock()
    trader_id = TraderId("TESTER-001")
    msgbus = MessageBus(trader_id=trader_id, clock=clock)
    cache = Cache()
    return msgbus, cache, clock


class TestThetaDataLiveDataClientFactory:
    def test_create_returns_data_client(self):
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = _msgbus_cache_clock()
        config = ThetaDataDataClientConfig()
        client = ThetaDataLiveDataClientFactory.create(
            loop=loop, name="THETADATA", config=config,
            msgbus=msgbus, cache=cache, clock=clock,
        )
        assert isinstance(client, ThetaDataDataClient)
        # No module-level singleton — two creates yield two clients.
        msgbus2, cache2, clock2 = _msgbus_cache_clock()
        client2 = ThetaDataLiveDataClientFactory.create(
            loop=loop, name="THETADATA", config=config,
            msgbus=msgbus2, cache=cache2, clock=clock2,
        )
        assert client is not client2

    def test_create_threads_config(self):
        loop = asyncio.new_event_loop()
        msgbus, cache, clock = _msgbus_cache_clock()
        config = ThetaDataDataClientConfig(http_url="http://localhost:99999", tier="pro")
        client = ThetaDataLiveDataClientFactory.create(
            loop=loop, name="THETADATA", config=config,
            msgbus=msgbus, cache=cache, clock=clock,
        )
        # Config flows into the http and ws subclients.
        assert client._http_client._config.tier == "pro"
        assert client._ws_client._config.http_url == "http://localhost:99999"
