# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  See LICENSE for full text.
# -------------------------------------------------------------------------------------------------
"""
Live factory facades for the Zerodha adapter.

Both factories share one ``PyZerodhaClient`` instance per `(http_url)` via a process-local
cache so the data + execution clients ride the same Kite WebSocket + REST session.
"""

from functools import lru_cache

from nautilus_trader.adapters.zerodha.config import ZerodhaDataClientConfig
from nautilus_trader.adapters.zerodha.config import ZerodhaExecClientConfig
from nautilus_trader.adapters.zerodha.constants import ZERODHA
from nautilus_trader.adapters.zerodha.data import ZerodhaDataClient
from nautilus_trader.adapters.zerodha.execution import ZerodhaExecutionClient
from nautilus_trader.adapters.zerodha.providers import ZerodhaInstrumentProvider
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId


@lru_cache(maxsize=4)
def _get_or_build_rust_client(
    http_url: str,
    ws_url: str,
    account_id: str | None,
    order_store_path: str | None,
) -> "nautilus_pyo3.zerodha.PyZerodhaClient":
    """Process-local cache so data + exec factories share one ``PyZerodhaClient``."""
    return nautilus_pyo3.zerodha.PyZerodhaClient(
        http_url=http_url,
        ws_url=ws_url,
        account_id=account_id,
        order_store_path=order_store_path,
    )


class ZerodhaLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a Zerodha live data client factory.
    """

    @staticmethod
    def create(  # type: ignore[override]
        loop,
        name,
        config: ZerodhaDataClientConfig,
        msgbus,
        cache,
        clock,
    ):
        rust_client = _get_or_build_rust_client(
            http_url=config.http_url,
            ws_url=config.ws_url,
            account_id=None,
            order_store_path=None,
        )
        provider = ZerodhaInstrumentProvider(config=config.instrument_provider)
        return ZerodhaDataClient(
            loop=loop,
            client_id=ClientId(name or ZERODHA),
            rust_client=rust_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )


class ZerodhaLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides a Zerodha live execution client factory.
    """

    @staticmethod
    def create(  # type: ignore[override]
        loop,
        name,
        config: ZerodhaExecClientConfig,
        msgbus,
        cache,
        clock,
    ):
        account_id = config.account_id or AccountId("ZERODHA-DEFAULT")
        rust_client = _get_or_build_rust_client(
            http_url=config.http_url,
            ws_url="wss://ws.kite.trade",  # exec doesn't open WS but the cache key includes it
            account_id=str(account_id),
            order_store_path=str(config.order_store_path) if config.order_store_path else None,
        )
        provider = ZerodhaInstrumentProvider(config=config.instrument_provider)
        return ZerodhaExecutionClient(
            loop=loop,
            client_id=ClientId(name or ZERODHA),
            account_id=account_id,
            rust_client=rust_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
