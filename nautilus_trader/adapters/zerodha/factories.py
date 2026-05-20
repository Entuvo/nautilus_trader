# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  See LICENSE for full text.
# -------------------------------------------------------------------------------------------------
"""
Live factory facades for the Zerodha adapter.

Each ``create()`` returns a Python ``LiveMarketDataClient`` / ``LiveExecutionClient``
subclass instance (the framework's data + exec engines drive these via msgbus).
"""

from nautilus_trader.adapters.zerodha.config import ZerodhaDataClientConfig
from nautilus_trader.adapters.zerodha.config import ZerodhaExecClientConfig
from nautilus_trader.adapters.zerodha.constants import ZERODHA
from nautilus_trader.adapters.zerodha.data import ZerodhaDataClient
from nautilus_trader.adapters.zerodha.execution import ZerodhaExecutionClient
from nautilus_trader.adapters.zerodha.providers import ZerodhaInstrumentProvider
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId


class ZerodhaLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a Zerodha live data client factory.
    """

    @staticmethod
    def create(  # type: ignore[override]
        loop,
        name,
        config,
        msgbus,
        cache,
        clock,
    ):
        provider = ZerodhaInstrumentProvider(config=config.instrument_provider)
        return ZerodhaDataClient(
            loop=loop,
            client_id=ClientId(name or ZERODHA),
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
        config,
        msgbus,
        cache,
        clock,
    ):
        provider = ZerodhaInstrumentProvider(config=config.instrument_provider)
        account_id = config.account_id or AccountId("ZERODHA-DEFAULT")
        return ZerodhaExecutionClient(
            loop=loop,
            client_id=ClientId(name or ZERODHA),
            account_id=account_id,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
