"""
ThetaData factory — builds a fresh ThetaDataDataClient set per Nautilus node.

No module-level singletons. aiohttp.ClientSession and websockets clients are
cheap to construct; sharing them across nodes invites loop-bound lifecycle
bugs (session bound to a dead loop, leaked tasks) for no real win. One node =
one client set = one teardown path.
"""

from __future__ import annotations

import asyncio

import websockets

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.data import ThetaDataDataClient
from nautilus_trader.adapters.thetadata.http import ThetaDataHttpClient
from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider
from nautilus_trader.adapters.thetadata.ws import ThetaDataWsClient, WsSocket
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory


class _WebsocketsAdapter(WsSocket):
    """Thin wrapper around `websockets` so ws.py sees the WsSocket protocol."""

    def __init__(self, ws):
        self._ws = ws

    async def send(self, msg: str) -> None:
        await self._ws.send(msg)

    async def recv(self) -> str:
        return await self._ws.recv()

    async def close(self) -> None:
        await self._ws.close()


async def _default_socket_factory(url: str) -> WsSocket:
    ws = await websockets.connect(url)
    return _WebsocketsAdapter(ws)


class ThetaDataLiveDataClientFactory(LiveDataClientFactory):
    """Construct a complete ThetaDataDataClient set."""

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: ThetaDataDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> ThetaDataDataClient:
        http_client = ThetaDataHttpClient(config=config)
        ws_client = ThetaDataWsClient(config=config, socket_factory=_default_socket_factory)
        provider = ThetaDataInstrumentProvider(http_client=http_client)
        return ThetaDataDataClient(
            loop=loop,
            http_client=http_client,
            ws_client=ws_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
