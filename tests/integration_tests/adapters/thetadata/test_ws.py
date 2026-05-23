"""
Tests for nautilus_trader.adapters.thetadata.ws (ThetaDataWsClient).

Uses an in-process FakeSocket that the test drives directly — no real WS
server. The socket's send() captures payloads; recv() yields scripted frames
from a queue. Reconnect is exercised by injecting a fresh fake socket via
socket_factory and triggering reconnect via _reconnect_loop().
"""

import asyncio
import json

import pytest

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.ws import (
    ConnState,
    SubKind,
    SubState,
    ThetaDataWsClient,
    WsSocket,
)
from nautilus_trader.model.identifiers import InstrumentId


_INSTRUMENT_ID = InstrumentId.from_str("AAPL231110P00360000.OPT-THETADATA")
_CONTRACT = {
    "security_type": "OPTION",
    "root": "AAPL",
    "expiration": 20240621,
    "strike": 1750000,  # $175.00 in 1/10¢ (streaming uses v2-style scaling)
    "right": "C",
}


class FakeSocket(WsSocket):
    """In-process scriptable socket. recv() pops from a queue; send() captures."""

    def __init__(self):
        self.sent: list[str] = []
        self._recv: asyncio.Queue[str] = asyncio.Queue()
        self.closed = False

    async def send(self, msg: str) -> None:
        self.sent.append(msg)

    async def recv(self) -> str:
        return await self._recv.get()

    async def close(self) -> None:
        self.closed = True

    def push(self, frame: dict) -> None:
        self._recv.put_nowait(json.dumps(frame))


def _make_client_with(socket: FakeSocket, **overrides):
    cfg = ThetaDataDataClientConfig(
        ws_url="ws://test/v1/events",
        reconnect_initial_backoff_secs=0.01,
        reconnect_max_backoff_secs=0.05,
        max_reconnects=3,
        **overrides,
    )

    async def factory(url: str) -> WsSocket:
        return socket

    return ThetaDataWsClient(cfg, factory)


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------


class TestConnect:
    @pytest.mark.asyncio
    async def test_connect_transitions_to_connected(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        await client.connect()
        assert client.state == ConnState.CONNECTED
        await client.close()
        assert client.state == ConnState.CLOSED
        assert sock.closed

    @pytest.mark.asyncio
    async def test_connect_failure_leaves_disconnected(self):
        cfg = ThetaDataDataClientConfig(ws_url="ws://test/v1/events")

        async def factory(url: str):
            raise OSError("refused")

        client = ThetaDataWsClient(cfg, factory)
        with pytest.raises(OSError, match="refused"):
            await client.connect()
        assert client.state == ConnState.DISCONNECTED


# ---------------------------------------------------------------------------
# State guards
# ---------------------------------------------------------------------------


class TestStateGuards:
    @pytest.mark.asyncio
    async def test_subscribe_quotes_before_connect_raises(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        with pytest.raises(ConnectionError, match="not connected"):
            await client.subscribe_quotes(_INSTRUMENT_ID, _CONTRACT)

    @pytest.mark.asyncio
    async def test_subscribe_trades_before_connect_raises(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        with pytest.raises(ConnectionError, match="not connected"):
            await client.subscribe_trades(_INSTRUMENT_ID, _CONTRACT)


# ---------------------------------------------------------------------------
# Subscribe payload + state transitions
# ---------------------------------------------------------------------------


class TestSubscribePayloads:
    @pytest.mark.asyncio
    async def test_subscribe_quote_payload(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        await client.connect()
        await client.subscribe_quotes(_INSTRUMENT_ID, _CONTRACT)
        assert len(sock.sent) == 1
        payload = json.loads(sock.sent[0])
        # v3 streaming shape per docs/architecture/notes/thetadata-wire-format.md
        assert payload["action"] == "subscribe"
        assert payload["stream"] == "quote"
        assert payload["symbols"] == ["AAPL"]
        assert payload["contract"] == _CONTRACT
        assert payload["id"] == 1
        assert client._subs[(_INSTRUMENT_ID, SubKind.QUOTE)] == SubState.STREAMING
        await client.close()

    @pytest.mark.asyncio
    async def test_subscribe_increments_id(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        await client.connect()
        await client.subscribe_quotes(_INSTRUMENT_ID, _CONTRACT)
        await client.subscribe_trades(_INSTRUMENT_ID, _CONTRACT)
        ids = [json.loads(p)["id"] for p in sock.sent]
        assert ids == [1, 2]
        await client.close()

    @pytest.mark.asyncio
    async def test_unsubscribe_quotes_transitions_to_idle(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        await client.connect()
        await client.subscribe_quotes(_INSTRUMENT_ID, _CONTRACT)
        await client.unsubscribe_quotes(_INSTRUMENT_ID)
        payloads = [json.loads(p) for p in sock.sent]
        assert payloads[-1]["action"] == "unsubscribe"
        assert client._subs[(_INSTRUMENT_ID, SubKind.QUOTE)] == SubState.IDLE
        await client.close()


# ---------------------------------------------------------------------------
# Frame dispatch
# ---------------------------------------------------------------------------


class TestFrameDispatch:
    @pytest.mark.asyncio
    async def test_quote_frame_dispatched_to_handler(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        ticks = []
        client.set_quote_handler(ticks.append)
        await client.connect()
        client.cache_instrument(_INSTRUMENT_ID, price_precision=2, size_precision=0)
        await client.subscribe_quotes(_INSTRUMENT_ID, _CONTRACT)
        sock.push({
            "header": {"status": "CONNECTED", "type": "QUOTE"},
            "contract": _CONTRACT,
            "quote": {"timestamp": "2024-06-20T09:30:01.000",
                      "bid_size": 42, "bid": 1.25, "ask_size": 30, "ask": 1.35},
        })
        # Yield to processor
        await asyncio.sleep(0.05)
        assert len(ticks) == 1
        assert str(ticks[0].bid_price) == "1.25"
        await client.close()

    @pytest.mark.asyncio
    async def test_trade_frame_dispatched_to_handler(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        ticks = []
        client.set_trade_handler(ticks.append)
        await client.connect()
        client.cache_instrument(_INSTRUMENT_ID, price_precision=2, size_precision=0)
        await client.subscribe_trades(_INSTRUMENT_ID, _CONTRACT)
        sock.push({
            "header": {"status": "CONNECTED", "type": "TRADE"},
            "contract": _CONTRACT,
            "trade": {"timestamp": "2024-06-20T09:30:00.334", "sequence": -1,
                      "size": 5, "condition": 145, "price": 1.06, "exchange": 65},
        })
        await asyncio.sleep(0.05)
        assert len(ticks) == 1
        assert str(ticks[0].price) == "1.06"
        await client.close()

    @pytest.mark.asyncio
    async def test_metadata_frame_logged_only(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        ticks = []
        client.set_quote_handler(ticks.append)
        await client.connect()
        # OHLC/STATUS/STATE/REQ_RESPONSE — none should reach the handler.
        for ftype in ("OHLC", "STATUS", "STATE", "REQ_RESPONSE"):
            sock.push({"header": {"type": ftype}, "contract": _CONTRACT})
        await asyncio.sleep(0.05)
        assert ticks == []
        await client.close()

    @pytest.mark.asyncio
    async def test_decode_failure_increments_counter(self):
        sock = FakeSocket()
        client = _make_client_with(sock)
        ticks = []
        client.set_quote_handler(ticks.append)
        await client.connect()
        client.cache_instrument(_INSTRUMENT_ID, price_precision=2, size_precision=0)
        await client.subscribe_quotes(_INSTRUMENT_ID, _CONTRACT)
        # Malformed: missing "bid"
        sock.push({
            "header": {"type": "QUOTE"},
            "contract": _CONTRACT,
            "quote": {"timestamp": "2024-06-20T09:30:01.000",
                      "bid_size": 42, "ask_size": 30, "ask": 1.35},
        })
        await asyncio.sleep(0.05)
        assert client._decode_failures >= 1
        assert ticks == []
        await client.close()


# ---------------------------------------------------------------------------
# Reconnect-replay
# ---------------------------------------------------------------------------


class TestReconnect:
    @pytest.mark.asyncio
    async def test_replay_resubscribes_streaming_entries(self):
        sock1 = FakeSocket()
        sock2 = FakeSocket()
        sockets = iter([sock1, sock2])

        cfg = ThetaDataDataClientConfig(
            ws_url="ws://test/v1/events",
            reconnect_initial_backoff_secs=0.01,
            reconnect_max_backoff_secs=0.05,
            max_reconnects=3,
        )

        async def factory(url: str) -> WsSocket:
            return next(sockets)

        client = ThetaDataWsClient(cfg, factory)
        await client.connect()
        client.cache_instrument(_INSTRUMENT_ID, 2, 0)
        await client.subscribe_quotes(_INSTRUMENT_ID, _CONTRACT)
        assert len(sock1.sent) == 1

        # Trigger reconnect.
        await client._reconnect_loop()
        # The new socket should have received the replayed subscribe.
        assert len(sock2.sent) == 1
        payload = json.loads(sock2.sent[0])
        assert payload["action"] == "subscribe"
        assert payload["contract"] == _CONTRACT
        assert client._subs[(_INSTRUMENT_ID, SubKind.QUOTE)] == SubState.STREAMING
        # Precision map survived reconnect.
        assert _INSTRUMENT_ID in client._precision_by_id
        await client.close()

    @pytest.mark.asyncio
    async def test_pending_unsub_dropped_on_reconnect(self):
        sock1 = FakeSocket()
        sock2 = FakeSocket()
        sockets = iter([sock1, sock2])

        cfg = ThetaDataDataClientConfig(
            ws_url="ws://test/v1/events",
            reconnect_initial_backoff_secs=0.01,
            reconnect_max_backoff_secs=0.05,
            max_reconnects=3,
        )

        async def factory(url: str) -> WsSocket:
            return next(sockets)

        client = ThetaDataWsClient(cfg, factory)
        await client.connect()
        await client.subscribe_quotes(_INSTRUMENT_ID, _CONTRACT)
        # Simulate PENDING_UNSUB state directly (race between unsub send and disconnect).
        client._subs[(_INSTRUMENT_ID, SubKind.QUOTE)] = SubState.PENDING_UNSUB

        await client._reconnect_loop()
        # No replay subscribe sent on sock2 for PENDING_UNSUB entries.
        assert sock2.sent == []
        assert client._subs[(_INSTRUMENT_ID, SubKind.QUOTE)] == SubState.IDLE
        await client.close()
