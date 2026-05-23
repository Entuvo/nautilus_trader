"""
ThetaData WebSocket client — live BBO + trade stream via local ThetaTerminal
(default ws://127.0.0.1:25520/v1/events).

Architecture:
  - Reader task: pulls raw frames off the socket, pushes onto an asyncio.Queue.
  - Processor task: pops frames, dispatches by header.type to registered handlers.
  - Reconnect supervisor: on disconnect, replays subscriptions in STREAMING/
    PENDING_SUB state; drops in-flight PENDING_UNSUB (server forgot them).
  - State machine per (instrument_id, kind):
        IDLE → PENDING_SUB → STREAMING → PENDING_UNSUB → IDLE
    Reconnect: STREAMING/PENDING_SUB → REPLAY → STREAMING; PENDING_UNSUB dropped.
  - Decode-precision map lives on ws_client (survives reconnect); processor reads
    it per frame and passes to decoder functions.

Task lifecycle is bounded by connect()/close(); no task escapes ws.py.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from enum import Enum

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.decode import (
    ws_quote_frame_to_quote_tick,
    ws_trade_frame_to_trade_tick,
)
from nautilus_trader.model.data import QuoteTick, TradeTick
from nautilus_trader.model.identifiers import InstrumentId


_log = logging.getLogger(__name__)


class ConnState(Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    CLOSED = "CLOSED"


class SubState(Enum):
    IDLE = "IDLE"
    PENDING_SUB = "PENDING_SUB"
    STREAMING = "STREAMING"
    PENDING_UNSUB = "PENDING_UNSUB"


class SubKind(Enum):
    QUOTE = "QUOTE"
    TRADE = "TRADE"


# Abstract socket protocol so tests can drive the client without standing up a
# real websocket server. The data_client wires the production socket via the
# `socket_factory` argument to connect().
class WsSocket:
    async def send(self, msg: str) -> None: ...
    async def recv(self) -> str: ...
    async def close(self) -> None: ...


SocketFactory = Callable[[str], Awaitable[WsSocket]]
QuoteHandler = Callable[[QuoteTick], None]
TradeHandler = Callable[[TradeTick], None]


class ThetaDataWsClient:
    """Live ThetaData stream client. See module docstring for state machine."""

    def __init__(self, config: ThetaDataDataClientConfig, socket_factory: SocketFactory):
        self._config = config
        self._socket_factory = socket_factory
        self._state: ConnState = ConnState.DISCONNECTED
        self._socket: WsSocket | None = None
        self._tasks: set[asyncio.Task] = set()
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=10_000)
        self._stop = asyncio.Event()

        # Subscription registry: (instrument_id, kind) -> state
        self._subs: dict[tuple[InstrumentId, SubKind], SubState] = {}
        # Last contract payload sent per (instrument_id, kind), for replay.
        self._contracts: dict[tuple[InstrumentId, SubKind], dict] = {}
        # Precision map populated by cache_instrument(); survives reconnect.
        self._precision_by_id: dict[InstrumentId, tuple[int, int]] = {}
        # Monotonic id counter for subscribe payloads.
        self._next_id = 0

        # Handlers — installed by ThetaDataDataClient.
        self._quote_handler: QuoteHandler | None = None
        self._trade_handler: TradeHandler | None = None

        # Metrics.
        self._decode_failures = 0
        self._dropped_frames = 0

    # -----------------------------------------------------------------------
    # Handler registration
    # -----------------------------------------------------------------------

    def set_quote_handler(self, handler: QuoteHandler) -> None:
        self._quote_handler = handler

    def set_trade_handler(self, handler: TradeHandler) -> None:
        self._trade_handler = handler

    def cache_instrument(self, instrument_id: InstrumentId, price_precision: int, size_precision: int) -> None:
        self._precision_by_id[instrument_id] = (price_precision, size_precision)

    # -----------------------------------------------------------------------
    # Connection lifecycle
    # -----------------------------------------------------------------------

    @property
    def state(self) -> ConnState:
        return self._state

    async def connect(self) -> None:
        if self._state in (ConnState.CONNECTED, ConnState.CONNECTING):
            return
        self._state = ConnState.CONNECTING
        try:
            self._socket = await self._socket_factory(self._config.ws_url)
        except Exception:
            self._state = ConnState.DISCONNECTED
            raise
        self._state = ConnState.CONNECTED
        self._stop.clear()
        self._spawn(self._reader_loop())
        self._spawn(self._processor_loop())

    async def close(self) -> None:
        self._state = ConnState.CLOSED
        self._stop.set()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                _log.warning("ThetaDataWsClient.close: tasks did not finish within 5s")
        self._tasks.clear()
        if self._socket is not None:
            try:
                await self._socket.close()
            except Exception:  # noqa: BLE001
                pass
        self._socket = None
        if self._decode_failures or self._dropped_frames:
            _log.info(
                "ThetaDataWsClient closed: decode_failures=%d dropped_frames=%d",
                self._decode_failures,
                self._dropped_frames,
            )

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _check_connected(self) -> None:
        if self._state != ConnState.CONNECTED:
            raise ConnectionError(
                f"ThetaDataWsClient not connected (state={self._state.value})"
            )

    # -----------------------------------------------------------------------
    # Subscribe / unsubscribe
    # -----------------------------------------------------------------------

    def _build_subscribe_payload(self, contract: dict, kind: SubKind, add: bool) -> dict:
        self._next_id += 1
        return {
            "msg_type": "STREAM",
            "sec_type": "OPTION",
            "req_type": kind.value,
            "add": add,
            "id": self._next_id,
            "contract": contract,
        }

    async def _send_subscribe(self, instrument_id: InstrumentId, kind: SubKind, contract: dict, add: bool) -> None:
        assert self._socket is not None
        payload = self._build_subscribe_payload(contract, kind, add)
        await self._socket.send(json.dumps(payload))

    async def subscribe_quotes(self, instrument_id: InstrumentId, contract: dict) -> None:
        self._check_connected()
        key = (instrument_id, SubKind.QUOTE)
        self._contracts[key] = contract
        self._subs[key] = SubState.PENDING_SUB
        await self._send_subscribe(instrument_id, SubKind.QUOTE, contract, add=True)
        # Without a separate ack path, promote PENDING_SUB → STREAMING on send.
        # Real ack would flow as REQ_RESPONSE; treating the send as good-enough
        # matches ThetaData's STREAM protocol (no per-stream ack guaranteed).
        self._subs[key] = SubState.STREAMING

    async def subscribe_trades(self, instrument_id: InstrumentId, contract: dict) -> None:
        self._check_connected()
        key = (instrument_id, SubKind.TRADE)
        self._contracts[key] = contract
        self._subs[key] = SubState.PENDING_SUB
        await self._send_subscribe(instrument_id, SubKind.TRADE, contract, add=True)
        self._subs[key] = SubState.STREAMING

    async def unsubscribe_quotes(self, instrument_id: InstrumentId) -> None:
        self._check_connected()
        key = (instrument_id, SubKind.QUOTE)
        contract = self._contracts.get(key)
        if contract is None:
            return
        self._subs[key] = SubState.PENDING_UNSUB
        await self._send_subscribe(instrument_id, SubKind.QUOTE, contract, add=False)
        self._subs[key] = SubState.IDLE
        self._contracts.pop(key, None)

    async def unsubscribe_trades(self, instrument_id: InstrumentId) -> None:
        self._check_connected()
        key = (instrument_id, SubKind.TRADE)
        contract = self._contracts.get(key)
        if contract is None:
            return
        self._subs[key] = SubState.PENDING_UNSUB
        await self._send_subscribe(instrument_id, SubKind.TRADE, contract, add=False)
        self._subs[key] = SubState.IDLE
        self._contracts.pop(key, None)

    # -----------------------------------------------------------------------
    # Reader / processor / reconnect
    # -----------------------------------------------------------------------

    async def _reader_loop(self) -> None:
        assert self._socket is not None
        try:
            while not self._stop.is_set():
                raw = await self._socket.recv()
                try:
                    frame = json.loads(raw) if isinstance(raw, str) else raw
                except Exception:  # noqa: BLE001
                    self._decode_failures += 1
                    continue
                try:
                    self._queue.put_nowait(frame)
                except asyncio.QueueFull:
                    self._dropped_frames += 1
                    _log.warning("ThetaDataWsClient reader queue full; dropping frame")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            _log.warning("ThetaDataWsClient reader loop ended: %r", exc)
            if not self._stop.is_set():
                self._spawn(self._reconnect_loop())

    async def _processor_loop(self) -> None:
        while not self._stop.is_set():
            try:
                frame = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            self._dispatch(frame)

    def _dispatch(self, frame: dict) -> None:
        header = frame.get("header") or {}
        ftype = header.get("type", "")
        if ftype == "QUOTE":
            self._handle_quote(frame)
        elif ftype == "TRADE":
            self._handle_trade(frame)
        elif ftype in ("OHLC", "STATUS", "STATE", "REQ_RESPONSE"):
            _log.debug("ThetaData WS metadata frame: %s", ftype)
        else:
            _log.debug("ThetaData WS unknown frame type: %s", ftype)

    def _instrument_for(self, frame: dict) -> InstrumentId | None:
        """Locate the instrument_id for a frame from the registry by contract match.

        Each subscribe call stored the exact contract dict under (instrument_id, kind);
        match on contract equality avoids re-parsing OCC on every tick.
        """
        contract = frame.get("contract")
        if not contract:
            return None
        for (instrument_id, _kind), stored in self._contracts.items():
            if stored == contract:
                return instrument_id
        return None

    def _handle_quote(self, frame: dict) -> None:
        if self._quote_handler is None:
            return
        instrument_id = self._instrument_for(frame)
        if instrument_id is None:
            self._decode_failures += 1
            return
        precision = self._precision_by_id.get(instrument_id)
        if precision is None:
            self._decode_failures += 1
            return
        price_precision, size_precision = precision
        try:
            tick = ws_quote_frame_to_quote_tick(
                frame, instrument_id, price_precision, size_precision,
                ts_init=self._wall_ns(),
            )
        except Exception as exc:  # noqa: BLE001
            self._decode_failures += 1
            _log.debug("Quote frame decode failure: %r", exc)
            return
        self._quote_handler(tick)

    def _handle_trade(self, frame: dict) -> None:
        if self._trade_handler is None:
            return
        instrument_id = self._instrument_for(frame)
        if instrument_id is None:
            self._decode_failures += 1
            return
        precision = self._precision_by_id.get(instrument_id)
        if precision is None:
            self._decode_failures += 1
            return
        price_precision, size_precision = precision
        try:
            tick = ws_trade_frame_to_trade_tick(
                frame, instrument_id, price_precision, size_precision,
                ts_init=self._wall_ns(),
            )
        except Exception as exc:  # noqa: BLE001
            self._decode_failures += 1
            _log.debug("Trade frame decode failure: %r", exc)
            return
        self._trade_handler(tick)

    @staticmethod
    def _wall_ns() -> int:
        # ts_init is the local arrival timestamp, by design (CLAUDE.md rule #2
        # scopes wall-clock prohibition to nautilus-formulas/src/).
        import time
        return time.time_ns()

    async def _reconnect_loop(self) -> None:
        if self._state in (ConnState.CLOSED, ConnState.RECONNECTING):
            return
        self._state = ConnState.RECONNECTING
        backoff = self._config.reconnect_initial_backoff_secs
        max_backoff = self._config.reconnect_max_backoff_secs
        for attempt in range(1, self._config.max_reconnects + 1):
            if self._stop.is_set():
                return
            try:
                self._socket = await self._socket_factory(self._config.ws_url)
            except Exception as exc:  # noqa: BLE001
                _log.warning("Reconnect attempt %d failed: %r", attempt, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue
            self._state = ConnState.CONNECTED
            # Replay subscriptions: STREAMING + PENDING_SUB only; drop PENDING_UNSUB.
            to_replay = [
                (key, contract) for key, contract in self._contracts.items()
                if self._subs.get(key) in (SubState.STREAMING, SubState.PENDING_SUB)
            ]
            for key, contract in to_replay:
                _instrument_id, kind = key
                try:
                    await self._send_subscribe(_instrument_id, kind, contract, add=True)
                    self._subs[key] = SubState.STREAMING
                except Exception as exc:  # noqa: BLE001
                    _log.warning("Replay subscribe %r failed: %r", key, exc)
            # Drop in-flight unsubs — server forgot them across disconnect.
            for key in list(self._subs):
                if self._subs[key] == SubState.PENDING_UNSUB:
                    self._subs[key] = SubState.IDLE
                    self._contracts.pop(key, None)
            # Re-spawn reader/processor on the new socket.
            self._spawn(self._reader_loop())
            self._spawn(self._processor_loop())
            return
        _log.error("Reconnect exhausted after %d attempts", self._config.max_reconnects)
        self._state = ConnState.DISCONNECTED
