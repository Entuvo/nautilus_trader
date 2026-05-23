"""
ThetaData v3 wire-format decoders. Pure functions: REST/WS row → Nautilus type.

Wire format: docs/architecture/notes/thetadata-wire-format.md.

Timestamps: v3 carries ISO 8601 strings ("2024-06-20T09:30:00.334") in ET local
time, no timezone suffix. Converted to UTC nanos via zoneinfo. Spring-forward
gap times raise; fall-back ambiguity resolved by caller via `fold` (default 0).

Aggressor: only OPRA condition codes 145 (Buyer) and 146 (Seller) produce an
inferred aggressor; everything else → NO_AGGRESSOR. R3 fills downstream.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from msgspec import DecodeError

from nautilus_trader.model.data import Bar, BarType, QuoteTick, TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import InstrumentId, TradeId
from nautilus_trader.model.objects import Price, Quantity


_NY = ZoneInfo("America/New_York")

# Cache: (date_yyyymmdd, fold) -> UTC nanos at midnight ET on that date.
_DATE_CACHE: dict[tuple[int, int], int] = {}

_AGGRESSOR_BUYER = 145
_AGGRESSOR_SELLER = 146


def _cached_date_base(date_yyyymmdd: int, fold: int = 0) -> int:
    """UTC nanos at midnight ET on `date_yyyymmdd`, cached per (date, fold)."""
    key = (date_yyyymmdd, fold)
    base = _DATE_CACHE.get(key)
    if base is not None:
        return base
    y = date_yyyymmdd // 10000
    m = (date_yyyymmdd // 100) % 100
    d = date_yyyymmdd % 100
    base = int(datetime(y, m, d, 0, 0, 0, tzinfo=_NY, fold=fold).timestamp() * 1_000_000_000)
    _DATE_CACHE[key] = base
    return base


def date_ms_to_unix_nanos(date_yyyymmdd: int, ms_of_day: int, fold: int = 0) -> int:
    """Convert ET-local (date, ms_of_day) to UTC nanos.

    Kept for backwards compat with the date-base cache and DST tests; v3 wire
    format uses ISO timestamps (see iso_ts_to_unix_nanos). Both paths share
    `_DATE_CACHE` and produce identical UTC nanos for the same wall-clock.
    """
    if not (0 <= ms_of_day < 86_400_000):
        raise DecodeError(f"ms_of_day out of range: {ms_of_day}")

    y = date_yyyymmdd // 10000
    m = (date_yyyymmdd // 100) % 100
    d = date_yyyymmdd % 100
    h = ms_of_day // 3_600_000
    mi = (ms_of_day // 60_000) % 60
    s = (ms_of_day // 1000) % 60
    us = (ms_of_day % 1000) * 1000

    local = datetime(y, m, d, h, mi, s, us, tzinfo=_NY, fold=fold)
    utc_nanos = int(local.timestamp() * 1_000_000_000)

    roundtrip = datetime.fromtimestamp(utc_nanos / 1_000_000_000, tz=_NY)
    if roundtrip.hour != h or roundtrip.day != d:
        raise DecodeError(
            f"Non-existent local time (DST spring-forward): date={date_yyyymmdd} ms={ms_of_day}"
        )

    key = (date_yyyymmdd, fold)
    if key not in _DATE_CACHE:
        _DATE_CACHE[key] = utc_nanos - (h * 3_600_000 + mi * 60_000 + s * 1000) * 1_000_000 - us * 1000

    return utc_nanos


def iso_ts_to_unix_nanos(iso_ts: str, fold: int = 0) -> int:
    """Convert a v3 ISO timestamp ("2024-06-20T09:30:00.334") in ET → UTC nanos.

    Raises DecodeError for DST spring-forward gap times.
    """
    # Format is "YYYY-MM-DDTHH:MM:SS" or "YYYY-MM-DDTHH:MM:SS.fff".
    try:
        if "." in iso_ts:
            naive = datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            naive = datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%S")
    except (TypeError, ValueError) as exc:
        raise DecodeError(f"Invalid ISO timestamp: {iso_ts!r}") from exc

    local = naive.replace(tzinfo=_NY, fold=fold)
    utc_nanos = int(local.timestamp() * 1_000_000_000)

    roundtrip = datetime.fromtimestamp(utc_nanos / 1_000_000_000, tz=_NY)
    if roundtrip.hour != naive.hour or roundtrip.day != naive.day:
        raise DecodeError(f"Non-existent local time (DST spring-forward): {iso_ts!r}")
    return utc_nanos


def _decimal_places(value: float | int) -> int:
    s = str(value)
    if "." in s:
        return len(s.rsplit(".", 1)[1])
    return 0


def _price_from_wire(value, price_precision: int) -> Price:
    """Convert a wire numeric value to a Price at the declared precision.

    Raises DecodeError on non-numeric input or precision drift.
    """
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise DecodeError(f"Invalid price (non-numeric): {value!r}")
    n = _decimal_places(value)
    if price_precision < n:
        raise DecodeError(
            f"Price precision mismatch (drops digits): wire={value!r} declared={price_precision}"
        )
    if price_precision not in (n, n + 1):
        raise DecodeError(
            f"Price precision mismatch: wire={value!r} (natural={n}) declared={price_precision}"
        )
    return Price(float(value), price_precision)


def _quantity_from_wire(value, size_precision: int) -> Quantity:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise DecodeError(f"Invalid size (non-numeric): {value!r}")
    if value < 0:
        raise DecodeError(f"Negative size: {value}")
    return Quantity(float(value), size_precision)


def _make_trade_id(iso_ts: str, sequence: int, exchange: int) -> TradeId:
    """Stable TradeId from wire fields. Same inputs → same id."""
    return TradeId(f"{iso_ts}-{sequence}-{exchange}")


# ---------------------------------------------------------------------------
# REST decoders (v3 named-field rows)
# ---------------------------------------------------------------------------


def rest_quote_row_to_quote_tick(
    row: dict,
    instrument_id: InstrumentId,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> QuoteTick:
    """Decode one v3 REST historical quote row → QuoteTick."""
    try:
        ts_event = iso_ts_to_unix_nanos(row["timestamp"])
        bid = _price_from_wire(row["bid"], price_precision)
        ask = _price_from_wire(row["ask"], price_precision)
        bid_size = _quantity_from_wire(row["bid_size"], size_precision)
        ask_size = _quantity_from_wire(row["ask_size"], size_precision)
    except KeyError as exc:
        raise DecodeError(f"REST quote row missing field: {exc}") from exc
    return QuoteTick(instrument_id, bid, ask, bid_size, ask_size, ts_event, ts_init)


def rest_trade_row_to_trade_tick(
    row: dict,
    instrument_id: InstrumentId,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> TradeTick:
    """Decode one v3 REST historical trade row → TradeTick."""
    try:
        iso = row["timestamp"]
        ts_event = iso_ts_to_unix_nanos(iso)
        price = _price_from_wire(row["price"], price_precision)
        size = _quantity_from_wire(row["size"], size_precision)
        condition = int(row.get("condition", -1))
        sequence = int(row.get("sequence", 0))
        exchange = int(row.get("exchange", 0))
    except KeyError as exc:
        raise DecodeError(f"REST trade row missing field: {exc}") from exc
    if condition == _AGGRESSOR_BUYER:
        aggressor = AggressorSide.BUYER
    elif condition == _AGGRESSOR_SELLER:
        aggressor = AggressorSide.SELLER
    else:
        aggressor = AggressorSide.NO_AGGRESSOR
    trade_id = _make_trade_id(iso, sequence, exchange)
    return TradeTick(instrument_id, price, size, aggressor, trade_id, ts_event, ts_init)


def rest_ohlc_row_to_bar(
    row: dict,
    bar_type: BarType,
    price_precision: int,
    ts_init: int,
) -> Bar:
    """Decode one v3 REST historical OHLC row → Bar. `vwap` is dropped (no Nautilus field)."""
    try:
        ts_event = iso_ts_to_unix_nanos(row["timestamp"])
        # v3 OHLC rows include zero-volume "padding" rows (open=high=low=close=0.0,
        # volume=0). Nautilus's Bar invariants don't accept that — high must be
        # > low, etc., and prices=0 violate the implicit non-negative tick.
        # Caller is responsible for filtering; emit the raw row here.
        open_ = _price_from_wire(row["open"], price_precision)
        high = _price_from_wire(row["high"], price_precision)
        low = _price_from_wire(row["low"], price_precision)
        close = _price_from_wire(row["close"], price_precision)
        volume = _quantity_from_wire(row["volume"], 0)
    except KeyError as exc:
        raise DecodeError(f"REST OHLC row missing field: {exc}") from exc
    return Bar(bar_type, open_, high, low, close, volume, ts_event, ts_init)


# ---------------------------------------------------------------------------
# WebSocket decoders (v3 frame shape)
# ---------------------------------------------------------------------------
#
# WS frame payload schema not fully documented for v3; the live-probe TODO in
# the wire-format note covers verification. These decoders assume the v2 shape
# with `quote.bid` / `trade.price` etc. — adjust here when live capture lands.


def ws_quote_frame_to_quote_tick(
    frame: dict,
    instrument_id: InstrumentId,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> QuoteTick:
    """Decode one WS QUOTE frame → QuoteTick."""
    try:
        quote = frame["quote"]
        bid = _price_from_wire(quote["bid"], price_precision)
        ask = _price_from_wire(quote["ask"], price_precision)
        bid_size = _quantity_from_wire(quote["bid_size"], size_precision)
        ask_size = _quantity_from_wire(quote["ask_size"], size_precision)
    except KeyError as exc:
        raise DecodeError(f"WS quote frame missing field: {exc}") from exc
    # v3 WS payloads either carry a `timestamp` ISO string or none at all;
    # fall back to ts_init for live (arrival) timestamp.
    ts_event = ts_init
    iso = quote.get("timestamp")
    if iso:
        try:
            ts_event = iso_ts_to_unix_nanos(iso)
        except DecodeError:
            ts_event = ts_init
    return QuoteTick(instrument_id, bid, ask, bid_size, ask_size, ts_event, ts_init)


def ws_trade_frame_to_trade_tick(
    frame: dict,
    instrument_id: InstrumentId,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> TradeTick:
    """Decode one WS TRADE frame → TradeTick."""
    try:
        trade = frame["trade"]
        price = _price_from_wire(trade["price"], price_precision)
        size = _quantity_from_wire(trade["size"], size_precision)
    except KeyError as exc:
        raise DecodeError(f"WS trade frame missing field: {exc}") from exc
    condition = trade.get("condition")
    if condition == _AGGRESSOR_BUYER:
        aggressor = AggressorSide.BUYER
    elif condition == _AGGRESSOR_SELLER:
        aggressor = AggressorSide.SELLER
    else:
        aggressor = AggressorSide.NO_AGGRESSOR
    iso = trade.get("timestamp") or ""
    sequence = int(trade.get("sequence") or 0)
    exchange = int(trade.get("exchange") or 0)
    trade_id = _make_trade_id(iso, sequence, exchange)
    ts_event = ts_init
    if iso:
        try:
            ts_event = iso_ts_to_unix_nanos(iso)
        except DecodeError:
            ts_event = ts_init
    return TradeTick(instrument_id, price, size, aggressor, trade_id, ts_event, ts_init)
