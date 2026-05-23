"""
ThetaData wire-format decoders. Pure functions: REST rows / WS frames → Nautilus types.

Timestamps: REST/WS payloads carry ET-local (date, ms_of_day); converted to UTC nanos
via zoneinfo (America/New_York). Spring-forward gap times raise; fall-back ambiguity
resolved by the caller via the `fold` argument (default 0 = earlier UTC).

Aggressor: only OPRA condition codes 145 (Buyer) and 146 (Seller) produce an inferred
aggressor; everything else → NO_AGGRESSOR. R3 fills downstream from the contemporaneous
NBBO. This layer never invents an aggressor.
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
# Speeds up the hot path: most ticks for a session share the same date.
_DATE_CACHE: dict[tuple[int, int], int] = {}

# OPRA condition codes that imply aggressor side. Everything else → NO_AGGRESSOR
# and the R3 inferer fills downstream from the contemporaneous NBBO.
_AGGRESSOR_BUYER = 145
_AGGRESSOR_SELLER = 146


def _cached_date_base(date_yyyymmdd: int, fold: int = 0) -> int:
    """Return UTC nanos at midnight ET on `date_yyyymmdd`, cached per (date, fold)."""
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

    Raises DecodeError for non-existent times (DST spring-forward gap). Ambiguous
    times during DST fall-back resolve via `fold` (0 = earlier UTC, 1 = later UTC).
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

    # Round-trip check: spring-forward gap times don't exist in local — they get
    # silently bumped to the next valid wall-clock. Catch that by comparing the
    # round-tripped hour.
    roundtrip = datetime.fromtimestamp(utc_nanos / 1_000_000_000, tz=_NY)
    if roundtrip.hour != h or roundtrip.day != d:
        raise DecodeError(
            f"Non-existent local time (DST spring-forward): date={date_yyyymmdd} ms={ms_of_day}"
        )

    # Prime the date cache so subsequent calls for the same date are fast.
    key = (date_yyyymmdd, fold)
    if key not in _DATE_CACHE:
        _DATE_CACHE[key] = utc_nanos - (h * 3_600_000 + mi * 60_000 + s * 1000) * 1_000_000 - us * 1000

    return utc_nanos


def _decimal_places(value: float | int) -> int:
    """Count decimal places in str(value)."""
    s = str(value)
    if "." in s:
        return len(s.rsplit(".", 1)[1])
    return 0


def _price_from_wire(value, price_precision: int) -> Price:
    """Convert a wire numeric value to a Price at the declared precision.

    Raises DecodeError on non-numeric input or when the declared precision is too
    coarse for the wire value (would discard fractional digits) or too far above
    the natural precision of the wire value (config drift).
    """
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise DecodeError(f"Invalid price (non-numeric): {value!r}")
    n = _decimal_places(value)
    # Allow declared precision to equal the wire's str precision, or be exactly one
    # greater (covers OPRA cents where wire "5.4" is meant as 5.40, trailing zero
    # trimmed by Python float repr). More than that is config drift; less drops
    # significant digits.
    if price_precision not in (n, n + 1) and price_precision < n:
        raise DecodeError(
            f"Price precision mismatch (drops digits): wire={value!r} declared={price_precision}"
        )
    if price_precision not in (n, n + 1):
        raise DecodeError(
            f"Price precision mismatch: wire={value!r} (natural={n}) declared={price_precision}"
        )
    return Price(float(value), price_precision)


def _quantity_from_wire(value, size_precision: int) -> Quantity:
    """Convert a wire numeric value to a Quantity. Rejects negative sizes."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise DecodeError(f"Invalid size (non-numeric): {value!r}")
    if value < 0:
        raise DecodeError(f"Negative size: {value}")
    return Quantity(float(value), size_precision)


def _make_trade_id(date_yyyymmdd: int, ms_of_day: int, sequence: int, exchange: int) -> TradeId:
    """Build a stable TradeId from wire fields. Same inputs → same id."""
    # Use sequence first (OPRA-assigned, can be negative due to wrap) plus the
    # date+ms+exchange tuple to disambiguate across venues and sequence collisions.
    return TradeId(f"{date_yyyymmdd}-{ms_of_day}-{sequence}-{exchange}")


# ---------------------------------------------------------------------------
# REST decoders
# ---------------------------------------------------------------------------

# Row index constants — wire format documented in
# docs/architecture/notes/thetadata-wire-format.md.

_Q_MS, _Q_BSZ, _Q_BX, _Q_BID, _Q_BC, _Q_ASZ, _Q_AX, _Q_ASK, _Q_AC, _Q_DATE = range(10)
_T_MS, _T_SEQ, _T_SIZE, _T_COND, _T_EX, _T_PRICE, _T_DATE = range(7)
_O_MS, _O_OPEN, _O_HIGH, _O_LOW, _O_CLOSE, _O_VOL, _O_COUNT, _O_DATE = range(8)


def rest_quote_row_to_quote_tick(
    row: list,
    instrument_id: InstrumentId,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> QuoteTick:
    """Decode one REST historical quote row → QuoteTick."""
    if len(row) < 10:
        raise DecodeError(f"REST quote row too short: len={len(row)}")
    bid = _price_from_wire(row[_Q_BID], price_precision)
    ask = _price_from_wire(row[_Q_ASK], price_precision)
    bid_size = _quantity_from_wire(row[_Q_BSZ], size_precision)
    ask_size = _quantity_from_wire(row[_Q_ASZ], size_precision)
    ts_event = date_ms_to_unix_nanos(int(row[_Q_DATE]), int(row[_Q_MS]))
    return QuoteTick(instrument_id, bid, ask, bid_size, ask_size, ts_event, ts_init)


def rest_trade_row_to_trade_tick(
    row: list,
    instrument_id: InstrumentId,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> TradeTick:
    """Decode one REST historical trade row → TradeTick."""
    if len(row) < 7:
        raise DecodeError(f"REST trade row too short: len={len(row)}")
    price = _price_from_wire(row[_T_PRICE], price_precision)
    size = _quantity_from_wire(row[_T_SIZE], size_precision)
    condition = int(row[_T_COND])
    if condition == _AGGRESSOR_BUYER:
        aggressor = AggressorSide.BUYER
    elif condition == _AGGRESSOR_SELLER:
        aggressor = AggressorSide.SELLER
    else:
        aggressor = AggressorSide.NO_AGGRESSOR
    trade_id = _make_trade_id(
        int(row[_T_DATE]), int(row[_T_MS]), int(row[_T_SEQ]), int(row[_T_EX])
    )
    ts_event = date_ms_to_unix_nanos(int(row[_T_DATE]), int(row[_T_MS]))
    return TradeTick(instrument_id, price, size, aggressor, trade_id, ts_event, ts_init)


def rest_ohlc_row_to_bar(
    row: list,
    bar_type: BarType,
    price_precision: int,
    ts_init: int,
) -> Bar:
    """Decode one REST historical OHLC row → Bar."""
    if len(row) < 8:
        raise DecodeError(f"REST OHLC row too short: len={len(row)}")
    open_ = _price_from_wire(row[_O_OPEN], price_precision)
    high = _price_from_wire(row[_O_HIGH], price_precision)
    low = _price_from_wire(row[_O_LOW], price_precision)
    close = _price_from_wire(row[_O_CLOSE], price_precision)
    volume = _quantity_from_wire(row[_O_VOL], 0)
    ts_event = date_ms_to_unix_nanos(int(row[_O_DATE]), int(row[_O_MS]))
    return Bar(bar_type, open_, high, low, close, volume, ts_event, ts_init)


# ---------------------------------------------------------------------------
# WebSocket decoders
# ---------------------------------------------------------------------------


def _ws_ts_event(payload: dict, contract: dict, ts_init: int) -> int:
    """Derive ts_event from a WS payload's date+ms_of_day, falling back to ts_init.

    Live streams typically carry `date` in the per-event payload. If absent, fall
    back to ts_init (local arrival) — better than a fabricated date.
    """
    date = payload.get("date")
    ms = payload.get("ms_of_day")
    if date and ms is not None:
        return date_ms_to_unix_nanos(int(date), int(ms))
    return ts_init


def ws_quote_frame_to_quote_tick(
    frame: dict,
    instrument_id: InstrumentId,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> QuoteTick:
    """Decode one WS QUOTE frame → QuoteTick."""
    quote = frame["quote"]
    contract = frame.get("contract", {})
    bid = _price_from_wire(quote["bid"], price_precision)
    ask = _price_from_wire(quote["ask"], price_precision)
    bid_size = _quantity_from_wire(quote["bid_size"], size_precision)
    ask_size = _quantity_from_wire(quote["ask_size"], size_precision)
    ts_event = _ws_ts_event(quote, contract, ts_init)
    return QuoteTick(instrument_id, bid, ask, bid_size, ask_size, ts_event, ts_init)


def ws_trade_frame_to_trade_tick(
    frame: dict,
    instrument_id: InstrumentId,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> TradeTick:
    """Decode one WS TRADE frame → TradeTick."""
    trade = frame["trade"]
    contract = frame.get("contract", {})
    price = _price_from_wire(trade["price"], price_precision)
    size = _quantity_from_wire(trade["size"], size_precision)
    condition = trade.get("condition")
    if condition == _AGGRESSOR_BUYER:
        aggressor = AggressorSide.BUYER
    elif condition == _AGGRESSOR_SELLER:
        aggressor = AggressorSide.SELLER
    else:
        aggressor = AggressorSide.NO_AGGRESSOR
    date = trade.get("date") or 0
    ms = trade.get("ms_of_day") or 0
    sequence = trade.get("sequence") or 0
    exchange = trade.get("exchange") or 0
    trade_id = _make_trade_id(int(date), int(ms), int(sequence), int(exchange))
    ts_event = _ws_ts_event(trade, contract, ts_init)
    return TradeTick(instrument_id, price, size, aggressor, trade_id, ts_event, ts_init)
