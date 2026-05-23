"""
Tests for nautilus_trader.adapters.thetadata.decode (v3 wire format).
"""

import json
import time
from pathlib import Path

import pytest
from msgspec import DecodeError

from nautilus_trader.adapters.thetadata.decode import (
    _cached_date_base,
    _DATE_CACHE,
    _make_trade_id,
    _price_from_wire,
    _quantity_from_wire,
    date_ms_to_unix_nanos,
    iso_ts_to_unix_nanos,
    rest_ohlc_row_to_bar,
    rest_quote_row_to_quote_tick,
    rest_trade_row_to_trade_tick,
    ws_quote_frame_to_quote_tick,
    ws_trade_frame_to_trade_tick,
)
from nautilus_trader.model.data import (
    Bar,
    BarAggregation,
    BarSpecification,
    BarType,
    QuoteTick,
    TradeTick,
)
from nautilus_trader.model.enums import AggressorSide, PriceType
from nautilus_trader.model.identifiers import InstrumentId, TradeId
from nautilus_trader.model.objects import Price, Quantity


_FIXTURES_DIR = Path(__file__).parent / "fixtures"
_INSTRUMENT_ID = InstrumentId.from_str("AAPL240621C00175000.OPT-THETADATA")


@pytest.fixture
def bar_type():
    spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
    return BarType(_INSTRUMENT_ID, spec)


# ---------------------------------------------------------------------------
# date_ms_to_unix_nanos — legacy helper, still used for DST tests
# ---------------------------------------------------------------------------


class TestDateMsToUnixNanos:
    def test_normal_day_midnight(self):
        result = date_ms_to_unix_nanos(20231103, 0)
        assert 1_600_000_000_000_000_000 < result < 1_800_000_000_000_000_000

    def test_cache_hit_reuses_base(self):
        _DATE_CACHE.clear()
        r1 = date_ms_to_unix_nanos(20231103, 3600000)
        r2 = date_ms_to_unix_nanos(20231103, 7200000)
        assert r2 - r1 == 3_600_000_000_000

    def test_dst_spring_forward_raises(self):
        with open(_FIXTURES_DIR / "dst_spring_forward_2025.json") as f:
            fixture = json.load(f)
        for ms in fixture["non_existent_ms_of_day"]:
            with pytest.raises(DecodeError, match="Non-existent"):
                date_ms_to_unix_nanos(fixture["date"], ms)

    def test_dst_spring_forward_boundary(self):
        with open(_FIXTURES_DIR / "dst_spring_forward_2025.json") as f:
            fixture = json.load(f)
        for ms in fixture["valid_ms_of_day"]:
            assert date_ms_to_unix_nanos(fixture["date"], ms) > 0

    def test_dst_fall_back_fold(self):
        with open(_FIXTURES_DIR / "dst_fall_back_2025.json") as f:
            fixture = json.load(f)
        for ms in fixture["ambiguous_ms_of_day"]:
            r0 = date_ms_to_unix_nanos(fixture["date"], ms, fold=0)
            r1 = date_ms_to_unix_nanos(fixture["date"], ms, fold=1)
            assert r1 - r0 == 3_600_000_000_000


# ---------------------------------------------------------------------------
# iso_ts_to_unix_nanos — v3 timestamp path
# ---------------------------------------------------------------------------


class TestIsoTsToUnixNanos:
    def test_normal_day_with_ms(self):
        result = iso_ts_to_unix_nanos("2024-06-20T09:30:00.334")
        assert result > 0

    def test_normal_day_no_ms(self):
        result = iso_ts_to_unix_nanos("2024-06-20T09:30:00")
        assert result > 0

    def test_diff_is_exact_for_normal_days(self):
        a = iso_ts_to_unix_nanos("2024-06-20T09:30:00.000")
        b = iso_ts_to_unix_nanos("2024-06-20T09:30:01.000")
        assert b - a == 1_000_000_000

    def test_dst_spring_forward_raises(self):
        # 2025-03-09 02:30 ET doesn't exist.
        with pytest.raises(DecodeError, match="Non-existent"):
            iso_ts_to_unix_nanos("2025-03-09T02:30:00.000")

    def test_dst_fall_back_fold(self):
        # 2025-11-02 01:30 ET is ambiguous; fold=0 earlier, fold=1 later.
        r0 = iso_ts_to_unix_nanos("2025-11-02T01:30:00.000", fold=0)
        r1 = iso_ts_to_unix_nanos("2025-11-02T01:30:00.000", fold=1)
        assert r1 - r0 == 3_600_000_000_000

    def test_invalid_format(self):
        with pytest.raises(DecodeError, match="Invalid ISO"):
            iso_ts_to_unix_nanos("not-a-timestamp")


# ---------------------------------------------------------------------------
# REST quote — v3 named-field row
# ---------------------------------------------------------------------------


class TestRestQuoteRowToQuoteTick:
    def _make_row(self):
        return {
            "timestamp": "2024-06-20T09:30:01.000",
            "bid_size": 5, "bid_exchange": 46, "bid": 39.00, "bid_condition": 50,
            "ask_size": 1, "ask_exchange": 46, "ask": 39.70, "ask_condition": 50,
        }

    def test_valid_row(self):
        row = self._make_row()
        tick = rest_quote_row_to_quote_tick(
            row, _INSTRUMENT_ID, price_precision=2, size_precision=0,
            ts_init=1700000000000000000,
        )
        assert isinstance(tick, QuoteTick)
        assert tick.bid_price == Price.from_str("39.00")
        assert tick.ask_price == Price.from_str("39.70")
        assert tick.bid_size == Quantity.from_str("5")
        assert tick.ask_size == Quantity.from_str("1")
        assert tick.ts_event > 0

    def test_missing_field_raises(self):
        row = self._make_row()
        del row["bid"]
        with pytest.raises(DecodeError, match="missing field"):
            rest_quote_row_to_quote_tick(row, _INSTRUMENT_ID, 2, 0, 0)

    def test_negative_size_raises(self):
        row = self._make_row()
        row["bid_size"] = -1
        with pytest.raises(DecodeError, match="Negative"):
            rest_quote_row_to_quote_tick(row, _INSTRUMENT_ID, 2, 0, 0)

    def test_precision_mismatch_raises(self):
        row = self._make_row()
        with pytest.raises(DecodeError, match="precision mismatch"):
            rest_quote_row_to_quote_tick(row, _INSTRUMENT_ID, price_precision=4, size_precision=0, ts_init=0)


# ---------------------------------------------------------------------------
# REST trade — v3 named-field row
# ---------------------------------------------------------------------------


class TestRestTradeRowToTradeTick:
    def _make_row(self, condition=18):
        return {
            "timestamp": "2024-06-20T09:30:00.334",
            "sequence": 1332218205,
            "size": 1, "price": 39.03,
            "condition": condition,
            "exchange": 1,
        }

    def test_valid_row(self):
        tick = rest_trade_row_to_trade_tick(
            self._make_row(), _INSTRUMENT_ID, 2, 0, 1700000000000000000
        )
        assert isinstance(tick, TradeTick)
        assert tick.price == Price.from_str("39.03")
        assert tick.size == Quantity.from_str("1")
        assert tick.aggressor_side == AggressorSide.NO_AGGRESSOR

    def test_aggressor_buyer(self):
        tick = rest_trade_row_to_trade_tick(
            self._make_row(condition=145), _INSTRUMENT_ID, 2, 0, 0
        )
        assert tick.aggressor_side == AggressorSide.BUYER

    def test_aggressor_seller(self):
        tick = rest_trade_row_to_trade_tick(
            self._make_row(condition=146), _INSTRUMENT_ID, 2, 0, 0
        )
        assert tick.aggressor_side == AggressorSide.SELLER

    def test_unknown_condition_no_aggressor(self):
        tick = rest_trade_row_to_trade_tick(
            self._make_row(condition=999), _INSTRUMENT_ID, 2, 0, 0
        )
        assert tick.aggressor_side == AggressorSide.NO_AGGRESSOR

    def test_trade_id_stable(self):
        r1 = rest_trade_row_to_trade_tick(self._make_row(), _INSTRUMENT_ID, 2, 0, 0)
        r2 = rest_trade_row_to_trade_tick(self._make_row(), _INSTRUMENT_ID, 2, 0, 0)
        assert r1.trade_id.value == r2.trade_id.value
        assert isinstance(r1.trade_id, TradeId)

    def test_negative_sequence_ok(self):
        # OPRA quirk per wire-format doc.
        row = self._make_row()
        row["sequence"] = -563040482
        tick = rest_trade_row_to_trade_tick(row, _INSTRUMENT_ID, 2, 0, 0)
        assert "-563040482" in tick.trade_id.value


# ---------------------------------------------------------------------------
# REST OHLC — v3 named-field row
# ---------------------------------------------------------------------------


class TestRestOhlcRowToBar:
    def _make_row(self):
        return {
            "timestamp": "2024-06-20T09:30:00.000",
            "open": 39.03, "high": 39.25, "low": 38.55, "close": 39.10,
            "volume": 87758, "vwap": 39.14, "count": 1319,
        }

    def test_valid_row(self, bar_type):
        bar = rest_ohlc_row_to_bar(self._make_row(), bar_type, 2, 1700000000000000000)
        assert isinstance(bar, Bar)
        assert bar.open == Price.from_str("39.03")
        assert bar.high == Price.from_str("39.25")
        assert bar.low == Price.from_str("38.55")
        assert bar.close == Price.from_str("39.10")
        assert bar.volume == Quantity.from_str("87758")

    def test_missing_field_raises(self, bar_type):
        row = self._make_row()
        del row["high"]
        with pytest.raises(DecodeError, match="missing field"):
            rest_ohlc_row_to_bar(row, bar_type, 2, 0)


# ---------------------------------------------------------------------------
# WS frames — shape from v2 docs, retained for v3 (full live capture pending)
# ---------------------------------------------------------------------------


class TestWsQuoteFrameToQuoteTick:
    def _frame(self):
        return {
            "header": {"status": "CONNECTED", "type": "QUOTE"},
            "contract": {"symbol": "AAPL", "expiration": 20240621, "strike": 1750000, "right": "C"},
            "quote": {
                "timestamp": "2024-06-20T09:30:01.000",
                "bid_size": 5, "bid": 39.00, "ask_size": 1, "ask": 39.70,
            },
        }

    def test_valid_frame(self):
        tick = ws_quote_frame_to_quote_tick(self._frame(), _INSTRUMENT_ID, 2, 0, 1700000000000000000)
        assert isinstance(tick, QuoteTick)
        assert tick.bid_price == Price.from_str("39.00")

    def test_missing_quote_field_raises(self):
        frame = self._frame()
        del frame["quote"]["bid"]
        with pytest.raises(DecodeError, match="missing field"):
            ws_quote_frame_to_quote_tick(frame, _INSTRUMENT_ID, 2, 0, 0)


class TestWsTradeFrameToTradeTick:
    def _frame(self, condition=18):
        return {
            "header": {"status": "CONNECTED", "type": "TRADE"},
            "contract": {"symbol": "AAPL", "expiration": 20240621, "strike": 1750000, "right": "C"},
            "trade": {
                "timestamp": "2024-06-20T09:30:00.334",
                "sequence": 1332218205,
                "size": 1, "condition": condition, "price": 39.03, "exchange": 1,
            },
        }

    def test_valid_frame(self):
        tick = ws_trade_frame_to_trade_tick(self._frame(), _INSTRUMENT_ID, 2, 0, 0)
        assert tick.price == Price.from_str("39.03")
        assert tick.aggressor_side == AggressorSide.NO_AGGRESSOR

    def test_aggressor_buyer(self):
        tick = ws_trade_frame_to_trade_tick(self._frame(condition=145), _INSTRUMENT_ID, 2, 0, 0)
        assert tick.aggressor_side == AggressorSide.BUYER

    def test_aggressor_seller(self):
        tick = ws_trade_frame_to_trade_tick(self._frame(condition=146), _INSTRUMENT_ID, 2, 0, 0)
        assert tick.aggressor_side == AggressorSide.SELLER


# ---------------------------------------------------------------------------
# Throughput
# ---------------------------------------------------------------------------


class TestDecodeThroughput:
    @pytest.mark.slow
    def test_decode_throughput(self):
        row = {
            "timestamp": "2024-06-20T09:30:01.000",
            "bid_size": 5, "bid_exchange": 46, "bid": 39.00, "bid_condition": 50,
            "ask_size": 1, "ask_exchange": 46, "ask": 39.70, "ask_condition": 50,
        }
        count = 10_000
        start = time.perf_counter()
        for _ in range(count):
            rest_quote_row_to_quote_tick(row, _INSTRUMENT_ID, 2, 0, 1700000000000000000)
        elapsed = time.perf_counter() - start
        assert elapsed < 0.5, f"{count} rows in {elapsed:.3f}s (limit 0.5s)"
