"""
Tests for nautilus_trader.adapters.thetadata.decode
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
    rest_ohlc_row_to_bar,
    rest_quote_row_to_quote_tick,
    rest_trade_row_to_trade_tick,
    ws_quote_frame_to_quote_tick,
    ws_trade_frame_to_trade_tick,
)
from nautilus_trader.model.data import Bar, BarSpecification, BarType, BarAggregation, QuoteTick, TradeTick
from nautilus_trader.model.enums import AggressorSide, PriceType
from nautilus_trader.model.identifiers import InstrumentId, TradeId
from nautilus_trader.model.objects import Price, Quantity

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_FIXTURES_DIR = Path(__file__).parent / "fixtures"

_INSTRUMENT_ID = InstrumentId.from_str("QQQ231110P00360000.OPT-THETADATA")


@pytest.fixture
def bar_type():
    spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
    return BarType(_INSTRUMENT_ID, spec)


# ---------------------------------------------------------------------------
# TestDateMsToUnixNanos
# ---------------------------------------------------------------------------


class TestDateMsToUnixNanos:
    def test_normal_day_midnight(self):
        """Midnight on a normal day should produce a valid UTC nanos."""
        # 2023-11-03 is a normal EST day (after fall-back)
        result = date_ms_to_unix_nanos(20231103, 0)
        assert result > 0
        # Should be a reasonable value: ~2023-11-03 05:00 UTC = 1698987600000000000 ns
        assert 1_600_000_000_000_000_000 < result < 1_800_000_000_000_000_000

    def test_normal_day_noon(self):
        """Noon on a normal day."""
        ms_noon = 12 * 3_600_000  # 43200000 ms
        result = date_ms_to_unix_nanos(20231103, ms_noon)
        assert result > 0

    def test_cache_hit_reuses_base(self):
        """Second call for same date should hit cache."""
        _DATE_CACHE.clear()
        result1 = date_ms_to_unix_nanos(20231103, 3600000)
        result2 = date_ms_to_unix_nanos(20231103, 7200000)
        # Both use same cached base; offset should differ by exactly 1 hour in ns
        diff = result2 - result1
        assert diff == 3_600_000_000_000  # 1 hour in nanos

    def test_dst_spring_forward_raises(self):
        """Non-existent times during spring-forward gap should raise DecodeError."""
        fixture_path = _FIXTURES_DIR / "dst_spring_forward_2025.json"
        with open(fixture_path) as f:
            fixture = json.load(f)

        date = fixture["date"]
        for ms in fixture["non_existent_ms_of_day"]:
            with pytest.raises(DecodeError, match="Non-existent"):
                date_ms_to_unix_nanos(date, ms)

    def test_dst_spring_forward_boundary(self):
        """Times just before and after the gap should work."""
        fixture_path = _FIXTURES_DIR / "dst_spring_forward_2025.json"
        with open(fixture_path) as f:
            fixture = json.load(f)

        date = fixture["date"]
        for ms in fixture["valid_ms_of_day"]:
            result = date_ms_to_unix_nanos(date, ms)
            assert result > 0

    def test_dst_fall_back_fold_0(self):
        """Fall-back ambiguous time with fold=0 resolves to earlier UTC."""
        fixture_path = _FIXTURES_DIR / "dst_fall_back_2025.json"
        with open(fixture_path) as f:
            fixture = json.load(f)

        date = fixture["date"]
        ms = fixture["ambiguous_ms_of_day"][0]  # 1:00 AM = 3600000

        result_f0 = date_ms_to_unix_nanos(date, ms, fold=0)
        result_f1 = date_ms_to_unix_nanos(date, ms, fold=1)

        # fold=1 should be 1 hour later in UTC
        assert result_f1 - result_f0 == 3_600_000_000_000

    def test_dst_fall_back_fold_1(self):
        """Fall-back ambiguous time with fold=1 resolves to later UTC."""
        fixture_path = _FIXTURES_DIR / "dst_fall_back_2025.json"
        with open(fixture_path) as f:
            fixture = json.load(f)

        date = fixture["date"]
        for ms in fixture["ambiguous_ms_of_day"]:
            result_f0 = date_ms_to_unix_nanos(date, ms, fold=0)
            result_f1 = date_ms_to_unix_nanos(date, ms, fold=1)
            assert result_f1 > result_f0
            assert result_f1 - result_f0 == 3_600_000_000_000


# ---------------------------------------------------------------------------
# TestRestQuoteRowToQuoteTick
# ---------------------------------------------------------------------------


class TestRestQuoteRowToQuoteTick:
    def _make_row(self, ms_of_day=35100000, date=20231103):
        """[ms_of_day, bid_size, bid_exchange, bid, bid_condition, ask_size, ask_exchange, ask, ask_condition, date]"""
        return [ms_of_day, 38, 69, 5.4, 50, 21, 69, 5.6, 50, date]

    def test_valid_row(self):
        row = self._make_row()
        result = rest_quote_row_to_quote_tick(
            row, _INSTRUMENT_ID, price_precision=2, size_precision=0, ts_init=1700000000000000000
        )
        assert isinstance(result, QuoteTick)
        assert result.instrument_id == _INSTRUMENT_ID
        assert result.bid_price == Price.from_str("5.40")
        assert result.ask_price == Price.from_str("5.60")
        assert result.bid_size == Quantity.from_str("38")
        assert result.ask_size == Quantity.from_str("21")
        assert result.ts_event > 0
        assert result.ts_init == 1700000000000000000

    def test_row_too_short(self):
        with pytest.raises(DecodeError, match="too short"):
            rest_quote_row_to_quote_tick(
                [35100000, 38, 69], _INSTRUMENT_ID, 2, 0, 1700000000000000000
            )

    def test_malformed_price_raises(self):
        row = self._make_row()
        row[3] = "not-a-number"  # corrupt bid
        with pytest.raises(DecodeError, match="Invalid price"):
            rest_quote_row_to_quote_tick(
                row, _INSTRUMENT_ID, price_precision=2, size_precision=0, ts_init=1700000000000000000
            )

    def test_precision_mismatch_raises(self):
        row = self._make_row()
        # 5.40 has precision 2, but we ask for 3
        with pytest.raises(DecodeError, match="precision mismatch"):
            rest_quote_row_to_quote_tick(
                row, _INSTRUMENT_ID, price_precision=3, size_precision=0, ts_init=1700000000000000000
            )

    def test_negative_size_raises(self):
        row = self._make_row()
        row[1] = -5  # negative bid_size
        with pytest.raises(DecodeError, match="Negative"):
            rest_quote_row_to_quote_tick(
                row, _INSTRUMENT_ID, price_precision=2, size_precision=0, ts_init=1700000000000000000
            )


# ---------------------------------------------------------------------------
# TestRestTradeRowToTradeTick
# ---------------------------------------------------------------------------


class TestRestTradeRowToTradeTick:
    def _make_row(self, condition=18, ms_of_day=35100000, sequence=1234, date=20231103):
        """[ms_of_day, sequence, size, condition, exchange, price, date, ...]"""
        return [ms_of_day, sequence, 5, condition, 65, 1.06, date]

    def test_valid_row(self):
        row = self._make_row()
        result = rest_trade_row_to_trade_tick(
            row, _INSTRUMENT_ID, price_precision=2, size_precision=0, ts_init=1700000000000000000
        )
        assert isinstance(result, TradeTick)
        assert result.instrument_id == _INSTRUMENT_ID
        assert result.price == Price.from_str("1.06")
        assert result.size == Quantity.from_str("5")
        assert result.aggressor_side == AggressorSide.NO_AGGRESSOR
        assert isinstance(result.trade_id, TradeId)

    def test_aggressor_buyer(self):
        row = self._make_row(condition=145)
        result = rest_trade_row_to_trade_tick(
            row, _INSTRUMENT_ID, price_precision=2, size_precision=0, ts_init=1700000000000000000
        )
        assert result.aggressor_side == AggressorSide.BUYER

    def test_aggressor_seller(self):
        row = self._make_row(condition=146)
        result = rest_trade_row_to_trade_tick(
            row, _INSTRUMENT_ID, price_precision=2, size_precision=0, ts_init=1700000000000000000
        )
        assert result.aggressor_side == AggressorSide.SELLER

    def test_unknown_condition_no_aggressor(self):
        row = self._make_row(condition=999)
        result = rest_trade_row_to_trade_tick(
            row, _INSTRUMENT_ID, price_precision=2, size_precision=0, ts_init=1700000000000000000
        )
        assert result.aggressor_side == AggressorSide.NO_AGGRESSOR

    def test_row_too_short(self):
        with pytest.raises(DecodeError, match="too short"):
            rest_trade_row_to_trade_tick(
                [35100000, 1234], _INSTRUMENT_ID, 2, 0, 1700000000000000000
            )

    def test_trade_id_stable(self):
        """Same inputs should produce same TradeId."""
        row1 = self._make_row()
        row2 = self._make_row()
        r1 = rest_trade_row_to_trade_tick(
            row1, _INSTRUMENT_ID, 2, 0, 1700000000000000000
        )
        r2 = rest_trade_row_to_trade_tick(
            row2, _INSTRUMENT_ID, 2, 0, 1700000000000000000
        )
        assert r1.trade_id.value == r2.trade_id.value


# ---------------------------------------------------------------------------
# TestRestOhlcRowToBar
# ---------------------------------------------------------------------------


class TestRestOhlcRowToBar:
    def _make_row(self, ms_of_day=45000000, date=20240102):
        """[ms_of_day, open, high, low, close, volume, count, date]"""
        return [ms_of_day, 186.615, 186.67, 186.53, 186.54, 87758, 1319, date]

    def test_valid_row(self, bar_type):
        row = self._make_row()
        result = rest_ohlc_row_to_bar(
            row, bar_type, price_precision=3, ts_init=1700000000000000000
        )
        assert isinstance(result, Bar)
        assert result.bar_type == bar_type
        assert result.open == Price.from_str("186.615")
        assert result.high == Price.from_str("186.67")
        assert result.low == Price.from_str("186.53")
        assert result.close == Price.from_str("186.54")
        assert result.volume == Quantity.from_str("87758")

    def test_row_too_short(self, bar_type):
        with pytest.raises(DecodeError, match="too short"):
            rest_ohlc_row_to_bar(
                [45000000, 186.615], bar_type, 3, 1700000000000000000
            )

    def test_malformed_price_raises(self, bar_type):
        row = self._make_row()
        row[1] = "bad"
        with pytest.raises(DecodeError, match="Invalid price"):
            rest_ohlc_row_to_bar(row, bar_type, 3, 1700000000000000000)

    def test_precision_mismatch_raises(self, bar_type):
        row = self._make_row()
        # 186.615 has precision 3, but we ask for 2
        with pytest.raises(DecodeError, match="precision mismatch"):
            rest_ohlc_row_to_bar(row, bar_type, 2, 1700000000000000000)


# ---------------------------------------------------------------------------
# TestWsQuoteFrameToQuoteTick
# ---------------------------------------------------------------------------


class TestWsQuoteFrameToQuoteTick:
    def _make_frame(self, ms_of_day=49531278, date=20231110):
        return {
            "header": {"status": "CONNECTED", "type": "QUOTE"},
            "contract": {
                "security_type": "OPTION",
                "root": "QQQ",
                "expiration": date,
                "strike": 360000,
                "right": "P",
            },
            "quote": {
                "ms_of_day": ms_of_day,
                "bid_size": 42,
                "bid": 1.25,
                "ask_size": 30,
                "ask": 1.35,
            },
        }

    def test_valid_frame(self):
        frame = self._make_frame()
        result = ws_quote_frame_to_quote_tick(
            frame, _INSTRUMENT_ID, price_precision=2, size_precision=0, ts_init=1700000000000000000
        )
        assert isinstance(result, QuoteTick)
        assert result.bid_price == Price.from_str("1.25")
        assert result.ask_price == Price.from_str("1.35")
        assert result.bid_size == Quantity.from_str("42")
        assert result.ask_size == Quantity.from_str("30")

    def test_missing_quote_key_raises(self):
        frame = {"header": {}, "contract": {}}
        with pytest.raises(KeyError):
            ws_quote_frame_to_quote_tick(
                frame, _INSTRUMENT_ID, 2, 0, 1700000000000000000
            )


# ---------------------------------------------------------------------------
# TestWsTradeFrameToTradeTick
# ---------------------------------------------------------------------------


class TestWsTradeFrameToTradeTick:
    def _make_frame(self, condition=18, ms_of_day=49531278, sequence=-563040482, date=20231103):
        return {
            "header": {"status": "CONNECTED", "type": "TRADE"},
            "contract": {
                "security_type": "OPTION",
                "root": "QQQ",
                "expiration": date,
                "strike": 360000,
                "right": "P",
            },
            "trade": {
                "ms_of_day": ms_of_day,
                "sequence": sequence,
                "size": 5,
                "condition": condition,
                "price": 1.06,
                "exchange": 65,
                "date": date,
            },
        }

    def test_valid_frame(self):
        frame = self._make_frame()
        result = ws_trade_frame_to_trade_tick(
            frame, _INSTRUMENT_ID, price_precision=2, size_precision=0, ts_init=1700000000000000000
        )
        assert isinstance(result, TradeTick)
        assert result.price == Price.from_str("1.06")
        assert result.size == Quantity.from_str("5")
        assert result.aggressor_side == AggressorSide.NO_AGGRESSOR

    def test_aggressor_buyer(self):
        frame = self._make_frame(condition=145)
        result = ws_trade_frame_to_trade_tick(
            frame, _INSTRUMENT_ID, 2, 0, 1700000000000000000
        )
        assert result.aggressor_side == AggressorSide.BUYER

    def test_aggressor_seller(self):
        frame = self._make_frame(condition=146)
        result = ws_trade_frame_to_trade_tick(
            frame, _INSTRUMENT_ID, 2, 0, 1700000000000000000
        )
        assert result.aggressor_side == AggressorSide.SELLER

    def test_missing_condition_defaults_no_aggressor(self):
        frame = self._make_frame()
        del frame["trade"]["condition"]
        result = ws_trade_frame_to_trade_tick(
            frame, _INSTRUMENT_ID, 2, 0, 1700000000000000000
        )
        assert result.aggressor_side == AggressorSide.NO_AGGRESSOR


# ---------------------------------------------------------------------------
# Throughput micro-benchmark
# ---------------------------------------------------------------------------


class TestDecodeThroughput:
    @pytest.mark.slow
    def test_decode_throughput(self):
        """Decode 10,000 WS quote frames in <500ms."""
        frame = {
            "header": {"status": "CONNECTED", "type": "QUOTE"},
            "contract": {
                "security_type": "OPTION",
                "root": "QQQ",
                "expiration": 20231110,
                "strike": 360000,
                "right": "P",
            },
            "quote": {
                "ms_of_day": 49531278,
                "bid_size": 42,
                "bid": 1.25,
                "ask_size": 30,
                "ask": 1.35,
            },
        }

        count = 10_000
        start = time.perf_counter()
        for _ in range(count):
            ws_quote_frame_to_quote_tick(
                frame, _INSTRUMENT_ID, price_precision=2, size_precision=0,
                ts_init=1700000000000000000
            )
        elapsed = time.perf_counter() - start

        assert elapsed < 0.5, f"Throughput: {count} frames in {elapsed:.3f}s (limit: 0.5s)"
