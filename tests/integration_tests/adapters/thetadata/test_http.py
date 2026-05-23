"""
Tests for nautilus_trader.adapters.thetadata.http (ThetaDataHttpClient).

Wire shape: v3 — `{"response": [...]}` with named-field objects. History
endpoints wrap rows in `{contract, data}` blocks; this client flattens to
`data` rows.
"""

import re
import time
from datetime import date

import pytest
from aioresponses import CallbackResult, aioresponses

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.http import (
    ThetaDataHttpClient,
    ThetaDataHttpError,
    _chunk_date_range,
    _right_to_wire,
)


HTTP_URL = "http://127.0.0.1:25503"


def _list_env(rows):
    return {"response": rows}


def _quote_block(rows, symbol="AAPL", exp="2024-06-21", strike=175.0, right="CALL"):
    return {
        "response": [
            {
                "contract": {"symbol": symbol, "expiration": exp, "strike": strike, "right": right},
                "data": rows,
            }
        ]
    }


def _make_client(rate=100.0, retries=2):
    cfg = ThetaDataDataClientConfig(
        http_url=HTTP_URL,
        http_rate_limit_per_sec=rate,
        http_max_retries=retries,
        http_timeout_secs=5,
    )
    return ThetaDataHttpClient(cfg)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestRightToWire:
    def test_call(self):
        assert _right_to_wire("C") == "call"

    def test_put(self):
        assert _right_to_wire("P") == "put"

    def test_lowercase_input(self):
        assert _right_to_wire("c") == "call"

    def test_invalid(self):
        with pytest.raises(ValueError, match="Invalid right"):
            _right_to_wire("X")


class TestChunkDateRange:
    def test_single_day(self):
        assert _chunk_date_range(20240101, 20240101) == [(20240101, 20240101)]

    def test_30_days(self):
        assert _chunk_date_range(20240101, 20240130) == [(20240101, 20240130)]

    def test_31_days_splits(self):
        out = _chunk_date_range(20240101, 20240131)
        assert out == [(20240101, 20240130), (20240131, 20240131)]

    def test_invalid_range_raises(self):
        with pytest.raises(ValueError, match="start"):
            _chunk_date_range(20240301, 20240101)


# ---------------------------------------------------------------------------
# Listing endpoints
# ---------------------------------------------------------------------------


class TestListEndpoints:
    @pytest.mark.asyncio
    async def test_list_stock_symbols(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(
                re.compile(r".*v3/stock/list/symbols.*"),
                payload=_list_env([{"symbol": "AAPL"}, {"symbol": "MSFT"}]),
            )
            out = await client.list_stock_symbols()
        await client.close()
        assert out == ["AAPL", "MSFT"]

    @pytest.mark.asyncio
    async def test_list_expirations_parses_iso(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(
                re.compile(r".*v3/option/list/expirations.*"),
                payload=_list_env([
                    {"symbol": "AAPL", "expiration": "2024-06-21"},
                    {"symbol": "AAPL", "expiration": "2024-12-20"},
                ]),
            )
            out = await client.list_expirations("AAPL")
        await client.close()
        assert out == [date(2024, 6, 21), date(2024, 12, 20)]

    @pytest.mark.asyncio
    async def test_list_strikes_returns_dollars(self):
        # v3 returns floats in dollars (not 1/10¢ as v2 did).
        client = _make_client()
        with aioresponses() as m:
            m.get(
                re.compile(r".*v3/option/list/strikes.*"),
                payload=_list_env([
                    {"symbol": "AAPL", "strike": 80.0},
                    {"symbol": "AAPL", "strike": 175.0},
                ]),
            )
            from decimal import Decimal
            out = await client.list_strikes("AAPL", date(2024, 6, 21))
        await client.close()
        assert out == [Decimal("80.0"), Decimal("175.0")]


# ---------------------------------------------------------------------------
# Option history endpoints
# ---------------------------------------------------------------------------


class TestOptionHistory:
    @pytest.mark.asyncio
    async def test_quotes_flattens_contract_blocks(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(
                re.compile(r".*v3/option/history/quote.*"),
                payload=_quote_block([
                    {"timestamp": "2024-06-20T09:30:00.000", "bid": 39.0, "ask": 39.7,
                     "bid_size": 5, "ask_size": 1},
                    {"timestamp": "2024-06-20T09:30:01.000", "bid": 38.55, "ask": 39.25,
                     "bid_size": 30, "ask_size": 30},
                ]),
            )
            rows = await client.option_hist_quotes(
                "AAPL", date(2024, 6, 21), 175.0, "C", 20240620, 20240620,
            )
        await client.close()
        assert len(rows) == 2
        assert rows[0]["bid"] == 39.0

    @pytest.mark.asyncio
    async def test_quotes_chunks_30_day_range(self):
        client = _make_client()
        chunks_seen = []
        with aioresponses() as m:
            def callback(url, **kwargs):
                chunks_seen.append((url.query.get("start_date"), url.query.get("end_date")))
                return CallbackResult(payload=_quote_block([
                    {"timestamp": "2024-01-01T09:30:00.000", "bid": 1.0, "ask": 1.1,
                     "bid_size": 1, "ask_size": 1}
                ]))
            # 31 days → 2 chunks; register two callbacks.
            m.get(re.compile(r".*v3/option/history/quote.*"), callback=callback)
            m.get(re.compile(r".*v3/option/history/quote.*"), callback=callback)
            rows = await client.option_hist_quotes(
                "AAPL", date(2024, 6, 21), 175.0, "C", 20240101, 20240131,
            )
        await client.close()
        assert len(chunks_seen) == 2
        assert len(rows) == 2  # one row per chunk

    @pytest.mark.asyncio
    async def test_request_includes_required_params(self):
        client = _make_client()
        captured = {}
        with aioresponses() as m:
            def callback(url, **kwargs):
                captured.update(dict(url.query))
                return CallbackResult(payload=_quote_block([]))

            m.get(re.compile(r".*v3/option/history/quote.*"), callback=callback)
            await client.option_hist_quotes(
                "AAPL", date(2024, 6, 21), 175.0, "C", 20240620, 20240620, interval="1m",
            )
        await client.close()
        assert captured["symbol"] == "AAPL"
        assert captured["expiration"] == "2024-06-21"
        assert captured["strike"] == "175.0"
        assert captured["right"] == "call"
        assert captured["interval"] == "1m"
        assert captured["format"] == "json"

    @pytest.mark.asyncio
    async def test_trades_endpoint(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(
                re.compile(r".*v3/option/history/trade.*"),
                payload=_quote_block([
                    {"timestamp": "2024-06-20T09:30:00.334", "price": 39.03, "size": 1,
                     "condition": 18, "sequence": 1, "exchange": 1},
                ]),
            )
            rows = await client.option_hist_trades(
                "AAPL", date(2024, 6, 21), 175.0, "C", 20240620, 20240620,
            )
        await client.close()
        assert rows[0]["price"] == 39.03

    @pytest.mark.asyncio
    async def test_ohlc_endpoint(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(
                re.compile(r".*v3/option/history/ohlc.*"),
                payload=_quote_block([
                    {"timestamp": "2024-06-20T09:30:00.000",
                     "open": 39.03, "high": 39.03, "low": 39.03, "close": 39.03,
                     "volume": 1, "vwap": 39.03, "count": 1},
                ]),
            )
            rows = await client.option_hist_ohlc(
                "AAPL", date(2024, 6, 21), 175.0, "C", 20240620, 20240620,
            )
        await client.close()
        assert rows[0]["close"] == 39.03


# ---------------------------------------------------------------------------
# Retry / no-retry policy
# ---------------------------------------------------------------------------


class TestRetryPolicy:
    @pytest.mark.asyncio
    async def test_503_retried_then_succeeds(self):
        client = _make_client(retries=3)
        with aioresponses() as m:
            m.get(re.compile(r".*v3/option/list/expirations.*"), status=503, payload={})
            m.get(re.compile(r".*v3/option/list/expirations.*"),
                  payload=_list_env([{"symbol": "AAPL", "expiration": "2024-06-21"}]))
            out = await client.list_expirations("AAPL")
        await client.close()
        assert out == [date(2024, 6, 21)]

    @pytest.mark.asyncio
    async def test_429_retried_then_succeeds(self):
        client = _make_client(retries=3)
        with aioresponses() as m:
            m.get(re.compile(r".*v3/option/list/expirations.*"), status=429, payload={})
            m.get(re.compile(r".*v3/option/list/expirations.*"),
                  payload=_list_env([{"symbol": "AAPL", "expiration": "2024-06-21"}]))
            out = await client.list_expirations("AAPL")
        await client.close()
        assert out == [date(2024, 6, 21)]

    @pytest.mark.asyncio
    async def test_400_not_retried(self):
        client = _make_client(retries=5)
        with aioresponses() as m:
            m.get(re.compile(r".*v3/option/list/expirations.*"), status=400, payload={})
            with pytest.raises(ThetaDataHttpError) as ei:
                await client.list_expirations("AAPL")
        await client.close()
        assert ei.value.status == 400


# ---------------------------------------------------------------------------
# Tier / auth plain-text error path
# ---------------------------------------------------------------------------


class TestPlainTextErrorPath:
    @pytest.mark.asyncio
    async def test_plain_text_response_treated_as_error(self):
        # v3 returns plain text (NOT JSON envelope) for tier/auth failures
        # even with HTTP 200. The client must detect and raise.
        client = _make_client(retries=0)
        with aioresponses() as m:
            m.get(
                re.compile(r".*v3/stock/list/symbols.*"),
                body="Requesting a stock endpoint requiring a value subscription...",
                content_type="text/plain",
            )
            with pytest.raises(ThetaDataHttpError, match="value subscription"):
                await client.list_stock_symbols()
        await client.close()


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


class TestRateLimit:
    @pytest.mark.asyncio
    async def test_rate_limit_enforced(self):
        client = _make_client(rate=2.0)
        with aioresponses() as m:
            for _ in range(4):
                m.get(re.compile(r".*v3/option/list/expirations.*"),
                      payload=_list_env([{"symbol": "AAPL", "expiration": "2024-06-21"}]))
            t0 = time.monotonic()
            for _ in range(4):
                await client.list_expirations("AAPL")
            elapsed = time.monotonic() - t0
        await client.close()
        assert elapsed > 0.9, f"rate limit ineffective: 4 calls in {elapsed:.2f}s"
