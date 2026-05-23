"""
Tests for nautilus_trader.adapters.thetadata.http (ThetaDataHttpClient).
"""

import re
import time

import pytest
from aioresponses import CallbackResult, aioresponses

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.http import (
    ThetaDataHttpClient,
    ThetaDataHttpError,
    _chunk_date_range,
)


HTTP_URL = "http://127.0.0.1:25510"


def _quote_env(rows):
    return {
        "header": {"status": "OK", "type": "QUOTE", "format": []},
        "response": rows,
    }


def _err_env(error_type="INVALID", message="bad", code=400):
    return {"header": {"error_type": error_type, "error_message": message, "error_code": code}}


def _make_client(rate=100.0, retries=2):
    cfg = ThetaDataDataClientConfig(
        http_url=HTTP_URL,
        http_rate_limit_per_sec=rate,
        http_max_retries=retries,
        http_timeout_secs=5,
    )
    return ThetaDataHttpClient(cfg)


# ---------------------------------------------------------------------------
# _chunk_date_range
# ---------------------------------------------------------------------------


class TestChunkDateRange:
    def test_single_day(self):
        assert _chunk_date_range(20240101, 20240101) == [(20240101, 20240101)]

    def test_29_days(self):
        out = _chunk_date_range(20240101, 20240129)
        assert out == [(20240101, 20240129)]
        assert len(out) == 1

    def test_30_days(self):
        out = _chunk_date_range(20240101, 20240130)
        assert out == [(20240101, 20240130)]
        assert len(out) == 1

    def test_31_days_splits(self):
        out = _chunk_date_range(20240101, 20240131)
        assert len(out) == 2
        assert out == [(20240101, 20240130), (20240131, 20240131)]

    def test_60_days_splits(self):
        out = _chunk_date_range(20240101, 20240301)
        # 60 days from Jan 1 to Mar 1 (2024 leap year, but date math handles it)
        assert len(out) == 3
        # Chunk 1: 30 days = Jan1..Jan30
        assert out[0] == (20240101, 20240130)

    def test_90_days_splits(self):
        out = _chunk_date_range(20240101, 20240331)
        assert len(out) == 4

    def test_invalid_range_raises(self):
        with pytest.raises(ValueError, match="start"):
            _chunk_date_range(20240301, 20240101)


# ---------------------------------------------------------------------------
# Historical endpoints
# ---------------------------------------------------------------------------


class TestHistQuotes:
    @pytest.mark.asyncio
    async def test_single_day(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(
                re.compile(r".*v2/hist/stock/quote.*"),
                payload=_quote_env([[35100000, 38, 69, 5.4, 50, 21, 69, 5.6, 50, 20231103]]),
            )
            rows = await client.hist_quotes("AAPL", 20231103, 20231103)
        await client.close()
        assert len(rows) == 1
        assert rows[0][3] == 5.4

    @pytest.mark.asyncio
    async def test_chunk_split_concatenates_in_order(self):
        client = _make_client()
        chunks_seen: list[tuple[str, str]] = []
        with aioresponses() as m:
            # Each chunk returns one identifiable row
            def callback(url, **kwargs):
                params = url.query
                chunks_seen.append((params.get("start_date"), params.get("end_date")))
                # Mark the row with the chunk index so we can assert ordering
                return CallbackResult(
                    payload=_quote_env([[len(chunks_seen), 0, 0, 0.0, 0, 0, 0, 0.0, 0, int(params.get("start_date"))]])
                )

            # 31 days → 2 chunks
            m.get(re.compile(r".*v2/hist/stock/quote.*"), callback=callback)
            m.get(re.compile(r".*v2/hist/stock/quote.*"), callback=callback)
            rows = await client.hist_quotes("AAPL", 20240101, 20240131)
        await client.close()
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2

    @pytest.mark.asyncio
    async def test_chunk_failure_propagates(self):
        client = _make_client(retries=0)
        with aioresponses() as m:
            # First chunk OK, second 500 — exception should propagate
            m.get(re.compile(r".*v2/hist/stock/quote.*"), payload=_quote_env([[1]]))
            m.get(re.compile(r".*v2/hist/stock/quote.*"), status=500, payload={"oops": True})
            with pytest.raises(ThetaDataHttpError) as ei:
                await client.hist_quotes("AAPL", 20240101, 20240131)
        await client.close()
        assert ei.value.status == 500


# ---------------------------------------------------------------------------
# Retry / no-retry policy
# ---------------------------------------------------------------------------


class TestRetryPolicy:
    @pytest.mark.asyncio
    async def test_503_retried_then_succeeds(self):
        client = _make_client(retries=3)
        with aioresponses() as m:
            m.get(re.compile(r".*v2/hist/stock/quote.*"), status=503, payload={})
            m.get(re.compile(r".*v2/hist/stock/quote.*"), status=503, payload={})
            m.get(re.compile(r".*v2/hist/stock/quote.*"), payload=_quote_env([[1]]))
            rows = await client.hist_quotes("AAPL", 20240101, 20240101)
        await client.close()
        assert rows == [[1]]

    @pytest.mark.asyncio
    async def test_429_retried_then_succeeds(self):
        client = _make_client(retries=3)
        with aioresponses() as m:
            m.get(re.compile(r".*v2/hist/stock/quote.*"), status=429, payload={})
            m.get(re.compile(r".*v2/hist/stock/quote.*"), payload=_quote_env([[1]]))
            rows = await client.hist_quotes("AAPL", 20240101, 20240101)
        await client.close()
        assert rows == [[1]]

    @pytest.mark.asyncio
    async def test_400_not_retried(self):
        client = _make_client(retries=5)
        with aioresponses() as m:
            m.get(re.compile(r".*v2/hist/stock/quote.*"), status=400, payload={})
            with pytest.raises(ThetaDataHttpError) as ei:
                await client.hist_quotes("AAPL", 20240101, 20240101)
        await client.close()
        assert ei.value.status == 400


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


class TestRateLimit:
    @pytest.mark.asyncio
    async def test_rate_limit_enforced(self):
        # 2 req/s — 4 requests should take at least ~1s (3 inter-arrival gaps capped by limiter).
        client = _make_client(rate=2.0)
        with aioresponses() as m:
            for _ in range(4):
                m.get(re.compile(r".*v2/list/expirations.*"), payload=_quote_env([20240101]))
            t0 = time.monotonic()
            for _ in range(4):
                await client.list_expirations("AAPL")
            elapsed = time.monotonic() - t0
        await client.close()
        # At 2 req/s with a burst budget of 2, 4 calls should need >0.9s.
        assert elapsed > 0.9, f"rate limit ineffective: 4 calls in {elapsed:.2f}s"


# ---------------------------------------------------------------------------
# Listing endpoints
# ---------------------------------------------------------------------------


class TestListEndpoints:
    @pytest.mark.asyncio
    async def test_list_expirations(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(re.compile(r".*v2/list/expirations.*"), payload=_quote_env([20241011, 20241018]))
            out = await client.list_expirations("AAPL")
        await client.close()
        assert out == [20241011, 20241018]

    @pytest.mark.asyncio
    async def test_list_strikes_converts_units(self):
        # Wire: strike in 1/10th cent. 140000 → $14.00.
        client = _make_client()
        with aioresponses() as m:
            m.get(re.compile(r".*v2/list/strikes.*"), payload=_quote_env([140000, 1500000]))
            out = await client.list_strikes("AAPL", 20240620)
        await client.close()
        from decimal import Decimal
        assert out == [Decimal("14.0000"), Decimal("150.0000")]

    @pytest.mark.asyncio
    async def test_list_contracts(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(
                re.compile(r".*v2/list/contracts/option/trade.*"),
                payload=_quote_env([["AAPL", 20230616, 260000, "P"]]),
            )
            out = await client.list_contracts("AAPL", 20230512)
        await client.close()
        assert out == [("AAPL", 20230616, 260000, "P")]


# ---------------------------------------------------------------------------
# Envelope error handling
# ---------------------------------------------------------------------------


class TestEnvelopeErrors:
    @pytest.mark.asyncio
    async def test_envelope_error_raises(self):
        client = _make_client()
        with aioresponses() as m:
            m.get(re.compile(r".*v2/list/expirations.*"), payload=_err_env("UNKNOWN_ROOT", "bad root"))
            with pytest.raises(ThetaDataHttpError, match="UNKNOWN_ROOT"):
                await client.list_expirations("BOGUS")
        await client.close()
