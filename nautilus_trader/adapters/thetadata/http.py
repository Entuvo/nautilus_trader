"""
ThetaData HTTP client — REST historical + listing endpoints via the local
ThetaTerminal proxy (default port 25510).

Wire format reference: docs/architecture/notes/thetadata-wire-format.md.
Responses follow the `{header, response}` envelope; `response` is a list of
positional-aligned arrays for historical endpoints, or a list of scalars/tuples
for listing endpoints.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import aiohttp
from aiolimiter import AsyncLimiter
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig


# 30-day historical chunk limit (per wire-format doc § "30-Day Chunk Limit").
_CHUNK_DAYS = 30


class ThetaDataHttpError(Exception):
    """Wrap a non-retryable HTTP failure with enough context to triage."""

    def __init__(self, status: int, message: str, url: str):
        super().__init__(f"{status} {message} for {url}")
        self.status = status
        self.message = message
        self.url = url


def _is_retryable(exc: BaseException) -> bool:
    """Retry on 5xx / 429 / transport errors; do not retry on 4xx (caller bug)."""
    if isinstance(exc, ThetaDataHttpError):
        return exc.status >= 500 or exc.status == 429
    return isinstance(exc, (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError))


def _parse_yyyymmdd(d: int | str) -> date:
    s = str(d)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def _fmt_yyyymmdd(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def _chunk_date_range(start: int, end: int, chunk_days: int = _CHUNK_DAYS) -> list[tuple[int, int]]:
    """Split [start, end] (inclusive, YYYYMMDD ints) into ≤chunk_days chunks."""
    s = _parse_yyyymmdd(start)
    e = _parse_yyyymmdd(end)
    if s > e:
        raise ValueError(f"start {start} > end {end}")
    chunks: list[tuple[int, int]] = []
    cur = s
    while cur <= e:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), e)
        chunks.append((_fmt_yyyymmdd(cur), _fmt_yyyymmdd(chunk_end)))
        cur = chunk_end + timedelta(days=1)
    return chunks


class ThetaDataHttpClient:
    """Async HTTP client for the local ThetaTerminal REST proxy.

    - Per-process aiohttp session with bounded connection pool (limit=20 covers
      parallel `load_ids_async` with headroom; ThetaTerminal-on-localhost
      doesn't need the aiohttp default of 100).
    - Token-bucket rate limiter (`aiolimiter`) — default 5 req/s.
    - Exponential+jitter retry on 5xx/429/transport errors. 4xx propagates.
    - 30-day chunk splitter for historical date ranges; chunk results concat
      in arrival order. A failed chunk after retries propagates — no partial
      returns (avoids silent gaps).
    """

    def __init__(self, config: ThetaDataDataClientConfig):
        self._config = config
        self._http_url = config.http_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=config.http_timeout_secs)
        self._limiter = AsyncLimiter(
            max_rate=config.http_rate_limit_per_sec,
            time_period=1.0,
        )
        self._max_retries = config.http_max_retries
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=20, limit_per_host=20, force_close=False)
            self._session = aiohttp.ClientSession(connector=connector, timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def __aenter__(self) -> ThetaDataHttpClient:
        await self._ensure_session()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    async def _get_envelope(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """GET a JSON envelope, retrying retryable failures and rate-limiting all calls."""
        session = await self._ensure_session()
        url = f"{self._http_url}{path}"

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries + 1),
            wait=wait_exponential_jitter(initial=0.5, max=10.0),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        ):
            with attempt:
                async with self._limiter:
                    async with session.get(url, params=params) as resp:
                        if resp.status >= 400:
                            text = (await resp.text())[:512]
                            raise ThetaDataHttpError(resp.status, text, url)
                        return await resp.json()

        raise RuntimeError("unreachable")  # tenacity reraise=True covers this

    @staticmethod
    def _envelope_rows(envelope: dict[str, Any]) -> list:
        """Extract `response` from the envelope, raising on `header.error_type`."""
        header = envelope.get("header") or {}
        err = header.get("error_type")
        if err:
            raise ThetaDataHttpError(
                status=header.get("error_code", 500),
                message=f"{err}: {header.get('error_message', '')}",
                url="<envelope>",
            )
        return envelope.get("response") or []

    # -----------------------------------------------------------------------
    # Historical endpoints — all chunked by 30 days
    # -----------------------------------------------------------------------

    async def _hist(self, path: str, root: str, start: int, end: int, **extra) -> list[list]:
        """Internal: chunked historical GET. Concatenates rows in order."""
        out: list[list] = []
        for chunk_start, chunk_end in _chunk_date_range(start, end):
            params = {
                "root": root,
                "start_date": str(chunk_start),
                "end_date": str(chunk_end),
                **extra,
            }
            envelope = await self._get_envelope(path, params)
            out.extend(self._envelope_rows(envelope))
        return out

    async def hist_quotes(self, root: str, start: int, end: int) -> list[list]:
        """GET /v2/hist/stock/quote, chunked. Returns positional row arrays."""
        return await self._hist("/v2/hist/stock/quote", root, start, end)

    async def hist_trades(self, root: str, start: int, end: int) -> list[list]:
        """GET /v2/hist/stock/trade, chunked."""
        return await self._hist("/v2/hist/stock/trade", root, start, end)

    async def hist_ohlc(self, root: str, start: int, end: int, ivl_ms: int = 60000) -> list[list]:
        """GET /v2/hist/stock/ohlc, chunked. `ivl_ms` = candle duration in ms."""
        return await self._hist("/v2/hist/stock/ohlc", root, start, end, ivl=str(ivl_ms))

    # -----------------------------------------------------------------------
    # Listing endpoints — not chunked
    # -----------------------------------------------------------------------

    async def list_expirations(self, root: str) -> list[int]:
        """GET /v2/list/expirations?root=...  → list of YYYYMMDD ints."""
        env = await self._get_envelope("/v2/list/expirations", {"root": root})
        return [int(d) for d in self._envelope_rows(env)]

    async def list_strikes(self, root: str, expiration: int) -> list[Decimal]:
        """GET /v2/list/strikes?root=...&exp=...  → strikes in dollars (decoded from 1/10¢)."""
        env = await self._get_envelope("/v2/list/strikes", {"root": root, "exp": str(expiration)})
        # Wire: strike in 1/10th of a cent (per wire-format doc).
        return [Decimal(int(s)) / Decimal(10000) for s in self._envelope_rows(env)]

    async def list_contracts(self, root: str, start_date: int) -> list[tuple]:
        """GET /v2/list/contracts/option/trade — `[root, expiration, strike(1/10¢), right]`."""
        env = await self._get_envelope(
            "/v2/list/contracts/option/trade",
            {"root": root, "start_date": str(start_date)},
        )
        return [tuple(row) for row in self._envelope_rows(env)]
