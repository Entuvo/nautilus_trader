"""
ThetaData HTTP client — REST historical + listing endpoints via the local
ThetaTerminal v3 proxy (default port 25503).

Wire format reference: docs/architecture/notes/thetadata-wire-format.md.
v3 envelope is `{"response": [...]}` (no `header` field). Historical
endpoints return per-contract `{contract, data}` pairs; listing endpoints
return flat lists of named-field objects. All endpoints require
`format=json` (default is CSV).
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


# 30-day chunk limit not explicitly documented in v3, but free/value tier
# enforcement makes splitting a defensive necessity.
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


def _right_to_wire(right: str) -> str:
    """OCC right ('C'/'P') → v3 REST request value ('call'/'put')."""
    r = right.upper()
    if r == "C":
        return "call"
    if r == "P":
        return "put"
    raise ValueError(f"Invalid right: {right!r}")


class ThetaDataHttpClient:
    """Async HTTP client for the local ThetaTerminal v3 REST proxy.

    - Per-process aiohttp session with bounded connection pool (limit=20).
    - Token-bucket rate limiter (`aiolimiter`) — default 5 req/s.
    - Exponential+jitter retry on 5xx/429/transport errors. 4xx propagates.
    - 30-day chunk splitter for historical date ranges; chunk results concat
      in arrival order. A failed chunk propagates after retries (no partial
      returns — avoids silent gaps).
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
        """GET a v3 JSON envelope, retrying retryable failures and rate-limiting."""
        # Default response is CSV; force JSON.
        if "format" not in params:
            params = {**params, "format": "json"}
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
                        body = await resp.text()
                        # v3 returns plain text for tier/auth errors with a 200 status.
                        # Detect: real envelope starts with '{', error text doesn't.
                        stripped = body.lstrip()
                        if not stripped.startswith("{"):
                            raise ThetaDataHttpError(
                                status=resp.status,
                                message=body[:512],
                                url=url,
                            )
                        # parse JSON
                        import json
                        return json.loads(body)

        raise RuntimeError("unreachable")  # tenacity reraise=True covers this

    @staticmethod
    def _envelope_rows(envelope: dict[str, Any]) -> list:
        return envelope.get("response") or []

    # -----------------------------------------------------------------------
    # Listing endpoints
    # -----------------------------------------------------------------------

    async def list_stock_symbols(self) -> list[str]:
        env = await self._get_envelope("/v3/stock/list/symbols", {})
        return [r["symbol"] for r in self._envelope_rows(env)]

    async def list_expirations(self, symbol: str) -> list[date]:
        """GET /v3/option/list/expirations — returns expiration dates."""
        env = await self._get_envelope("/v3/option/list/expirations", {"symbol": symbol})
        rows = self._envelope_rows(env)
        return [date.fromisoformat(r["expiration"]) for r in rows]

    async def list_strikes(self, symbol: str, expiration: date) -> list[Decimal]:
        """GET /v3/option/list/strikes — strikes in dollars (already)."""
        env = await self._get_envelope(
            "/v3/option/list/strikes",
            {"symbol": symbol, "expiration": expiration.isoformat()},
        )
        return [Decimal(str(r["strike"])) for r in self._envelope_rows(env)]

    # -----------------------------------------------------------------------
    # Option historical — chunked by 30 days
    # -----------------------------------------------------------------------

    async def _option_hist(
        self,
        path: str,
        symbol: str,
        expiration: date,
        strike: Decimal | float | str,
        right: str,
        start: int,
        end: int,
        interval: str = "1m",
    ) -> list[dict]:
        """Internal: chunked option historical GET.

        Returns a flat list of `data` rows across all returned contracts. When
        the v3 response wraps each contract in `{contract, data}`, this
        flattens to `data` rows only — callers that need the contract metadata
        per row should not use this method (none in the adapter today).
        """
        out: list[dict] = []
        for chunk_start, chunk_end in _chunk_date_range(start, end):
            params: dict[str, Any] = {
                "symbol": symbol,
                "expiration": expiration.isoformat(),
                "strike": str(strike) if not isinstance(strike, str) else strike,
                "right": _right_to_wire(right) if right not in ("call", "put", "both") else right,
                "start_date": str(chunk_start),
                "end_date": str(chunk_end),
                "interval": interval,
            }
            envelope = await self._get_envelope(path, params)
            for contract_block in self._envelope_rows(envelope):
                # `response` is either a list of {contract, data} blocks (when
                # multiple contracts match) or a single object — normalize.
                if isinstance(contract_block, dict) and "data" in contract_block:
                    out.extend(contract_block["data"])
                else:
                    # Some endpoints return rows directly under response.
                    out.append(contract_block)
        return out

    async def option_hist_quotes(
        self,
        symbol: str,
        expiration: date,
        strike: Decimal | float,
        right: str,
        start: int,
        end: int,
        interval: str = "1m",
    ) -> list[dict]:
        return await self._option_hist(
            "/v3/option/history/quote", symbol, expiration, strike, right, start, end, interval,
        )

    async def option_hist_trades(
        self,
        symbol: str,
        expiration: date,
        strike: Decimal | float,
        right: str,
        start: int,
        end: int,
        interval: str = "tick",
    ) -> list[dict]:
        return await self._option_hist(
            "/v3/option/history/trade", symbol, expiration, strike, right, start, end, interval,
        )

    async def option_hist_ohlc(
        self,
        symbol: str,
        expiration: date,
        strike: Decimal | float,
        right: str,
        start: int,
        end: int,
        interval: str = "1m",
    ) -> list[dict]:
        return await self._option_hist(
            "/v3/option/history/ohlc", symbol, expiration, strike, right, start, end, interval,
        )
