#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Tick-trade backfill for SPX + SPXW options into a Nautilus ``ParquetDataCatalog``.

Python counterpart to ``crates/adapters/thetadata/examples/backfill_trades.rs``.
For each weekday in the lookback window:

1. Fetch the underlying index close (SPX EOD).
2. List active expirations within the next ``EXPIRATION_LOOKAHEAD_DAYS`` days.
3. For each expiration, pick ``STRIKES_PER_SIDE`` strikes on each side of the close.
4. Fan out ``hist_trades`` requests under a semaphore (Standard-tier default: 4).
5. Write per-instrument ticks into the catalog under
   ``$THETADATA_CATALOG_DIR/data/trade_tick/<safe_instrument_id>/``.

Environment:

- ``THETADATA_CATALOG_DIR``  (default ``./data/thetadata-catalog``)
- ``THETADATA_DAYS_BACK``    (default ``30``)
- ``THETADATA_CONCURRENCY``  (default ``4``)
- ``THETADATA_HTTP_URL``     (default local terminal at ``127.0.0.1:25503``)

Requires a running ThetaTerminal that the configured ``THETADATA_HTTP_URL`` points to.
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections import defaultdict
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from nautilus_trader.adapters.thetadata.constants import DEFAULT_HTTP_URL
from nautilus_trader.adapters.thetadata.constants import THETADATA_VENUE
from nautilus_trader.core.nautilus_pyo3 import InstrumentId as Pyo3InstrumentId
from nautilus_trader.core.nautilus_pyo3 import ThetaDataHttpClient  # pyright: ignore[reportAttributeAccessIssue]
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog


ROOTS: list[str] = ["SPX", "SPXW"]
INDEX_UNDERLYING: dict[str, str] = {"SPX": "SPX", "SPXW": "SPX"}
STRIKES_PER_SIDE = 20
EXPIRATION_LOOKAHEAD_DAYS = 45
PRICE_PRECISION = 2
SIZE_PRECISION = 0
DEFAULT_DAYS_BACK = 30
DEFAULT_CONCURRENCY = 4
HTTP_TIMEOUT_SECS = 60

NY = ZoneInfo("America/New_York")


def log(msg: str) -> None:
    print(msg, flush=True)


def is_weekday(d: date) -> bool:
    return d.weekday() < 5


def build_occ_instrument_id(
    root: str,
    expiration: date,
    strike_dollars: float,
    right: str,
) -> InstrumentId:
    # OCC-style symbol: ROOT + YYMMDD + (C|P) + 8-digit strike in thousandths of a dollar.
    # Mirrors ThetaOptionContract::to_instrument_id in crates/adapters/thetadata/src/symbology.rs.
    strike_thousandths = int(round(strike_dollars * 1000))
    yy = expiration.strftime("%y")
    mmdd = expiration.strftime("%m%d")
    symbol = f"{root}{yy}{mmdd}{right}{strike_thousandths:08d}"
    return InstrumentId.from_str(f"{symbol}.{THETADATA_VENUE.value}")


def pick_atm_strikes(
    strikes: list[float],
    close: float,
    per_side: int,
) -> list[float]:
    uniq = sorted({round(s, 6) for s in strikes})
    below = [s for s in uniq if s <= close]
    above = [s for s in uniq if s > close]
    return list(reversed(below))[: per_side + 1] + above[:per_side]


def with_ts_init_from_event(tick: TradeTick) -> TradeTick:
    # Catalog determinism: pin ts_init to ts_event so reruns produce identical files.
    return TradeTick(
        instrument_id=tick.instrument_id,
        price=tick.price,
        size=tick.size,
        aggressor_side=tick.aggressor_side,
        trade_id=tick.trade_id,
        ts_event=tick.ts_event,
        ts_init=tick.ts_event,
    )


async def fetch_close(
    http: ThetaDataHttpClient,
    root: str,
    day: date,
) -> float | None:
    symbol = INDEX_UNDERLYING.get(root, root)
    iso = day.isoformat()
    rows = await http.hist_index_eod(symbol, iso, iso)
    if not rows:
        return None
    # Row tuple is (last_trade, open, high, low, close, volume, count).
    return rows[0][4]


async def fetch_trades_for_contract(
    http: ThetaDataHttpClient,
    instrument_id: InstrumentId,
    day: date,
    semaphore: asyncio.Semaphore,
) -> tuple[InstrumentId, list[TradeTick]]:
    iso = day.isoformat()
    async with semaphore:
        try:
            pyo3_id = Pyo3InstrumentId.from_str(instrument_id.value)
            pyo3_ticks = await http.hist_trades(
                pyo3_id,
                iso,
                iso,
                PRICE_PRECISION,
                SIZE_PRECISION,
                None,
            )
        except Exception as e:
            # 472 (no-data) and similar are normal for OTM strikes with no prints.
            msg = str(e)
            if "472" not in msg:
                log(f"[backfill] hist_trades {instrument_id} {day}: {e}")
            return instrument_id, []
    return instrument_id, TradeTick.from_pyo3_list(pyo3_ticks)


async def process_day_root(
    http: ThetaDataHttpClient,
    catalog: ParquetDataCatalog,
    semaphore: asyncio.Semaphore,
    day: date,
    root: str,
    counters: dict[str, int],
) -> None:
    try:
        close = await fetch_close(http, root, day)
    except Exception as e:
        log(f"[backfill] {root} {day} close error: {e}")
        return
    if close is None:
        log(f"[backfill] {root} {day} no close (non-trading day?)")
        return

    try:
        expirations_raw = await http.list_expirations(root)
    except Exception as e:
        log(f"[backfill] {root} list_expirations error: {e}")
        return

    expirations: list[date] = []
    for e_str in expirations_raw:
        try:
            expirations.append(date.fromisoformat(e_str))
        except ValueError:
            continue
    max_exp = day + timedelta(days=EXPIRATION_LOOKAHEAD_DAYS)
    active = [e for e in expirations if day <= e <= max_exp]

    tasks: list[asyncio.Task] = []
    contract_count = 0
    for exp in active:
        try:
            strikes = await http.list_strikes(root, exp.isoformat())
        except Exception as e:
            log(f"[backfill] {root} {exp} list_strikes error: {e}")
            continue
        atm = pick_atm_strikes(strikes, close, STRIKES_PER_SIDE)
        for strike in atm:
            for right in ("C", "P"):
                try:
                    instrument_id = build_occ_instrument_id(root, exp, strike, right)
                except Exception as e:
                    log(f"[backfill] bad contract: {e}")
                    continue
                tasks.append(
                    asyncio.create_task(
                        fetch_trades_for_contract(http, instrument_id, day, semaphore),
                    ),
                )
                counters["requests"] += 1
                contract_count += 1

    results = await asyncio.gather(*tasks, return_exceptions=True)

    grouped: dict[InstrumentId, list[TradeTick]] = defaultdict(list)
    for r in results:
        if isinstance(r, BaseException):
            log(f"[backfill] task error: {r}")
            continue
        instrument_id, ticks = r
        for tick in ticks:
            grouped[instrument_id].append(with_ts_init_from_event(tick))

    day_trades = 0
    day_files = 0
    for instrument_id, ticks in grouped.items():
        if not ticks:
            continue
        ticks.sort(key=lambda t: t.ts_event)
        try:
            await asyncio.to_thread(
                catalog.write_data,
                ticks,
                skip_disjoint_check=True,
            )
            day_trades += len(ticks)
            day_files += 1
        except Exception as e:
            log(f"[backfill] write error for {instrument_id}: {e}")

    counters["trades"] += day_trades
    counters["files"] += day_files
    log(
        f"[backfill] {root} {day} contracts={contract_count} "
        f"trades={day_trades} files={day_files} "
        f"(req_total={counters['requests']} "
        f"trade_total={counters['trades']} "
        f"file_total={counters['files']})",
    )


async def amain() -> int:
    catalog_dir = Path(
        os.environ.get("THETADATA_CATALOG_DIR", "./data/thetadata-catalog"),
    ).resolve()
    catalog_dir.mkdir(parents=True, exist_ok=True)

    days_back = int(os.environ.get("THETADATA_DAYS_BACK", DEFAULT_DAYS_BACK))
    concurrency = int(os.environ.get("THETADATA_CONCURRENCY", DEFAULT_CONCURRENCY))
    http_url = os.environ.get("THETADATA_HTTP_URL", DEFAULT_HTTP_URL)

    http = ThetaDataHttpClient(http_url, HTTP_TIMEOUT_SECS)
    catalog = ParquetDataCatalog(str(catalog_dir))
    semaphore = asyncio.Semaphore(concurrency)

    today_et = datetime.now(NY).date()
    candidate_days = (today_et - timedelta(days=i) for i in range(1, days_back + 1))
    trading_days = sorted(d for d in candidate_days if is_weekday(d))

    log(
        f"[backfill] catalog={catalog_dir} days_back={days_back} "
        f"trading_days={len(trading_days)} concurrency={concurrency} "
        f"http_url={http_url}",
    )

    counters: dict[str, int] = {"requests": 0, "trades": 0, "files": 0}
    for day in trading_days:
        for root in ROOTS:
            await process_day_root(http, catalog, semaphore, day, root, counters)

    log(
        f"[backfill] DONE total_requests={counters['requests']} "
        f"total_trades={counters['trades']} total_files={counters['files']}",
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
