"""
ThetaData InstrumentProvider — resolves OCC option symbols to OptionContract.

ThetaData has too many contracts for a global preload; load_all_async returns
empty with one INFO log (operators should pass an explicit instrument set via
config.instrument_ids).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from nautilus_trader.adapters.thetadata.constants import THETADATA_VENUE
from nautilus_trader.adapters.thetadata.http import ThetaDataHttpClient
from nautilus_trader.adapters.thetadata.symbology import DecodeError, decode_occ
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.enums import AssetClass, OptionKind
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import OptionContract
from nautilus_trader.model.objects import Currency, Price, Quantity


# OPRA equity options: $100 multiplier, USD currency.
# Per-root overrides should land here when documented (mini-index XSP=$10, etc.)
_DEFAULT_MULTIPLIER = Quantity.from_int(100)
_DEFAULT_LOT_SIZE = Quantity.from_int(1)
_DEFAULT_PRICE_PRECISION = 2
_DEFAULT_TICK_SIZE = Price.from_str("0.01")


def _occ_to_instrument_id(raw_symbol: str) -> InstrumentId:
    """Wrap a raw OCC symbol as a venue-qualified InstrumentId."""
    return InstrumentId(Symbol(raw_symbol), THETADATA_VENUE)


def _expiry_to_ns(expiry_date) -> int:
    """Expiry midnight UTC nanos. Conservative for OPRA equity options."""
    dt = datetime(expiry_date.year, expiry_date.month, expiry_date.day, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


class ThetaDataInstrumentProvider(InstrumentProvider):
    """Resolves option contracts via OCC parse + ThetaData REST list_contracts.

    `load_async` builds an OptionContract from the OCC symbology alone (no
    network call needed — OCC carries root, expiry, kind, strike). `load_ids_async`
    parallelizes per-instrument loads with a concurrency cap. `load_all_async`
    is intentionally a no-op (ThetaData has hundreds of thousands of contracts).
    """

    def __init__(self, http_client: ThetaDataHttpClient, config=None):
        super().__init__(config=config)
        self._http_client = http_client
        self._semaphore = asyncio.Semaphore(8)

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        if instrument_id.venue != THETADATA_VENUE:
            raise ValueError(
                f"InstrumentId venue {instrument_id.venue} != {THETADATA_VENUE}"
            )
        try:
            root, expiry, right, strike = decode_occ(instrument_id.symbol.value)
        except DecodeError as exc:
            raise ValueError(f"Could not parse OCC symbol {instrument_id.symbol}: {exc}") from exc

        kind = OptionKind.CALL if right == "C" else OptionKind.PUT
        strike_price = Price(float(strike), _DEFAULT_PRICE_PRECISION)
        now_ns = self._wall_ns()
        expiration_ns = _expiry_to_ns(expiry)

        contract = OptionContract(
            instrument_id=instrument_id,
            raw_symbol=instrument_id.symbol,
            asset_class=AssetClass.EQUITY,
            currency=Currency.from_str("USD"),
            price_precision=_DEFAULT_PRICE_PRECISION,
            price_increment=_DEFAULT_TICK_SIZE,
            multiplier=_DEFAULT_MULTIPLIER,
            lot_size=_DEFAULT_LOT_SIZE,
            underlying=root,
            option_kind=kind,
            strike_price=strike_price,
            activation_ns=0,
            expiration_ns=expiration_ns,
            ts_event=now_ns,
            ts_init=now_ns,
        )
        self.add(contract)

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        if not instrument_ids:
            return

        async def one(iid: InstrumentId) -> None:
            async with self._semaphore:
                await self.load_async(iid, filters)

        await asyncio.gather(*[one(iid) for iid in instrument_ids])

    async def load_all_async(self, filters: dict | None = None) -> None:
        self._log.info(
            "ThetaData has too many contracts for a global load — "
            "use load_ids_async or load_async with an explicit set"
        )

    @staticmethod
    def _wall_ns() -> int:
        import time
        return time.time_ns()
