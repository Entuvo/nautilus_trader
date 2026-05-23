"""
Tests for nautilus_trader.adapters.thetadata.providers.
"""

from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.thetadata.constants import THETADATA_VENUE
from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider
from nautilus_trader.model.enums import AssetClass, OptionKind
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue


_OCC_SYMBOL = "AAPL250620C00175500"
_INSTRUMENT_ID = InstrumentId(Symbol(_OCC_SYMBOL), THETADATA_VENUE)


def _make_provider():
    http = MagicMock()
    return ThetaDataInstrumentProvider(http_client=http)


class TestLoadAsync:
    @pytest.mark.asyncio
    async def test_load_async_builds_option_contract(self, caplog):
        provider = _make_provider()
        await provider.load_async(_INSTRUMENT_ID)
        contract = provider.find(_INSTRUMENT_ID)
        assert contract is not None
        assert contract.asset_class == AssetClass.EQUITY
        assert contract.option_kind == OptionKind.CALL
        assert str(contract.strike_price) == "175.50"
        assert contract.underlying == "AAPL"
        assert str(contract.multiplier) == "100"

    @pytest.mark.asyncio
    async def test_load_async_put(self):
        put_iid = InstrumentId(Symbol("AAPL250620P00150000"), THETADATA_VENUE)
        provider = _make_provider()
        await provider.load_async(put_iid)
        contract = provider.find(put_iid)
        assert contract.option_kind == OptionKind.PUT
        assert str(contract.strike_price) == "150.00"

    @pytest.mark.asyncio
    async def test_load_async_wrong_venue_raises(self):
        provider = _make_provider()
        wrong_venue = InstrumentId(Symbol(_OCC_SYMBOL), Venue("NASDAQ"))
        with pytest.raises(ValueError, match="venue"):
            await provider.load_async(wrong_venue)

    @pytest.mark.asyncio
    async def test_load_async_malformed_occ_raises(self):
        provider = _make_provider()
        bad = InstrumentId(Symbol("BAD"), THETADATA_VENUE)
        with pytest.raises(ValueError, match="OCC"):
            await provider.load_async(bad)


class TestLoadIdsAsync:
    @pytest.mark.asyncio
    async def test_load_ids_async_loads_each(self):
        provider = _make_provider()
        iids = [
            _INSTRUMENT_ID,
            InstrumentId(Symbol("AAPL250620P00150000"), THETADATA_VENUE),
        ]
        await provider.load_ids_async(iids)
        assert provider.count == 2

    @pytest.mark.asyncio
    async def test_load_ids_async_empty_noop(self):
        provider = _make_provider()
        await provider.load_ids_async([])
        assert provider.count == 0


class TestLoadAllAsync:
    @pytest.mark.asyncio
    async def test_load_all_async_returns_empty(self, caplog):
        provider = _make_provider()
        await provider.load_all_async()
        assert provider.count == 0
