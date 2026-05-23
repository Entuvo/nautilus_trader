"""
Tests for nautilus_trader.adapters.thetadata.data (ThetaDataDataClient).

The data client orchestrates http_client + ws_client + provider, so tests
inject mocks for the IO collaborators and assert routing decisions.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from nautilus_trader.adapters.thetadata.config import ThetaDataDataClientConfig
from nautilus_trader.adapters.thetadata.constants import THETADATA_VENUE
from nautilus_trader.adapters.thetadata.data import (
    ThetaDataDataClient,
    _contract_from_instrument_id,
)
from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.data import BarAggregation, BarSpecification, BarType
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import InstrumentId, Symbol, TraderId


_INSTRUMENT_ID = InstrumentId(Symbol("AAPL250620C00175500"), THETADATA_VENUE)


def _build_client(ws_client=None, http_client=None, instrument_ids=None):
    loop = asyncio.new_event_loop()
    clock = LiveClock()
    trader_id = TraderId("TESTER-001")
    msgbus = MessageBus(trader_id=trader_id, clock=clock)
    cache = Cache()
    http_client = http_client or MagicMock()
    http_client.close = AsyncMock()
    http_client.option_hist_quotes = AsyncMock(return_value=[])
    http_client.option_hist_trades = AsyncMock(return_value=[])
    http_client.option_hist_ohlc = AsyncMock(return_value=[])

    ws_client = ws_client or MagicMock()
    ws_client.connect = AsyncMock()
    ws_client.close = AsyncMock()
    ws_client.subscribe_quotes = AsyncMock()
    ws_client.subscribe_trades = AsyncMock()
    ws_client.unsubscribe_quotes = AsyncMock()
    ws_client.unsubscribe_trades = AsyncMock()
    ws_client.cache_instrument = MagicMock()
    ws_client.set_quote_handler = MagicMock()
    ws_client.set_trade_handler = MagicMock()

    provider = ThetaDataInstrumentProvider(http_client=http_client)
    config = ThetaDataDataClientConfig(instrument_ids=instrument_ids)
    client = ThetaDataDataClient(
        loop=loop,
        http_client=http_client,
        ws_client=ws_client,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=config,
    )
    return client, ws_client, http_client, provider, loop


class TestContractFromInstrumentId:
    def test_call_175_50(self):
        contract = _contract_from_instrument_id(_INSTRUMENT_ID)
        assert contract == {
            "security_type": "OPTION",
            "root": "AAPL",
            "expiration": 20250620,
            "strike": 1755000,  # 175.500 → 1/10¢ = ×10000
            "right": "C",
        }

    def test_put(self):
        iid = InstrumentId(Symbol("AAPL250620P00150000"), THETADATA_VENUE)
        contract = _contract_from_instrument_id(iid)
        assert contract["right"] == "P"
        assert contract["strike"] == 1500000


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_connect_opens_ws(self):
        client, ws_client, *_ = _build_client()
        await client._connect()
        ws_client.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_disconnect_closes_both(self):
        client, ws_client, http_client, *_ = _build_client()
        await client._disconnect()
        ws_client.close.assert_awaited_once()
        http_client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_preloads_configured_instruments(self):
        client, ws_client, _http, _provider, _loop = _build_client(
            instrument_ids=[_INSTRUMENT_ID]
        )
        await client._connect()
        ws_client.cache_instrument.assert_called()


class TestSubscribeRouting:
    @pytest.mark.asyncio
    async def test_subscribe_quote_ticks(self):
        client, ws_client, *_ = _build_client()
        cmd = MagicMock()
        cmd.instrument_id = _INSTRUMENT_ID
        await client._subscribe_quote_ticks(cmd)
        ws_client.subscribe_quotes.assert_awaited_once()
        # Decode context was cached for this instrument.
        ws_client.cache_instrument.assert_called()

    @pytest.mark.asyncio
    async def test_subscribe_trade_ticks(self):
        client, ws_client, *_ = _build_client()
        cmd = MagicMock()
        cmd.instrument_id = _INSTRUMENT_ID
        await client._subscribe_trade_ticks(cmd)
        ws_client.subscribe_trades.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unsubscribe_quote_ticks(self):
        client, ws_client, *_ = _build_client()
        cmd = MagicMock()
        cmd.instrument_id = _INSTRUMENT_ID
        await client._unsubscribe_quote_ticks(cmd)
        ws_client.unsubscribe_quotes.assert_awaited_once_with(_INSTRUMENT_ID)


class TestRequestPaths:
    @pytest.mark.asyncio
    async def test_request_quote_ticks_decodes_rows(self):
        client, _ws, http_client, *_ = _build_client()
        http_client.option_hist_quotes = AsyncMock(return_value=[
            {"timestamp": "2024-06-20T09:30:01.000",
             "bid_size": 5, "bid": 39.00, "ask_size": 1, "ask": 39.70}
        ])
        req = MagicMock()
        req.instrument_id = _INSTRUMENT_ID
        req.start = datetime(2024, 6, 20, tzinfo=timezone.utc)
        req.end = datetime(2024, 6, 20, tzinfo=timezone.utc)
        req.correlation_id = UUID4()
        req.params = None
        await client._request_quote_ticks(req)
        # Verify call signature uses v3 client method with parsed OCC.
        http_client.option_hist_quotes.assert_awaited_once()
        _, kwargs = http_client.option_hist_quotes.call_args
        assert kwargs["symbol"] == "AAPL"
        assert kwargs["right"] == "C"
        assert kwargs["start"] == 20240620
        assert kwargs["end"] == 20240620


class TestBarValidation:
    @pytest.mark.asyncio
    async def test_request_bars_rejects_tick_aggregation(self):
        client, *_ = _build_client()
        spec = BarSpecification(1, BarAggregation.TICK, PriceType.LAST)
        bar_type = BarType(_INSTRUMENT_ID, spec)
        req = MagicMock()
        req.bar_type = bar_type
        with pytest.raises(ValueError, match="aggregation"):
            await client._request_bars(req)

    @pytest.mark.asyncio
    async def test_request_bars_rejects_mid_price_type(self):
        client, *_ = _build_client()
        spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.MID)
        bar_type = BarType(_INSTRUMENT_ID, spec)
        req = MagicMock()
        req.bar_type = bar_type
        with pytest.raises(ValueError, match="price_type"):
            await client._request_bars(req)

    @pytest.mark.asyncio
    async def test_request_bars_minute_succeeds(self):
        client, _ws, http_client, *_ = _build_client()
        # Option prices: 2 decimal precision (matches OptionContract default).
        http_client.option_hist_ohlc = AsyncMock(return_value=[
            {"timestamp": "2024-06-20T09:30:00.000",
             "open": 1.86, "high": 1.95, "low": 1.80, "close": 1.90,
             "volume": 87758, "vwap": 1.88, "count": 1319}
        ])
        spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
        bar_type = BarType(_INSTRUMENT_ID, spec)
        req = MagicMock()
        req.bar_type = bar_type
        req.start = datetime(2024, 6, 20, tzinfo=timezone.utc)
        req.end = datetime(2024, 6, 20, tzinfo=timezone.utc)
        req.correlation_id = UUID4()
        req.params = None
        await client._request_bars(req)
        http_client.option_hist_ohlc.assert_awaited_once()
        _, kwargs = http_client.option_hist_ohlc.call_args
        assert kwargs["interval"] == "1m"
        assert kwargs["symbol"] == "AAPL"
