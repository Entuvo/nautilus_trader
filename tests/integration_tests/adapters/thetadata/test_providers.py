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

import pytest

from nautilus_trader.adapters.thetadata.providers import ThetaDataInstrumentProvider
from nautilus_trader.model.enums import OptionKind


@pytest.mark.asyncio
class TestThetaDataInstrumentProvider:
    async def test_load_all_async_raises_not_implemented(self, mock_http_client) -> None:
        provider = ThetaDataInstrumentProvider(client=mock_http_client)
        with pytest.raises(NotImplementedError):
            await provider.load_all_async()

    async def test_load_async_builds_option_contract_for_occ_symbol(
        self,
        mock_http_client,
        option_instrument_id,
    ) -> None:
        provider = ThetaDataInstrumentProvider(client=mock_http_client)
        await provider.load_async(option_instrument_id)

        instrument = provider.find(option_instrument_id)
        assert instrument is not None
        assert instrument.id == option_instrument_id
        assert instrument.option_kind == OptionKind.CALL
        assert instrument.underlying == "SPXW"
        assert str(instrument.strike_price) == "480.00"
        assert instrument.multiplier.as_decimal() == 100

    async def test_load_ids_async_iterates(
        self,
        mock_http_client,
        option_instrument_id,
    ) -> None:
        provider = ThetaDataInstrumentProvider(client=mock_http_client)
        await provider.load_ids_async([option_instrument_id])

        assert provider.find(option_instrument_id) is not None
