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

from typing import Any

from nautilus_trader.adapters.thetadata.constants import THETADATA_VENUE
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import instruments_from_pyo3


class ThetaDataInstrumentProvider(InstrumentProvider):
    """
    Provides Nautilus option-contract instrument definitions from ThetaData.

    ThetaData does not publish a single "all instruments" endpoint — every
    option chain is enumerated by `(symbol, expiration)`. ``load_all_async``
    therefore raises ``NotImplementedError``; callers must either preload a
    specific list of ``InstrumentId``s via ``load_ids_async``, or query the
    underlying ``nautilus_pyo3.ThetaDataHttpClient`` directly for bulk
    listing via ``list_expirations`` + ``list_strikes`` + ``list_contracts``.

    Parameters
    ----------
    client : nautilus_pyo3.ThetaDataHttpClient
        The ThetaData HTTP client used for symbology lookups.
    config : InstrumentProviderConfig, optional
        The instrument provider configuration.

    """

    def __init__(
        self,
        client: nautilus_pyo3.ThetaDataHttpClient,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._client = client
        self._log_warnings = config.log_warnings if config else True

        self._instruments_pyo3: list[Any] = []

    def instruments_pyo3(self) -> list[Any]:
        """
        Return all ThetaData PyO3 instrument definitions held by the provider.

        Returns
        -------
        list[nautilus_pyo3.OptionContract]

        """
        return self._instruments_pyo3

    async def load_all_async(self, filters: dict | None = None) -> None:
        raise NotImplementedError(
            "ThetaData does not expose a global instrument list. "
            "Use load_ids_async(...) with specific InstrumentIds, or call "
            "list_expirations / list_strikes / list_contracts on the underlying "
            "nautilus_pyo3.ThetaDataHttpClient.",
        )

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        if not instrument_ids:
            self._log.warning("No instrument IDs given for loading")
            return

        for instrument_id in instrument_ids:
            PyCondition.equal(
                instrument_id.venue,
                THETADATA_VENUE,
                "instrument_id.venue",
                "THETADATA",
            )
            await self.load_async(instrument_id, filters)

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        PyCondition.not_none(instrument_id, "instrument_id")
        PyCondition.equal(
            instrument_id.venue,
            THETADATA_VENUE,
            "instrument_id.venue",
            "THETADATA",
        )

        pyo3_instrument_id = nautilus_pyo3.InstrumentId.from_str(instrument_id.value)
        pyo3_venue = nautilus_pyo3.Venue(THETADATA_VENUE.value)
        try:
            pyo3_contract = nautilus_pyo3.ThetaDataHttpClient.option_contract_from_id(
                pyo3_instrument_id,
                pyo3_venue,
            )
        except Exception as e:
            if self._log_warnings:
                self._log.warning(f"Failed to build OptionContract for {instrument_id}: {e}")
            return

        self._instruments_pyo3.append(pyo3_contract)
        for instrument in instruments_from_pyo3([pyo3_contract]):
            self.add(instrument=instrument)
