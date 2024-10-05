from sysdata.sim.crypto_sim_data import cryptoSimData

from sysdata.crypto.crypto import cryptoPricesData
from sysdata.crypto.instruments import cryptoInstrumentData
from sysdata.crypto.spread_costs import spreadCostData
from sysdata.data_blob import dataBlob


from sysobjects.instruments import (
    cryptoInstrumentWithMetaData,
)
from syscore.exceptions import missingData
from sysobjects.crypto_prices import cryptoPrices


class genericBlobUsingCryptoSimData(cryptoSimData):
    """
    dataBlob must have the appropriate classes added or it won't work
    """

    def __init__(self, data: dataBlob):
        super().__init__(log=data.log)
        self._data = data

    def get_instrument_list(self):
        return self.db_crypto_prices_data.get_list_of_instruments()

    def get_all_instrument_data_as_df(self):
        all_instrument_data = (
            self.db_crypto_instrument_data.get_all_instrument_data_as_df()
        )
        instrument_list = self.get_instrument_list()
        all_instrument_data = all_instrument_data[
            all_instrument_data.index.isin(instrument_list)
        ]

        return all_instrument_data

    def get_crypto_prices(
        self, instrument_code: str
    ) -> cryptoPrices:
        data = self.db_crypto_prices_data.get_crypto_prices(instrument_code)

        return data

    def get_instrument_meta_data(
        self, instrument_code: str
    ) -> cryptoInstrumentWithMetaData:
        ## cost and other meta data stored in the same place
        return self.get_instrument_object_with_meta_data(instrument_code)

    def get_instrument_object_with_meta_data(
        self, instrument_code: str
    ) -> cryptoInstrumentWithMetaData:
        instrument = self.db_crypto_instrument_data.get_instrument_data(
            instrument_code
        )

        return instrument

    def get_spread_cost(self, instrument_code: str) -> float:
        return self.db_spread_cost_data.get_spread_cost(instrument_code)

    @property
    def data(self):
        return self._data

    @property
    def db_crypto_prices_data(self) -> cryptoPricesData:
        return self.data.db_crypto_prices

    @property
    def db_crypto_instrument_data(self) -> cryptoInstrumentData:
        return self.data.db_crypto_instrument

    @property
    def db_spread_cost_data(self) -> spreadCostData:
        return self.data.db_spread_cost

