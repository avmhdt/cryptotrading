import datetime

import pandas as pd

from syscore.exceptions import missingInstrument
from sysdata.sim.sim_data import simData

from sysobjects.instruments import (
    instrumentCosts,
    cryptoInstrumentWithMetaData
)
from sysobjects.crypto_prices import cryptoPrices
from sysobjects.spot_fx_prices import fxPrices


class cryptoSimData(simData):
    def __repr__(self):
        return "cryptoSimData object with %d instruments" % len(
            self.get_instrument_list()
        )

    def length_of_history_in_days_for_instrument(self, instrument_code: str) -> int:
        return len(self.daily_prices(instrument_code))

    def get_raw_price_from_start_date(
        self, instrument_code: str, start_date
    ) -> pd.DataFrame:
        """
        For crypto the price is the spot price

        :param instrument_code:
        :return: price
        """
        price = self.get_crypto_prices(instrument_code)
        if len(price) == 0:
            raise Exception("Instrument code %s has no data!" % instrument_code)

        return price.loc[start_date:]

    def get_raw_cost_data(self, instrument_code: str) -> instrumentCosts:
        """
        Gets cost data for an instrument

        Get cost data

        Execution slippage [half spread] price units
        Commission (local currency) per block
        Commission - percentage of value (0.01 is 1%)
        Commission (local currency) per block

        :param instrument_code: instrument to value for
        :type instrument_code: str

        :returns: dict of floats

        """

        try:
            cost_data_object = self.get_instrument_object_with_meta_data(
                instrument_code
            )
        except missingInstrument:
            self.log.warning(
                "Cost data missing for %s will use zero costs" % instrument_code
            )
            return instrumentCosts()

        spread_cost = self.get_spread_cost(instrument_code)

        instrument_meta_data = cost_data_object.meta_data
        instrument_costs = instrumentCosts.from_meta_data_and_spread_cost(
            instrument_meta_data, spread_cost=spread_cost
        )

        return instrument_costs

    def get_instrument_currency(self, instrument_code: str) -> str:
        """
        What is the currency that this instrument is priced in?

        :param instrument_code: instrument to get value for
        :type instrument_code: str

        :returns: str

        """
        instr_object = self.get_instrument_object_with_meta_data(instrument_code)
        meta_data = instr_object.meta_data
        currency = meta_data.Currency

        return currency

    def get_spread_cost(self, instrument_code: str) -> float:
        raise NotImplementedError

    def get_crypto_prices(
        self, instrument_code: str
    ) -> cryptoPrices:
        """

        :param instrument_code:
        :return:
        """

        raise NotImplementedError()

    def get_instrument_object_with_meta_data(
        self, instrument_code: str
    ) -> cryptoInstrumentWithMetaData:
        """
        Get data about an instrument, as a futuresInstrument

        :param instrument_code:
        :return: futuresInstrument object
        """

        raise NotImplementedError()

    def get_value_of_block_price_move(self, instrument_code: str) -> float:
        """
        How much is a $1 move worth in value terms?

        :param instrument_code: instrument to get value for
        :type instrument_code: str

        :returns: float

        """

        instr_object = self.get_instrument_object_with_meta_data(instrument_code)
        meta_data = instr_object.meta_data
        block_move_value = meta_data.Pointsize

        return block_move_value

    def _get_fx_data_from_start_date(
        self, currency1: str, currency2: str, start_date: datetime.datetime
    ) -> fxPrices:
        """
        Get the FX rate currency1/currency2 between two currencies
        Or return None if not available

        (Normally we'd over ride this with a specific source)


        """
        instrument_code = f'{currency1}_{currency2}'
        crypto_prices_df = self.get_raw_price_from_start_date(instrument_code, start_date)

        return fxPrices(crypto_prices_df.FINAL)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
