from systems.stage import SystemStage
from sysdata.config.configdata import Config
from sysdata.sim.crypto_sim_data import cryptoSimData

import pandas as pd
from copy import copy

from systems.system_cache import input, diagnostic, output
from syscore.dateutils import (
    MINUTES_IN_A_YEAR, ROOT_MINUTES_IN_A_YEAR, ROOT_DAYS_IN_YEAR,
    from_config_frequency_pandas_resample, from_config_frequency_to_frequency,
)
from syscore.genutils import list_intersection
from syscore.exceptions import missingData
from syscore.objects import resolve_function


class cryptoMinuteRawData(SystemStage):
    @output()
    def get_aggregated_minute_prices(self, instrument_code: str):
        minuteprice = self.get_minute_prices(instrument_code)
        agg_minuteprice = minuteprice.resample(self.barsize).agg(
            {
                'OPEN': 'first',
                'HIGH': 'max',
                'LOW': 'min',
                'FINAL': 'last',
                'VOLUME': 'sum',
            }
        )

        return agg_minuteprice

    @input
    def get_minute_prices(self, instrument_code: str):
        self.log.debug(
            "Calculating minute prices for %s" % instrument_code,
            instrument_code=instrument_code,
        )
        minuteprice = self.data_stage.minute_prices(instrument_code)

        if len(minuteprice) == 0:
            raise Exception(
                "Data for %s not found! Remove from instrument list, or add to config.ignore_instruments"
                % instrument_code
            )

        return minuteprice

    @output()
    def get_minute_final_prices(self, instrument_code: str):
        return self.get_minute_prices(instrument_code)['FINAL']

    @output()
    def get_minute_high_prices(self, instrument_code: str):
        return self.get_minute_prices(instrument_code)['HIGH']

    @output()
    def get_minute_low_prices(self, instrument_code: str):
        return self.get_minute_prices(instrument_code)['LOW']

    @output()
    def get_aggregated_minute_final_prices(self, instrument_code: str):
        minute_final_prices = self.get_minute_final_prices(instrument_code)
        agg_minute_final_prices = minute_final_prices.resample(self.barsize).agg(func='last').dropna()

        return agg_minute_final_prices

    @output()
    def get_aggregated_minute_high_prices(self, instrument_code: str):
        minute_final_prices = self.get_minute_high_prices(instrument_code)
        agg_minute_final_prices = minute_final_prices.resample(self.barsize).agg(func='max').dropna()

        return agg_minute_final_prices

    @output()
    def get_aggregated_minute_low_prices(self, instrument_code: str):
        minute_final_prices = self.get_minute_low_prices(instrument_code)
        agg_minute_final_prices = minute_final_prices.resample(self.barsize).agg(func='min').dropna()

        return agg_minute_final_prices

    @property
    def barsize(self):
        barsize = from_config_frequency_pandas_resample(
            from_config_frequency_to_frequency(self.config.barsize)
        )

        return barsize

    @property
    def name(self):
        return "rawdata"

    @property
    def data_stage(self) -> cryptoSimData:
        return self.parent.data

    @property
    def config(self) -> Config:
        return self.parent.config

    def get_raw_cost_data(self, instrument_code: str):
        return self.data_stage.get_raw_cost_data(instrument_code)

    @input
    def get_natural_frequency_prices(self, instrument_code: str) -> pd.DataFrame:
        self.log.debug(
            "Retrieving natural prices for %s" % instrument_code,
            instrument_code=instrument_code,
        )

        natural_prices = self.data_stage.get_raw_price(instrument_code)

        if len(natural_prices) == 0:
            raise Exception(
                "Data for %s not found! Remove from instrument list, or add to config.ignore_instruments"
            )

        return natural_prices

    @output()
    def aggregated_minute_returns(self, instrument_code: str) -> pd.Series:
        """
        Gets minute returns (not % returns)

        :param instrument_code: Instrument to get prices for
        :type trading_rules: str

        :returns: Tx1 pd.DataFrame


        """
        minute_prices = self.get_aggregated_minute_final_prices(instrument_code)
        minute_returns = minute_prices.diff()

        return minute_returns

    @output()
    def minute_returns(self, instrument_code: str) -> pd.Series:
        """
        Gets minute returns (not % returns)

        :param instrument_code: Instrument to get prices for
        :type trading_rules: str

        :returns: Tx1 pd.DataFrame


        """
        minute_prices = self.get_minute_final_prices(instrument_code)
        minute_returns = minute_prices.diff()

        return minute_returns

    @output()
    def annualised_returns_volatility(self, instrument_code: str) -> pd.Series:
        returns_volatility = self.returns_volatility(instrument_code)

        return returns_volatility * ROOT_DAYS_IN_YEAR

    @output()
    def returns_volatility(self, instrument_code: str) -> pd.Series:
        """
        Gets volatility of minute returns (not % returns)

        This is done using a user defined function

        We get this from:
          the configuration object
          or if not found, system.defaults.py

        The dict must contain func key; anything else is optional

        :param instrument_code: Instrument to get prices for
        :type trading_rules: str

        :returns: Tx1 pd.DataFrame

        >>> from systems.tests.testdata import get_test_object
        >>> from systems.basesystem import System
        >>>
        >>> (rawdata, data, config)=get_test_object()
        >>> system=System([rawdata], data)
        >>> ## uses defaults
        >>> system.rawdata.daily_returns_volatility("SOFR").tail(2)
                         vol
        2015-12-10  0.054145
        2015-12-11  0.058522
        >>>
        >>> from sysdata.config.configdata import Config
        >>> config=Config("systems.provided.example.exampleconfig.yaml")
        >>> system=System([rawdata], data, config)
        >>> system.rawdata.daily_returns_volatility("SOFR").tail(2)
                         vol
        2015-12-10  0.054145
        2015-12-11  0.058522
        >>>
        >>> config=Config(dict(volatility_calculation=dict(func="sysquant.estimators.vol.robust_vol_calc", days=200)))
        >>> system2=System([rawdata], data, config)
        >>> system2.rawdata.daily_returns_volatility("SOFR").tail(2)
                         vol
        2015-12-10  0.057946
        2015-12-11  0.058626

        """
        self.log.debug(
            "Calculating minute volatility for %s" % instrument_code,
            instrument_code=instrument_code,
        )

        volconfig = copy(self.config.volatility_calculation)

        which_returns = volconfig.pop("name_returns_attr_in_rawdata")
        returns_func = getattr(self, which_returns)
        price_returns = returns_func(instrument_code)

        # volconfig contains 'func' and some other arguments
        # we turn func which could be a string into a function, and then
        # call it with the other ags
        vol_multiplier = volconfig.pop("multiplier_to_get_daily_vol")

        volfunction = resolve_function(volconfig.pop("func"))
        raw_vol = volfunction(price_returns, **volconfig)

        vol = vol_multiplier * raw_vol

        return vol

    @output()
    def get_percentage_returns(self, instrument_code: str) -> pd.Series:
        """
        Get percentage returns

        Useful statistic, also used for some trading rules

        This is an optional subsystem; forecasts can go straight to system.data
        :param instrument_code: Instrument to get prices for
        :type trading_rules: str

        :returns: Tx1 pd.DataFrame
        """

        # UGLY
        denom_price = self.get_aggregated_minute_final_prices(instrument_code)
        num_returns = self.aggregated_minute_returns(instrument_code)
        perc_returns = num_returns / denom_price.ffill()

        return perc_returns

    @output()
    def get_percentage_volatility(self, instrument_code: str) -> pd.Series:
        """
        Get percentage returns normalised by recent vol

        Useful statistic, also used for some trading rules

        This is an optional subsystem; forecasts can go straight to system.data
        :param instrument_code: Instrument to get prices for
        :type trading_rules: str

        :returns: Tx1 pd.DataFrame

        >>> from systems.tests.testdata import get_test_object
        >>> from systems.basesystem import System
        >>>
        >>> (rawdata, data, config)=get_test_object()
        >>> system=System([rawdata], data)
        >>> system.rawdata.get_daily_percentage_volatility("SOFR").tail(2)
                         vol
        2015-12-10  0.055281
        2015-12-11  0.059789
        """
        denom_price = self.get_aggregated_minute_final_prices(instrument_code)
        return_vol = self.returns_volatility(instrument_code)
        (denom_price, return_vol) = denom_price.align(return_vol, join="right")
        perc_vol = 100.0 * (return_vol / denom_price.ffill().abs())

        return perc_vol

    @diagnostic()
    def get_vol_normalised_returns(self, instrument_code: str) -> pd.Series:
        """
        Get returns normalised by recent vol

        Useful statistic, also used for some trading rules

        This is an optional subsystem; forecasts can go straight to system.data
        :param instrument_code: Instrument to get prices for
        :type trading_rules: str

        :returns: Tx1 pd.DataFrame

        >>> from systems.tests.testdata import get_test_object
        >>> from systems.basesystem import System
        >>>
        >>> (rawdata, data, config)=get_test_object()
        >>> system=System([rawdata], data)
        >>> system.rawdata.get_daily_vol_normalised_returns("SOFR").tail(2)
                    norm_return
        2015-12-10    -1.219510
        2015-12-11     1.985413
        """
        self.log.debug(
            "Calculating normalised return for %s" % instrument_code,
            instrument_code=instrument_code,
        )

        returnvol = self.returns_volatility(instrument_code).shift(1)
        minutereturns = self.aggregated_minute_returns(instrument_code)
        norm_return = minutereturns / returnvol

        return norm_return

    @diagnostic()
    def get_cumulative_vol_normalised_returns(
        self, instrument_code: str
    ) -> pd.Series:
        """
        Returns a cumulative normalised return. This is like a price, but with equal expected vol
        Used for a few different trading rules

        :param instrument_code: str
        :return: pd.Series
        """

        self.log.debug(
            "Calculating cumulative normalised return for %s" % instrument_code,
            instrument_code=instrument_code,
        )

        norm_returns = self.get_vol_normalised_returns(instrument_code)

        cum_norm_returns = norm_returns.cumsum()

        return cum_norm_returns

    @diagnostic()
    def _aggregate_vol_normalised_returns_for_list_of_instruments(
        self, list_of_instruments: list
    ) -> pd.Series:
        """
        Average normalised returns across an asset class

        :param asset_class: str
        :return: pd.Series
        """

        aggregate_returns_across_instruments_list = [
            self.get_vol_normalised_returns(instrument_code)
            for instrument_code in list_of_instruments
        ]

        aggregate_returns_across_instruments = pd.concat(
            aggregate_returns_across_instruments_list, axis=1
        )

        # we don't ffill before working out the median as this could lead to
        # bad data
        median_returns = aggregate_returns_across_instruments.median(axis=1)

        return median_returns

    @diagnostic()
    def _vol_normalised_price_for_list_of_instruments(
        self, list_of_instruments: list
    ) -> pd.Series:
        norm_returns = (
            self._aggregate_vol_normalised_returns_for_list_of_instruments(
                list_of_instruments
            )
        )
        norm_price = norm_returns.cumsum()

        return norm_price

    @diagnostic()
    def _by_asset_class_vol_normalised_price_for_asset_class(
        self
    ) -> pd.Series:
        """
        Price for an asset class, built up from cumulative returns

        :param asset_class: str
        :return: pd.Series
        """

        instruments_in_crypto_asset_class = self.instrument_list()

        norm_price = self._vol_normalised_price_for_list_of_instruments(
            instruments_in_crypto_asset_class
        )

        return norm_price

    @diagnostic()
    def vol_normalised_price_for_asset_class_with_redundant_instrument_code(
        self, instrument_code: str, asset_class: str
    ) -> pd.Series:
        """
        Price for an asset class, built up from cumulative returns

        :param asset_class: str
        :return: pd.Series
        """

        return self._by_asset_class_vol_normalised_price_for_asset_class(
            asset_class
        )

    @diagnostic()
    def system_with_redundant_instrument_code_passed(
        self, instrument_code: str, asset_class: str
    ):
        ## allows ultimate flexibility when creating trading rules but be careful!

        return self.parent

    @diagnostic()
    def instrument_code(self, instrument_code: str) -> pd.Series:
        ## allows ultimate flexibility when creating trading rules

        return instrument_code

    @output()
    def normalised_price_for_asset_class(self, instrument_code: str) -> pd.Series:
        """

        :param instrument_code:
        :return:
        """

        normalised_price_for_asset_class = (
            self._by_asset_class_vol_normalised_price_for_asset_class()
        )
        normalised_price_this_instrument = (
            self.get_cumulative_vol_normalised_returns(instrument_code)
        )

        # Align for an easy life
        # As usual forward fill at last moment
        normalised_price_for_asset_class_aligned = (
            normalised_price_for_asset_class.reindex(
                normalised_price_this_instrument.index
            ).ffill()
        )

        return normalised_price_for_asset_class_aligned

    def instrument_list(self) -> list:
        instrument_list = self.parent.get_instrument_list()
        return instrument_list

    def get_value_of_block_price_move(self, instrument_code: str) -> float:
        return self.data_stage.get_value_of_block_price_move(instrument_code)

    def get_fx_for_instrument(self, instrument_code: str, base_currency: str):
        return self.data_stage.get_fx_for_instrument(
            instrument_code=instrument_code, base_currency=base_currency
        )

