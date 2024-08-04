import pandas as pd
import datetime
from copy import copy

from syscore.dateutils import ROOT_BDAYS_INYEAR
from syscore.exceptions import missingData
from syscore.genutils import str2Bool, list_union
from syscore.pandas.pdutils import (
    from_dict_of_values_to_df,
    from_scalar_values_to_ts,
)
from syscore.pandas.find_data import get_row_of_df_aligned_to_weights_as_dict
from syscore.pandas.strategy_functions import (
    weights_sum_to_one,
    fix_weights_vs_position_or_forecast,
)
from syscore.objects import resolve_function
from syscore.constants import arg_not_supplied

from sysdata.config.configdata import Config

from sysquant.estimators.stdev_estimator import stdevEstimates, seriesOfStdevEstimates
from sysquant.estimators.correlations import (
    correlationEstimate,
    create_boring_corr_matrix,
    CorrelationList,
)
from sysquant.estimators.covariance import (
    covarianceEstimate,
    covariance_from_stdev_and_correlation,
)
from sysquant.estimators.turnover import turnoverDataAcrossSubsystems
from sysquant.portfolio_risk import (
    calc_portfolio_risk_series,
    calc_sum_annualised_risk_given_portfolio_weights,
)
from sysquant.optimisation.pre_processing import returnsPreProcessor
from sysquant.optimisation.weights import portfolioWeights, seriesOfPortfolioWeights

from sysquant.returns import (
    dictOfReturnsForOptimisationWithCosts,
    returnsForOptimisationWithCosts,
)

from systems.buffering import (
    calculate_buffers,
    calculate_actual_buffers,
    apply_buffers_to_position,
)
from systems.stage import SystemStage
from systems.system_cache import input, dont_cache, diagnostic, output
from private.systems.orion.positionsizing.positionsizing import OrionPositionSizing as PositionSizing
from private.systems.orion.accounts.curves.account_curve_group import accountCurveGroup
from systems.risk_overlay import get_risk_multiplier
from systems.basesystem import get_instrument_weights_from_config

"""
Stage for portfolios

Gets the position, accounts for instrument weights and diversification
multiplier


Note: At this stage we're dealing with a notional, fixed, amount of capital.
     We'll need to work out p&l to scale positions properly
"""


class OrionPortfolios(SystemStage):
    @property
    def name(self):
        return "portfolio"

    # actual positions and buffers
    @output()
    def get_actual_position(self, instrument_code: str) -> pd.Series:
        """
        Gets the actual position, accounting for cap multiplier

        :param instrument_code: instrument to get values for
        :type instrument_code: str

        :returns: Tx1 pd.Series

        KEY OUTPUT
        """

        self.log.debug(
            "Calculating actual position for %s" % instrument_code,
            instrument_code=instrument_code,
        )

        notional_position = self.get_notional_position(instrument_code)
        cap_multiplier = self.capital_multiplier()
        cap_multiplier = cap_multiplier.reindex(notional_position.index).ffill()

        actual_position = notional_position * cap_multiplier

        return actual_position

    ## notional position
    @output()
    def get_notional_position(self, instrument_code: str) -> pd.Series:
        """
        Gets the position, accounts for instrument weights and diversification multiplier

        Note: At this stage we're dealing with a notional, fixed, amount of capital.
             We'll need to work out p&l to scale positions properly

        :param instrument_code: instrument to get values for
        :type instrument_code: str

        :returns: Tx1 pd.DataFrame

        KEY OUTPUT
        >>> from systems.tests.testdata import get_test_object_futures_with_pos_sizing
        >>> from systems.basesystem import System
        >>> (posobject, combobject, capobject, rules, rawdata, data, config)=get_test_object_futures_with_pos_sizing()
        >>> system=System([rawdata, rules, posobject, combobject, capobject,Portfolios()], data, config)
        >>>
        >>> ## from config
        >>> system.portfolio.get_notional_position("EDOLLAR").tail(2)
                         pos
        2015-12-10  1.086879
        2015-12-11  1.526759

        """

        self.log.debug(
            "Calculating notional position for %s" % instrument_code,
            instrument_code=instrument_code,
        )

        instr_weights = self.get_raw_fixed_instrument_weights()

        # unknown frequency
        subsys_position = self.get_subsystem_position(instrument_code)

        # daily
        instrument_weight_this_code = instr_weights[instrument_code]

        inst_weight_this_code_reindexed = instrument_weight_this_code.reindex(
            subsys_position.index, method="ffill"
        )

        notional_position_without_idm = (
                subsys_position * inst_weight_this_code_reindexed
        )

        # subsystem frequency
        return notional_position_without_idm


    ## INSTRUMENT WEIGHTS
    @diagnostic()
    def get_subsystem_positions_for_instrument_list(
        self, instrument_list: list
    ) -> pd.DataFrame:
        subsystem_positions = [
            self.get_subsystem_position(instrument_code)
            for instrument_code in instrument_list
        ]

        subsystem_positions = pd.concat(subsystem_positions, axis=1).ffill()
        subsystem_positions.columns = instrument_list

        return subsystem_positions

    ## FIXED INSTRUMENT WEIGHTS
    @diagnostic()
    def get_raw_fixed_instrument_weights(self) -> pd.DataFrame:
        """
        Get the instrument weights
        These are 'raw' because we need to account for potentially missing positions, and weights that don't add up.
        From: (a) passed into subsystem when created
              (b) ... if not found then: in system.config.instrument_weights
        :returns: TxK pd.DataFrame containing weights, columns are instrument names, T covers all subsystem positions
        >>> from systems.tests.testdata import get_test_object_futures_with_pos_sizing
        >>> from systems.basesystem import System
        >>> (posobject, combobject, capobject, rules, rawdata, data, config)=get_test_object_futures_with_pos_sizing()
        >>> config.instrument_weights=dict(EDOLLAR=0.1, US10=0.9)
        >>> system=System([rawdata, rules, posobject, combobject, capobject,Portfolios()], data, config)
        >>>
        >>> ## from config
        >>> system.portfolio.get_instrument_weights().tail(2)
                    EDOLLAR  US10
        2015-12-10      0.1   0.9
        2015-12-11      0.1   0.9
        >>>
        >>> del(config.instrument_weights)
        >>> system2=System([rawdata, rules, posobject, combobject, capobject,Portfolios()], data, config)
        >>> system2.portfolio.get_instrument_weights().tail(2)
        WARNING: No instrument weights  - using equal weights of 0.3333 over all 3 instruments in data
                        BUND   EDOLLAR      US10
        2015-12-10  0.333333  0.333333  0.333333
        2015-12-11  0.333333  0.333333  0.333333
        """

        self.log.debug("Calculating raw instrument weights")
        instrument_weights_dict = self.get_fixed_instrument_weights_from_config()

        # Now we have a dict, fixed_weights.
        # Need to turn into a timeseries covering the range of subsystem positions
        instrument_list = self.get_instrument_list()

        subsystem_positions = self._get_all_subsystem_positions()
        position_series_index = subsystem_positions.index

        # CHANGE TO TXN DATAFRAME
        instrument_weights = from_dict_of_values_to_df(
            instrument_weights_dict, position_series_index, columns=instrument_list
        )

        return instrument_weights

    @diagnostic()
    def get_fixed_instrument_weights_from_config(self) -> dict:
        try:
            instrument_weights_dict = get_instrument_weights_from_config(self.config)
        except:
            instrument_weights_dict = self.get_equal_instrument_weights_dict()

        instrument_weights_dict = self._add_zero_instrument_weights(
            instrument_weights_dict
        )

        return instrument_weights_dict

    @dont_cache
    def get_equal_instrument_weights_dict(self) -> dict:
        instruments_with_weights = self.get_instrument_list(for_instrument_weights=True)
        weight = 1.0 / len(instruments_with_weights)

        warn_msg = (
            "WARNING: No instrument weights  - using equal weights of %.4f over all %d instruments in data"
            % (weight, len(instruments_with_weights))
        )

        self.log.warning(warn_msg)

        instrument_weights = dict(
            [(instrument_code, weight) for instrument_code in instruments_with_weights]
        )

        return instrument_weights

    def _add_zero_instrument_weights(self, instrument_weights: dict) -> dict:
        copy_instrument_weights = copy(instrument_weights)
        instruments_with_zero_weights = (
            self.allocate_zero_instrument_weights_to_these_instruments()
        )
        for instrument_code in instruments_with_zero_weights:
            copy_instrument_weights[instrument_code] = 0.0

        return copy_instrument_weights

    def _remove_zero_weighted_instruments_from_df(
        self, some_data_frame: pd.DataFrame
    ) -> pd.DataFrame:
        copy_df = copy(some_data_frame)
        instruments_with_zero_weights = (
            self.allocate_zero_instrument_weights_to_these_instruments()
        )
        copy_df.drop(labels=instruments_with_zero_weights)

        return copy_df

    ## INPUT
    @diagnostic()
    def _get_all_subsystem_positions(self) -> pd.DataFrame:
        """

        :return: single pd.matrix of all the positions
        """
        instrument_list = self.get_instrument_list()

        positions = self.get_subsystem_positions_for_instrument_list(instrument_list)

        return positions

    ## ESTIMATED WEIGHTS
    def _add_zero_weights_to_instrument_weights_df(
        self, instrument_weights: pd.DataFrame
    ) -> pd.DataFrame:
        instrument_list_to_add = (
            self.allocate_zero_instrument_weights_to_these_instruments()
        )
        weight_index = instrument_weights.index
        new_pd_as_dict = dict(
            [
                (instrument_code, pd.Series([0.0] * len(weight_index), index=weight_index))
                for instrument_code in instrument_list_to_add
            ]
        )
        new_pd = pd.DataFrame(new_pd_as_dict)

        padded_instrument_weights = pd.concat([instrument_weights, new_pd], axis=1)

        return padded_instrument_weights

    @diagnostic()
    def allocate_zero_instrument_weights_to_these_instruments(
        self, auto_remove_bad_instruments: bool = False
    ) -> list:
        config_allocate_zero_instrument_weights_to_these_instruments = (
            self.config_allocates_zero_instrument_weights_to_these_instruments(
                auto_remove_bad_instruments=auto_remove_bad_instruments
            )
        )

        instruments_without_data_or_weights = self.instruments_without_data_or_weights()

        all_instruments_to_allocate_zero_to = list_union(
            instruments_without_data_or_weights,
            config_allocate_zero_instrument_weights_to_these_instruments,
        )

        return all_instruments_to_allocate_zero_to

    def config_allocates_zero_instrument_weights_to_these_instruments(
        self, auto_remove_bad_instruments: bool = False
    ):
        bad_from_config = self.parent.get_list_of_markets_not_trading_but_with_data()
        config = self.config
        config_allocates_zero_instrument_weights_to_these_instruments = getattr(
            config, "allocate_zero_instrument_weights_to_these_instruments", []
        )
        instrument_list = self.get_instrument_list()
        config_marks_bad_and_in_instrument_list = list(
            set(instrument_list).intersection(set(bad_from_config))
        )
        configured_bad_but_not_configured_zero_allocation = list(
            set(config_marks_bad_and_in_instrument_list).difference(
                set(config_allocates_zero_instrument_weights_to_these_instruments)
            )
        )

        allocate_zero_instrument_weights_to_these_instruments = copy(
            config_allocates_zero_instrument_weights_to_these_instruments
        )
        if len(configured_bad_but_not_configured_zero_allocation) > 0:
            if auto_remove_bad_instruments:
                self.log.warning(
                    "*** Following instruments are listed as trading_restrictions and/or bad_markets and will be removed from instrument weight optimisation: ***\n%s"
                    % str(configured_bad_but_not_configured_zero_allocation)
                )
                allocate_zero_instrument_weights_to_these_instruments = (
                    allocate_zero_instrument_weights_to_these_instruments
                    + configured_bad_but_not_configured_zero_allocation
                )
            else:
                self.log.warning(
                    "*** Following instruments are listed as trading_restrictions and/or bad_markets but still included in instrument weight optimisation: ***\n%s"
                    % str(configured_bad_but_not_configured_zero_allocation)
                )
                self.log.warning(
                    "This is fine for dynamic systems where we remove them in later optimisation, but may be problematic for static systems"
                )
                self.log.warning(
                    "Consider adding to config element allocate_zero_instrument_weights_to_these_instruments"
                )

        if len(allocate_zero_instrument_weights_to_these_instruments) > 0:
            self.log.debug(
                "Following instruments will have zero weight in optimisation of instrument weights as configured zero or auto removal of configured bad%s"
                % str(allocate_zero_instrument_weights_to_these_instruments)
            )

        return allocate_zero_instrument_weights_to_these_instruments

    def instruments_without_data_or_weights(self) -> list:
        subsystem_positions = copy(self._get_all_subsystem_positions())
        subsystem_positions[subsystem_positions.isna()] = 0
        not_zero = subsystem_positions != 0
        index_of_empty_markets = not_zero.sum(axis=0) == 0
        list_of_empty_markets = [
            instrument_code
            for instrument_code, empty in index_of_empty_markets.items()
            if empty
        ]

        self.log.debug(
            "Following instruments will have zero weight in optimisation of instrument weights as they have no positions (possibly too expensive?) %s"
            % str(list_of_empty_markets)
        )

        return list_of_empty_markets

    @input
    def get_subsystem_position(self, instrument_code: str) -> pd.Series:
        """
        Get the position assuming all capital in one position, from a previous
        module

        :param instrument_code: instrument to get values for
        :type instrument_code: str

        :returns: Tx1 pd.DataFrame

        KEY INPUT

        >>> from systems.tests.testdata import get_test_object_futures_with_pos_sizing
        >>> from systems.basesystem import System
        >>> (posobject, combobject, capobject, rules, rawdata, data, config)=get_test_object_futures_with_pos_sizing()
        >>> system=System([rawdata, rules, posobject, combobject, capobject,Portfolios()], data, config)
        >>>
        >>> ## from config
        >>> system.portfolio.get_subsystem_position("EDOLLAR").tail(2)
                    ss_position
        2015-12-10     1.811465
        2015-12-11     2.544598

        """

        return self.position_size_stage.get_subsystem_position(instrument_code)

    @input
    def pandl_across_subsystems(
        self, instrument_list: list = arg_not_supplied
    ) -> accountCurveGroup:
        """
        Return profitability of each instrument

        KEY INPUT

        :param instrument_code:
        :type str:

        :returns: accountCurveGroup object
        """

        try:
            accounts = self.accounts_stage
        except missingData as e:
            error_msg = "You need an accounts stage in the system to estimate instrument weights or IDM"
            self.log.critical(error_msg)
            raise missingData(error_msg) from e

        if instrument_list is arg_not_supplied:
            instrument_list = self.get_instrument_list()

        ## roundpositions=True required to make IDM work with order simulator
        return accounts.pandl_across_subsystems_given_instrument_list(
            instrument_list, roundpositions=True
        )

    @input
    def capital_multiplier(self):
        try:
            accounts_stage = self.accounts_stage
        except missingData as e:
            msg = "If using capital_multiplier to work out actual positions, need an accounts module"
            self.log.critical(msg)
            raise missingData(msg) from e
        else:
            return accounts_stage.capital_multiplier()

    ## RISK
    @diagnostic()
    def get_leverage_for_original_position(self) -> pd.Series:
        portfolio_weights = self.get_original_portfolio_weight_df()
        leverage = portfolio_weights.get_sum_leverage()

        return leverage

    ## PORTFOLIO WEIGHTS
    def get_position_contracts_for_relevant_date(
        self, relevant_date: datetime.datetime = arg_not_supplied
    ) -> portfolioWeights:
        position_contracts_as_df = self.get_position_contracts_as_df()
        position_contracts_at_date = get_row_of_df_aligned_to_weights_as_dict(
            position_contracts_as_df, relevant_date
        )

        position_contracts = portfolioWeights(position_contracts_at_date)

        return position_contracts

    def get_per_contract_value(
        self, relevant_date: datetime.datetime = arg_not_supplied
    ) -> portfolioWeights:
        df_of_values = self.get_per_contract_value_as_proportion_of_capital_df()
        values_at_date = get_row_of_df_aligned_to_weights_as_dict(
            df_of_values, relevant_date
        )
        contract_values = portfolioWeights(values_at_date)

        return contract_values

    @diagnostic()
    def common_index(self):
        subsystem_positions = self._get_all_subsystem_positions()
        common_index = subsystem_positions.index

        return common_index

    @diagnostic()
    def get_per_contract_value_as_proportion_of_capital_df(self) -> pd.DataFrame:
        instrument_list = self.get_instrument_list()
        values_as_dict = dict(
            [
                (
                    instrument_code,
                    self.get_per_contract_value_as_proportion_of_capital(
                        instrument_code
                    ),
                )
                for instrument_code in instrument_list
            ]
        )

        values_as_pd = pd.DataFrame(values_as_dict)
        common_index = self.common_index()

        values_as_pd = values_as_pd.reindex(common_index)
        values_as_pd = values_as_pd.ffill()

        ## slight cheating
        values_as_pd = values_as_pd.bfill()

        return values_as_pd

    def get_position_contracts_as_df(self) -> pd.DataFrame:
        instrument_list = self.get_instrument_list()
        values_as_dict = dict(
            [
                (
                    instrument_code,
                    self.get_notional_position(instrument_code),
                )
                for instrument_code in instrument_list
            ]
        )

        values_as_pd = pd.DataFrame(values_as_dict)
        common_index = self.common_index()

        values_as_pd = values_as_pd.reindex(common_index)
        values_as_pd = values_as_pd.ffill()

        return values_as_pd

    def get_per_contract_value_as_proportion_of_capital(
        self, instrument_code: str
    ) -> pd.Series:
        trading_capital = self.get_trading_capital()
        contract_values = self.get_baseccy_value_per_contract(instrument_code)

        per_contract_value_as_proportion_of_capital = contract_values / trading_capital

        return per_contract_value_as_proportion_of_capital

    def get_baseccy_value_per_contract(self, instrument_code: str) -> pd.Series:
        contract_prices = self.get_contract_prices(instrument_code)
        contract_multiplier = self.get_contract_multiplier(instrument_code)
        fx_rate = self.get_fx_for_contract(instrument_code)

        fx_rate_aligned = fx_rate.reindex(contract_prices.index, method="ffill")

        return fx_rate_aligned * contract_prices * contract_multiplier

    ## INPUT
    def get_instrument_list(
        self, for_instrument_weights=False, auto_remove_bad_instruments=False
    ) -> list:
        instrument_list = self.parent.get_instrument_list()
        if for_instrument_weights:
            instrument_list = copy(instrument_list)
            allocate_zero_instrument_weights_to_these_instruments = (
                self.allocate_zero_instrument_weights_to_these_instruments(
                    auto_remove_bad_instruments
                )
            )

            for (
                instrument_code_to_remove
            ) in allocate_zero_instrument_weights_to_these_instruments:
                instrument_list.remove(instrument_code_to_remove)

        return instrument_list

    ## INPUTS
    def get_trading_capital(self) -> float:
        return self.position_size_stage.get_notional_trading_capital()

    def get_contract_prices(self, instrument_code: str) -> pd.Series:
        return self.position_size_stage.get_underlying_price(instrument_code)

    def get_contract_multiplier(self, instrument_code: str) -> float:
        return float(self.data.get_value_of_block_price_move(instrument_code))

    def get_fx_for_contract(self, instrument_code: str) -> pd.Series:
        return self.position_size_stage.get_fx_rate(instrument_code)

    ## stages
    @property
    def rawdata(self):
        return self.parent.rawdata

    @property
    def data(self):
        return self.parent.data

    @property
    def accounts_stage(self):
        try:
            accounts_stage = getattr(self.parent, "accounts")
        except AttributeError as e:
            raise missingData from e

        return accounts_stage

    @property
    def config(self) -> Config:
        return self.parent.config

    @property
    def position_size_stage(self) -> PositionSizing:
        return self.parent.positionSize



if __name__ == "__main__":
    import doctest

    doctest.testmod()
