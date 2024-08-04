from systems.system_cache import diagnostic, dont_cache
from private.systems.orion.accounts.account_costs import accountCosts
from private.systems.orion.accounts.pandl_calculators.pandl_SR_cost import pandlCalculationWithSRCosts
from private.systems.orion.accounts.pandl_calculators.pandl_cash_costs import (
    pandlCalculationWithCashCostsAndFills,
)
from private.systems.orion.accounts.curves.account_curve import accountCurve
from private.systems.orion.accounts.curves.account_curve_group import accountCurveGroup
from private.systems.orion.accounts.curves.dict_of_account_curves import dictOfAccountCurves


class accountSubsystem(accountCosts):
    @diagnostic(not_pickable=True)
    def pandl_across_subsystems(
        self, delayfill=True, roundpositions=False
    ) -> accountCurveGroup:
        instrument_list = self.get_instrument_list()

        pandl_across_subsystems = self.pandl_across_subsystems_given_instrument_list(
            instrument_list, delayfill=delayfill, roundpositions=roundpositions
        )

        return pandl_across_subsystems

    @diagnostic(not_pickable=True)
    def pandl_across_subsystems_given_instrument_list(
        self, instrument_list: list, delayfill=True, roundpositions=False
    ) -> accountCurveGroup:
        dict_of_pandl_across_subsystems = dict(
            [
                (
                    instrument_code,
                    self.pandl_for_subsystem(
                        instrument_code,
                        delayfill=delayfill,
                        roundpositions=roundpositions,
                    ),
                )
                for instrument_code in instrument_list
            ]
        )

        dict_of_pandl_across_subsystems = dictOfAccountCurves(
            dict_of_pandl_across_subsystems
        )

        capital = self.get_notional_capital()

        pandl_across_subsystems = accountCurveGroup(
            dict_of_pandl_across_subsystems, capital=capital, weighted=False
        )

        return pandl_across_subsystems

    # dont cache switch statement
    @dont_cache
    def pandl_for_subsystem(
        self, instrument_code, delayfill=True, roundpositions=False
    ) -> accountCurve:
        """
        Get the p&l for one instrument

        :param instrument_code: instrument to get values for
        :type instrument_code: str

        :param delayfill: Lag fills by one day
        :type delayfill: bool

        :param roundpositions: Round positions to whole contracts
        :type roundpositions: bool

        :returns: accountCurve

        >>> from systems.basesystem import System
        >>> from systems.tests.testdata import get_test_object_futures_with_portfolios
        >>> (portfolio, posobject, combobject, capobject, rules, rawdata, data, config)=get_test_object_futures_with_portfolios()
        >>> system=System([portfolio, posobject, combobject, capobject, rules, rawdata, Account()], data, config)
        >>>
        >>> system.accounts.pandl_for_subsystem("US10", percentage=True).ann_std()
        0.23422378634127036
        """

        self.log.debug(
            "Calculating pandl for subsystem for instrument %s" % instrument_code,
            instrument_code=instrument_code,
        )

        pandl = self._pandl_for_subsystem_with_cash_costs(
            instrument_code, delayfill=delayfill, roundpositions=roundpositions
        )

        return pandl

    @diagnostic(not_pickable=True)
    def _pandl_for_subsystem_with_cash_costs(
        self, instrument_code, delayfill=True, roundpositions=True
    ) -> accountCurve:
        pandl_calculator = self._pandl_calculator_for_subsystem_with_cash_costs(
            instrument_code=instrument_code,
            delayfill=delayfill,
            roundpositions=roundpositions,
        )

        account_curve = accountCurve(pandl_calculator)

        return account_curve

    @diagnostic(not_pickable=True)
    def _pandl_calculator_for_subsystem_with_cash_costs(
        self, instrument_code, delayfill=True, roundpositions=True
    ) -> pandlCalculationWithCashCostsAndFills:
        raw_costs = self.get_raw_cost_data(instrument_code)
        positions = self.get_subsystem_position(instrument_code)
        price = self.get_instrument_prices_for_position_or_forecast(
            instrument_code, position_or_forecast=positions
        )  ### here!

        fx = self.get_fx_rate(instrument_code)

        value_of_price_point = self.get_value_of_block_price_move(instrument_code)

        capital = self.get_notional_capital()

        vol_normalise_currency_costs = self.config.vol_normalise_currency_costs
        rolls_per_year = self.get_rolls_per_year(instrument_code)

        pandl_calculator = pandlCalculationWithCashCostsAndFills(
            price,
            raw_costs=raw_costs,
            positions=positions,
            capital=capital,
            value_per_point=value_of_price_point,
            delayfill=delayfill,
            fx=fx,
            roundpositions=roundpositions,
            vol_normalise_currency_costs=vol_normalise_currency_costs,
            rolls_per_year=rolls_per_year,
        )

        return pandl_calculator
