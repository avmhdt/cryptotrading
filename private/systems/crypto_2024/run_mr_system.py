"""
import matplotlib
matplotlib.use("TkAgg")
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from private.systems.crypto_2024.accounts.curves.dict_of_account_curves import nestedDictOfAccountCurves
from private.systems.crypto_2024.accounts.curves.nested_account_curve_group import nestedAccountCurveGroup
from syscore.constants import arg_not_supplied
from syscore.fileutils import get_resolved_pathname

from sysdata.sim.csv_crypto_sim_data import csvCryptoSimData
# from sysdata.sim.db_futures_sim_data import dbCryptoSimData
from sysdata.config.configdata import Config

from private.systems.crypto_2024.forecasting import Rules
from systems.basesystem import System
from private.systems.crypto_2024.forecast_combine import ForecastCombine
from private.systems.crypto_2024.forecast_scale_cap import ForecastScaleCap
from private.systems.crypto_2024.attenuate_vol.vol_attenuation_forecast_scale_cap import (
    volAttenForecastScaleCap,
)
from private.systems.crypto_2024.rawdata import cryptoMinuteRawData
from private.systems.crypto_2024.positionsizing import PositionSizing
from private.systems.crypto_2024.portfolio import Portfolios
# from private.systems.crypto_2024.accounts.order_simulator.minute_limit_orders import AccountWithOrderSimulatorForLimitOrders as Account
from private.systems.crypto_2024.accounts.accounts_stage import Account
from private.systems.crypto_2024.risk import Risk

import pandas as pd


def mr_crypto_system(
    sim_data=arg_not_supplied, config_filename="private.systems.crypto_2024.safer_fast_mr.yaml",
        rules=arg_not_supplied, attenuate_vol=False,
):
    if sim_data is arg_not_supplied:
        sim_data = csvCryptoSimData()

    config = Config(config_filename)

    if rules is arg_not_supplied:
        rules = Rules()

    forcast_scale_cap = volAttenForecastScaleCap() if attenuate_vol else ForecastScaleCap()

    system = System(
        [
            # Risk(),
            Account(),
            Portfolios(),
            PositionSizing(),
            ForecastCombine(),
            forcast_scale_cap,
            rules,
            cryptoMinuteRawData(),
        ],
        sim_data,
        config,
    )

    return system


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    import plotly.express as px

    system = mr_crypto_system(attenuate_vol=False)

    portfolio = system.accounts.portfolio(delayfill=False, roundpositions=False)
    curve = portfolio.curve() # portfolio.percent.curve()
    drawdown = portfolio.drawdown() # portfolio.percent.drawdown()

    plt.figure()
    plt.plot(curve)
    plt.grid()
    plt.show()

    plt.figure()
    plt.plot(drawdown)
    plt.grid()
    plt.show()

    instrument_list = system.get_instrument_list()
    plt.figure()
    notional_position_list = []
    subsystem_position_list = []
    combined_forecast_list = []
    for instrument_code in instrument_list:
        notional_position_list.append(
            system.portfolio.get_notional_position(instrument_code)
        )
        subsystem_position_list.append(
            system.positionSize.get_subsystem_position(instrument_code)
        )
        combined_forecast_list.append(
            system.combForecast.get_combined_forecast(instrument_code)
        )
        plt.plot(
            system.accounts.pandl_for_instrument(
                instrument_code, delayfill=False, roundpositions=False
            ).percent.curve()
        )

    plt.grid()
    plt.show()
    plt.legend(instrument_list)
    notional_position_df = pd.concat(notional_position_list, axis=1)
    notional_position_df.columns = instrument_list

    plt.figure()
    plt.plot(notional_position_df)
    plt.grid()
    plt.show()

    orders_df = notional_position_df.loc['2023-07-01':'2023-08-01'].diff()
    orders_df.iloc[0] = notional_position_df.loc['2023-07-01':].iloc[0]

    subsystem_position_df = pd.concat(subsystem_position_list, axis=1)
    subsystem_position_df.columns = instrument_list

    subsystem_orders_df = subsystem_position_df.loc['2023-07-01':'2023-08-01'].diff()
    subsystem_orders_df.iloc[0] = subsystem_position_df.loc['2023-07-01':].iloc[0]

    combined_forecast_df = pd.concat(combined_forecast_list, axis=1)
    combined_forecast_df.columns = instrument_list

    # combined_forecast_df = combined_forecast_df.loc['2023-07-01':'2023-08-01']
    plt.figure()
    plt.plot(combined_forecast_df)
    plt.grid()
    plt.show()

    # px.line(curve).show()
    # px.area(drawdown).show()
    # px.line(notional_position_df).show()
    # px.line(combined_forecast_df).show()
    # px.line(system.positionSize.get_underlying_price(instrument_list[0])).show()
    #
    # idm = system.portfolio.get_instrument_diversification_multiplier()
    #
    # optimal_position = subsystem_position_df[instrument_list[0]].multiply(idm)
    # my_curve = optimal_position.shift(1).multiply(system.positionSize.get_underlying_price(instrument_list[0]).diff()).cumsum()
    #
    # px.line(my_curve).show()

