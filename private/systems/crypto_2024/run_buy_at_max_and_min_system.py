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
from private.systems.crypto_2024.attenuate_vol.vol_attenuation_forecast_scale_cap import (
    volAttenForecastScaleCap,
)
from private.systems.crypto_2024.forecast_scale_cap import ForecastScaleCap
from private.systems.crypto_2024.rawdata import cryptoMinuteRawData
from private.systems.crypto_2024.positionsizing import PositionSizing
from private.systems.crypto_2024.portfolio import Portfolios
from private.systems.crypto_2024.accounts.accounts_stage import Account
from private.systems.crypto_2024.risk import Risk


def crypto_system(
    sim_data=arg_not_supplied, config_filename="private.systems.crypto_2024.buy_at_max_and_min_config.yaml",
        rules=arg_not_supplied, attenuate_vol=False
):
    if sim_data is arg_not_supplied:
        sim_data = csvCryptoSimData()

    config = Config(config_filename)

    if rules is arg_not_supplied:
        rules = Rules()

    forcast_scale_cap = volAttenForecastScaleCap() if attenuate_vol else ForecastScaleCap()

    system = System(
        [
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
    from private.systems.crypto_2024.utils import drawdown

    system = crypto_system(attenuate_vol=False)

    portfolio = system.accounts.portfolio(delayfill=False, roundpositions=False)
    curve = portfolio.curve()
    drawdown = portfolio.drawdown()

    plt.figure()
    plt.plot(curve)
    plt.grid()
    plt.show()

    plt.figure()
    plt.plot(drawdown)
    plt.grid()
    plt.show()

    import pandas as pd
    combined_forecast = pd.concat(
        [
            system.combForecast.get_combined_forecast(instrument_code)
            for instrument_code in system.get_instrument_list()
        ],
        axis=1,
    ).abs().sum(axis=1)

    plt.figure()
    plt.plot(combined_forecast)
    plt.grid()
    plt.show()



