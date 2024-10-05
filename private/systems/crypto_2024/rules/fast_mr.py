from private.systems.crypto_2024.rules.ewmac import ewmac_forecast_with_defaults
import numpy as np
import pandas as pd


def fast_mr(price, vol, daily_eq_span: int=5):
    daily_price = price.resample('D').agg('last')
    equilibrium = daily_price.ewm(daily_eq_span).mean().reindex(price.index, method='ffill')

    raw_forecast = equilibrium - price
    risk_adjusted_forecast = raw_forecast / vol

    return risk_adjusted_forecast


def safer_fast_mr(price, vol, daily_eq_span: int=5):
    daily_price = price.resample('D').agg('last').shift(1)
    equilibrium = daily_price.ewm(daily_eq_span).mean().reindex(price.index, method='ffill')
    # equilibrium = price.ewm(daily_eq_span * 24).mean()

    raw_forecast = equilibrium - price
    risk_adjusted_forecast = raw_forecast / vol

    threshold = 3.0

    risk_adjusted_forecast = risk_adjusted_forecast.clip(lower=-threshold, upper=threshold)
    risk_adjusted_forecast.loc[risk_adjusted_forecast.le(0.0) & risk_adjusted_forecast.shift(1).gt(0.0)] = 0.0
    risk_adjusted_forecast.loc[risk_adjusted_forecast.ge(0.0) & risk_adjusted_forecast.shift(1).lt(0.0)] = 0.0
    risk_adjusted_forecast = risk_adjusted_forecast.where(risk_adjusted_forecast.abs().eq(threshold) | risk_adjusted_forecast.abs().eq(0.0), np.nan)
    risk_adjusted_forecast = risk_adjusted_forecast.ffill().fillna(0)

    trend_following_forecast = ewmac_forecast_with_defaults(daily_price, Lfast=16, Lslow=64)
    trend_following_forecast = trend_following_forecast.reindex(price.index, method='ffill')

    risk_adjusted_forecast.loc[np.sign(risk_adjusted_forecast) != np.sign(trend_following_forecast)] = 0.0

    return risk_adjusted_forecast

