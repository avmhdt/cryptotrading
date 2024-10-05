import pandas as pd
import numpy as np


def buy_at_max_and_min(price, lookback_days: int=10):
    daily_price = price.resample('D').agg('last').shift(1)
    daily_max = daily_price.rolling(lookback_days).max()
    daily_min = daily_price.rolling(lookback_days).min()

    high_forecast = daily_price.eq(daily_max).astype(int).astype(float).reindex(price.index, method='ffill').fillna(0.0)
    low_forecast = 0.0 #daily_price.eq(daily_min).astype(int).astype(float).reindex(price.index, method='ffill').fillna(0.0)
    # below_high_forecast = -(
    #         daily_price.lt(daily_max) & daily_price.gt(daily_min)
    # ).astype(int).astype(float).reindex(price.index, method='ffill').fillna(0.0)

    forecast = high_forecast + low_forecast #+ below_high_forecast

    return forecast


def buy_at_max_mean_reversion_below(price, vol, lookback_days: int=10):
    daily_price = price.resample('D').agg('last').shift(1)
    daily_max = daily_price.rolling(lookback_days).max()
    daily_min = daily_price.rolling(lookback_days).min()

    high_forecast = daily_price.eq(daily_max).astype(int).astype(float).reindex(price.index, method='ffill').fillna(0.0)

    daily_mean = (daily_max + daily_min) / 2.0
    equilibrium = daily_mean.reindex(price.index, method='ffill')
    risk_adjusted_forecast = (equilibrium - price) / vol

    threshold = 3.0

    risk_adjusted_forecast = risk_adjusted_forecast.clip(lower=-threshold, upper=threshold)
    risk_adjusted_forecast.loc[risk_adjusted_forecast.le(0.0) & risk_adjusted_forecast.shift(1).gt(0.0)] = 0.0
    risk_adjusted_forecast.loc[risk_adjusted_forecast.ge(0.0) & risk_adjusted_forecast.shift(1).lt(0.0)] = 0.0
    risk_adjusted_forecast = risk_adjusted_forecast.where(
        risk_adjusted_forecast.abs().eq(threshold) | risk_adjusted_forecast.abs().eq(0.0), np.nan)
    risk_adjusted_forecast = risk_adjusted_forecast.ffill().fillna(0)

    mr_forecast = risk_adjusted_forecast.where(
        daily_price.lt(daily_max).reindex(price.index, method='ffill').fillna(False), 0.0
    )

    return mr_forecast

