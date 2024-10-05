from sklearn.linear_model import LinearRegression
from sklearn import preprocessing
from scipy.special import logit
from scipy.stats.mstats import zscore
import pandas as pd
import numpy as np


def get_ols_slope(price):
    linreg = LinearRegression(fit_intercept=True).fit(
        X=zscore(list(price.reset_index(drop=True).index)).reshape(-1, 1),
        y=zscore(price.values)
    )

    return linreg.coef_[0]


def normalize(series: pd.Series):
    normalized = (series.iloc[-1] - series.min()) / (series.max() - series.min())

    return normalized


def logit_ols_slope(price, lookback: int=240):
    rolling_ols_slope = price.rolling(lookback, min_periods=10).apply(get_ols_slope)
    rolling_ols_slope = rolling_ols_slope.fillna(0.0).rolling(lookback, min_periods=10).apply(normalize)

    forecast = logit(rolling_ols_slope) / 40.0

    return forecast


def logit_ols_slope_inverted_when_trend_is_weak(price, lookback: int=240):
    logit_forecast = logit_ols_slope(price, lookback)
    logit_forecast.loc[logit_forecast.abs() < 0.01] *= -1

    return logit_forecast


def mr_logit_ols_slope(price, vol, lookback: int=240):
    equilibrium = price.ewm(lookback).mean()
    mr_forecast = (equilibrium - price) / vol

    mr_forecast = mr_forecast.where(mr_forecast.abs().ge(3.0), 0.0)

    trend_following_forecast = logit_ols_slope(price, lookback)

    return mr_forecast / trend_following_forecast.abs()


def breakout(price, lookback=60, smooth=None):

    if smooth is None:
        smooth = max(int(lookback / 4.0), 1)

    assert smooth < lookback

    roll_max = price.rolling(
        lookback, min_periods=int(min(len(price), np.ceil(lookback / 2.0)))
    ).max()
    roll_min = price.rolling(
        lookback, min_periods=int(min(len(price), np.ceil(lookback / 2.0)))
    ).min()

    roll_mean = (roll_max + roll_min) / 2.0

    # gives a nice natural scaling
    output = ((price - roll_mean) / (roll_max - roll_min))
    smoothed_output = output.ewm(span=smooth, min_periods=np.ceil(smooth / 2.0)).mean()

    return smoothed_output


def logit_breakout(price, lookback: int=60):
    bo = breakout(price, lookback)
    forecast = logit(bo) / 40.0

    return forecast
