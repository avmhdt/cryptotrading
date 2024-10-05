import pandas as pd


def drawdown(curve: pd.Series) -> pd.Series:
    high_watermark = curve.expanding().max()
    dd = curve.subtract(high_watermark)

    return dd.where(dd < 0, other=0.0)
