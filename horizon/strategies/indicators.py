"""Shared technical indicators — pure functions on pandas Series.

Standard, round-number formulas. No proprietary tuning lives here; strategy
parameters live in the strategy modules so they are explicit and auditable.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252


def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=n).mean()


def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False, min_periods=n).mean()


def rsi(series: pd.Series, n: int) -> pd.Series:
    """Wilder's RSI. RSI(2) is the Connors mean-reversion oscillator."""
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    avg_loss = loss.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    out = 100.0 - 100.0 / (1.0 + rs)
    # avg_loss == 0 -> pure uptrend -> RSI 100
    out = out.where(avg_loss != 0.0, 100.0)
    # avg_gain == 0 -> pure downtrend -> RSI 0
    out = out.where(avg_gain != 0.0, 0.0)
    return out


def realized_vol(closes: pd.Series, n: int,
                 periods_per_year: int = TRADING_DAYS_PER_YEAR) -> pd.Series:
    """Annualized realized volatility of daily simple returns."""
    rets = closes.pct_change()
    return rets.rolling(n, min_periods=n).std(ddof=0) * np.sqrt(periods_per_year)


def momentum(series: pd.Series, lookback: int, skip: int = 0) -> pd.Series:
    """Total return over `lookback` periods, optionally skipping the most
    recent `skip` periods (the classic 12-1 momentum uses skip=21)."""
    recent = series.shift(skip) if skip > 0 else series
    past = series.shift(lookback + skip)
    return recent / past - 1.0


def rolling_high(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=1).max()


def rolling_low(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=1).min()


def last(series: pd.Series, default: float = float("nan")) -> float:
    """Last non-NaN-safe scalar of a series."""
    if series is None or len(series) == 0:
        return default
    val = series.iloc[-1]
    return float(val) if pd.notna(val) else default
