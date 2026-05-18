"""On-disk cache of daily bars.

One file per symbol. Each cached frame carries split-adjusted OHLCV, a
`dividend` column (cash dividend on its ex-date), and `tr_close` — a
total-return index used for performance and for ROTATION's momentum ranking.

Total return matters: BIL and TLT earn most of their return as dividends, so a
price-only backtest of those would be badly wrong.
"""

from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from ..paths import cache_dir
from . import universe
from .polygon_client import PolygonClient

# Fetch window: a wide buffer before the backtest start gives strategies their
# momentum/MA warmup. FETCH_END tracks "today" so the live engine always has
# current data; the backtest simply windows to its own configured end date.
FETCH_START = "2004-06-01"
FETCH_END = date.today().isoformat()


def _cache_path(symbol: str):
    return cache_dir() / f"{symbol.replace(':', '_')}.pkl"


def _total_return_close(bars: pd.DataFrame) -> pd.Series:
    """Total-return index, normalized to the first close."""
    close = bars["close"]
    prev = close.shift(1)
    daily_ret = (close + bars["dividend"]) / prev - 1.0
    daily_ret.iloc[0] = 0.0
    return (1.0 + daily_ret).cumprod() * float(close.iloc[0])


def _attach_dividends(bars: pd.DataFrame, divs: pd.DataFrame) -> pd.DataFrame:
    bars = bars.copy()
    bars["dividend"] = 0.0
    if divs is None or divs.empty:
        return bars
    for ex_date, amt in divs["dividend"].items():
        if ex_date in bars.index:
            bars.loc[ex_date, "dividend"] += amt
        else:
            # Snap a non-trading-day ex-date to the next trading day.
            future = bars.index[bars.index >= ex_date]
            if len(future):
                bars.loc[future[0], "dividend"] += amt
    return bars


# Some funds changed ticker mid-history. The Invesco Nasdaq-100 ETF traded as
# QQQQ from 2004-12 to 2011-03; Polygon keys that era's bars under QQQQ. We
# stitch the eras into one continuous series — it is the same fund, with no
# split at the ticker change, so the price series joins cleanly.
_TICKER_ALIASES = {
    "QQQ": ["QQQ", "QQQQ"],
}


def fetch_symbol(symbol: str, client: Optional[PolygonClient] = None,
                 force: bool = False) -> pd.DataFrame:
    """Return a cached or freshly-fetched daily frame for one symbol."""
    path = _cache_path(symbol)
    if path.exists() and not force:
        try:
            cached = pd.read_pickle(path)
            if not cached.empty:
                fresh_enough = (cached.index.max()
                                >= pd.Timestamp(FETCH_END) - pd.Timedelta(days=7))
                if fresh_enough:
                    return cached
        except Exception:
            pass  # corrupt cache — refetch

    client = client or PolygonClient()
    tickers = _TICKER_ALIASES.get(symbol, [symbol])
    bar_frames, div_frames = [], []
    for ticker in tickers:
        bars = client.daily_bars(ticker, FETCH_START, FETCH_END)
        if not bars.empty:
            bar_frames.append(bars)
        divs = client.dividends(ticker)
        if not divs.empty:
            div_frames.append(divs)
    if not bar_frames:
        raise RuntimeError(f"Polygon returned no daily bars for {symbol}")

    bars = pd.concat(bar_frames).sort_index()
    bars = bars[~bars.index.duplicated(keep="first")]
    if div_frames:
        divs = pd.concat(div_frames)
        divs = divs.groupby(level=0)["dividend"].sum().to_frame()
    else:
        divs = pd.DataFrame(columns=["dividend"]).rename_axis("date")
    bars = _attach_dividends(bars, divs)
    bars["tr_close"] = _total_return_close(bars)
    bars.to_pickle(path)
    return bars


def load_dataset(symbols: Optional[List[str]] = None, force: bool = False
                 ) -> Dict[str, pd.DataFrame]:
    """Load (fetching/caching as needed) the full Horizon universe."""
    symbols = symbols or universe.all_symbols()
    client = PolygonClient()
    out: Dict[str, pd.DataFrame] = {}
    for sym in symbols:
        out[sym] = fetch_symbol(sym, client=client, force=force)
    return out
