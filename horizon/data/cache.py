"""On-disk cache of daily bars.

One file per symbol. Each cached frame carries split-adjusted OHLCV, a
`dividend` column (cash dividend on its ex-date), and `tr_close` — a
total-return index used for performance and for ROTATION's momentum ranking.

Total return matters: BIL and TLT earn most of their return as dividends, so a
price-only backtest of those would be badly wrong.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from ..paths import cache_dir
from . import universe
from .polygon_client import PolygonClient

# Fetch window: a wide buffer before the backtest start gives strategies their
# momentum/MA warmup. The fetch END is computed PER CALL (see fetch_end()) —
# never at import time. 2026-08-24..09-04 incident: a module-level
# `FETCH_END = date.today()` froze on the day the long-running live container
# started, and the 7-day freshness tolerance below then accepted the same stale
# cache on every subsequent cycle, so the engine decided on Aug-24 data for two
# weeks (project_notes/horizon_stale_data_2026-09-05.md).
FETCH_START = "2004-06-01"
_ET = ZoneInfo("America/New_York")
SESSION_CLOSE_ET = (16, 15)   # a daily bar is "complete" after 16:15 ET


def completed_through(now: Optional[datetime] = None) -> pd.Timestamp:
    """Last calendar date whose daily bar can be complete right now.

    Today if it is a weekday and the session has closed; otherwise the previous
    weekday. Holidays are NOT modeled here — on the day after a holiday the
    cache simply looks one day older than this and is re-fetched (harmless).
    Bars dated after this are partial (pre-/intra-session) and are dropped, so
    a 09:00 ET cycle can never see a half-day bar as "yesterday's close".
    """
    now = now or datetime.now(_ET)
    if now.tzinfo is None:
        now = now.replace(tzinfo=_ET)
    now = now.astimezone(_ET)
    d = now.date()
    closed = (now.hour, now.minute) >= SESSION_CLOSE_ET
    if now.weekday() >= 5 or not closed:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return pd.Timestamp(d)


def fetch_end() -> str:
    """Polygon range end for a fetch — evaluated at call time, never cached."""
    return date.today().isoformat()


def is_fresh(frame: pd.DataFrame, now: Optional[datetime] = None) -> bool:
    """True if `frame` already contains the last completed session's bar."""
    if frame is None or frame.empty:
        return False
    return pd.Timestamp(frame.index.max()) >= completed_through(now)


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
    cutoff = completed_through()
    if path.exists() and not force:
        try:
            cached = pd.read_pickle(path)
            if is_fresh(cached):
                return cached.loc[:cutoff]
        except Exception:
            pass  # corrupt cache — refetch

    client = client or PolygonClient()
    tickers = _TICKER_ALIASES.get(symbol, [symbol])
    bar_frames, div_frames = [], []
    for ticker in tickers:
        bars = client.daily_bars(ticker, FETCH_START, fetch_end())
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
    bars = bars.loc[:cutoff]          # drop any partial (still-open) session bar
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
