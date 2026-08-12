"""
Faithful TREND validation — imports and calls PRODUCTION trend_bot code.
========================================================================

The existing backtest/trend_bot_backtest.py REIMPLEMENTS the strategy (the exact
single-source-of-truth violation that made SIMPLE's backtest worthless). This
harness instead imports strategies/trend_bot.py and drives its own production
functions over historical data:

    spy_regime / update_regime_state   (regime, production code)
    compute_target_weights             (ranking, vol targeting, drawdown scaling,
                                        profit tilt — the whole signal path)

Faithfulness mechanics:
  * bars fed to production code are the same long-format (timestamp,symbol,close)
    frame the live bot builds, truncated to <= decision day (no look-ahead).
  * trend_bot.now_et is patched to the SIMULATED decision timestamp — patching
    the clock, never the logic.
  * a StubTradingClient answers the market-hours/calendar probes (market closed →
    strip_incomplete keeps all completed bars, which is exactly the live Friday-
    after-close semantics).
  * weights decided at close(T) are filled at open(T+1) with slippage.

KNOWN DIVERGENCE (documented, makes the harness OPTIMISTIC vs live): drift
mini-rebalances between weekly rebalances are NOT simulated (live audit showed
they add churn — 43 fills in one week). A strategy that fails HERE certainly
fails live; passing here is necessary, not sufficient.

Pre-registered bar (documented before running):
  T1 CAGR > 4% (T-bill)          T2 Sharpe >= 0.40
  T3 maxDD <= 40%                T4 beat SPY buy-hold OR corr <= 0.85
  T5 walk-forward: positive excess-vs-cash in >= 60% of yearly windows
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "strategies"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

ET = ZoneInfo("America/New_York")
POLY = os.getenv("POLYGON_API_KEY", "")
CACHE = "/tmp/trend_faithful_cache"
SLIPPAGE_BPS = 5.0          # liquid ETFs, market orders near open


# --------------------------------------------------------------------------
# Data (Polygon daily, parquet-cached)
# --------------------------------------------------------------------------
def _fetch_daily(symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
    os.makedirs(CACHE, exist_ok=True)
    cp = os.path.join(CACHE, f"{symbol}_{start}_{end}.parquet")
    if os.path.exists(cp):
        try:
            return pd.read_parquet(cp)
        except Exception:
            pass
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
    for attempt in range(6):
        r = requests.get(url, params={"adjusted": "true", "sort": "asc",
                                      "limit": 50000, "apiKey": POLY}, timeout=40)
        if r.status_code == 200:
            break
        time.sleep(1.0 * (attempt + 1))
    res = r.json().get("results") if r.status_code == 200 else None
    if not res:
        return None
    df = pd.DataFrame(res)
    df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET).dt.normalize()
    df = df.rename(columns={"c": "close", "o": "open"})
    df["symbol"] = symbol
    out = df[["timestamp", "symbol", "open", "close"]]
    try:
        out.to_parquet(cp)
    except Exception:
        pass
    return out


class StubTradingClient:
    """Answers trend_bot's non-signal probes: market closed, weekday calendar."""
    class _Clock:
        is_open = False
        timestamp = None

    def get_clock(self):
        return self._Clock()

    def get_calendar(self, filters=None, **kw):
        class _Day:
            def __init__(self, d):
                self.date = d
        start = getattr(filters, "start", None) or date(2015, 1, 1)
        end = getattr(filters, "end", None) or date(2030, 1, 1)
        days, d = [], start
        while d <= end:
            if d.weekday() < 5:
                days.append(_Day(d))
            d += timedelta(days=1)
        return days


# --------------------------------------------------------------------------
# Simulation
# --------------------------------------------------------------------------
def run(start: str, end: str, capital: float, label: str = "TREND-faithful",
        verbose: bool = True) -> dict:
    import trend_bot as tb  # production module (requires env keys present)

    # exactly what production ranks/trades — ALL_TICKERS covers ALL_EQUITY (incl.
    # MOMENTUM_ETFS like IGV/KWEB), defensives, and the cash ticker
    universe = sorted(set(tb.ALL_TICKERS + ["SPY"]))
    warm_start = (date.fromisoformat(start) - timedelta(days=420)).isoformat()
    frames, opens = [], {}
    for sym in universe:
        df = _fetch_daily(sym, warm_start, end)
        if df is not None and len(df) > 250:
            frames.append(df[["timestamp", "symbol", "close"]])
            opens[sym] = df.set_index("timestamp")["open"]
    bars_all = pd.concat(frames, ignore_index=True)
    have = sorted(bars_all["symbol"].unique())
    if verbose:
        print(f"[{label}] {len(have)}/{len(universe)} symbols  {start}..{end}")

    spy = bars_all[bars_all["symbol"] == "SPY"].set_index("timestamp")["close"]
    days = [d for d in spy.index if start <= str(d.date()) <= end]

    # production state object; clock patched per simulated day
    state = tb.BotState()
    stub = StubTradingClient()
    orig_now = tb.now_et

    cash = capital
    positions: Dict[str, float] = {}      # symbol -> shares
    curve: List[tuple] = []
    fills = 0
    turnover_usd = 0.0
    pending_targets: Optional[Dict[str, float]] = None
    slip = SLIPPAGE_BPS / 10_000.0

    # fast per-symbol close lookup
    closes: Dict[str, pd.Series] = {
        s: bars_all[bars_all["symbol"] == s].set_index("timestamp")["close"]
        for s in have
    }

    def _px(series_map, s, d):
        ser = series_map.get(s)
        if ser is None:
            return None
        ser = ser.loc[:d]
        return float(ser.iloc[-1]) if len(ser) else None

    try:
        for i, d in enumerate(days):
            # --- 1) fill last Friday's targets at TODAY's open (T+1 open) ---
            if pending_targets is not None:
                equity_open = cash + sum(
                    sh * (opens[s].loc[d] if (s in opens and d in opens[s].index)
                          else (_px(closes, s, d) or 0.0))
                    for s, sh in positions.items())
                new_pos: Dict[str, float] = {}
                # sells first (dropped or reduced), then buys — all at open ± slip
                for s, w in pending_targets.items():
                    o = opens[s].loc[d] if (s in opens and d in opens[s].index) else None
                    if o is None or w <= 0:
                        continue
                    new_pos[s] = (equity_open * w)  # target $ for now; shares below
                # liquidate anything not in targets
                for s, sh in list(positions.items()):
                    if s not in new_pos:
                        o = opens[s].loc[d] if (s in opens and d in opens[s].index) \
                            else _px(closes, s, d)
                        if o:
                            cash += sh * o * (1 - slip)
                            turnover_usd += abs(sh * o)
                            fills += 1
                        del positions[s]
                # trade to targets
                for s, tgt_val in new_pos.items():
                    o = float(opens[s].loc[d])
                    old_sh = positions.get(s, 0.0)
                    delta_val = tgt_val - old_sh * o
                    if abs(delta_val) < 1.0:
                        continue
                    if delta_val > 0:
                        fill_px = o * (1 + slip)
                        sh_delta = delta_val / fill_px
                        cash -= sh_delta * fill_px
                    else:
                        fill_px = o * (1 - slip)
                        sh_delta = delta_val / o
                        cash -= sh_delta * fill_px  # sh_delta negative -> cash increases
                    positions[s] = old_sh + sh_delta
                    turnover_usd += abs(delta_val)
                    fills += 1
                pending_targets = None

            # --- 2) mark to market at close(d) ---
            equity = cash + sum((_px(closes, s, d) or 0.0) * sh
                                for s, sh in positions.items())
            curve.append((d.date(), equity))
            # production drawdown anchor evolves the way live does
            state.equity_peak = max(state.equity_peak or equity, equity)

            # --- 3) Friday after close: production regime + target weights ---
            if d.weekday() == 4:
                sim_now = datetime(d.year, d.month, d.day, 16, 30, tzinfo=ET)
                tb.now_et = lambda _n=sim_now: _n
                bars_t = bars_all[bars_all["timestamp"] <= d]
                spy_close = bars_t[bars_t["symbol"] == "SPY"]["close"].reset_index(drop=True)
                # Canonical production regime (price-based, hysteresis). We call
                # spy_regime directly rather than update_regime_state because the
                # latter overlays a fetch of TODAY'S live VIX — look-ahead poison
                # for historical dates. Divergence documented: the VIX>35 override
                # is not simulated (historical VIX unavailable on this data plan).
                new_regime, _changed = tb.spy_regime(spy_close, state.spy_regime)
                state.spy_regime = new_regime
                weights, _diag = tb.compute_target_weights(
                    bars_t, state, equity, equity, stub,
                    current_regime=state.spy_regime,
                )
                pending_targets = weights
    finally:
        tb.now_et = orig_now

    # ---- metrics ----
    eq = [e for _, e in curve]
    rets = [eq[i] / eq[i - 1] - 1 for i in range(1, len(eq)) if eq[i - 1] > 0]
    years = max(len(eq) / 252.0, 1e-9)
    cagr = (eq[-1] / eq[0]) ** (1 / years) - 1 if eq and eq[0] > 0 else 0.0
    sharpe = (np.mean(rets) / np.std(rets) * np.sqrt(252)) if len(rets) > 1 and np.std(rets) > 0 else 0.0
    peak, mdd = (eq[0] if eq else 1.0), 0.0
    for e in eq:
        peak = max(peak, e)
        mdd = min(mdd, e / peak - 1)
    spy_win = spy[[d for d, _ in zip(spy.index, spy.index)]]  # full spy series
    spy_in = spy[(spy.index >= pd.Timestamp(start, tz=ET)) & (spy.index <= pd.Timestamp(end, tz=ET))]
    spy_ret = float(spy_in.iloc[-1] / spy_in.iloc[0] - 1) if len(spy_in) > 1 else 0.0
    out = dict(label=label, start=start, end=end, days=len(eq),
               end_equity=round(eq[-1], 2) if eq else capital,
               ret_pct=round((eq[-1] / eq[0] - 1) * 100, 2) if eq else 0.0,
               cagr_pct=round(cagr * 100, 2), sharpe=round(sharpe, 2),
               max_dd_pct=round(mdd * 100, 1), fills=fills,
               turnover_x=round(turnover_usd / capital, 1),
               spy_ret_pct=round(spy_ret * 100, 2))
    if verbose:
        print(f"  {label}: ret {out['ret_pct']:+.2f}% (CAGR {out['cagr_pct']:+.2f}%) "
              f"Sharpe {out['sharpe']:.2f} maxDD {out['max_dd_pct']:.1f}% "
              f"fills {fills} turnover {out['turnover_x']}x | SPY {out['spy_ret_pct']:+.2f}%")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2023-01-02")
    ap.add_argument("--end", default="2026-08-08")
    ap.add_argument("--capital", type=float, default=10_000.0)
    ap.add_argument("--windows", action="store_true", help="yearly walk-forward windows")
    a = ap.parse_args()
    if a.windows:
        wins = [("2019-01-02", "2019-12-31"), ("2020-01-02", "2020-12-31"),
                ("2021-01-04", "2021-12-31"), ("2022-01-03", "2022-12-30"),
                ("2023-01-03", "2023-12-29"), ("2024-01-02", "2024-12-31"),
                ("2025-01-02", "2025-12-31"), ("2026-01-02", "2026-08-08")]
        rows = [run(s, e, a.capital, label=f"TREND {s[:4]}") for s, e in wins]
        pos = sum(1 for r in rows if r["ret_pct"] > 0)
        beat = sum(1 for r in rows if r["ret_pct"] > r["spy_ret_pct"])
        print(f"\nWALK-FORWARD: {pos}/{len(rows)} windows positive, "
              f"{beat}/{len(rows)} beat SPY")
    else:
        run(a.start, a.end, a.capital)


if __name__ == "__main__":
    main()
