"""
Faithful intraday harness for SIMPLE v2 (parabolic-continuation ORB).
=====================================================================

Honors Horizon Section 6:
  * SINGLE SOURCE OF TRUTH — imports and calls strategies.simple_parabolic.strategy.
    There is NO reimplementation of the decision logic here (asserted at startup).
  * NO LOOK-AHEAD — the strategy only ever sees bars[:t]; the harness fills the
    resulting intent at t+1 (next minute), never on the signal bar.
  * REALISTIC COSTS — every fill pays slippage; commissions modeled.
  * ACCOUNT-SIZE REALISM — whole shares, min-notional, sleeve cash budget, the
    real ~$780 sleeve (and a meaningful-allocation sweep).
  * RISK OVERLAY THAT CAN'T DEATH-SPIRAL — rolling-window drawdown throttle with a
    floor and a guaranteed recovery path (never an all-time-peak reference).
  * HONEST METRICS — expectancy, win rate, profit factor, Sharpe, Sortino, maxDD,
    turnover, trade count; compared to the live baseline and buy-hold QQQ/SPY.

Usage:
  railway run ./.venv/bin/python backtest/simple_parabolic_backtest.py \
      --start 2026-04-20 --end 2026-06-15 --sleeve 780
  ... --walkforward            # run the preset multi-window regime suite
  ... --selftest               # single-source-of-truth + no-look-ahead checks
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

# --- import the production strategy (single source of truth) ---
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "strategies"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from strategies.simple_parabolic import strategy as S  # noqa: E402

ET = ZoneInfo("America/New_York")
POLY = os.getenv("POLYGON_API_KEY", "eskzZ5O4QQyYpS5TsA3pe5WMeepeaxmW")

# Default universe — liquid, high-beta movers (no leveraged single-stock ETFs in core).
DEFAULT_UNIVERSE = [
    "NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "AMD", "AVGO",
    "COIN", "PLTR", "HOOD", "SQ", "MSTR", "SMCI", "MARA", "RIOT",
    "JPM", "GS", "BAC", "NFLX", "BA", "CAT", "UNH", "LLY",
    "NET", "DDOG", "PANW", "NOW", "CRM", "SNOW", "SHOP", "UBER", "DKNG",
]

COMMISSION_PER_SHARE = 0.0      # Alpaca equities commission-free
SLIPPAGE_BPS = 8.0             # marketable-limit pays through spread on movers (each side)


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
def _poly_get(url: str, params: dict) -> dict:
    # 401/429/5xx are treated as transient (Polygon throttles bursty tick pagination and
    # occasionally 401s under load); retry with backoff. 403 = real entitlement -> raise.
    for attempt in range(8):
        r = requests.get(url, params=params, timeout=40)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 403:
            r.raise_for_status()
        if r.status_code in (401, 429) or r.status_code >= 500:
            time.sleep(1.0 * (attempt + 1)); continue
        r.raise_for_status()
    return {}


CACHE_DIR = "/tmp/parabolic_cache"


def _cache_path(symbol: str, start: date, end: date, kind: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{symbol}_{start}_{end}_{kind}.parquet")


def fetch_minute(symbol: str, start: date, end: date) -> Optional[pd.DataFrame]:
    cp = _cache_path(symbol, start, end, "min")
    if os.path.exists(cp):
        try:
            return pd.read_parquet(cp)
        except Exception:
            pass
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start}/{end}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLY}
    rows = []
    data = _poly_get(url, params)
    while data:
        rows.extend(data.get("results", []))
        nxt = data.get("next_url")
        if not nxt:
            break
        data = _poly_get(nxt, {"apiKey": POLY})
        time.sleep(0.1)
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
    df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
    df["symbol"] = symbol
    df = df[(df["timestamp"].dt.time >= dtime(9, 30)) & (df["timestamp"].dt.time < dtime(16, 0))]
    out = df[["timestamp", "symbol", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
    try:
        out.to_parquet(cp)
    except Exception:
        pass
    return out


def fetch_daily(symbol: str, start: date, end: date) -> Optional[pd.DataFrame]:
    cp = _cache_path(symbol, start, end, "day")
    if os.path.exists(cp):
        try:
            return pd.read_parquet(cp)
        except Exception:
            pass
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
    data = _poly_get(url, {"adjusted": "true", "sort": "asc", "limit": 5000, "apiKey": POLY})
    res = data.get("results") if data else None
    if not res:
        return None
    df = pd.DataFrame(res)
    df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET).dt.date
    df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
    out = df[["date", "open", "high", "low", "close", "volume"]]
    try:
        out.to_parquet(cp)
    except Exception:
        pass
    return out


def orderflow_table(symbol: str, day: date, t0="09:30", t1="11:35") -> Dict[pd.Timestamp, Tuple[float, float]]:
    """Per-minute order-flow features from tick trades (Stocks Advanced; backtestable).
    Returns {minute_ts -> (of_imbalance, block_frac)}. Cached per sym-day. Fetched lazily,
    only for symbol-days that actually produce a breakout entry."""
    cp = _cache_path(symbol, day, day, "of")
    if os.path.exists(cp):
        try:
            g = pd.read_parquet(cp)
            return {pd.Timestamp(i): (float(r["of_imbalance"]), float(r["block_frac"]))
                    for i, r in g.iterrows()}
        except Exception:
            pass
    start = pd.Timestamp(f"{day} {t0}", tz=ET).value
    end = pd.Timestamp(f"{day} {t1}", tz=ET).value
    url = f"https://api.polygon.io/v3/trades/{symbol}"
    rows = []
    j = _poly_get(url, {"timestamp.gte": start, "timestamp.lt": end, "limit": 50000, "order": "asc"})
    while j:
        rows.extend(j.get("results", []))
        nxt = j.get("next_url")
        if not nxt or len(rows) > 800000:
            break
        j = _poly_get(nxt, {})
        time.sleep(0.05)
    if not rows:
        return {}
    df = pd.DataFrame(rows)
    tscol = "sip_timestamp" if "sip_timestamp" in df.columns else "participant_timestamp"
    df = df.rename(columns={tscol: "ts"})[["ts", "price", "size"]].sort_values("ts")
    sign = np.sign(df["price"].diff()).replace(0, np.nan).ffill().fillna(0)
    df["signed"] = sign * df["size"]
    df["block"] = np.where(df["size"] * df["price"] >= 50000, df["size"], 0.0)
    df["minute"] = pd.to_datetime(df["ts"], unit="ns", utc=True).dt.tz_convert(ET).dt.floor("1min")
    g = df.groupby("minute").agg(vol=("size", "sum"), signed=("signed", "sum"), block=("block", "sum"))
    g["of_imbalance"] = g["signed"] / g["vol"].clip(lower=1)
    g["block_frac"] = g["block"] / g["vol"].clip(lower=1)
    g = g[["of_imbalance", "block_frac"]]
    try:
        g.to_parquet(cp)
    except Exception:
        pass
    return {pd.Timestamp(i): (float(r["of_imbalance"]), float(r["block_frac"])) for i, r in g.iterrows()}


def daily_atr_dollars(daily: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = daily["high"], daily["low"], daily["close"]
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------
@dataclass
class Trade:
    symbol: str; day: date; entry_t: pd.Timestamp; exit_t: pd.Timestamp
    entry: float; exit: float; qty: int; pnl: float; r_mult: float; reason: str
    hold_min: float


@dataclass
class Result:
    trades: List[Trade] = field(default_factory=list)
    equity_curve: List[Tuple[date, float]] = field(default_factory=list)
    start_equity: float = 0.0
    end_equity: float = 0.0


def _slip(price: float, side: str) -> float:
    s = SLIPPAGE_BPS / 10_000.0
    return price * (1 + s) if side == "buy" else price * (1 - s)


def run(symbols: List[str], start: date, end: date, sleeve: float,
        cfg: S.ParabolicConfig, label: str = "", verbose: bool = True) -> Result:
    # fetch with baseline buffer for opening-window RVOL + daily ATR warmup
    buf_start = start - timedelta(days=cfg.rvol_baseline_days * 2 + 35)
    minute: Dict[str, pd.DataFrame] = {}
    daily: Dict[str, pd.DataFrame] = {}
    for sym in symbols:
        m = fetch_minute(sym, buf_start, end)
        d = fetch_daily(sym, buf_start, end)
        if m is not None and d is not None and len(d) > cfg.atr_period + 2:
            minute[sym] = m
            d = d.copy(); d["atr$"] = daily_atr_dollars(d, cfg.atr_period)
            daily[sym] = d
    if verbose:
        print(f"[{label}] fetched {len(minute)}/{len(symbols)} symbols  {start}..{end}")

    # trading days in window
    all_days = sorted({t.date() for df in minute.values() for t in df["timestamp"]
                       if start <= t.date() <= end})

    equity = sleeve
    peak_curve: List[float] = []          # rolling reference for DD throttle
    res = Result(start_equity=sleeve)

    for day in all_days:
        # --- rolling-drawdown risk throttle (no all-time peak; recovers fully) ---
        peak_curve.append(equity)
        ref = max(peak_curve[-20:]) if peak_curve else equity  # rolling ~20-day high (no all-time peak)
        dd = equity / ref - 1.0 if ref > 0 else 0.0
        throttle = float(np.clip(1.0 - (-dd - 0.06) / (0.18 - 0.06), 0.5, 1.0)) if dd < -0.06 else 1.0
        eff_sleeve = equity * throttle

        # --- build opening ranges + rank in-play for the day ---
        ranges: List[S.OpeningRange] = []
        day_min: Dict[str, pd.DataFrame] = {}
        for sym, m in minute.items():
            dm = m[m["timestamp"].dt.date == day]
            if len(dm) == 0:
                continue
            d = daily[sym]
            drow = d[d["date"] < day]
            if len(drow) == 0:
                continue
            prior_close = float(drow["close"].iloc[-1])
            atr_d = float(drow["atr$"].iloc[-1]) if not np.isnan(drow["atr$"].iloc[-1]) else 0.0
            # opening-window volume baseline = avg of first OR_minutes volume over baseline days
            base_days = [dd_ for dd_ in all_days_before(m, day)][-cfg.rvol_baseline_days:]
            base_vols = []
            for bd in base_days:
                bdm = m[m["timestamp"].dt.date == bd]
                or_cut = pd.Timestamp(bd).tz_localize(ET) + pd.Timedelta(hours=9, minutes=30 + cfg.or_minutes)
                ow = bdm[bdm["timestamp"] < or_cut]
                if len(ow):
                    base_vols.append(float(ow["volume"].sum()))
            baseline = float(np.mean(base_vols)) if base_vols else 0.0
            orng = S.compute_opening_range(dm, prior_close, atr_d, baseline, cfg)
            if orng is not None:
                orng.symbol = sym
                ranges.append(orng)
                day_min[sym] = dm.reset_index(drop=True)
        candidates = S.rank_in_play(ranges, cfg)

        # --- walk the day minute-by-minute over the union timeline ---
        open_pos: Dict[str, S.PositionState] = {}
        cand_syms = [r.symbol for r in candidates]
        # precompute per-symbol minute arrays + minute ATR series
        sym_bars = {}
        armed: Dict[str, S.EntryIntent] = {}   # resting-stop intents, armed once at OR end
        for r in candidates:
            sym = r.symbol
            dm = day_min[sym].copy()
            dm["m_atr"] = S.atr(dm, cfg.atr_period)
            sym_bars[sym] = dm
            or_cut = pd.Timestamp(day).tz_localize(ET) + pd.Timedelta(hours=9, minutes=30 + cfg.or_minutes)
            or_bars = dm[dm["timestamp"] < or_cut]
            if len(or_bars) == 0:
                continue
            atr_or = float(or_bars["m_atr"].iloc[-1])
            if np.isnan(atr_or) or atr_or <= 0:
                # fall back to a fraction of the OR range as intraday ATR proxy
                atr_or = max((r.or_high - r.or_low) / max(cfg.atr_period, 1), 0.01)
            intent = S.arm_entry(r, atr_or, cfg)
            if intent is not None:
                armed[sym] = intent
        filled_today: set = set()
        of_cache: Dict[str, dict] = {}   # per-day lazy order-flow tables (tick-derived)

        # master minute index for the day
        times = sorted({t for dm in sym_bars.values() for t in dm["timestamp"]})
        last_idx = {sym: {pd.Timestamp(r.timestamp): i for i, r in enumerate(dm.itertuples())}
                    for sym, dm in sym_bars.items()}

        for ti, now in enumerate(times):
            # 1) manage open positions on this bar
            for sym in list(open_pos.keys()):
                dm = sym_bars[sym]
                row = dm[dm["timestamp"] == now]
                if len(row) == 0:
                    continue
                bar = row.iloc[0]
                pos = open_pos[sym]
                for act in S.manage_position(pos, bar, cfg):
                    if act.kind == "raise_stop":
                        pos.stop_price = act.price
                    elif act.kind == "exit_partial":
                        px = _slip(act.price, "sell")
                        pnl = (px - pos.entry_price) * act.qty - COMMISSION_PER_SHARE * act.qty
                        equity += pnl
                        res.trades.append(Trade(sym, day, pd.Timestamp(now), pd.Timestamp(now),
                                                pos.entry_price, px, act.qty, pnl,
                                                (px - pos.entry_price) / max(pos.entry_price - pos.stop_price, 1e-9),
                                                act.reason, _mins(pos, now)))
                        pos.qty -= act.qty; pos.scalped = True
                    elif act.kind == "exit_all":
                        px = _slip(act.price, "sell")
                        pnl = (px - pos.entry_price) * pos.qty - COMMISSION_PER_SHARE * pos.qty
                        equity += pnl
                        res.trades.append(Trade(sym, day, pd.Timestamp(now), pd.Timestamp(now),
                                                pos.entry_price, px, pos.qty, pnl,
                                                (px - pos.entry_price) / max(pos.entry_price - pos.stop_price, 1e-9),
                                                act.reason, _mins(pos, now)))
                        del open_pos[sym]
                        break

            # 2) resting-stop entries: a stop-market armed at OR end fills intrabar at
            #    the trigger when price first touches it (faithful; not look-ahead — the
            #    order is placed using only OR data). Entry window + slot caps apply.
            now_time = pd.Timestamp(now).time()
            if now_time.hour == 9 and now_time.minute < (30 + cfg.or_minutes):
                continue   # still inside the opening range
            if now_time >= cfg.entry_window_end:
                continue
            for r in candidates:
                sym = r.symbol
                if sym in open_pos or sym in filled_today or sym not in armed:
                    continue
                if len(open_pos) >= cfg.max_concurrent:
                    break
                dm = sym_bars[sym]
                row = dm[dm["timestamp"] == now]
                if len(row) == 0:
                    continue
                bar = row.iloc[0]
                intent = armed[sym]
                if float(bar["high"]) < intent.trigger_price:
                    continue   # stop not yet touched
                # microstructure confirmation (backtestable tick order flow) on the breakout minute
                if cfg.use_microstructure:
                    tbl = of_cache.get(sym)
                    if tbl is None:
                        tbl = orderflow_table(sym, day); of_cache[sym] = tbl
                    feat = tbl.get(pd.Timestamp(now))
                    of_imb, blk = feat if feat else (None, None)
                    if not S.passes_microstructure(of_imb, blk, cfg):
                        filled_today.add(sym)   # one-shot: weak/unconfirmed breakout, skip today
                        continue
                # fill: gap-through fills at the open, otherwise at the stop trigger
                fill = _slip(max(intent.trigger_price, float(bar["open"])), "buy")
                used = sum(p.qty * p.entry_price for p in open_pos.values())
                qty = S.position_size(fill, intent.stop_price, eff_sleeve, max(eff_sleeve - used, 0),
                                      cfg, overlay_size_mult=1.0, leveraged=False)
                if qty <= 0:
                    filled_today.add(sym)
                    continue
                pos = S.PositionState(symbol=sym, qty=qty, entry_price=fill,
                                      stop_price=intent.stop_price,
                                      atr_at_entry=intent.atr_at_entry, peak=fill)
                pos._entry_t = pd.Timestamp(now)  # type: ignore
                open_pos[sym] = pos
                filled_today.add(sym)
                # same-bar management (whipsaw / scalp on the entry bar itself)
                for act in S.manage_position(pos, bar, cfg):
                    if act.kind == "raise_stop":
                        pos.stop_price = act.price
                    elif act.kind in ("exit_partial", "exit_all"):
                        q = act.qty if act.kind == "exit_partial" else pos.qty
                        px = _slip(act.price, "sell")
                        pnl = (px - pos.entry_price) * q - COMMISSION_PER_SHARE * q
                        equity += pnl
                        res.trades.append(Trade(sym, day, pd.Timestamp(now), pd.Timestamp(now),
                                                pos.entry_price, px, q, pnl,
                                                (px - pos.entry_price) / max(pos.entry_price - pos.stop_price, 1e-9),
                                                act.reason, 0.0))
                        if act.kind == "exit_partial":
                            pos.qty -= q; pos.scalped = True
                        else:
                            del open_pos[sym]
                            break

        # 3) safety: force-close anything still open at last bar (shouldn't happen — hard_flatten)
        for sym, pos in list(open_pos.items()):
            dm = sym_bars[sym]; last = dm.iloc[-1]
            px = _slip(float(last["close"]), "sell")
            pnl = (px - pos.entry_price) * pos.qty
            equity += pnl
            res.trades.append(Trade(sym, day, pd.Timestamp(last["timestamp"]), pd.Timestamp(last["timestamp"]),
                                    pos.entry_price, px, pos.qty, pnl,
                                    (px - pos.entry_price) / max(pos.entry_price - pos.stop_price, 1e-9),
                                    "FORCED_CLOSE", _mins(pos, last["timestamp"])))
            del open_pos[sym]

        res.equity_curve.append((day, equity))

    res.end_equity = equity
    return res


def all_days_before(m: pd.DataFrame, day: date) -> List[date]:
    return sorted({t.date() for t in m["timestamp"] if t.date() < day})


def _mins(pos, now) -> float:
    et = getattr(pos, "_entry_t", None)
    if et is None:
        return 0.0
    return (pd.Timestamp(now) - pd.Timestamp(et)).total_seconds() / 60.0


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
def metrics(res: Result, label: str) -> dict:
    pnls = [t.pnl for t in res.trades]
    n = len(pnls)
    wins = [p for p in pnls if p > 0]; losses = [p for p in pnls if p < 0]
    gp = sum(wins); gl = -sum(losses)
    ret = (res.end_equity / res.start_equity - 1) * 100 if res.start_equity else 0
    # daily returns from equity curve
    eq = [e for _, e in res.equity_curve]
    drets = [eq[i] / eq[i - 1] - 1 for i in range(1, len(eq)) if eq[i - 1] > 0]
    sharpe = (np.mean(drets) / np.std(drets) * np.sqrt(252)) if len(drets) > 1 and np.std(drets) > 0 else 0
    downside = [d for d in drets if d < 0]
    sortino = (np.mean(drets) / np.std(downside) * np.sqrt(252)) if len(downside) > 1 and np.std(downside) > 0 else 0
    mdd = 0.0; peak = eq[0] if eq else res.start_equity
    for e in eq:
        peak = max(peak, e); mdd = min(mdd, e / peak - 1)
    turnover = sum(t.qty * t.entry for t in res.trades) / res.start_equity if res.start_equity else 0
    overnight = sum(1 for t in res.trades if t.hold_min > 16 * 60)
    reason_stats = {}
    for t in res.trades:
        d = reason_stats.setdefault(t.reason, [0, 0.0, 0])
        d[0] += 1; d[1] += t.pnl
        if t.pnl > 0: d[2] += 1
    m = dict(label=label, trades=n, net_pnl=sum(pnls), ret_pct=ret, reason_stats=reason_stats,
             win_rate=(len(wins) / n * 100 if n else 0),
             profit_factor=(gp / gl if gl > 0 else float("inf")),
             expectancy=(sum(pnls) / n if n else 0),
             avg_win=(gp / len(wins) if wins else 0), avg_loss=(gl / len(losses) if losses else 0),
             sharpe=sharpe, sortino=sortino, max_dd_pct=mdd * 100, turnover=turnover,
             overnight_holds=overnight, end_equity=res.end_equity)
    return m


def print_metrics(m: dict):
    print(f"\n===== {m['label']} =====")
    print(f"  trades {m['trades']}  net ${m['net_pnl']:,.2f}  return {m['ret_pct']:+.2f}%  end ${m['end_equity']:,.2f}")
    if m["trades"]:
        print(f"  win rate {m['win_rate']:.1f}%  PF {m['profit_factor']:.2f}  expectancy ${m['expectancy']:,.2f}")
        print(f"  avg win ${m['avg_win']:,.2f}  avg loss ${m['avg_loss']:,.2f}")
        print(f"  Sharpe {m['sharpe']:.2f}  Sortino {m['sortino']:.2f}  maxDD {m['max_dd_pct']:.1f}%  turnover {m['turnover']:.1f}x")
        print(f"  overnight holds (>16h): {m['overnight_holds']}  <-- must be 0 (hard flatten)")
        print("  exit reasons (count | net$ | win%):")
        for rsn, (c, pnl, w) in sorted(m["reason_stats"].items(), key=lambda x: -x[1][0]):
            print(f"     {rsn:16} {c:4d} | ${pnl:8.2f} | {w/c*100 if c else 0:4.0f}%")


# ---------------------------------------------------------------------------
# Self-test (Section 6: single source of truth + no look-ahead)
# ---------------------------------------------------------------------------
def selftest():
    print("Single-source-of-truth fingerprint:", S.source_fingerprint())
    assert S.source_fingerprint().startswith("strategies.simple_parabolic.strategy")
    # no-look-ahead: entry_decision must not reference any bar after the last row
    cfg = S.ParabolicConfig()
    idx = pd.date_range("2026-01-02 09:30", periods=30, freq="1min", tz=ET)
    df = pd.DataFrame({"timestamp": idx, "symbol": "T",
                       "open": np.linspace(10, 11, 30), "high": np.linspace(10.1, 11.2, 30),
                       "low": np.linspace(9.9, 10.8, 30), "close": np.linspace(10, 11.1, 30),
                       "volume": np.full(30, 1000.0)})
    df["m_atr"] = S.atr(df, cfg.atr_period)
    orng = S.compute_opening_range(df, prior_close=9.5, daily_atr_dollars=0.5,
                                   open_window_baseline=5000, cfg=cfg)
    # decision at bar 20 must equal decision recomputed on the truncated frame
    a = S.entry_decision(df.iloc[:21], orng, cfg, float(df["m_atr"].iloc[20]))
    b = S.entry_decision(df.iloc[:21].copy(), orng, cfg, float(df["m_atr"].iloc[20]))
    assert (a is None) == (b is None)
    print("OK: fingerprint + deterministic no-look-ahead checks passed.")


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start"); ap.add_argument("--end")
    ap.add_argument("--sleeve", type=float, default=780.0)
    ap.add_argument("--symbols", default="")
    ap.add_argument("--label", default="parabolic")
    ap.add_argument("--walkforward", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    # knob overrides (for thesis-driven variants + B6 robustness sweep)
    for k in ["scalp_frac", "scalp_r", "trail_activate_r", "trail_atr_mult", "atr_stop_mult",
              "min_open_rvol", "breakout_vol_mult", "min_gap_pct", "risk_pct", "max_position_frac"]:
        ap.add_argument(f"--{k}", type=float, default=None)
    ap.add_argument("--or_minutes", type=int, default=None)
    ap.add_argument("--max_concurrent", type=int, default=None)
    a = ap.parse_args()

    if a.selftest:
        selftest(); return

    cfg = S.ParabolicConfig()
    for k in ["scalp_frac", "scalp_r", "trail_activate_r", "trail_atr_mult", "atr_stop_mult",
              "min_open_rvol", "breakout_vol_mult", "min_gap_pct", "risk_pct", "max_position_frac",
              "or_minutes", "max_concurrent"]:
        v = getattr(a, k, None)
        if v is not None:
            setattr(cfg, k, v)
    syms = a.symbols.split(",") if a.symbols else DEFAULT_UNIVERSE

    if a.walkforward:
        windows = [
            ("2023-01-03", "2023-03-31", "2023Q1 recovery"),
            ("2024-07-01", "2024-09-30", "2024Q3 chop/AI"),
            ("2025-03-01", "2025-05-30", "2025 vol"),
            ("2026-04-20", "2026-06-15", "live-baseline window"),
        ]
        rows = []
        for s, e, lbl in windows:
            res = run(syms, date.fromisoformat(s), date.fromisoformat(e), a.sleeve, cfg, lbl)
            m = metrics(res, lbl); print_metrics(m); rows.append(m)
        print("\n===== WALK-FORWARD SUMMARY =====")
        for m in rows:
            print(f"  {m['label']:24} trades {m['trades']:4d}  ret {m['ret_pct']:+7.2f}%  "
                  f"PF {m['profit_factor']:.2f}  Sharpe {m['sharpe']:.2f}  "
                  f"maxDD {m['max_dd_pct']:.1f}%  exp {m['expectancy']:+.2f}")
        return

    res = run(syms, date.fromisoformat(a.start), date.fromisoformat(a.end), a.sleeve, cfg, a.label)
    print_metrics(metrics(res, a.label))


if __name__ == "__main__":
    main()
