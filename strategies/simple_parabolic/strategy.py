"""
SIMPLE v2 — Parabolic-Continuation (Opening-Range Breakout for Stocks in Play)
=============================================================================

SINGLE SOURCE OF TRUTH. These are pure, deterministic functions over intraday
minute bars. Both the faithful backtest harness (`backtest/simple_parabolic_backtest.py`)
and the live executor import and call THIS module — there is no separate
"backtest version" of the logic (Horizon Section 6, requirement #1).

Thesis (inverts the current SIMPLE scorer's anti-extension bias):
    The current SIMPLE bot scans high-RVOL movers but its scorer PENALISES the
    extension those movers show (VWAP>3%, 5min-mom>1.5%, ADX>35 all docked) — so
    it scans momentum and buys mean-reversion (live: PF 0.19, 27.8% WR). This
    redesign instead REWARDS extension and trades CONTINUATION:

      1. Select "stocks in play": liquid names whose opening volume is abnormally
         high vs their own recent baseline (relative-volume rank) AND that opened
         with a directional drive (gap and/or up opening-range bar).
      2. Define an opening range (first OR_MINUTES). Enter LONG on a *breakout
         above the range high* — i.e. continuation of the drive — with a
         marketable/stop entry that actually fills fast movers (fixes COIN-style
         TIMEOUT_NO_FILL).
      3. ATR-based stop below the range; scalp a partial at +Rscalp, trail the
         runner. HARD intraday flatten — never hold overnight (fixes the live
         "scalp held 3 days" drift).

No look-ahead: every function only reads bars with index <= the decision index.
Live-only predictive overlays (options flow / dark pool / short vol / sentiment)
enter ONLY through `overlay_size_mult`, which is clamped to [1.0, max] — an overlay
can scale a *validated* signal up, but can never create one. Default overlay = 1.0,
so the backtested edge and the live edge are the same edge.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import time as dtime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

STRATEGY_ID = "SIMPLE"          # same sleeve id; this is SIMPLE's replacement logic
STRATEGY_VARIANT = "parabolic_orb_v2"


# ---------------------------------------------------------------------------
# Configuration (every tunable in one place; harness sweeps these for B6)
# ---------------------------------------------------------------------------
@dataclass
class ParabolicConfig:
    # --- universe / selection (stocks in play) ---
    or_minutes: int = 15               # opening-range length
    min_price: float = 5.0             # liquidity floor (QC: >$5)
    min_atr_dollars: float = 0.25      # needs room to move (QC: ATR>$0.50, relaxed)
    min_open_rvol: float = 2.0         # opening-window vol / trailing baseline
    rvol_baseline_days: int = 14       # baseline window for opening RVOL
    min_gap_pct: float = 0.5           # require an overnight drive (gap up >= 0.5%)
    require_up_or_bar: bool = True     # OR must close above its open (directional)
    max_concurrent: int = 2            # small account -> few, meaningful positions
    top_k_in_play: int = 6             # rank universe, consider best K each morning

    # --- entry (continuation breakout) ---
    entry_window_end: dtime = dtime(11, 30)   # only open new trades before this (ET)
    breakout_buffer_bps: float = 5.0          # trigger = OR_high * (1 + buffer)
    require_breakout_volume: bool = True      # breakout bar vol > recent avg
    breakout_vol_mult: float = 1.2            # breakout bar volume confirmation

    # --- risk / sizing ---
    risk_pct: float = 0.012            # risk per trade as frac of sleeve equity (~1.2%)
    atr_period: int = 14               # ATR on minute bars (intraday)
    atr_stop_mult: float = 1.5         # stop = entry - mult*ATR (below OR)
    min_stop_pct: float = 0.6          # floor on stop distance (%)
    max_stop_pct: float = 4.0          # cap on stop distance (%)
    max_position_frac: float = 0.50    # cap one position at 50% of sleeve (small acct)
    min_notional: float = 25.0         # skip if affordable size is sub-scale

    # --- exits (coherent with thesis; intraday only) ---
    scalp_r: float = 1.0               # take partial at +1R
    scalp_frac: float = 0.5            # fraction sold at scalp
    trail_activate_r: float = 1.0      # arm trail once +1R
    trail_atr_mult: float = 2.0        # runner trails by mult*ATR
    hard_flatten: dtime = dtime(15, 50)  # HARD intraday flatten (no overnight, ever)

    # --- microstructure confirmation (BACKTESTABLE: tick trades on Stocks Advanced) ---
    # Separates real continuation from false breakouts: require net buy-side order-flow
    # and institutional block participation on the breakout minute.
    use_microstructure: bool = True
    min_of_imbalance: float = 0.15     # signed (uptick-rule) volume / total volume
    min_block_frac: float = 0.25       # fraction of minute volume in >= $50k prints

    # --- live overlay clamp (predictive data may only scale up) ---
    overlay_max_mult: float = 1.5

    # symbols that are leveraged/inverse ETFs -> size divided (kept out of core by default)
    leveraged_divisor: float = 3.0


# ---------------------------------------------------------------------------
# Deterministic indicators (same formulas as the proven backtest helpers)
# ---------------------------------------------------------------------------
def atr(df: pd.DataFrame, period: int) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([high - low, (high - close.shift(1)).abs(),
                    (low - close.shift(1)).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


# ---------------------------------------------------------------------------
# Opening range + selection (computed ONCE per symbol/day at OR end)
# ---------------------------------------------------------------------------
@dataclass
class OpeningRange:
    symbol: str
    or_high: float
    or_low: float
    or_open: float
    or_close: float
    or_volume: float
    open_rvol: float        # opening-window vol vs trailing baseline
    gap_pct: float          # today's open vs prior close
    up_bar: bool            # OR closed >= OR open
    atr_dollars: float      # daily ATR (room to move)


def compute_opening_range(
    day_minute: pd.DataFrame,      # minute bars for ONE symbol, ONE day, time-asc, ET tz
    prior_close: float,
    daily_atr_dollars: float,
    open_window_baseline: float,   # avg opening-window volume over baseline_days
    cfg: ParabolicConfig,
) -> Optional[OpeningRange]:
    """Build the opening range from the first `or_minutes` of RTH. No look-ahead:
    only the opening window is read. Returns None if data is insufficient."""
    if day_minute is None or len(day_minute) == 0 or prior_close <= 0:
        return None
    rth = day_minute[(day_minute["timestamp"].dt.time >= dtime(9, 30)) &
                     (day_minute["timestamp"].dt.time < dtime(16, 0))]
    if len(rth) == 0:
        return None
    or_end = (pd.Timestamp(rth["timestamp"].iloc[0]).normalize()
              + pd.Timedelta(hours=9, minutes=30 + cfg.or_minutes))
    or_bars = rth[rth["timestamp"] < or_end]
    if len(or_bars) < max(2, cfg.or_minutes // 2):
        return None
    or_high = float(or_bars["high"].max())
    or_low = float(or_bars["low"].min())
    or_open = float(or_bars["open"].iloc[0])
    or_close = float(or_bars["close"].iloc[-1])
    or_vol = float(or_bars["volume"].sum())
    open_rvol = or_vol / open_window_baseline if open_window_baseline > 0 else 0.0
    gap_pct = (or_open / prior_close - 1.0) * 100.0
    return OpeningRange(
        symbol=day_minute["symbol"].iloc[0] if "symbol" in day_minute else "?",
        or_high=or_high, or_low=or_low, or_open=or_open, or_close=or_close,
        or_volume=or_vol, open_rvol=open_rvol, gap_pct=gap_pct,
        up_bar=(or_close >= or_open), atr_dollars=daily_atr_dollars,
    )


def is_in_play(orng: OpeningRange, cfg: ParabolicConfig) -> bool:
    """Selection filter — REWARDS extension/volume (opposite of current scorer)."""
    if orng is None:
        return False
    if orng.or_open < cfg.min_price:
        return False
    if orng.atr_dollars < cfg.min_atr_dollars:
        return False
    if orng.open_rvol < cfg.min_open_rvol:        # must be abnormally active
        return False
    if cfg.require_up_or_bar and not orng.up_bar:  # directional drive
        return False
    if orng.gap_pct < cfg.min_gap_pct:             # overnight catalyst/drive
        return False
    return True


def rank_in_play(ranges: List[OpeningRange], cfg: ParabolicConfig) -> List[OpeningRange]:
    """Rank the day's in-play names; best-first. Reward RVOL and a strong drive."""
    eligible = [r for r in ranges if is_in_play(r, cfg)]
    # composite: opening RVOL (conviction) * (1 + gap drive) — both reward extension
    eligible.sort(key=lambda r: r.open_rvol * (1.0 + max(0.0, r.gap_pct) / 100.0),
                  reverse=True)
    return eligible[: cfg.top_k_in_play]


# ---------------------------------------------------------------------------
# Entry decision (continuation breakout), evaluated bar-by-bar AFTER the OR
# ---------------------------------------------------------------------------
@dataclass
class EntryIntent:
    symbol: str
    trigger_price: float    # marketable/stop level (OR_high + buffer)
    stop_price: float
    atr_at_entry: float
    reason: str = ""


def entry_decision(
    bars_through_t: pd.DataFrame,   # one symbol's minute bars, index 0..t (<= now)
    orng: OpeningRange,
    cfg: ParabolicConfig,
    minute_atr: float,
) -> Optional[EntryIntent]:
    """Decide at bar t whether the continuation breakout has triggered.

    Pure & no-look-ahead: reads only bars_through_t (the last row is bar t). The
    harness fills at t+1; the live executor places a stop/marketable order so the
    fill happens on the same forward move. Returns None if no trigger at t."""
    if orng is None or len(bars_through_t) == 0 or minute_atr <= 0:
        return None
    last = bars_through_t.iloc[-1]
    now_t = pd.Timestamp(last["timestamp"]).time()
    if now_t < dtime(9, 30 + 0):  # must be after OR window
        return None
    or_end_min = 30 + cfg.or_minutes
    if (now_t.hour == 9 and now_t.minute < or_end_min):
        return None
    if now_t >= cfg.entry_window_end:
        return None

    trigger = orng.or_high * (1.0 + cfg.breakout_buffer_bps / 10_000.0)
    # breakout: this bar's high pierced the trigger (continuation of the drive)
    if float(last["high"]) < trigger:
        return None
    if cfg.require_breakout_volume:
        recent = bars_through_t["volume"].iloc[-(cfg.or_minutes + 1):-1]
        if len(recent) and float(last["volume"]) < cfg.breakout_vol_mult * float(recent.mean()):
            return None

    # stop below the range, ATR-scaled, floored/capped
    raw_stop = trigger - cfg.atr_stop_mult * minute_atr
    stop_dist_pct = (trigger - raw_stop) / trigger * 100.0
    stop_dist_pct = float(np.clip(stop_dist_pct, cfg.min_stop_pct, cfg.max_stop_pct))
    stop_price = trigger * (1.0 - stop_dist_pct / 100.0)
    return EntryIntent(symbol=orng.symbol, trigger_price=trigger, stop_price=stop_price,
                       atr_at_entry=minute_atr,
                       reason=f"ORB-cont rvol={orng.open_rvol:.1f} gap={orng.gap_pct:.1f}%")


def arm_entry(
    orng: OpeningRange, minute_atr_at_or_end: float, cfg: ParabolicConfig,
) -> Optional[EntryIntent]:
    """Arm a RESTING STOP entry once, at the opening-range close. No look-ahead:
    uses only the opening range + ATR measured at OR end. The executor (live) places
    a stop-market at `trigger_price`; the harness fills it intrabar when price touches
    the trigger (the faithful model for a resting stop — not a chased next-open fill).

    Returns None if the symbol is not in play."""
    if orng is None or minute_atr_at_or_end <= 0:
        return None
    if not is_in_play(orng, cfg):
        return None
    trigger = orng.or_high * (1.0 + cfg.breakout_buffer_bps / 10_000.0)
    raw_stop = trigger - cfg.atr_stop_mult * minute_atr_at_or_end
    stop_dist_pct = float(np.clip((trigger - raw_stop) / trigger * 100.0,
                                  cfg.min_stop_pct, cfg.max_stop_pct))
    stop_price = trigger * (1.0 - stop_dist_pct / 100.0)
    return EntryIntent(symbol=orng.symbol, trigger_price=trigger, stop_price=stop_price,
                       atr_at_entry=minute_atr_at_or_end,
                       reason=f"ORB-cont rvol={orng.open_rvol:.1f} gap={orng.gap_pct:.1f}%")


def passes_microstructure(
    of_imbalance: Optional[float], block_frac: Optional[float], cfg: ParabolicConfig,
) -> bool:
    """Backtestable order-flow confirmation at the breakout minute. The data layer
    (harness or live executor) supplies per-minute features computed from tick trades;
    this gate is pure. When features are unavailable, do NOT block (permissive fallback
    so the price-only signal still stands — the gate only ever *removes* weak breakouts)."""
    if not cfg.use_microstructure:
        return True
    if of_imbalance is None or block_frac is None:
        return True
    return (of_imbalance >= cfg.min_of_imbalance) and (block_frac >= cfg.min_block_frac)


def position_size(
    entry_price: float, stop_price: float, sleeve_equity: float,
    sleeve_available: float, cfg: ParabolicConfig,
    overlay_size_mult: float = 1.0, leveraged: bool = False,
) -> int:
    """Whole-share size: risk `risk_pct` of sleeve to the stop, capped by sleeve
    capacity and max_position_frac; respects min_notional and account realism.

    `overlay_size_mult` is the ONLY way live predictive data influences sizing,
    and it is clamped to [1.0, overlay_max_mult] — overlays can scale up a
    validated signal, never manufacture or shrink it below the validated size."""
    risk_per_share = max(entry_price - stop_price, 0.01)
    mult = float(np.clip(overlay_size_mult, 1.0, cfg.overlay_max_mult))
    risk_dollars = sleeve_equity * cfg.risk_pct * mult
    shares = risk_dollars / risk_per_share
    # caps
    shares = min(shares, (sleeve_equity * cfg.max_position_frac) / entry_price)
    shares = min(shares, sleeve_available / entry_price)
    if leveraged:
        shares /= cfg.leveraged_divisor
    shares = int(shares)
    if shares * entry_price < cfg.min_notional:
        return 0
    return shares


# ---------------------------------------------------------------------------
# Open-position management (scalp / trail / hard intraday flatten)
# ---------------------------------------------------------------------------
@dataclass
class PositionState:
    symbol: str
    qty: int
    entry_price: float
    stop_price: float
    atr_at_entry: float
    scalped: bool = False
    peak: float = 0.0       # high-water for trailing


@dataclass
class ManageAction:
    kind: str               # "exit_partial" | "exit_all" | "raise_stop" | "hold"
    qty: int = 0
    price: float = 0.0      # reference price (stop or market)
    reason: str = ""


def manage_position(
    pos: PositionState, bar: pd.Series, cfg: ParabolicConfig,
) -> List[ManageAction]:
    """Given the current bar, return management actions. Pure; reads only `bar`
    (the current minute) and `pos` state. Stop/target are evaluated against the
    bar's low/high (harness models intrabar touch; live uses resting bracket)."""
    actions: List[ManageAction] = []
    now_t = pd.Timestamp(bar["timestamp"]).time()
    high, low, close = float(bar["high"]), float(bar["low"]), float(bar["close"])
    r = pos.entry_price - pos.stop_price
    if r <= 0:
        r = max(pos.entry_price * cfg.min_stop_pct / 100.0, 0.01)

    # 1) HARD intraday flatten — overrides everything (no overnight, ever)
    if now_t >= cfg.hard_flatten:
        actions.append(ManageAction("exit_all", pos.qty, close, "EOD_HARD_FLATTEN"))
        return actions

    # 2) stop hit (intrabar low <= stop)
    if low <= pos.stop_price:
        actions.append(ManageAction("exit_all", pos.qty, pos.stop_price, "STOP"))
        return actions

    pos.peak = max(pos.peak, high)

    # 3) scalp partial at +scalp_r
    if (not pos.scalped) and cfg.scalp_frac > 0 and high >= pos.entry_price + cfg.scalp_r * r:
        q = max(1, int(pos.qty * cfg.scalp_frac))
        if q < pos.qty:
            actions.append(ManageAction("exit_partial", q,
                                        pos.entry_price + cfg.scalp_r * r, "SCALP"))

    # 4) trail the runner once armed
    if pos.peak >= pos.entry_price + cfg.trail_activate_r * r:
        new_stop = pos.peak - cfg.trail_atr_mult * pos.atr_at_entry
        if new_stop > pos.stop_price:
            actions.append(ManageAction("raise_stop", price=new_stop, reason="TRAIL"))

    if not actions:
        actions.append(ManageAction("hold"))
    return actions


# Single-source-of-truth marker — the harness asserts it imports THIS module.
def source_fingerprint() -> str:
    return f"{__name__}:{STRATEGY_VARIANT}"
