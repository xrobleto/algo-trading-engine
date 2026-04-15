"""
Shared market-microstructure utilities (WS2).

Provides pure-function regime signals consumable by:
  - strategies/engine/intelligence.py (WS3 SIMPLE breadth gate, WS4 TREND chop dampener)
  - strategies/trend_bot.py (refactor — replaces inline RSP/IWM breadth block at lines 2585-2708)

Design:
  - All functions pure: take price series → return float / dict
  - No I/O, no network, no side effects — caller is responsible for data fetch
  - Session-level caching is the caller's responsibility (we don't reach into ai_manager)
  - Behavior-preserving: breadth_signal() replicates the exact scoring in trend_bot
    today so the refactor can be verified with a parity test (≤1e-6 delta)

Signals exposed:
  - breadth_signal(spy, rsp, iwm, equity_closes)  → dict (drop-in for trend_bot)
  - narrowness_z(spy, rsp, window)                → float Z-score (WS3)
  - chop_score(ohlc, period)                      → float in [0,1] (WS4)
"""

from __future__ import annotations

from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd


# =============================================================================
# BREADTH — behaviour-preserving extraction of trend_bot.py:2585-2708
# =============================================================================

def _sma(series: pd.Series, period: int) -> float:
    """Return last SMA value. Matches trend_bot's `sma()` contract."""
    if len(series) < period:
        return float("nan")
    return float(series.iloc[-period:].mean())


def breadth_signal(
    spy_close: Optional[pd.Series] = None,
    rsp_close: Optional[pd.Series] = None,
    iwm_close: Optional[pd.Series] = None,
    equity_closes: Optional[Dict[str, pd.Series]] = None,
) -> Dict[str, object]:
    """
    Score market breadth using RSP/SPY (equal-weight vs cap-weight) and IWM/SPY
    (small caps vs large caps). Falls back to % above SMA50 across an equity
    universe if RSP/IWM unavailable.

    Returns a dict with the same keys trend_bot's inline block currently writes
    into its `components` dict (see trend_bot.py:2587-2706):

        {
            "breadth_score": float,
            "breadth_method": "rsp_iwm" | "rsp_only" | "iwm_only" | "legacy_sma50",
            "rsp_score": float | None,
            "iwm_score": float | None,
            "rsp_spy_ratio": float | None,
            "rsp_spy_sma20": float | None,
            "iwm_spy_ratio": float | None,
            "iwm_spy_sma20": float | None,
            "breadth_pct": float | None,  # legacy path only
        }

    Any of the input series may be None (source unavailable) — the function will
    fall through the precedence chain. If nothing is usable, returns
    breadth_score=50.0 (neutral).
    """
    out: Dict[str, object] = {
        "breadth_score": 50.0,
        "breadth_method": None,
        "rsp_score": None,
        "iwm_score": None,
        "rsp_spy_ratio": None,
        "rsp_spy_sma20": None,
        "iwm_spy_ratio": None,
        "iwm_spy_sma20": None,
        "breadth_pct": None,
    }

    # --- RSP/SPY ratio ---
    rsp_score: Optional[float] = None
    if (rsp_close is not None and spy_close is not None
            and len(rsp_close) >= 50 and len(spy_close) >= 50):
        min_len = min(len(rsp_close), len(spy_close))
        r = rsp_close.iloc[-min_len:].reset_index(drop=True)
        s = spy_close.iloc[-min_len:].reset_index(drop=True)
        ratio = r / s
        ratio_sma20 = _sma(ratio, 20)
        current = float(ratio.iloc[-1])
        if not np.isnan(ratio_sma20):
            if current > ratio_sma20 * 1.01:
                rsp_score = 80.0
            elif current > ratio_sma20:
                rsp_score = 65.0
            elif current > ratio_sma20 * 0.99:
                rsp_score = 45.0
            else:
                rsp_score = 25.0
            out["rsp_score"] = rsp_score
            out["rsp_spy_ratio"] = current
            out["rsp_spy_sma20"] = ratio_sma20

    # --- IWM/SPY ratio ---
    iwm_score: Optional[float] = None
    if (iwm_close is not None and spy_close is not None
            and len(iwm_close) >= 50 and len(spy_close) >= 50):
        min_len = min(len(iwm_close), len(spy_close))
        i = iwm_close.iloc[-min_len:].reset_index(drop=True)
        s = spy_close.iloc[-min_len:].reset_index(drop=True)
        ratio = i / s
        ratio_sma20 = _sma(ratio, 20)
        current = float(ratio.iloc[-1])
        if not np.isnan(ratio_sma20):
            if current > ratio_sma20 * 1.02:
                iwm_score = 85.0
            elif current > ratio_sma20:
                iwm_score = 65.0
            elif current > ratio_sma20 * 0.98:
                iwm_score = 40.0
            else:
                iwm_score = 20.0
            out["iwm_score"] = iwm_score
            out["iwm_spy_ratio"] = current
            out["iwm_spy_sma20"] = ratio_sma20

    # --- Combine ---
    if rsp_score is not None and iwm_score is not None:
        out["breadth_score"] = 0.55 * rsp_score + 0.45 * iwm_score
        out["breadth_method"] = "rsp_iwm"
    elif rsp_score is not None:
        out["breadth_score"] = rsp_score
        out["breadth_method"] = "rsp_only"
    elif iwm_score is not None:
        out["breadth_score"] = iwm_score
        out["breadth_method"] = "iwm_only"
    elif equity_closes:
        # Legacy fallback: % above SMA50 across a universe
        above, total = 0, 0
        for sym, close in equity_closes.items():
            if close is None or len(close) < 50:
                continue
            sym_sma50 = _sma(close, 50)
            if np.isnan(sym_sma50):
                continue
            if float(close.iloc[-1]) > sym_sma50:
                above += 1
            total += 1
        if total > 0:
            breadth_pct = above / total
            out["breadth_score"] = breadth_pct * 100.0
            out["breadth_pct"] = breadth_pct
        out["breadth_method"] = "legacy_sma50"

    return out


# =============================================================================
# NARROWNESS — WS3 SIMPLE breadth gate signal
# =============================================================================
#
# Metric design:
#
# We want to detect "narrow leadership" — when SPY is rallying but RSP
# (equal-weight) is flat or declining, i.e. a handful of mega-caps carrying
# the index. This is the P5 2023 AI rally signature.
#
# Use a LEVEL-based gap: (RSP_return_window - SPY_return_window). Negative
# means RSP underperformed SPY over the window = narrow. 63-day window
# (≈ 1 quarter) is the right horizon — narrow leadership is a months-long
# phenomenon, not a 20-day blip.
#
# Ratio-of-change Z-score (previous design) fails because a sustained narrow
# regime compresses both the signal AND its lookback variance → Z-score
# collapses to ~0. Gap-based level is robust to that.


def narrowness_gap(
    spy_close: pd.Series,
    rsp_close: pd.Series,
    window: int = 63,
) -> float:
    """
    Return (RSP_window_return - SPY_window_return).

    Negative = RSP underperformed SPY = narrow-leadership regime.
    Typical values:
      +0.05  — RSP leading SPY by 5pp (broad risk-on)
       0.00 — balanced
      -0.05  — RSP trailing SPY by 5pp (narrowing)
      -0.10  — RSP trailing SPY by 10pp (severe narrow leadership, P5 signature)
    """
    if spy_close is None or rsp_close is None:
        return 0.0
    n = min(len(spy_close), len(rsp_close))
    if n <= window:
        return 0.0
    s = spy_close.iloc[-n:].reset_index(drop=True)
    r = rsp_close.iloc[-n:].reset_index(drop=True)
    s_ret = float(s.iloc[-1] / s.iloc[-window - 1] - 1.0)
    r_ret = float(r.iloc[-1] / r.iloc[-window - 1] - 1.0)
    return r_ret - s_ret


def narrowness_score(
    spy_close: pd.Series,
    rsp_close: pd.Series,
    window: int = 63,
    saturation_gap: float = 0.10,
) -> float:
    """
    0-1 narrowness score. 0.5 = balanced, 1.0 = RSP trails SPY by >= saturation_gap,
    0.0 = RSP leads SPY by >= saturation_gap.

    Mapping: score = clip(0.5 - gap/saturation_gap, 0, 1).
    """
    gap = narrowness_gap(spy_close, rsp_close, window=window)
    return float(np.clip(0.5 - gap / saturation_gap, 0.0, 1.0))


def narrowness_sustained(
    spy_close: pd.Series,
    rsp_close: pd.Series,
    threshold_score: float = 0.65,
    sustain_days: int = 10,
    window: int = 63,
    saturation_gap: float = 0.10,
) -> bool:
    """
    Return True if narrowness_score has been >= threshold_score for each of
    the last `sustain_days` trading days.

    This is the WS3 gate primitive — distinguishes P5-style sustained narrow
    rallies from P1-style transient narrow drift.
    """
    if spy_close is None or rsp_close is None:
        return False
    n = min(len(spy_close), len(rsp_close))
    if n < window + sustain_days + 1:
        return False
    s = spy_close.iloc[-n:].reset_index(drop=True)
    r = rsp_close.iloc[-n:].reset_index(drop=True)
    for offset in range(sustain_days):
        idx = n - 1 - offset
        if idx - window < 0:
            return False
        s_ret = float(s.iloc[idx] / s.iloc[idx - window] - 1.0)
        r_ret = float(r.iloc[idx] / r.iloc[idx - window] - 1.0)
        gap = r_ret - s_ret
        score = float(np.clip(0.5 - gap / saturation_gap, 0.0, 1.0))
        if score < threshold_score:
            return False
    return True


# =============================================================================
# CHOP — WS4 TREND chop dampener signal
# =============================================================================

def _true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    tr = _true_range(high, low, close)
    # Wilder smoothing approximated by EWM with alpha = 1/period
    return tr.ewm(alpha=1.0 / period, adjust=False).mean()


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder ADX — standard technical implementation."""
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    plus_dm = pd.Series(plus_dm, index=high.index)
    minus_dm = pd.Series(minus_dm, index=high.index)
    tr = _true_range(high, low, close)
    atr_w = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    plus_di = 100.0 * (plus_dm.ewm(alpha=1.0 / period, adjust=False).mean() / atr_w.replace(0, np.nan))
    minus_di = 100.0 * (minus_dm.ewm(alpha=1.0 / period, adjust=False).mean() / atr_w.replace(0, np.nan))
    dx = 100.0 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
    return dx.ewm(alpha=1.0 / period, adjust=False).mean()


def chop_score(
    ohlc: pd.DataFrame,
    period: int = 14,
    range_window: int = 20,
    flip_window: int = 20,
    return_components: bool = False,
):
    """
    Return a chop score in [0, 1]. Higher = more choppy, sideways, trend-hostile.

    Blended components (weighted 40/25/20/15):
      - ADX component: 1.0 at ADX=0, 0.0 at ADX>=25 (textbook chop threshold)
      - Range-compression: 1.0 when ATR is small vs window range,
                           0.0 when ATR >= 0.25 × window range
      - Sign-flip density: fraction of last `flip_window` days with sign flips
      - Trend-exhaust proxy: close-to-close net move over the window, divided
                             by total absolute path length. Low ratio = chop.

    Expects `ohlc` with columns high, low, close. Returns 0.0 if insufficient data.
    If return_components=True, returns a dict with all sub-scores for debugging.
    """
    required = {"high", "low", "close"}
    if not required.issubset(ohlc.columns):
        raise ValueError(f"chop_score expects columns {required}; got {set(ohlc.columns)}")
    if len(ohlc) < max(period * 3, range_window + 5, flip_window + 2):
        return {} if return_components else 0.0

    high = ohlc["high"].astype(float)
    low = ohlc["low"].astype(float)
    close = ohlc["close"].astype(float)

    # ADX — steeper ramp: full chop at ADX<=12, no chop at ADX>=25
    adx = _adx(high, low, close, period=period)
    adx_last = float(adx.iloc[-1]) if not np.isnan(adx.iloc[-1]) else 20.0
    adx_component = float(np.clip((25.0 - adx_last) / 13.0, 0.0, 1.0))

    # Range-compression: ATR vs window range
    atr = _atr(high, low, close, period=period)
    window_range = high.rolling(range_window).max() - low.rolling(range_window).min()
    atr_last = float(atr.iloc[-1])
    rng_last = float(window_range.iloc[-1])
    if rng_last <= 0:
        compress_component = 0.0
    else:
        compress_ratio = atr_last / rng_last
        # Higher ATR/range = trending; low = trending inside tight band
        # We want CHOP = moderate ATR with moderate range. Signal is strongest
        # when ratio is between 0.10 and 0.20. Use a tent function.
        if compress_ratio < 0.05 or compress_ratio > 0.30:
            compress_component = 0.0
        elif compress_ratio < 0.15:
            compress_component = (compress_ratio - 0.05) / 0.10
        else:
            compress_component = max(0.0, (0.30 - compress_ratio) / 0.15)

    # Sign-flip density
    rets = close.pct_change().iloc[-flip_window:]
    signs = np.sign(rets.fillna(0.0))
    flips = (signs != signs.shift(1)).iloc[1:].sum()
    flip_component = float(flips) / float(flip_window - 1) if flip_window > 1 else 0.0

    # Trend-exhaust: net move / total path over window
    # Pure trend: path ≈ net move → ratio ≈ 1 → chop=0
    # Pure chop:  path >> net move  → ratio ≈ 0 → chop=1
    window_close = close.iloc[-range_window:]
    if len(window_close) >= 2:
        net = abs(float(window_close.iloc[-1] - window_close.iloc[0]))
        path = float(window_close.diff().abs().sum())
        if path > 0:
            efficiency = net / path
            trend_exhaust = float(np.clip(1.0 - efficiency * 2.5, 0.0, 1.0))
        else:
            trend_exhaust = 0.0
    else:
        trend_exhaust = 0.0

    score = (
        0.40 * adx_component
        + 0.25 * compress_component
        + 0.20 * flip_component
        + 0.15 * trend_exhaust
    )
    score = float(np.clip(score, 0.0, 1.0))

    if return_components:
        return {
            "chop_score": score,
            "adx_last": adx_last,
            "adx_component": adx_component,
            "compress_ratio": float(atr_last / rng_last) if rng_last > 0 else 0.0,
            "compress_component": compress_component,
            "flips": int(flips),
            "flip_component": flip_component,
            "trend_exhaust": trend_exhaust,
        }
    return score
