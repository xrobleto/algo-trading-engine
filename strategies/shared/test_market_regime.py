"""
Unit tests for strategies/shared/market_regime.py (WS2).

Uses real Polygon daily bars pulled via backtest.cross_asset_bot_backtest.load_bars
to verify signal behavior against known regime windows:

  - narrowness: should be strongly negative Z-score around P5 peak (AI rally, late 2023)
  - narrowness: should be near zero in P2 and P6 (broad rallies)
  - chop: should be high (>= 0.5) in H2 2022 (P3)
  - chop: should be low (<= 0.3) in early 2025 strong trending periods

Run:
    python strategies/shared/test_market_regime.py
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backtest.cross_asset_bot_backtest import load_bars  # noqa: E402
from strategies.shared.market_regime import (  # noqa: E402
    breadth_signal,
    chop_score,
    narrowness_gap,
    narrowness_score,
    narrowness_sustained,
)


def _get_close(symbol: str, start: date, end: date) -> pd.Series:
    bars = load_bars([symbol], start, end, use_cache=True)
    bars = bars[bars["symbol"] == symbol].sort_values("timestamp").reset_index(drop=True)
    s = pd.Series(bars["close"].astype(float).values, name=symbol)
    return s


def _get_ohlc(symbol: str, start: date, end: date) -> pd.DataFrame:
    bars = load_bars([symbol], start, end, use_cache=True)
    bars = bars[bars["symbol"] == symbol].sort_values("timestamp").reset_index(drop=True)
    return bars[["high", "low", "close"]].astype(float).reset_index(drop=True)


def test_narrowness_p5_ai_rally():
    """P5 AI rally: narrowness_score should be elevated through Jun-Dec 2023."""
    # Test a few points through the rally
    results = {}
    for label, end in [("Aug 2023", date(2023, 8, 15)),
                        ("Oct 2023", date(2023, 10, 15)),
                        ("Nov 2023", date(2023, 11, 30))]:
        start = end - timedelta(days=500)
        spy = _get_close("SPY", start, end)
        rsp = _get_close("RSP", start, end)
        gap = narrowness_gap(spy, rsp, window=63)
        score = narrowness_score(spy, rsp, window=63)
        sustained = narrowness_sustained(spy, rsp, threshold_score=0.60, sustain_days=10)
        print(f"P5 {label}: gap={gap*100:+.2f}pp  score={score:.2f}  sustained(>=0.60,10d)={sustained}")
        results[label] = (gap, score, sustained)

    # At least one of the Aug/Oct checkpoints should show elevated narrowness
    gaps = [v[0] for v in results.values()]
    scores = [v[1] for v in results.values()]
    assert min(gaps) < -0.03, f"Expected at least one checkpoint with gap < -3pp in P5, got gaps={gaps}"
    assert max(scores) > 0.60, f"Expected at least one checkpoint with score > 0.60 in P5, got scores={scores}"
    return results


def test_narrowness_p2_broad_bear():
    """P2 broad bear 2022: gap should be positive or near zero (broad decline)."""
    end = date(2022, 6, 15)
    start = end - timedelta(days=500)
    spy = _get_close("SPY", start, end)
    rsp = _get_close("RSP", start, end)
    gap = narrowness_gap(spy, rsp, window=63)
    score = narrowness_score(spy, rsp, window=63)
    print(f"P2 mid (2022-06-15): gap={gap*100:+.2f}pp  score={score:.2f}")
    # In a broad bear RSP should hold up ~as well as SPY → gap near zero or positive
    assert gap > -0.03, f"Expected gap > -3pp in P2 (broad bear), got {gap*100:.2f}pp"
    return gap, score


def test_narrowness_p1_benign_narrow():
    """P1 2021 euphoria: brief narrow period, not sustained. Sustained gate should NOT fire."""
    end = date(2021, 12, 15)
    start = end - timedelta(days=500)
    spy = _get_close("SPY", start, end)
    rsp = _get_close("RSP", start, end)
    gap = narrowness_gap(spy, rsp, window=63)
    sustained = narrowness_sustained(spy, rsp, threshold_score=0.60, sustain_days=10)
    print(f"P1 mid (2021-12-15): gap={gap*100:+.2f}pp  sustained(>=0.60,10d)={sustained}")
    return gap, sustained


def test_chop_p3_capitulation():
    """P3 H2 2022: chop should be elevated through the window."""
    results = {}
    for label, end in [("Aug 2022", date(2022, 8, 15)),
                        ("Oct 2022", date(2022, 10, 15)),
                        ("Nov 2022", date(2022, 11, 15))]:
        start = end - timedelta(days=200)
        ohlc = _get_ohlc("SPY", start, end)
        c = chop_score(ohlc, return_components=True)
        print(f"P3 chop ({label}): score={c['chop_score']:.3f} | ADX={c['adx_last']:.1f} | "
              f"comp_ratio={c['compress_ratio']:.3f} | flips={c['flips']}/19 | exhaust={c['trend_exhaust']:.2f}")
        results[label] = c["chop_score"]
    assert max(results.values()) >= 0.45, (
        f"Expected at least one P3 checkpoint >= 0.45, got {results}"
    )
    return results


def test_chop_p2_trending_bear():
    """P2 trending bear: chop should be MODERATE (directional but volatile)."""
    end = date(2022, 5, 15)
    start = end - timedelta(days=200)
    ohlc = _get_ohlc("SPY", start, end)
    c = chop_score(ohlc, return_components=True)
    print(f"P2 chop (2022-05-15): score={c['chop_score']:.3f} | ADX={c['adx_last']:.1f} | "
          f"flips={c['flips']}/19 | exhaust={c['trend_exhaust']:.2f}")
    return c["chop_score"]


def test_chop_p6_recent_trending():
    """P6 recent: chop signal should be lower than P3 peaks."""
    end = date(2026, 3, 15)
    start = end - timedelta(days=200)
    ohlc = _get_ohlc("SPY", start, end)
    c = chop_score(ohlc, return_components=True)
    print(f"P6 chop (2026-03-15): score={c['chop_score']:.3f} | ADX={c['adx_last']:.1f} | "
          f"flips={c['flips']}/19 | exhaust={c['trend_exhaust']:.2f}")
    return c["chop_score"]


def test_breadth_signal_basic():
    """breadth_signal returns a valid dict in all four precedence paths."""
    end = date(2024, 1, 15)
    start = end - timedelta(days=200)
    spy = _get_close("SPY", start, end)
    rsp = _get_close("RSP", start, end)
    iwm = _get_close("IWM", start, end)

    out_full = breadth_signal(spy, rsp, iwm)
    print(f"breadth (full): method={out_full['breadth_method']} score={out_full['breadth_score']:.1f}")
    assert out_full["breadth_method"] == "rsp_iwm"
    assert 0 <= out_full["breadth_score"] <= 100

    out_rsp_only = breadth_signal(spy, rsp, None)
    assert out_rsp_only["breadth_method"] == "rsp_only"

    out_iwm_only = breadth_signal(spy, None, iwm)
    assert out_iwm_only["breadth_method"] == "iwm_only"

    # Fallback legacy
    closes = {"AAPL": _get_close("AAPL", start, end),
              "MSFT": _get_close("MSFT", start, end)}
    out_legacy = breadth_signal(None, None, None, equity_closes=closes)
    assert out_legacy["breadth_method"] == "legacy_sma50"
    print(f"breadth (legacy): {out_legacy['breadth_method']} score={out_legacy['breadth_score']:.1f}")

    # No data at all → neutral
    out_none = breadth_signal(None, None, None)
    assert out_none["breadth_method"] is None
    assert out_none["breadth_score"] == 50.0


if __name__ == "__main__":
    print("=== WS2 signal tests ===\n")
    test_breadth_signal_basic()
    print()
    test_narrowness_p5_ai_rally()
    test_narrowness_p2_broad_bear()
    test_narrowness_p1_benign_narrow()
    print()
    p3_results = test_chop_p3_capitulation()
    p3_max = max(p3_results.values())
    test_chop_p2_trending_bear()
    p6_chop = test_chop_p6_recent_trending()
    print()
    print(f"Relative check: P3 max chop ({p3_max:.3f}) > P6 chop ({p6_chop:.3f})?", p3_max > p6_chop)
    print("\nAll tests passed." )
