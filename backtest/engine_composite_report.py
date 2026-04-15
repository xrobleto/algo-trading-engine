"""
Engine Composite Report Builder
================================

Consumes a run_dir produced by backtest/engine_regime_backtest.py and builds:

1. Per-strategy daily equity series for each regime window.
2. Engine-weighted composite equity curve per window
   (TREND 65% + SIMPLE 20% + XASSET 12% + cash 3%).
3. SPY buy-and-hold benchmark per window.
4. Stats table: total_return, CAGR, vol, Sharpe, Sortino, max_drawdown for each
   (strategy, window) + composite + SPY.
5. Optional: matplotlib PNG plots per window.

Outputs land inside the run_dir:
- composite/composite_stats.csv           — one row per (window, strategy)
- composite/composite_equity_<window>.csv — daily equity for each strategy + composite + SPY
- composite/plot_<window>.png             — (optional) equity curve + drawdown
- composite/summary.md                    — human-readable summary table

Usage:
    python backtest/engine_composite_report.py <run_dir>
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Reuse the XASSET backtest's cached Polygon daily-bar loader for SPY
from backtest.cross_asset_bot_backtest import load_bars as load_daily_bars  # noqa: E402
from backtest.regime_windows import REGIME_WINDOWS, by_label  # noqa: E402


# ============================================================
# CONFIG
# ============================================================

CASH_WEIGHT = 0.03          # Static cash sleeve per engine config
TRADING_DAYS_PER_YEAR = 252

# Initial capital used by the underlying per-strategy backtests. We normalize to 1.0
# before compositing, so the exact number doesn't matter — but we document it.
DEFAULT_INITIAL_CAPITAL = 100_000


# ============================================================
# STRATEGY EQUITY LOADERS
# ============================================================

def _load_trend_equity(results_json_path: str) -> Optional[pd.Series]:
    """TREND saves daily_equity in its results JSON."""
    if not Path(results_json_path).exists():
        return None
    with open(results_json_path, "r") as f:
        data = json.load(f)
    snapshots = data.get("daily_equity") or []
    if not snapshots:
        return None
    df = pd.DataFrame(snapshots)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    s = pd.Series(df["equity"].values, index=df["date"])
    return s.sort_index()


def _load_xasset_equity(equity_csv_path: str) -> Optional[pd.Series]:
    """XASSET writes a daily equity CSV directly."""
    if not Path(equity_csv_path).exists():
        return None
    df = pd.read_csv(equity_csv_path)
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"]).dt.date
    s = pd.Series(df["equity"].values, index=df["date"])
    return s.sort_index()


def _load_equity_from_trades(
    trades_csv_path: str, window_start: date, window_end: date,
    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
) -> Optional[pd.Series]:
    """
    Build a daily equity series from a trades CSV (SIMPLE / DIRECTIONAL).
    PnL is booked to the trade's exit date. Carry forward on no-trade days.
    """
    if not Path(trades_csv_path).exists():
        return None
    df = pd.read_csv(trades_csv_path)
    if df.empty:
        # Strategy ran but produced no trades — flat equity
        idx = pd.bdate_range(window_start, window_end).date
        return pd.Series([initial_capital] * len(idx), index=list(idx))

    # Mixed-tz strings in exit_time → use string-slice to get YYYY-MM-DD
    df["exit_date"] = pd.to_datetime(df["exit_time"].astype(str).str[:10]).dt.date
    daily_pnl = df.groupby("exit_date")["pnl"].sum()

    # Build full business-day index over window and forward-fill
    idx = pd.bdate_range(window_start, window_end).date
    pnl_series = pd.Series(0.0, index=list(idx))
    for d, pnl in daily_pnl.items():
        if d in pnl_series.index:
            pnl_series.loc[d] = pnl
    equity = initial_capital + pnl_series.cumsum()
    return equity


def _load_spy_equity(window_start: date, window_end: date,
                    initial_capital: float = DEFAULT_INITIAL_CAPITAL) -> Optional[pd.Series]:
    """SPY buy-and-hold benchmark over the window."""
    # Pull SPY bars via the xasset bar loader (which caches)
    fetch_start = window_start - timedelta(days=7)
    fetch_end = window_end + timedelta(days=2)
    try:
        bars = load_daily_bars(["SPY"], fetch_start, fetch_end, use_cache=True)
    except Exception as e:
        print(f"[WARN] SPY benchmark fetch failed: {e}")
        return None
    spy = bars[(bars["symbol"] == "SPY") &
               (bars["timestamp"] >= window_start) &
               (bars["timestamp"] <= window_end)].copy()
    if spy.empty:
        return None
    spy = spy.sort_values("timestamp")
    start_px = float(spy["close"].iloc[0])
    if start_px <= 0:
        return None
    equity = initial_capital * (spy["close"] / start_px)
    return pd.Series(equity.values, index=pd.to_datetime(spy["timestamp"]).dt.date)


# ============================================================
# NORMALIZATION & COMPOSITE
# ============================================================

def normalize(equity: pd.Series) -> pd.Series:
    """Normalize a daily equity series so it starts at 1.0."""
    if equity is None or len(equity) == 0:
        return equity
    first = equity.iloc[0]
    if first <= 0:
        return equity
    return equity / first


def align_and_merge(series_map: Dict[str, pd.Series]) -> pd.DataFrame:
    """Merge multiple daily equity series onto a common date index (union, then ffill)."""
    frames = [s.rename(name).to_frame() for name, s in series_map.items() if s is not None and len(s) > 0]
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, axis=1, join="outer").sort_index()
    # Forward-fill gaps (weekends, missing data days)
    merged = merged.ffill()
    # Drop leading rows with any NaN (before all strategies start)
    merged = merged.dropna(how="any")
    return merged


def build_composite(norm_frame: pd.DataFrame, weights: Dict[str, float]) -> pd.Series:
    """Weighted sum of normalized equity series + cash weight (static 1.0)."""
    missing_weight = sum(w for name, w in weights.items() if name not in norm_frame.columns)
    if missing_weight > 0:
        # Re-park missing sleeve weight into cash (treat as no-change)
        pass
    total = pd.Series(0.0, index=norm_frame.index)
    for name, w in weights.items():
        if name in norm_frame.columns:
            total = total + w * norm_frame[name]
        else:
            # Treat as cash (constant 1.0 × weight)
            total = total + w * 1.0
    total = total + CASH_WEIGHT * 1.0
    return total


# ============================================================
# STATS
# ============================================================

@dataclass
class StatsRow:
    window: str
    strategy: str
    total_return_pct: float
    cagr_pct: float
    vol_pct: float
    sharpe: float
    sortino: float
    max_drawdown_pct: float
    max_dd_duration_days: int
    trading_days: int


def compute_stats(window_label: str, strategy: str, equity: pd.Series) -> StatsRow:
    if equity is None or len(equity) < 2:
        return StatsRow(window_label, strategy, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0)

    start_eq = float(equity.iloc[0])
    end_eq = float(equity.iloc[-1])
    total_return = (end_eq / start_eq - 1.0) if start_eq > 0 else 0.0

    # CAGR
    start_date = equity.index[0]
    end_date = equity.index[-1]
    days = (end_date - start_date).days
    years = days / 365.25 if days > 0 else 1 / 252
    cagr = (end_eq / start_eq) ** (1 / years) - 1 if start_eq > 0 and years > 0 else 0.0

    # Daily returns
    rets = equity.pct_change().dropna()
    if len(rets) == 0:
        vol = sharpe = sortino = 0.0
    else:
        vol = float(rets.std() * np.sqrt(TRADING_DAYS_PER_YEAR))
        sharpe = float(rets.mean() / rets.std() * np.sqrt(TRADING_DAYS_PER_YEAR)) if rets.std() > 0 else 0.0
        downside = rets[rets < 0]
        dstd = float(downside.std() * np.sqrt(TRADING_DAYS_PER_YEAR)) if len(downside) > 0 else vol
        sortino = float(rets.mean() * TRADING_DAYS_PER_YEAR / dstd) if dstd > 0 else 0.0

    # Max drawdown
    rolling_max = equity.cummax()
    dd = (rolling_max - equity) / rolling_max
    max_dd = float(dd.max()) if len(dd) > 0 else 0.0

    # Drawdown duration (longest stretch where dd > 0.1%)
    in_dd = dd > 0.001
    if in_dd.any():
        groups = (~in_dd).cumsum()
        lengths = in_dd.groupby(groups).sum()
        max_dd_dur = int(lengths.max())
    else:
        max_dd_dur = 0

    return StatsRow(
        window=window_label,
        strategy=strategy,
        total_return_pct=round(total_return * 100, 3),
        cagr_pct=round(cagr * 100, 3),
        vol_pct=round(vol * 100, 3),
        sharpe=round(sharpe, 3),
        sortino=round(sortino, 3),
        max_drawdown_pct=round(max_dd * 100, 3),
        max_dd_duration_days=max_dd_dur,
        trading_days=len(equity),
    )


# ============================================================
# PLOTS (optional)
# ============================================================

def _try_plot(window_label: str, frame: pd.DataFrame, out_path: Path) -> bool:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 7), sharex=True)

    for col in frame.columns:
        style = {"linewidth": 1.5}
        if col == "COMPOSITE":
            style["color"] = "black"
            style["linewidth"] = 2.5
        elif col == "SPY":
            style["color"] = "gray"
            style["linestyle"] = "--"
        ax1.plot(frame.index, frame[col], label=col, **style)
    ax1.set_title(f"{window_label} — normalized equity (start = 1.0)")
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc="best", fontsize=9)
    ax1.set_ylabel("Normalized equity")

    # Drawdown panel for composite + SPY
    for col in ("COMPOSITE", "SPY"):
        if col in frame.columns:
            ser = frame[col]
            dd = (ser.cummax() - ser) / ser.cummax() * 100
            ax2.fill_between(frame.index, 0, -dd, alpha=0.4, label=f"{col} drawdown")
    ax2.set_ylabel("Drawdown (%)")
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc="best", fontsize=9)
    ax2.set_xlabel("Date")

    fig.tight_layout()
    fig.savefig(out_path, dpi=110)
    plt.close(fig)
    return True


# ============================================================
# REPORT BUILDER
# ============================================================

def build_report(run_dir: Path) -> None:
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"No manifest at {manifest_path}")

    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    # Engine sleeve weights from manifest (only strategies with nonzero weight count in composite)
    strategy_specs = manifest["strategy_specs"]
    weights = {s["name"]: float(s["engine_weight"]) for s in strategy_specs if s["engine_weight"] > 0}

    out_dir = run_dir / "composite"
    out_dir.mkdir(parents=True, exist_ok=True)

    all_stats: List[StatsRow] = []
    summary_rows: List[Dict] = []
    window_totals: Dict[str, Dict[str, float]] = {}

    # Group runs by window
    runs_by_window: Dict[str, List[dict]] = {}
    for r in manifest["runs"]:
        runs_by_window.setdefault(r["window"], []).append(r)

    for window_label in sorted(runs_by_window.keys()):
        window = by_label(window_label)
        print(f"\n=== Window {window.label} ({window.start} -> {window.end}) ===")
        runs = runs_by_window[window_label]

        # Load equity series per strategy
        series: Dict[str, pd.Series] = {}
        for r in runs:
            strategy = r["strategy"]
            status = r["status"]
            if status == "failure":
                print(f"  [SKIP] {strategy}: previous run FAILED ({r.get('error','')[:80]})")
                continue
            outputs = r["outputs"]
            if strategy == "TREND":
                eq = _load_trend_equity(outputs.get("results_json", ""))
            elif strategy == "XASSET":
                eq = _load_xasset_equity(outputs.get("equity_csv", ""))
            elif strategy in ("SIMPLE", "DIRECTIONAL"):
                eq = _load_equity_from_trades(outputs.get("trades_csv", ""), window.start, window.end)
            else:
                eq = None

            if eq is None or len(eq) == 0:
                print(f"  [SKIP] {strategy}: empty equity series")
                continue

            # Clip to window
            eq = eq[(eq.index >= window.start) & (eq.index <= window.end)]
            if len(eq) == 0:
                print(f"  [SKIP] {strategy}: no points inside window")
                continue
            series[strategy] = eq

        # SPY benchmark
        spy = _load_spy_equity(window.start, window.end)
        if spy is not None and len(spy) > 0:
            series["SPY"] = spy

        if not series:
            print(f"  [WARN] No data for {window.label}; skipping window")
            continue

        # Normalize each
        norm_series = {k: normalize(v) for k, v in series.items()}
        norm_frame = align_and_merge(norm_series)

        if norm_frame.empty:
            print(f"  [WARN] Empty merge for {window.label}; skipping")
            continue

        # Composite (engine weights + cash) — only if ALL engine sleeves present
        engine_present = [k for k in weights.keys() if k in norm_frame.columns]
        engine_missing = [k for k in weights.keys() if k not in norm_frame.columns]
        composite = build_composite(norm_frame, weights)
        norm_frame["COMPOSITE"] = composite

        # Stats (on normalized series: total_return, Sharpe, etc. are identical since multiplicative)
        for col in norm_frame.columns:
            stats = compute_stats(window_label, col, norm_frame[col])
            all_stats.append(stats)

        # Persist per-window equity
        window_csv = out_dir / f"composite_equity_{window_label}.csv"
        norm_frame.to_csv(window_csv, index_label="date")
        print(f"  wrote {window_csv.name}")

        # Optional plot
        plot_path = out_dir / f"plot_{window_label}.png"
        if _try_plot(window_label, norm_frame, plot_path):
            print(f"  wrote {plot_path.name}")
        else:
            print(f"  (matplotlib unavailable — skipped PNG)")

        # Summary row
        composite_stats = next(s for s in all_stats if s.window == window_label and s.strategy == "COMPOSITE")
        spy_stats = next((s for s in all_stats if s.window == window_label and s.strategy == "SPY"), None)
        summary_rows.append({
            "window": window_label,
            "description": window.description,
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
            "engine_sleeves_present": ",".join(engine_present),
            "engine_sleeves_missing": ",".join(engine_missing) if engine_missing else "-",
            "composite_return_pct": composite_stats.total_return_pct,
            "composite_cagr_pct": composite_stats.cagr_pct,
            "composite_sharpe": composite_stats.sharpe,
            "composite_max_dd_pct": composite_stats.max_drawdown_pct,
            "spy_return_pct": spy_stats.total_return_pct if spy_stats else None,
            "spy_cagr_pct": spy_stats.cagr_pct if spy_stats else None,
            "alpha_vs_spy_pct": round((composite_stats.total_return_pct - spy_stats.total_return_pct), 3) if spy_stats else None,
        })

    # Write master stats CSV
    stats_df = pd.DataFrame([s.__dict__ for s in all_stats])
    stats_csv = out_dir / "composite_stats.csv"
    stats_df.to_csv(stats_csv, index=False)
    print(f"\n[WROTE] {stats_csv}")

    # Write summary table (markdown)
    summary_md = out_dir / "summary.md"
    with open(summary_md, "w", encoding="utf-8") as f:
        f.write("# Engine Multi-Regime Backtest — Summary\n\n")
        f.write(f"Run dir: `{run_dir}`\n")
        f.write(f"Generated: {datetime.utcnow().isoformat()}Z\n\n")
        f.write("## Per-Window Composite vs SPY\n\n")
        f.write("| Window | Period | Composite Return | Composite CAGR | Sharpe | Max DD | SPY Return | Alpha | Missing Sleeves |\n")
        f.write("|---|---|---|---|---|---|---|---|---|\n")
        for row in summary_rows:
            f.write(
                f"| {row['window']} | {row['start']} → {row['end']} | "
                f"{row['composite_return_pct']:+.2f}% | {row['composite_cagr_pct']:+.2f}% | "
                f"{row['composite_sharpe']:.2f} | {row['composite_max_dd_pct']:.2f}% | "
                f"{(str(row['spy_return_pct']) + '%') if row['spy_return_pct'] is not None else '-'} | "
                f"{(('+' if row['alpha_vs_spy_pct'] and row['alpha_vs_spy_pct'] >= 0 else '') + str(row['alpha_vs_spy_pct']) + 'pp') if row['alpha_vs_spy_pct'] is not None else '-'} | "
                f"{row['engine_sleeves_missing']} |\n"
            )
        f.write("\n## Per-Strategy Per-Window Stats\n\n")
        f.write(stats_df.to_markdown(index=False) if hasattr(stats_df, "to_markdown") else stats_df.to_csv(index=False))
        f.write("\n\n---\n")
        f.write("*Composite = engine sleeve weights (TREND 0.65 + SIMPLE 0.20 + XASSET 0.12) "
                f"+ cash {CASH_WEIGHT:.2f}. Missing sleeves are parked into cash for the window.*\n")
    print(f"[WROTE] {summary_md}")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Build composite engine report from an orchestrator run_dir")
    parser.add_argument("run_dir", type=str, help="Path to a run directory containing manifest.json")
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        print(f"[ERROR] No such directory: {run_dir}")
        sys.exit(1)

    build_report(run_dir)


if __name__ == "__main__":
    main()
