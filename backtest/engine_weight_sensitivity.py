"""
Engine Weight Sensitivity
=========================

Reads composite_equity_<window>.csv files produced by engine_composite_report.py
and recomputes composite equity under alternative weight schemes WITHOUT re-running
the backtests. Writes a CSV + markdown table summarizing per-scheme per-window
total return, Sharpe, max_dd, and alpha vs SPY.

Usage:
    python backtest/engine_weight_sensitivity.py <run_dir>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252

# Weight schemes to compare — (name, TREND, SIMPLE, XASSET, cash)
SCHEMES = [
    ("baseline_65_20_12",  0.65, 0.20, 0.12, 0.03),
    ("sim_heavy_55_30_12", 0.55, 0.30, 0.12, 0.03),
    ("sim_mid_60_27_10",   0.60, 0.27, 0.10, 0.03),
    ("trend_heavy_75_10_12", 0.75, 0.10, 0.12, 0.03),
    ("xasset_heavy_60_20_17", 0.60, 0.20, 0.17, 0.03),
    ("equal_sleeves_33_33_31", 0.33, 0.33, 0.31, 0.03),
]


def stats_for(equity: pd.Series) -> dict:
    if len(equity) < 2:
        return {"total_return_pct": 0.0, "cagr_pct": 0.0, "sharpe": 0.0, "max_dd_pct": 0.0}
    start_eq = float(equity.iloc[0]); end_eq = float(equity.iloc[-1])
    total_return = end_eq / start_eq - 1.0
    days = (equity.index[-1] - equity.index[0]).days
    years = days / 365.25 if days > 0 else 1 / 252
    cagr = (end_eq / start_eq) ** (1 / years) - 1 if years > 0 else 0.0
    rets = equity.pct_change().dropna()
    sharpe = float(rets.mean() / rets.std() * np.sqrt(TRADING_DAYS_PER_YEAR)) if len(rets) and rets.std() > 0 else 0.0
    rolling_max = equity.cummax()
    dd = (rolling_max - equity) / rolling_max
    max_dd = float(dd.max()) if len(dd) else 0.0
    return {
        "total_return_pct": round(total_return * 100, 3),
        "cagr_pct": round(cagr * 100, 3),
        "sharpe": round(sharpe, 3),
        "max_dd_pct": round(max_dd * 100, 3),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=str)
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    composite_dir = run_dir / "composite"
    csvs = sorted(composite_dir.glob("composite_equity_*.csv"))
    if not csvs:
        print(f"[ERROR] No composite_equity_*.csv in {composite_dir}")
        sys.exit(1)

    rows = []
    for csv_path in csvs:
        window = csv_path.stem.replace("composite_equity_", "")
        df = pd.read_csv(csv_path, parse_dates=["date"], index_col="date")
        # Columns should include TREND, SIMPLE, XASSET, SPY (and maybe DIRECTIONAL, COMPOSITE)
        for scheme_name, w_t, w_s, w_x, w_c in SCHEMES:
            missing = [c for c in ("TREND", "SIMPLE", "XASSET") if c not in df.columns]
            synth = pd.Series(0.0, index=df.index)
            for col, w in (("TREND", w_t), ("SIMPLE", w_s), ("XASSET", w_x)):
                if col in df.columns:
                    synth = synth + w * df[col]
                else:
                    synth = synth + w * 1.0  # park missing sleeve weight in cash-equivalent
            synth = synth + w_c * 1.0
            st = stats_for(synth)
            spy_ret = stats_for(df["SPY"])["total_return_pct"] if "SPY" in df.columns else None
            rows.append({
                "window": window,
                "scheme": scheme_name,
                "weights": f"T={w_t:.2f}/S={w_s:.2f}/X={w_x:.2f}/C={w_c:.2f}",
                **st,
                "spy_return_pct": spy_ret,
                "alpha_vs_spy_pct": round(st["total_return_pct"] - spy_ret, 3) if spy_ret is not None else None,
                "missing_sleeves": ",".join(missing) if missing else "-",
            })

    out_df = pd.DataFrame(rows)
    out_csv = composite_dir / "weight_sensitivity.csv"
    out_df.to_csv(out_csv, index=False)
    print(f"[WROTE] {out_csv}")

    # Pivot: per-scheme summary (average alpha, hit rate, worst window)
    summary = []
    for scheme, g in out_df.groupby("scheme", sort=False):
        avg_ret = g["total_return_pct"].mean()
        avg_alpha = g["alpha_vs_spy_pct"].mean()
        hit_rate = (g["alpha_vs_spy_pct"] > 0).mean() * 100
        worst_ret = g["total_return_pct"].min()
        worst_window = g.loc[g["total_return_pct"].idxmin(), "window"]
        avg_maxdd = g["max_dd_pct"].mean()
        worst_maxdd = g["max_dd_pct"].max()
        avg_sharpe = g["sharpe"].mean()
        summary.append({
            "scheme": scheme,
            "weights": g["weights"].iloc[0],
            "avg_return_pct": round(avg_ret, 3),
            "avg_alpha_vs_spy_pp": round(avg_alpha, 3),
            "spy_beat_rate_pct": round(hit_rate, 1),
            "worst_window_return_pct": round(worst_ret, 3),
            "worst_window": worst_window,
            "avg_max_dd_pct": round(avg_maxdd, 3),
            "worst_max_dd_pct": round(worst_maxdd, 3),
            "avg_sharpe": round(avg_sharpe, 3),
        })
    summary_df = pd.DataFrame(summary)
    summary_csv = composite_dir / "weight_sensitivity_summary.csv"
    summary_df.to_csv(summary_csv, index=False)
    print(f"[WROTE] {summary_csv}")

    # Markdown
    md_lines = ["# Weight Sensitivity\n"]
    md_lines.append("## Summary — averaged across 6 regime windows\n")
    md_lines.append("| Scheme | Weights (T/S/X/C) | Avg Return | Avg Alpha vs SPY | Beat-SPY Rate | Worst Window | Avg MaxDD | Worst MaxDD | Avg Sharpe |")
    md_lines.append("|---|---|---|---|---|---|---|---|---|")
    for r in summary:
        md_lines.append(
            f"| {r['scheme']} | {r['weights']} | {r['avg_return_pct']:+.2f}% | "
            f"{r['avg_alpha_vs_spy_pp']:+.2f}pp | {r['spy_beat_rate_pct']:.0f}% | "
            f"{r['worst_window_return_pct']:+.2f}% ({r['worst_window']}) | "
            f"{r['avg_max_dd_pct']:.2f}% | {r['worst_max_dd_pct']:.2f}% | {r['avg_sharpe']:.2f} |"
        )
    md_lines.append("\n## Per-Window Detail\n")
    md_lines.append("| Window | Scheme | Return | Sharpe | MaxDD | SPY | Alpha |")
    md_lines.append("|---|---|---|---|---|---|---|")
    for r in rows:
        md_lines.append(
            f"| {r['window']} | {r['scheme']} | {r['total_return_pct']:+.2f}% | "
            f"{r['sharpe']:.2f} | {r['max_dd_pct']:.2f}% | "
            f"{(str(r['spy_return_pct']) + '%') if r['spy_return_pct'] is not None else '-'} | "
            f"{(('+' if r['alpha_vs_spy_pct'] and r['alpha_vs_spy_pct'] >= 0 else '') + str(r['alpha_vs_spy_pct']) + 'pp') if r['alpha_vs_spy_pct'] is not None else '-'} |"
        )
    md_out = composite_dir / "weight_sensitivity.md"
    md_out.write_text("\n".join(md_lines), encoding="utf-8")
    print(f"[WROTE] {md_out}")


if __name__ == "__main__":
    main()
