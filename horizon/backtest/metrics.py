"""Honest performance metrics.

Every metric is computed from the realized daily equity curve. No stubs, no
placeholders. When a benchmark is supplied, the headline number is
`excess_cagr` — CAGR minus the benchmark's CAGR over the identical window.
"""

from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

TRADING_DAYS = 252


def _max_drawdown(equity: pd.Series):
    cummax = equity.cummax()
    dd = equity / cummax - 1.0
    max_dd = float(dd.min())
    underwater = dd < -1e-9
    longest = cur = 0
    for flag in underwater:
        cur = cur + 1 if flag else 0
        longest = max(longest, cur)
    return max_dd, longest


def compute_metrics(equity: pd.Series, benchmark: Optional[pd.Series] = None,
                    risk_free_annual: float = 0.04) -> Dict[str, float]:
    equity = equity.dropna()
    if len(equity) < 3:
        return {"n_days": len(equity), "error": "insufficient data"}

    rets = equity.pct_change().dropna()
    n_days = len(equity)
    years = n_days / TRADING_DAYS
    total_return = float(equity.iloc[-1] / equity.iloc[0] - 1.0)
    cagr = float((equity.iloc[-1] / equity.iloc[0]) ** (1.0 / years) - 1.0)
    vol = float(rets.std(ddof=0) * np.sqrt(TRADING_DAYS))

    rf_daily = risk_free_annual / TRADING_DAYS
    excess = rets - rf_daily
    sharpe = (float(excess.mean() / excess.std(ddof=0) * np.sqrt(TRADING_DAYS))
              if excess.std(ddof=0) > 0 else 0.0)
    downside = rets[rets < 0]
    sortino = (float((rets.mean() - rf_daily) * TRADING_DAYS
                     / (downside.std(ddof=0) * np.sqrt(TRADING_DAYS)))
               if len(downside) > 1 and downside.std(ddof=0) > 0 else 0.0)

    max_dd, dd_days = _max_drawdown(equity)
    calmar = float(cagr / abs(max_dd)) if max_dd < -1e-9 else 0.0

    gains = rets[rets > 0].sum()
    losses = -rets[rets < 0].sum()
    profit_factor = float(gains / losses) if losses > 0 else float("inf")

    out = {
        "cagr": cagr,
        "total_return": total_return,
        "volatility": vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_dd,
        "max_dd_days": int(dd_days),
        "calmar": calmar,
        "pct_positive_days": float((rets > 0).mean()),
        "profit_factor": profit_factor,
        "n_days": n_days,
        "years": round(years, 2),
        "final_equity": float(equity.iloc[-1]),
    }

    if benchmark is not None:
        bench = benchmark.dropna()
        common = equity.index.intersection(bench.index)
        if len(common) > 3:
            b = bench.loc[common]
            b_years = len(b) / TRADING_DAYS
            b_cagr = float((b.iloc[-1] / b.iloc[0]) ** (1.0 / b_years) - 1.0)
            b_rets = b.pct_change().dropna()
            e_rets = equity.loc[common].pct_change().dropna()
            j = e_rets.index.intersection(b_rets.index)
            corr = float(e_rets.loc[j].corr(b_rets.loc[j])) if len(j) > 3 else 0.0
            var_b = float(b_rets.loc[j].var(ddof=0))
            beta = (float(np.cov(e_rets.loc[j], b_rets.loc[j])[0, 1] / var_b)
                    if var_b > 0 else 0.0)
            out.update({
                "benchmark_cagr": b_cagr,
                "excess_cagr": cagr - b_cagr,     # the headline "beat QQQ by" figure
                "correlation_to_benchmark": corr,
                "beta_to_benchmark": beta,
            })
    return out


def format_metrics(m: Dict[str, float]) -> str:
    """One-line human-readable summary."""
    if "cagr" not in m:
        return str(m)
    s = (f"CAGR {m['cagr']*100:6.2f}%  Sharpe {m['sharpe']:5.2f}  "
         f"MaxDD {m['max_drawdown']*100:6.1f}%  Calmar {m['calmar']:4.2f}")
    if "excess_cagr" in m:
        s += (f"  vs QQQ {m['benchmark_cagr']*100:5.1f}%  "
              f"excess {m['excess_cagr']*100:+5.1f}pp  "
              f"corr {m['correlation_to_benchmark']:+.2f}")
    return s
