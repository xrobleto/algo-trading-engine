"""PULSE volatility-target study -> docs/LEVERAGE_STUDY.md.

Criteria and selection rule: docs/LEVERAGE_STUDY_PREREG.md (committed before this ran).
Reuses the original gating code (run_validation), the account simulator and the tax model
unchanged. Run from the repo root with backtest-basis symbols:
    HORIZON_SYMBOL_EQUIVALENTS=0 python -m horizon.backtest.run_leverage_study
"""

from __future__ import annotations

import io
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import requests

from ..config import build_default_config
from ..data import cache
from ..paths import PACKAGE_DIR, cache_dir
from ..strategies.pulse import PulseStrategy
from ..strategies.rotation import RotationStrategy
from . import run_validation as V
from .account_sim import simulate
from .harness import benchmark_curve, run_strategy
from .metrics import compute_metrics
from .tax import TaxRates, after_tax_curve, buy_and_hold_after_tax, compute_taxes
from .walkforward import run_walkforward

WIN_END = "2026-10-09"
FULL = ("2008-01-02", WIN_END)
HALVES = [("2008-01-02", "2017-12-29"), ("2018-01-02", WIN_END)]
CRASHES = {"GFC": ("2008-01-02", "2009-06-30"), "COVID": ("2020-02-03", "2020-06-30"),
           "2022": ("2022-01-03", "2022-12-30")}
BASE_TV, CANDIDATES = 0.22, (0.25, 0.275)
ROT = {"lookbacks": (47, 95, 189)}
PRIMARY = TaxRates(0.32, 0.15)
SENS = [TaxRates(0.24, 0.15), TaxRates(0.37, 0.20)]
key = lambda r: f"{r.st:.2f}/{r.lt:.2f}"


def _cagr(e: pd.Series) -> float:
    y = (e.index[-1] - e.index[0]).days / 365.25
    return float((e.iloc[-1] / e.iloc[0]) ** (1 / y) - 1)


def _mdd(e: pd.Series) -> float:
    return float((e / e.cummax() - 1).min())


# --- L1: the original per-strategy gating bar ---------------------------------

def gate(ds, tv: float) -> Dict:
    V.PARAMS.clear()
    V.PARAMS.update({"PULSE": {"target_vol": tv, "leverage_via": "levered_etf"},
                     "ROTATION": dict(ROT), "REVERT": {}, "DRIFT": {}})
    V.OOS = ("2018-01-02", WIN_END)          # A6 window, aligned with this study
    strategies = {s: V._build(s) for s in ("PULSE", "ROTATION")}
    results = {s: run_strategy(st, ds, *FULL, V.EQ) for s, st in strategies.items()}
    bench = benchmark_curve(ds, "QQQ", *FULL)
    metrics = {s: compute_metrics(r.equity, bench) for s, r in results.items()}
    qqq_cagr = compute_metrics(bench, bench)["cagr"]
    tbill_cagr = compute_metrics(benchmark_curve(ds, "BIL", *FULL))["cagr"]
    walk = {s: run_walkforward(st, ds) for s, st in strategies.items()}
    admitted, report = V._gate(metrics, V._corr_matrix(results), qqq_cagr, tbill_cagr, walk)
    a6 = {s: V._a6(s, ds) for s in admitted}
    final = [s for s in admitted if a6[s][0]]
    return {"metrics": metrics, "report": report, "a6": a6, "admitted": final,
            "walk": walk, "pass": set(final) == {"PULSE", "ROTATION"}}


# --- L2-L7: account simulator, after tax --------------------------------------

def account(ds, cfg, tv: float, window) -> Dict:
    strats = {"PULSE": PulseStrategy(target_vol=tv, leverage_via="levered_etf"),
              "ROTATION": RotationStrategy(**ROT)}
    r = simulate(ds, strats, cfg, *window, band=cfg.rebalance_band,
                 min_trade_frac=cfg.rebalance_min_trade,
                 signal_override=cfg.rebalance_signal_override)
    e = r.equity
    d = e.pct_change().dropna()
    out = {"pre": _cagr(e), "sharpe": float(d.mean() / d.std() * np.sqrt(252)),
           "mdd": _mdd(e), "turnover": r.turnover(), "equity": e}
    for rates in [PRIMARY] + SENS:
        rep = compute_taxes(r.trades, r.dividends, r.final_marks, e.index[-1], rates)
        out[key(rates)] = _cagr(after_tax_curve(e, rep))
        if rates is PRIMARY:
            out["st_share"] = rep.st_share
    return out


# --- L8: dot-com stress (model fixed in the pre-registration) -----------------

def _fred(series_id: str) -> pd.Series:
    path = cache_dir() / f"fred_{series_id}.csv"
    if not path.exists():
        resp = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv",
                            params={"id": series_id}, timeout=60)
        resp.raise_for_status()
        path.write_text(resp.text)
    return pd.read_csv(path, na_values=".", parse_dates=[0], index_col=0).iloc[:, 0]


def dotcom(tv: float) -> Tuple[float, float]:
    nd = _fred("NASDAQ100").dropna()
    tb = _fred("DTB3").reindex(nd.index).ffill() / 100.0
    r = nd.pct_change().fillna(0.0)
    rv = r.rolling(20).std(ddof=0) * np.sqrt(252)
    L = (tv / rv).clip(0.5, 2.0).shift(1).fillna(1.0)
    fin = (tb.shift(1).fillna(0.0) + 0.0095) / 252
    pulse_ret = np.where(L <= 1.0, L * r, L * r - (L - 1.0) * fin)
    tbill_ret = tb.shift(1).fillna(0.0) / 252
    book = pd.Series((1 + 0.643 * pulse_ret + 0.357 * tbill_ret).cumprod(), index=nd.index)
    s = book.loc["1999-06-01":"2003-12-31"]
    return _mdd(s), _cagr(book.loc["1990-01-02":WIN_END])


def main() -> None:
    cfg = build_default_config()
    ds = cache.load_dataset()
    tvs = (BASE_TV,) + CANDIDATES
    G, A, D = {}, {}, {}
    for tv in tvs:
        G[tv] = gate(ds, tv)
        A[tv] = {"full": account(ds, cfg, tv, FULL), "h1": account(ds, cfg, tv, HALVES[0]),
                 "h2": account(ds, cfg, tv, HALVES[1])}
        A[tv]["full"]["crash"] = {k: _mdd(A[tv]["full"]["equity"].loc[a:b])
                                  for k, (a, b) in CRASHES.items()}
        D[tv] = dotcom(tv)
        f, pm = A[tv]["full"], G[tv]["metrics"]["PULSE"]
        print(f"tv {tv}: gate {'PASS' if G[tv]['pass'] else 'FAIL'} (PULSE MaxDD {pm['max_drawdown']:.1%}) | "
              f"after-tax {f[key(PRIMARY)]:.2%} pre {f['pre']:.2%} Sharpe {f['sharpe']:.2f} "
              f"MaxDD {f['mdd']:.1%} | dot-com {D[tv][0]:.1%}", flush=True)

    base = A[BASE_TV]
    def checks(tv) -> Dict[str, Tuple[bool, str]]:
        g, a, b = G[tv], A[tv], base
        pm = g["metrics"]["PULSE"]
        c = {}
        c["L1 gating bar"] = (g["pass"], f"admitted {g['admitted']}; PULSE MaxDD {pm['max_drawdown']:.1%} "
                                         f"(limit -45%), Sharpe {pm['sharpe']:.2f}")
        gain = a["full"][key(PRIMARY)] - b["full"][key(PRIMARY)]
        c["L2 gain >= 0.50pp"] = (gain >= 0.005, f"{gain*100:+.2f}pp after tax")
        c["L3 both halves"] = (all(a[h][key(PRIMARY)] > b[h][key(PRIMARY)] for h in ("h1", "h2")),
                               f"{a['h1'][key(PRIMARY)]:.2%} vs {b['h1'][key(PRIMARY)]:.2%}; "
                               f"{a['h2'][key(PRIMARY)]:.2%} vs {b['h2'][key(PRIMARY)]:.2%}")
        c["L4 every bracket"] = (all(a["full"][key(r)] > b["full"][key(r)] for r in SENS),
                                 "; ".join(f"{r.label}: {a['full'][key(r)]:.2%} vs {b['full'][key(r)]:.2%}"
                                           for r in SENS))
        c["L5 Sharpe"] = (a["full"]["sharpe"] >= b["full"]["sharpe"] - 0.10,
                          f"{a['full']['sharpe']:.2f} vs {b['full']['sharpe']:.2f}")
        c["L6 MaxDD >= -35%"] = (a["full"]["mdd"] >= -0.35, f"{a['full']['mdd']:.1%}")
        c["L7 crash windows >= -35%"] = (all(v >= -0.35 for v in a["full"]["crash"].values()),
                                         ", ".join(f"{k} {v:.1%}" for k, v in a["full"]["crash"].items()))
        c["L8 dot-com >= -50%"] = (D[tv][0] >= -0.50, f"{D[tv][0]:.1%}")
        return c

    verdict = {tv: checks(tv) for tv in CANDIDATES}
    passing = [tv for tv in CANDIDATES if all(ok for ok, _ in verdict[tv].values())]
    chosen = max(passing, key=lambda tv: A[tv]["full"][key(PRIMARY)]) if passing else None

    qqq = ds["QQQ"].loc[FULL[0]:FULL[1]]
    qqq_at = {r.label: _cagr(buy_and_hold_after_tax(qqq["tr_close"], qqq["close"], qqq["dividend"], r))
              for r in [PRIMARY] + SENS}

    md: List[str] = [
        "# PULSE volatility-target study — RESULTS", "",
        "Pre-registration: [LEVERAGE_STUDY_PREREG.md](LEVERAGE_STUDY_PREREG.md), committed before "
        "this ran. Window 2008-01-02 → 2026-10-09. Account figures use the live execution "
        "(40% band, signal override 10%) and the tax model of the tax study.", "",
        "## Account results", "",
        "| target vol | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | dot-com stress | turnover | "
        "ST share | after-tax 32/15 | 24/15 | 37/20 | half 1 | half 2 |",
        "|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|"]
    for tv in tvs:
        f = A[tv]["full"]
        md.append(f"| {tv}{' (live)' if tv == BASE_TV else ''} | {f['pre']:.2%} | {f['sharpe']:.2f} | "
                  f"{f['mdd']:.1%} | " + " | ".join(f"{v:.1%}" for v in f["crash"].values())
                  + f" | {D[tv][0]:.1%} | {f['turnover']:.1f}x | {f['st_share']:.0%} | "
                  f"**{f[key(PRIMARY)]:.2%}** | {f[key(SENS[0])]:.2%} | {f[key(SENS[1])]:.2%} | "
                  f"{A[tv]['h1'][key(PRIMARY)]:.2%} | {A[tv]['h2'][key(PRIMARY)]:.2%} |")
    md += ["", "QQQ buy-and-hold after tax: " + ", ".join(f"{k} {v:.2%}" for k, v in qqq_at.items())
           + f"; pre-tax {_cagr(qqq['tr_close']):.2%}.", "",
           "Dot-com stress is the synthetic model fixed in the pre-registration (price index, no "
           "dividends, ROTATION as T-bills); 1990–2026 synthetic book CAGR: "
           + ", ".join(f"{tv} {D[tv][1]:.1%}" for tv in tvs) + ".", "",
           "## Original gating bar (PULSE standalone, full window)", "",
           "| target vol | CAGR | Sharpe | MaxDD (limit −45%) | worst regime-window DD | A6 robustness | admitted |",
           "|---|--:|--:|--:|--:|---|---|"]
    for tv in tvs:
        pm = G[tv]["metrics"]["PULSE"]
        wdd = min(m.get("max_drawdown", 0.0) for _, m in G[tv]["walk"]["PULSE"])
        a6 = G[tv]["a6"].get("PULSE")
        md.append(f"| {tv} | {pm['cagr']:.1%} | {pm['sharpe']:.2f} | {pm['max_drawdown']:.1%} | {wdd:.1%} | "
                  f"{'PASS' if a6 and a6[0] else ('FAIL' if a6 else 'not reached')}"
                  f"{(': ' + a6[1]) if a6 else ''} | {'yes' if G[tv]['pass'] else 'no'} |")
    md += ["", "## Criteria", ""]
    for tv in CANDIDATES:
        md.append(f"**target_vol {tv}** — {'PASSES ALL' if tv in passing else 'FAILS'}")
        md.append("")
        for name, (ok, detail) in verdict[tv].items():
            md.append(f"- {'PASS' if ok else 'FAIL'} — {name}: {detail}")
        md.append("")
    md.append(f"**Selection: {('target_vol ' + str(chosen)) if chosen else 'none qualifies; live stays at 0.22'}.** "
              "Per the pre-registration this is NOT deployed automatically; the user decides.")
    (PACKAGE_DIR / "docs" / "LEVERAGE_STUDY.md").write_text("\n".join(md) + "\n")
    print("\nCHOSEN:", chosen)
    for tv in CANDIDATES:
        print(f"  {tv}: " + ", ".join(f"{n.split()[0]}={'ok' if ok else 'FAIL'}" for n, (ok, _) in verdict[tv].items()))
    print("QQQ after tax:", {k: f"{v:.2%}" for k, v in qqq_at.items()})


if __name__ == "__main__":
    main()
