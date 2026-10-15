"""Core + overlay study -> docs/CORE_OVERLAY_STUDY.md.

Design, criteria and selection rule: docs/CORE_OVERLAY_PREREG.md (committed before this ran).
    HORIZON_SYMBOL_EQUIVALENTS=0 python -m horizon.backtest.run_core_overlay_study
"""

from __future__ import annotations

import copy
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from ..config import build_default_config
from ..data import cache
from ..paths import PACKAGE_DIR
from ..strategies.pulse import PulseStrategy
from ..strategies.rotation import RotationStrategy
from .account_sim import simulate
from .run_leverage_study import _fred
from .tax import TaxRates, after_tax_curve, buy_and_hold_after_tax, compute_taxes

WIN_END = "2026-10-09"
FULL = ("2008-01-02", WIN_END)
HALVES = {"h1": ("2008-01-02", "2017-12-29"), "h2": ("2018-01-02", WIN_END)}
CRASHES = {"GFC": ("2008-01-02", "2009-06-30"), "COVID": ("2020-02-03", "2020-06-30"),
           "2022": ("2022-01-03", "2022-12-30")}
TVS = (0.22, 0.25)
CORES = (0.0, 0.25, 0.50, 0.75)
PRIMARY = TaxRates(0.32, 0.15)
SENS = [TaxRates(0.24, 0.15), TaxRates(0.37, 0.20)]
key = lambda r: f"{r.st:.2f}/{r.lt:.2f}"


def _cagr(e: pd.Series) -> float:
    y = (e.index[-1] - e.index[0]).days / 365.25
    return float((e.iloc[-1] / e.iloc[0]) ** (1 / y) - 1)


def _mdd(e: pd.Series) -> float:
    return float((e / e.cummax() - 1).min())


def run(ds, cfg, tv: float, c: float, window) -> Dict:
    strats = {"PULSE": PulseStrategy(target_vol=tv, leverage_via="levered_etf"),
              "ROTATION": RotationStrategy(lookbacks=(47, 95, 189))}
    r = simulate(ds, strats, cfg, *window, band=cfg.rebalance_band,
                 min_trade_frac=cfg.rebalance_min_trade,
                 signal_override=cfg.rebalance_signal_override,
                 core_symbol="QQQ" if c > 0 else None, core_frac=c, levered_symbol="QLD")
    e = r.equity
    d = e.pct_change().dropna()
    out = {"pre": _cagr(e), "sharpe": float(d.mean() / d.std() * np.sqrt(252)),
           "mdd": _mdd(e), "turnover": r.turnover(), "equity": e,
           "core_end": (r.core_shares * float(ds["QQQ"]["close"].loc[:e.index[-1]].iloc[-1]) / float(e.iloc[-1]))
           if c > 0 else 0.0}
    for rates in [PRIMARY] + SENS:
        rep = compute_taxes(r.trades, r.dividends, r.final_marks, e.index[-1], rates)
        out[key(rates)] = _cagr(after_tax_curve(e, rep))
        if rates is PRIMARY:
            out["st_share"] = rep.st_share
            never = copy.copy(rep)
            never.terminal = 0.0
            out["never"] = _cagr(after_tax_curve(e, never))
    return out


def dotcom(tv: float, c: float) -> float:
    """Synthetic stress (pre-registered): exposure = max(0.643 x L, core / equity),
    remainder in T-bills; the core is bought at the start of the window."""
    nd = _fred("NASDAQ100").dropna()
    tb = _fred("DTB3").reindex(nd.index).ffill() / 100.0
    r = nd.pct_change().fillna(0.0)
    rv = r.rolling(20).std(ddof=0) * np.sqrt(252)
    L = (tv / rv).clip(0.5, 2.0).shift(1).fillna(1.0)
    fin = (tb.shift(1).fillna(0.0) + 0.0095) / 252
    bill = tb.shift(1).fillna(0.0) / 252
    idx = nd.loc["1999-06-01":"2003-12-31"].index
    V, K, curve = 1.0, c, []
    for t in idx:
        l, rr = float(L.loc[t]), float(r.loc[t])
        k = K / V
        if 0.643 * l >= k:
            pulse = l * rr - (max(l - 1.0, 0.0) * float(fin.loc[t]))
            ret = 0.643 * pulse + 0.357 * float(bill.loc[t])
        else:
            ret = k * rr + (1.0 - k) * float(bill.loc[t])
        V *= 1.0 + ret
        K *= 1.0 + rr
        curve.append(V)
    return _mdd(pd.Series(curve, index=idx))


def main() -> None:
    cfg = build_default_config()
    ds = cache.load_dataset()
    R: Dict[Tuple[float, float], Dict] = {}
    for tv in TVS:
        for c in CORES:
            full = run(ds, cfg, tv, c, FULL)
            full["crash"] = {k: _mdd(full["equity"].loc[a:b]) for k, (a, b) in CRASHES.items()}
            full["dotcom"] = dotcom(tv, c)
            R[(tv, c)] = {"full": full, **{h: run(ds, cfg, tv, c, w) for h, w in HALVES.items()}}
            f = full
            print(f"tv {tv} core {c:.2f}: after-tax sold {f[key(PRIMARY)]:.2%} never {f['never']:.2%} | "
                  f"pre {f['pre']:.2%} MaxDD {f['mdd']:.1%} dot-com {f['dotcom']:.1%} | "
                  f"ST share {f['st_share']:.0%} core at end {f['core_end']:.0%}", flush=True)

    q = ds["QQQ"].loc[FULL[0]:FULL[1]]
    q_sold = {r.label: _cagr(buy_and_hold_after_tax(q["tr_close"], q["close"], q["dividend"], r))
              for r in [PRIMARY] + SENS}
    # QQQ never sold: dividend tax only (undo the terminal sale; basis = 1 per the function).
    y = (q.index[-1] - q.index[0]).days / 365.25
    sold_val = (1 + q_sold[PRIMARY.label]) ** y
    q_never = ((sold_val - 0.15) / 0.85) ** (1 / y) - 1
    q_mdd = _mdd(q["tr_close"])
    nd = _fred("NASDAQ100").dropna().loc["1999-06-01":"2003-12-31"]
    q_dot = _mdd(nd)

    def checks(tv, c):
        a, b = R[(tv, c)], R[(tv, 0.0)]
        out = {}
        gain = a["full"][key(PRIMARY)] - b["full"][key(PRIMARY)]
        out["K1 +0.50pp after tax"] = (gain >= 0.005, f"{gain*100:+.2f}pp")
        out["K2 both halves"] = (all(a[h][key(PRIMARY)] > b[h][key(PRIMARY)] for h in HALVES),
                                 "; ".join(f"{a[h][key(PRIMARY)]:.2%} vs {b[h][key(PRIMARY)]:.2%}" for h in HALVES))
        out["K3 every bracket"] = (all(a["full"][key(r)] > b["full"][key(r)] for r in SENS),
                                   "; ".join(f"{r.label} {a['full'][key(r)]:.2%} vs {b['full'][key(r)]:.2%}" for r in SENS))
        dd_ok = a["full"]["mdd"] >= b["full"]["mdd"] - 0.05 and all(
            a["full"]["crash"][k] >= b["full"]["crash"][k] - 0.05 for k in CRASHES)
        out["K4 drawdowns within 5pp"] = (dd_ok, f"MaxDD {a['full']['mdd']:.1%} vs {b['full']['mdd']:.1%}; "
                                          + ", ".join(f"{k} {a['full']['crash'][k]:.1%} vs {b['full']['crash'][k]:.1%}" for k in CRASHES))
        out["K5 dot-com >= -50%"] = (a["full"]["dotcom"] >= -0.50, f"{a['full']['dotcom']:.1%}")
        return out

    verdict = {(tv, c): checks(tv, c) for tv in TVS for c in CORES if c > 0}
    eligible = [k for k, v in verdict.items() if all(ok for ok, _ in v.values())]
    pool = [(tv, 0.0) for tv in TVS] + eligible
    best = max(pool, key=lambda k: R[k]["full"][key(PRIMARY)])
    bf = R[best]["full"]
    margin_sold = bf[key(PRIMARY)] - q_sold[PRIMARY.label]
    margin_never = bf["never"] - q_never
    earns = margin_sold >= 0.01 and margin_never >= 0.01

    md: List[str] = [
        "# Core + overlay study — RESULTS", "",
        "Pre-registration: [CORE_OVERLAY_PREREG.md](CORE_OVERLAY_PREREG.md), committed before this ran. "
        "2008-01-02 → 2026-10-09, live execution (40% band), FIFO, ST 32% / LT 15% unless noted.", "",
        "| target vol | core | after tax, sold at end | after tax, never sold | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | "
        "dot-com stress | ST share of gains | core share at end | eligible |",
        "|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|"]
    for (tv, c), rr in R.items():
        f = rr["full"]
        elig = "reference" if c == 0 else ("yes" if (tv, c) in eligible else "no")
        md.append(f"| {tv} | {c:.0%} | **{f[key(PRIMARY)]:.2%}** | {f['never']:.2%} | {f['pre']:.2%} | {f['sharpe']:.2f} | "
                  f"{f['mdd']:.1%} | " + " | ".join(f"{f['crash'][k]:.1%}" for k in CRASHES)
                  + f" | {f['dotcom']:.1%} | {f['st_share']:.0%} | {f['core_end']:.0%} | {elig} |")
    md.append(f"| QQQ buy-and-hold | 100% | **{q_sold[PRIMARY.label]:.2%}** | {q_never:.2%} | {_cagr(q['tr_close']):.2%} | — | "
              f"{q_mdd:.1%} | — | — | — | {q_dot:.1%} | — | — | — |")
    md += ["", "## Criteria (each core variant vs the plain engine at the same target vol)", ""]
    for k, v in verdict.items():
        md.append(f"**target vol {k[0]}, core {k[1]:.0%}** — {'ELIGIBLE' if k in eligible else 'not eligible'}")
        md += [f"- {'PASS' if ok else 'FAIL'} — {n}: {d}" for n, (ok, d) in v.items()] + [""]
    md += ["## Verdict", "",
           f"Best configuration: target vol {best[0]}, core {best[1]:.0%}: {bf[key(PRIMARY)]:.2%} after tax sold "
           f"at the end, {bf['never']:.2%} never sold.",
           f"Against QQQ buy-and-hold ({q_sold[PRIMARY.label]:.2%} sold at the end, {q_never:.2%} never sold): "
           f"{margin_sold*100:+.2f}pp and {margin_never*100:+.2f}pp.", "",
           ("**The engine earns its keep in the taxable account** (≥ +1.0pp a year under both conventions)."
            if earns else
            "**No configuration beats QQQ by a meaningful margin after tax** (the pre-registered bar is +1.0pp a "
            "year under both conventions). The choice comes down to whether the engine's lower drawdowns are worth it.")]
    (PACKAGE_DIR / "docs" / "CORE_OVERLAY_STUDY.md").write_text("\n".join(md) + "\n")
    print("\nELIGIBLE:", eligible, "| BEST:", best, f"| vs QQQ sold {margin_sold*100:+.2f}pp never {margin_never*100:+.2f}pp | earns keep: {earns}")
    for k, v in verdict.items():
        print(f"  {k}: " + ", ".join(f"{n.split()[0]}={'ok' if ok else 'FAIL'}" for n, (ok, _) in v.items()))
    print(f"QQQ: sold {q_sold[PRIMARY.label]:.2%} never {q_never:.2%} MaxDD {q_mdd:.1%} dot-com {q_dot:.1%}")


if __name__ == "__main__":
    main()
