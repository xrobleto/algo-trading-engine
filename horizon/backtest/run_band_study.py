"""Addendum B of the pre-registered tax study: bands wider than 0.20.

Criteria and selection rule: docs/TAX_STUDY_PREREG.md, addendum B (committed
before this ran). Appends results to docs/TAX_STUDY.md.
    HORIZON_SYMBOL_EQUIVALENTS=0 python -m horizon.backtest.run_band_study
"""

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from ..config import build_default_config
from ..data import cache
from ..paths import PACKAGE_DIR
from ..strategies.registry import build
from .account_sim import simulate
from .tax import TaxRates, after_tax_curve, compute_taxes

FULL = ("2008-01-02", "2026-09-04")
HALVES = [("2008-01-02", "2017-12-29"), ("2018-01-02", "2026-09-04")]
CRASHES = {"GFC": ("2008-01-02", "2009-06-30"), "COVID": ("2020-02-03", "2020-06-30"),
           "2022": ("2022-01-03", "2022-12-30")}
BASE_BAND = 0.20
BANDS = [0.20, 0.25, 0.30, 0.35, 0.40]
MIN_TRADE, OVERRIDE = 0.005, 0.10
PRIMARY = TaxRates(0.32, 0.15)
SENS = [TaxRates(0.24, 0.15), TaxRates(0.37, 0.20)]
key = lambda r: f"{r.st:.2f}/{r.lt:.2f}"


def _cagr(e):
    y = (e.index[-1] - e.index[0]).days / 365.25
    return float((e.iloc[-1] / e.iloc[0]) ** (1 / y) - 1)


def _mdd(e):
    return float((e / e.cummax() - 1).min())


def run(ds, cfg, band, window, want_deferrals=False) -> Dict:
    deferred = []

    def obs(d, target, mv, eq, hit):
        if not hit:
            for s, v in mv.items():
                if target.get(s, 0.0) <= 0 and v >= OVERRIDE * eq:
                    deferred.append((d, s))

    res = simulate(ds, {s: build(s) for s in ("PULSE", "ROTATION")}, cfg, *window,
                   band=band, min_trade_frac=MIN_TRADE, signal_override=OVERRIDE,
                   observer=obs if want_deferrals else None)
    e = res.equity; d = e.pct_change().dropna()
    out = {"pre": _cagr(e), "sharpe": float(d.mean() / d.std() * np.sqrt(252)),
           "mdd": _mdd(e), "turnover": res.turnover(), "equity": e,
           "rebal": res.rebalance_days / max(res.decision_days, 1), "deferred": deferred}
    for r in [PRIMARY] + SENS:
        rep = compute_taxes(res.trades, res.dividends, res.final_marks, e.index[-1], r)
        out[key(r)] = _cagr(after_tax_curve(e, rep))
        if r is PRIMARY:
            out["st_share"] = rep.st_share
    return out


def main() -> None:
    cfg = build_default_config()
    ds = cache.load_dataset(); ds["QLD"] = cache.fetch_symbol("QLD")
    R = {}
    for b in BANDS:
        full = run(ds, cfg, b, FULL, want_deferrals=True)
        full["crash"] = {k: _mdd(full["equity"].loc[a:z]) for k, (a, z) in CRASHES.items()}
        R[b] = {"full": full, "h1": run(ds, cfg, b, HALVES[0]), "h2": run(ds, cfg, b, HALVES[1])}
        f = full
        print(f"band {b:.2f}: pre {f['pre']*100:.2f}% after {f[key(PRIMARY)]*100:.2f}% "
              f"Sharpe {f['sharpe']:.2f} MaxDD {f['mdd']*100:.1f}% crash "
              + " ".join(f"{k} {v*100:.1f}%" for k, v in f["crash"].items())
              + f" turnover {f['turnover']:.1f}x rebal {f['rebal']*100:.0f}% deferred {len(f['deferred'])}",
              flush=True)
    base = R[BASE_BAND]

    def checks(b):
        g, bf = R[b], base["full"]; why = []
        if g["full"]["sharpe"] < bf["sharpe"] - 0.05: why.append("Sharpe")
        if g["full"]["mdd"] < bf["mdd"] - 0.03: why.append("MaxDD")
        for k in CRASHES:
            if g["full"]["crash"][k] < bf["crash"][k] - 0.03: why.append(f"crash {k}")
        for h in ("h1", "h2"):
            if g[h][key(PRIMARY)] <= base[h][key(PRIMARY)]: why.append(f"half {h}")
        for r in SENS:
            if g["full"][key(r)] <= bf[key(r)]: why.append(key(r))
        if g["full"]["deferred"]: why.append("deferred exits")
        return why

    def plateau(b):
        i = BANDS.index(b)
        nb = [BANDS[j] for j in (i - 1, i, i + 1) if 0 <= j < len(BANDS)]
        return min(R[x]["full"][key(PRIMARY)] for x in nb)

    elig = [b for b in BANDS[1:] if not checks(b)]
    best = max(elig, key=plateau) if elig else None
    gain = (R[best]["full"][key(PRIMARY)] - base["full"][key(PRIMARY)]) if best else 0.0
    chosen = best if (best is not None and gain >= 0.002) else BASE_BAND

    md = ["", "## Addendum B — wider bands (pre-registered before any result)", "",
          "Baseline = current live (band 0.20, override 0.10). Crash-window drawdowns are measured on "
          "the full-period run.", "",
          "| band | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | turnover | rebalance days | "
          "ST share | after-tax 32/15 | 24/15 | 37/20 | half 1 | half 2 | plateau | eligible |",
          "|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|"]
    for b in BANDS:
        f = R[b]["full"]; why = checks(b)
        md.append(f"| {b:.2f} | {f['pre']*100:.2f}% | {f['sharpe']:.2f} | {f['mdd']*100:.1f}% | "
                  + " | ".join(f"{f['crash'][k]*100:.1f}%" for k in CRASHES)
                  + f" | {f['turnover']:.1f}x | {f['rebal']*100:.0f}% | {f['st_share']*100:.0f}% | "
                  f"**{f[key(PRIMARY)]*100:.2f}%** | {f[key(SENS[0])]*100:.2f}% | {f[key(SENS[1])]*100:.2f}% | "
                  f"{R[b]['h1'][key(PRIMARY)]*100:.2f}% | {R[b]['h2'][key(PRIMARY)]*100:.2f}% | "
                  f"{plateau(b)*100:.2f}% | "
                  f"{'baseline (live)' if b == BASE_BAND else ('yes' if not why else 'no: ' + ', '.join(why))} |")
    md += ["", f"**Result: {'switch live to band ' + format(chosen, '.2f') if chosen != BASE_BAND else 'keep live at band 0.20'}** "
           f"(best eligible: {best if best is not None else 'none'}; after-tax gain vs live "
           f"{gain*100:+.2f}pp; switch threshold +0.20pp)."]
    with open(PACKAGE_DIR / "docs" / "TAX_STUDY.md", "a") as fh:
        fh.write("\n".join(md) + "\n")
    print(f"\nCHOSEN band={chosen:.2f} (best eligible {best}, gain {gain*100:+.2f}pp)")
    for b in BANDS[1:]:
        print(f"  {b:.2f}: {'eligible' if not checks(b) else 'fails ' + ', '.join(checks(b))}")


if __name__ == "__main__":
    main()
