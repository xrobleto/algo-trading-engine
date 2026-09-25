"""Pre-registered tax-aware execution study -> docs/TAX_STUDY.md.

Criteria and selection rule: docs/TAX_STUDY_PREREG.md (committed before this
ran). Run from the repo root with backtest-basis symbols:
    HORIZON_SYMBOL_EQUIVALENTS=0 python -m horizon.backtest.run_tax_study
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from ..config import build_default_config
from ..data import cache
from ..strategies.pulse import PulseStrategy
from ..strategies.rotation import RotationStrategy
from .account_sim import simulate
from .tax import TaxRates, after_tax_curve, buy_and_hold_after_tax, compute_taxes

FULL = ("2008-01-02", "2026-09-04")
HALVES = [("2008-01-02", "2017-12-29"), ("2018-01-02", "2026-09-04")]
BANDS = [0.0, 0.02, 0.05, 0.10, 0.15, 0.20]
RETAIN = [None, 3]
MIN_TRADE = 0.005
PRIMARY = TaxRates(0.32, 0.15)
SENS = [TaxRates(0.24, 0.15), TaxRates(0.37, 0.20)]


def _cagr(e: pd.Series) -> float:
    y = (e.index[-1] - e.index[0]).days / 365.25
    return float((e.iloc[-1] / e.iloc[0]) ** (1 / y) - 1)


def _sharpe(e: pd.Series) -> float:
    d = e.pct_change().dropna()
    return float(d.mean() / d.std() * np.sqrt(252))


def _mdd(e: pd.Series) -> float:
    return float((e / e.cummax() - 1).min())


def run_one(ds, cfg, band, retain, window, override=0.0) -> Dict[str, float]:
    p = dict(cfg.strategy_params["PULSE"]); r = dict(cfg.strategy_params["ROTATION"])
    strats = {"PULSE": PulseStrategy(**p), "ROTATION": RotationStrategy(**r, retain_rank=retain)}
    res = simulate(ds, strats, cfg, *window, band=band,
                   min_trade_frac=MIN_TRADE if band > 0 else 0.0,
                   signal_override=override)
    out = {"pre_cagr": _cagr(res.equity), "sharpe": _sharpe(res.equity),
           "mdd": _mdd(res.equity), "turnover": res.turnover(),
           "rebal_days": res.rebalance_days / max(res.decision_days, 1)}
    for rates in [PRIMARY] + SENS:
        rep = compute_taxes(res.trades, res.dividends, res.final_marks,
                            res.equity.index[-1], rates)
        at = after_tax_curve(res.equity, rep)
        out[f"at_{rates.st:.2f}_{rates.lt:.2f}"] = _cagr(at)
        if rates is PRIMARY:
            out["st_share"] = rep.st_share
            out["wash"] = rep.wash_disallowed / float(res.equity.iloc[0])
    return out


def main() -> None:
    cfg = build_default_config()
    ds = cache.load_dataset()
    ds["QLD"] = cache.fetch_symbol("QLD")
    key = lambda r: f"at_{r.st:.2f}_{r.lt:.2f}"
    grid: Dict[Tuple[float, object], Dict[str, Dict[str, float]]] = {}
    for retain in RETAIN:
        for b in BANDS:
            grid[(b, retain)] = {"full": run_one(ds, cfg, b, retain, FULL),
                                 "h1": run_one(ds, cfg, b, retain, HALVES[0]),
                                 "h2": run_one(ds, cfg, b, retain, HALVES[1])}
            g = grid[(b, retain)]["full"]
            print(f"band {b:.2f} retain {retain}: pre {g['pre_cagr']*100:.2f}% "
                  f"after {g[key(PRIMARY)]*100:.2f}% sharpe {g['sharpe']:.2f} "
                  f"mdd {g['mdd']*100:.1f}% turnover {g['turnover']:.1f}x "
                  f"ST share {g['st_share']*100:.0f}%", flush=True)

    base = grid[(0.0, None)]
    def eligible(k) -> Tuple[bool, List[str]]:
        g = grid[k]; why = []
        if g["full"]["sharpe"] < base["full"]["sharpe"] - 0.05: why.append("Sharpe")
        if g["full"]["mdd"] < base["full"]["mdd"] - 0.03: why.append("MaxDD")
        for h in ("h1", "h2"):
            if g[h][key(PRIMARY)] <= base[h][key(PRIMARY)]: why.append(f"half {h}")
        for r in SENS:
            if g["full"][key(r)] <= base["full"][key(r)]: why.append(r.label)
        return (not why and k != (0.0, None)), why

    # Plateau selection among no-buffer bands.
    def plateau(b) -> float:
        i = BANDS.index(b)
        nb = [BANDS[j] for j in (i - 1, i, i + 1) if 0 <= j < len(BANDS)]
        return min(grid[(x, None)]["full"][key(PRIMARY)] for x in nb)
    elig_bands = [b for b in BANDS if eligible((b, None))[0]]
    chosen_band = max(elig_bands, key=plateau) if elig_bands else 0.0
    use_retain = False
    if chosen_band > 0 or elig_bands:
        k = (chosen_band, 3)
        gain = grid[k]["full"][key(PRIMARY)] - grid[(chosen_band, None)]["full"][key(PRIMARY)]
        use_retain = eligible(k)[0] and gain >= 0.002

    # Benchmarks after tax.
    qqq = ds["QQQ"].loc[FULL[0]:FULL[1]]
    bench = {r.label: _cagr(buy_and_hold_after_tax(qqq["tr_close"], qqq["close"],
                                                   qqq["dividend"], r))
             for r in [PRIMARY] + SENS}

    md = ["# Tax-aware execution study — RESULTS", "",
          "Pre-registration: [TAX_STUDY_PREREG.md](TAX_STUDY_PREREG.md) (committed before this ran). "
          "Account-level simulator, 2008-01-02 → 2026-09-04, backtest-basis symbols, "
          "taxes per `backtest/tax.py`. Fidelity: F1 and F2 passed (see the pre-registration).", "",
          f"QQQ buy-and-hold after tax: " + ", ".join(f"{k} {v*100:.1f}%" for k, v in bench.items())
          + f"; pre-tax {_cagr(qqq['tr_close'])*100:.1f}%.", "",
          "| band | hold buffer | pre-tax CAGR | Sharpe | MaxDD | turnover | ST share | "
          "after-tax 32/15 | 24/15 | 37/20 | half 1 | half 2 | eligible |",
          "|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|"]
    for (b, rt), g in grid.items():
        f = g["full"]; ok, why = eligible((b, rt))
        md.append(f"| {b:.2f} | {rt or '—'} | {f['pre_cagr']*100:.2f}% | {f['sharpe']:.2f} | "
                  f"{f['mdd']*100:.1f}% | {f['turnover']:.1f}x | {f['st_share']*100:.0f}% | "
                  f"**{f[key(PRIMARY)]*100:.2f}%** | {f[key(SENS[0])]*100:.2f}% | "
                  f"{f[key(SENS[1])]*100:.2f}% | {g['h1'][key(PRIMARY)]*100:.2f}% | "
                  f"{g['h2'][key(PRIMARY)]*100:.2f}% | "
                  f"{'baseline' if (b, rt) == (0.0, None) else ('yes' if ok else 'no: ' + ', '.join(why))} |")
    md += ["", "## Selection (pre-registered rule)", "",
           "Plateau scores (min after-tax CAGR of a band and its neighbours, no buffer): "
           + ", ".join(f"{b:.2f} → {plateau(b)*100:.2f}%" for b in BANDS) + ".", "",
           f"**Chosen: band {chosen_band:.2f}, hold buffer {'ON (retain_rank=3)' if use_retain else 'off'}.**"]
    (cache.cache_dir().parent.parent / "docs" / "TAX_STUDY.md").write_text("\n".join(md) + "\n")
    print(f"\nCHOSEN band={chosen_band} retain={'3' if use_retain else None}")
    print("QQQ after-tax:", {k: round(v * 100, 2) for k, v in bench.items()})


if __name__ == "__main__":
    main()
