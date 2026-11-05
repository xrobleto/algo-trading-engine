"""PULSE recovery-mode study -> docs/RECOVERY_STUDY.md.

Criteria and selection rule: docs/RECOVERY_STUDY_PREREG.md (written before this ran; SHA-256
recorded in the session). Reuses the original gating code (run_validation), the account
simulator and the tax model unchanged, and the leverage study's index model (L8) extended to
recovery mode. Run from the repo root with backtest-basis symbols:
    HORIZON_SYMBOL_EQUIVALENTS=0 python -m horizon.backtest.run_recovery_study
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ..config import build_default_config
from ..data import cache
from ..paths import PACKAGE_DIR
from ..strategies.pulse import MODE_RECOVERY, PulseStrategy, recovery_modes
from ..strategies.rotation import RotationStrategy
from . import run_validation as V
from .account_sim import simulate
from .harness import benchmark_curve, run_strategy
from .metrics import compute_metrics
from .run_leverage_study import _cagr, _fred, _mdd
from .tax import TaxRates, after_tax_curve, buy_and_hold_after_tax, compute_taxes
from .walkforward import run_walkforward

WIN_END = "2026-11-04"
FULL = ("2008-01-02", WIN_END)
HALVES = [("2008-01-02", "2017-12-29"), ("2018-01-02", WIN_END)]
CRASHES = {"GFC": ("2008-01-02", "2009-06-30"), "COVID": ("2020-02-03", "2020-06-30"),
           "2022": ("2022-01-03", "2022-12-30")}
BASE = {"target_vol": 0.25, "leverage_via": "levered_etf"}
ROT = {"lookbacks": (47, 95, 189)}
ARM, SMA = 0.10, 50
CANDIDATES = (2.0, 1.5)
GRID_ARM, GRID_SMA = (0.075, 0.10, 0.15), (20, 50, 100)
PRIMARY = TaxRates(0.32, 0.15)
SENS = [TaxRates(0.24, 0.15), TaxRates(0.37, 0.20)]
key = lambda r: f"{r.st:.2f}/{r.lt:.2f}"
PULSE_W, CASH_W, SPREAD = 0.643, 0.357, 0.0095


def pulse_params(l_rec: Optional[float], arm: float = ARM, sma: int = SMA) -> dict:
    p = dict(BASE)
    if l_rec:
        p.update({"recovery_dd": arm, "recovery_sma": sma, "recovery_leverage": l_rec})
    return p


# --- R1: the original per-strategy gating bar ---------------------------------

def gate(ds, params: dict) -> Dict:
    V.PARAMS.clear()
    V.PARAMS.update({"PULSE": dict(params), "ROTATION": dict(ROT), "REVERT": {}, "DRIFT": {}})
    V.OOS = ("2018-01-02", WIN_END)
    strategies = {s: V._build(s) for s in ("PULSE", "ROTATION")}
    results = {s: run_strategy(st, ds, *FULL, V.EQ) for s, st in strategies.items()}
    bench = benchmark_curve(ds, "QQQ", *FULL)
    metrics = {s: compute_metrics(r.equity, bench) for s, r in results.items()}
    qqq_cagr = compute_metrics(bench, bench)["cagr"]
    tbill_cagr = compute_metrics(benchmark_curve(ds, "BIL", *FULL))["cagr"]
    walk = {s: run_walkforward(V._build(s), ds) for s in strategies}
    admitted, report = V._gate(metrics, V._corr_matrix(results), qqq_cagr, tbill_cagr, walk)
    a6 = {s: V._a6(s, ds) for s in admitted}
    final = [s for s in admitted if a6[s][0]]
    return {"metrics": metrics, "report": report, "a6": a6, "admitted": final,
            "walk": walk, "pass": set(final) == {"PULSE", "ROTATION"}}


# --- R2-R6, R10: account simulator, after tax ---------------------------------

def account(ds, cfg, params: dict, window, observer=None) -> Dict:
    strats = {"PULSE": PulseStrategy(**params), "ROTATION": RotationStrategy(**ROT)}
    r = simulate(ds, strats, cfg, *window, band=cfg.rebalance_band,
                 min_trade_frac=cfg.rebalance_min_trade,
                 signal_override=cfg.rebalance_signal_override, observer=observer)
    e = r.equity
    d = e.pct_change().dropna()
    out = {"pre": _cagr(e), "sharpe": float(d.mean() / d.std() * np.sqrt(252)),
           "mdd": _mdd(e), "turnover": r.turnover(), "equity": e,
           "rebal_days": r.rebalance_days}
    for rates in [PRIMARY] + SENS:
        rep = compute_taxes(r.trades, r.dividends, r.final_marks, e.index[-1], rates)
        out[key(rates)] = _cagr(after_tax_curve(e, rep))
        if rates is PRIMARY:
            out["st_share"] = rep.st_share
    return out


# --- R7-R9: index model (leverage study L8, extended) -------------------------

def _rate(series_id: str, index: pd.DatetimeIndex) -> pd.Series:
    s = _fred(series_id).dropna() / 100.0
    return s.reindex(s.index.union(index)).ffill().reindex(index).fillna(0.0)


def index_book(px: pd.Series, rate: pd.Series, l_rec: Optional[float] = None,
               arm: float = ARM, sma: int = SMA, tv: float = 0.25) -> pd.Series:
    """64.3% PULSE + 35.7% cash on a price index; leverage set at close T, held T+1."""
    px = px.dropna()
    rate = rate.reindex(px.index).ffill().fillna(0.0)
    r = px.pct_change().fillna(0.0)
    rv = r.rolling(20).std(ddof=0) * np.sqrt(252)
    lev = (tv / rv).clip(0.5, 2.0)
    if l_rec:
        modes = recovery_modes(px, arm, sma)
        lev = lev.where(modes != MODE_RECOVERY, np.maximum(lev.fillna(0.0), l_rec))
    lev = lev.shift(1).fillna(1.0)
    fin = (rate.shift(1).fillna(0.0) + SPREAD) / 252
    pulse = np.where(lev <= 1.0, lev * r, lev * r - (lev - 1.0) * fin)
    cash = rate.shift(1).fillna(0.0) / 252
    return pd.Series((1 + PULSE_W * pulse + CASH_W * cash).cumprod(), index=px.index)


def index_tests(l_rec: Optional[float]) -> Dict[str, float]:
    ndx = _fred("NASDAQ100").dropna()
    comp = _fred("NASDAQCOM").dropna()
    nik = _fred("NIKKEI225").dropna()
    b_ndx = index_book(ndx, _rate("DTB3", ndx.index), l_rec)
    b_comp = index_book(comp, _rate("DTB3", comp.index), l_rec)
    b_nik = index_book(nik, _rate("IRSTCI01JPM156N", nik.index), l_rec)
    nik_w = b_nik.loc["1989-12-29":"2012-12-28"]
    return {"dotcom_mdd": _mdd(b_ndx.loc["1999-06-01":"2003-12-31"]),
            "ndx_cagr": _cagr(b_ndx.loc["1990-01-02":]),
            "comp_cagr": _cagr(b_comp), "comp_mdd": _mdd(b_comp),
            "nik_cagr": _cagr(nik_w), "nik_mdd": _mdd(nik_w)}


# --- information only ---------------------------------------------------------

def episodes(ds) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """QQQ drawdowns of 10%+ (peak, trough), total-return closes, full window."""
    s = ds["QQQ"]["tr_close"].loc[FULL[0]:FULL[1]]
    out, peak, pdate, tdate, tval = [], s.iloc[0], s.index[0], None, None
    for d, v in s.items():
        if v >= peak:
            if tval is not None and tval / peak - 1 <= -0.10:
                out.append((pdate, tdate))
            peak, pdate, tdate, tval = v, d, None, None
        elif tval is None or v < tval:
            tdate, tval = d, v
    if tval is not None and tval / peak - 1 <= -0.10:
        out.append((pdate, tdate))
    return out


def capture(eq: pd.Series, troughs, n: int) -> float:
    vals = []
    for t in troughs:
        i = eq.index.get_indexer([t])[0]
        if i < 0 or i + n >= len(eq):
            continue
        vals.append(float(eq.iloc[i + n] / eq.iloc[i] - 1))
    return float(np.mean(vals)) if vals else float("nan")


def rec_episodes(px: pd.Series) -> Dict:
    modes = recovery_modes(px, ARM, SMA)
    out, start = [], None
    for i in range(1, len(modes)):
        if modes[i] == MODE_RECOVERY and modes[i - 1] != MODE_RECOVERY:
            start = i
        elif start is not None and modes[i] != MODE_RECOVERY:
            out.append({"start": px.index[start], "end": px.index[i],
                        "ret": float(px.iloc[i] / px.iloc[start] - 1),
                        "success": bool(modes[i] == 0)})
            start = None
    if start is not None:
        out.append({"start": px.index[start], "end": None,
                    "ret": float(px.iloc[-1] / px.iloc[start] - 1), "success": None})
    return {"list": out, "modes": modes}


def main() -> None:
    cfg = build_default_config()
    ds = cache.load_dataset()
    levels = (None,) + CANDIDATES
    name = lambda l: "baseline (live)" if l is None else f"L_rec {l:.1f}"
    G, A, X = {}, {}, {}
    for l in levels:
        p = pulse_params(l)
        G[l] = gate(ds, p)
        A[l] = {"full": account(ds, cfg, p, FULL), "h1": account(ds, cfg, p, HALVES[0]),
                "h2": account(ds, cfg, p, HALVES[1])}
        A[l]["full"]["crash"] = {k: _mdd(A[l]["full"]["equity"].loc[a:b])
                                 for k, (a, b) in CRASHES.items()}
        X[l] = index_tests(l)
        f, pm = A[l]["full"], G[l]["metrics"]["PULSE"]
        print(f"{name(l)}: gate {'PASS' if G[l]['pass'] else 'FAIL'} (PULSE MaxDD {pm['max_drawdown']:.1%}) | "
              f"after-tax {f[key(PRIMARY)]:.2%} pre {f['pre']:.2%} Sharpe {f['sharpe']:.2f} "
              f"MaxDD {f['mdd']:.1%} | dot-com {X[l]['dotcom_mdd']:.1%} | "
              f"COMP {X[l]['comp_cagr']:.2%}/{X[l]['comp_mdd']:.1%} | NIK {X[l]['nik_cagr']:.2%}/{X[l]['nik_mdd']:.1%}",
              flush=True)

    grid = {}
    for l in CANDIDATES:
        for arm in GRID_ARM:
            for sma in GRID_SMA:
                if (arm, sma) == (ARM, SMA):
                    grid[(l, arm, sma)] = A[l]["full"][key(PRIMARY)]
                else:
                    grid[(l, arm, sma)] = account(ds, cfg, pulse_params(l, arm, sma), FULL)[key(PRIMARY)]
        print(f"grid L_rec {l}: " + ", ".join(f"{a:.3g}/{s}: {grid[(l, a, s)]:.2%}"
                                              for a in GRID_ARM for s in GRID_SMA), flush=True)

    b, bx = A[None], X[None]

    def checks(l) -> Dict[str, Tuple[bool, str]]:
        g, a, x = G[l], A[l], X[l]
        pm = g["metrics"]["PULSE"]
        c = {}
        c["R1 gating bar"] = (g["pass"], f"admitted {g['admitted']}; PULSE MaxDD {pm['max_drawdown']:.1%} "
                                         f"(limit -45%), Sharpe {pm['sharpe']:.2f}")
        gain = a["full"][key(PRIMARY)] - b["full"][key(PRIMARY)]
        c["R2 gain >= 1.00pp"] = (gain >= 0.01, f"{gain*100:+.2f}pp after tax")
        c["R3 both halves"] = (all(a[h][key(PRIMARY)] >= b[h][key(PRIMARY)] for h in ("h1", "h2")),
                               f"{a['h1'][key(PRIMARY)]:.2%} vs {b['h1'][key(PRIMARY)]:.2%}; "
                               f"{a['h2'][key(PRIMARY)]:.2%} vs {b['h2'][key(PRIMARY)]:.2%}")
        c["R4 every bracket"] = (all(a["full"][key(r)] > b["full"][key(r)] for r in SENS),
                                 "; ".join(f"{r.label}: {a['full'][key(r)]:.2%} vs {b['full'][key(r)]:.2%}"
                                           for r in SENS))
        c["R5 Sharpe"] = (a["full"]["sharpe"] >= b["full"]["sharpe"] - 0.10,
                          f"{a['full']['sharpe']:.2f} vs {b['full']['sharpe']:.2f}")
        c["R6 drawdowns >= -35%"] = (a["full"]["mdd"] >= -0.35 and
                                     all(v >= -0.35 for v in a["full"]["crash"].values()),
                                     f"MaxDD {a['full']['mdd']:.1%}; " +
                                     ", ".join(f"{k} {v:.1%}" for k, v in a["full"]["crash"].items()))
        c["R7 dot-com >= -50%"] = (x["dotcom_mdd"] >= -0.50, f"{x['dotcom_mdd']:.1%}")
        c["R8 Nasdaq 1971+"] = (x["comp_cagr"] > bx["comp_cagr"] and x["comp_mdd"] >= bx["comp_mdd"] - 0.10,
                                f"CAGR {x['comp_cagr']:.2%} vs {bx['comp_cagr']:.2%}; "
                                f"MaxDD {x['comp_mdd']:.1%} vs {bx['comp_mdd']:.1%}")
        c["R9 Japan 1990-2012"] = (x["nik_cagr"] >= bx["nik_cagr"] - 0.01 and x["nik_mdd"] >= bx["nik_mdd"] - 0.10,
                                   f"CAGR {x['nik_cagr']:.2%} vs {bx['nik_cagr']:.2%}; "
                                   f"MaxDD {x['nik_mdd']:.1%} vs {bx['nik_mdd']:.1%}")
        wins = sum(grid[(l, a_, s_)] > b["full"][key(PRIMARY)] for a_ in GRID_ARM for s_ in GRID_SMA)
        c["R10 robustness >= 6/9"] = (wins >= 6, f"{wins}/9 cells beat the baseline after tax")
        return c

    verdict = {l: checks(l) for l in CANDIDATES}
    passing = [l for l in CANDIDATES if all(ok for ok, _ in verdict[l].values())]
    chosen = max(passing, key=lambda l: A[l]["full"][key(PRIMARY)]) if passing else None

    # information only
    qqq = ds["QQQ"].loc[FULL[0]:FULL[1]]
    qqq_at = {r.label: _cagr(buy_and_hold_after_tax(qqq["tr_close"], qqq["close"], qqq["dividend"], r))
              for r in [PRIMARY] + SENS}
    troughs = [t for _, t in episodes(ds)]
    cap = {l: {n: capture(A[l]["full"]["equity"], troughs, n) for n in (21, 63, 126)} for l in levels}
    cap["QQQ"] = {n: capture(qqq["tr_close"], troughs, n) for n in (21, 63, 126)}
    rq = rec_episodes(ds["QQQ"]["close"].loc[:WIN_END])
    eps = [e for e in rq["list"] if e["start"] >= pd.Timestamp(FULL[0])]
    x3 = index_tests(3.0)
    px = ds["QQQ"]["close"].loc[:WIN_END]
    sma_now = float(px.iloc[-SMA:].mean())
    hi_now = float(px.iloc[-252:].max())
    mode_now = int(rq["modes"][-1])

    md: List[str] = [
        "# Recovery-mode study — RESULTS", "",
        "Pre-registration: [RECOVERY_STUDY_PREREG.md](RECOVERY_STUDY_PREREG.md), written before this ran "
        "(SHA-256 bfa5a62b…fe53, 2026-11-04 19:56 ET). Window 2008-01-02 → 2026-11-04. Account figures use the "
        "live execution (40% band, signal override 10%, recovery starts and failures force a rebalance) and the "
        "tax model of the tax study.", "",
        "## Account results", "",
        "| PULSE | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | turnover | ST share | after-tax 32/15 | "
        "24/15 | 37/20 | half 1 | half 2 |",
        "|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|"]
    for l in levels:
        f = A[l]["full"]
        md.append(f"| {name(l)} | {f['pre']:.2%} | {f['sharpe']:.2f} | {f['mdd']:.1%} | "
                  + " | ".join(f"{v:.1%}" for v in f["crash"].values())
                  + f" | {f['turnover']:.1f}x | {f['st_share']:.0%} | **{f[key(PRIMARY)]:.2%}** | "
                  f"{f[key(SENS[0])]:.2%} | {f[key(SENS[1])]:.2%} | "
                  f"{A[l]['h1'][key(PRIMARY)]:.2%} | {A[l]['h2'][key(PRIMARY)]:.2%} |")
    md += ["", "QQQ buy-and-hold after tax: " + ", ".join(f"{k} {v:.2%}" for k, v in qqq_at.items())
           + f"; pre-tax {_cagr(qqq['tr_close']):.2%}.", "",
           "## Index model (no band, costs or tax; ROTATION as cash)", "",
           "| PULSE | dot-com book MaxDD 1999-06 → 2003-12 | NDX book CAGR 1990+ | Nasdaq Composite 1971+ CAGR / MaxDD | "
           "Nikkei 1990-2012 CAGR / MaxDD |", "|---|--:|--:|--:|--:|"]
    for l, x in [(l, X[l]) for l in levels] + [("3x (information only)", x3)]:
        md.append(f"| {name(l) if not isinstance(l, str) else l} | {x['dotcom_mdd']:.1%} | {x['ndx_cagr']:.2%} | "
                  f"{x['comp_cagr']:.2%} / {x['comp_mdd']:.1%} | {x['nik_cagr']:.2%} / {x['nik_mdd']:.1%} |")
    md += ["", "## Original gating bar (PULSE standalone, full window)", "",
           "| PULSE | CAGR | Sharpe | MaxDD (limit −45%) | worst regime-window DD | A6 robustness | admitted |",
           "|---|--:|--:|--:|--:|---|---|"]
    for l in levels:
        pm = G[l]["metrics"]["PULSE"]
        wdd = min(m.get("max_drawdown", 0.0) for _, m in G[l]["walk"]["PULSE"])
        a6 = G[l]["a6"].get("PULSE")
        md.append(f"| {name(l)} | {pm['cagr']:.1%} | {pm['sharpe']:.2f} | {pm['max_drawdown']:.1%} | {wdd:.1%} | "
                  f"{'PASS' if a6 and a6[0] else ('FAIL' if a6 else 'not reached')}"
                  f"{(': ' + a6[1]) if a6 else ''} | {'yes' if G[l]['pass'] else 'no'} |")
    md += ["", "## Parameter grid (R10): after-tax CAGR 32/15, full window", "",
           "| L_rec | arm \\ SMA | " + " | ".join(str(s) for s in GRID_SMA) + " |", "|---|---|--:|--:|--:|"]
    for l in CANDIDATES:
        for a_ in GRID_ARM:
            md.append(f"| {l:.1f} | {a_:.1%} | " + " | ".join(f"{grid[(l, a_, s_)]:.2%}" for s_ in GRID_SMA) + " |")
    md += [f"", f"Baseline: {b['full'][key(PRIMARY)]:.2%}.", "", "## Criteria", ""]
    for l in CANDIDATES:
        md.append(f"**{name(l)}** — {'PASSES ALL' if l in passing else 'FAILS'}")
        md.append("")
        for k, (ok, why) in verdict[l].items():
            md.append(f"- {'PASS' if ok else 'FAIL'} — {k}: {why}")
        md.append("")
    md.append(f"**Selection: {name(chosen)}.** Per the pre-registration this is NOT deployed automatically; "
              "the user decides." if chosen else
              "**Selection: none.** No candidate meets every criterion; PULSE stays unchanged (negative result).")
    md += ["", "## Information only (not criteria)", "",
           "Average return after the troughs of the 10%+ QQQ drawdowns in the window:", "",
           "| | +1 month | +3 months | +6 months |", "|---|--:|--:|--:|"]
    for l in ("QQQ",) + levels:
        md.append(f"| {l if isinstance(l, str) else name(l)} | " +
                  " | ".join(f"{cap[l][n]:+.1%}" for n in (21, 63, 126)) + " |")
    succ = [e for e in eps if e["success"]]
    fail = [e for e in eps if e["success"] is False]
    md += ["", f"Recovery-mode episodes on QQQ since 2008: {len(eps)} "
               f"({len(succ)} reached a new high, {len(fail)} failed"
               f"{', 1 open' if eps and eps[-1]['success'] is None else ''}); average QQQ move "
               f"while in recovery mode: successes {np.mean([e['ret'] for e in succ]):+.1%}, "
               f"failures {np.mean([e['ret'] for e in fail]):+.1%}.", "",
           "| start | end | QQQ move | outcome |", "|---|---|--:|---|"]
    for e in eps:
        md.append(f"| {e['start'].date()} | {e['end'].date() if e['end'] is not None else 'open'} | "
                  f"{e['ret']:+.1%} | {'new high' if e['success'] else ('failed' if e['success'] is False else 'open')} |")
    md += ["", f"Last completed session {px.index[-1].date()}: QQQ {px.iloc[-1]:.2f}, "
               f"{px.iloc[-1] / hi_now - 1:+.1%} from its 252-session high {hi_now:.2f}; 50-day SMA {sma_now:.2f}. "
               f"Mode: {['normal', 'armed (in a 10%+ drawdown, not confirmed)', 'RECOVERY'][mode_now]}. "
               f"Recovery mode would start on a close above {sma_now * 1.015:.2f} "
               f"(the 50-day SMA moves daily).", ""]
    out = PACKAGE_DIR / "docs" / "RECOVERY_STUDY.md"
    out.write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


if __name__ == "__main__":
    main()
