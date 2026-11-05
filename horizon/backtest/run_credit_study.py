"""PULSE credit-confirmation study -> docs/CREDIT_STUDY.md.

Criteria and selection rule: docs/CREDIT_STUDY_PREREG.md (committed before this ran, f219e4b).
Reuses the recovery study's gating, account and index-model code. The credit spread joins the
dataset as a pseudo-symbol whose close on each date is the value known at that decision. Run
from the repo root with backtest-basis symbols:
    HORIZON_SYMBOL_EQUIVALENTS=0 python -m horizon.backtest.run_credit_study
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ..config import build_default_config
from ..data import cache
from ..paths import PACKAGE_DIR
from ..strategies.pulse import (CONFIRM_BOTH, CONFIRM_CREDIT, CONFIRM_PRICE, MODE_NORMAL,
                                MODE_RECOVERY, RecoveryTracker, recovery_modes)
from . import run_recovery_study as R
from .run_leverage_study import _cagr, _fred, _mdd
from .tax import buy_and_hold_after_tax

CREDIT = "BAA10Y"
WIN_END, FULL, HALVES, CRASHES = R.WIN_END, R.FULL, R.HALVES, R.CRASHES
ARM, SMA, CSMA, L_REC, HYST = 0.10, 50, 50, 2.0, 0.015
PRIMARY, SENS, key = R.PRIMARY, R.SENS, R.key
RULES = {None: None, "P": CONFIRM_PRICE, "A": CONFIRM_BOTH, "B": CONFIRM_CREDIT}
CANDIDATES = ("A", "B")
NAME = {None: "baseline (live)", "P": "price-only (reference)", "A": "A — confirmed",
        "B": "B — credit-led"}
GRID_ARM, GRID_CSMA = (0.075, 0.10, 0.15), (20, 50, 100)
DOTCOM, LONG = ("1999-06-01", "2003-12-31"), ("1986-01-02", WIN_END)
SEGMENTS = (("1986-01-02", "2007-12-31"), ("2008-01-02", WIN_END))


def known_spread(index: pd.DatetimeIndex) -> pd.Series:
    """BAA10Y as known at each decision on `index`: the latest value dated strictly before."""
    s = _fred(CREDIT).dropna()
    return s.reindex(s.index.union(index)).ffill().shift(1).reindex(index)


def pulse_params(rule: Optional[str], arm: float = ARM, csma: int = CSMA) -> dict:
    p = R.pulse_params(L_REC if rule else None, arm, SMA)
    if rule in CANDIDATES:
        p.update({"recovery_confirm": RULES[rule], "recovery_credit": CREDIT,
                  "recovery_credit_sma": csma})
    return p


def rule_modes(px: pd.Series, rule: str, credit: Optional[pd.Series] = None) -> np.ndarray:
    if rule in CANDIDATES and credit is None:
        credit = known_spread(px.index)
    return recovery_modes(px, ARM, SMA, HYST, credit=credit if rule in CANDIDATES else None,
                          confirm=RULES[rule], credit_sma=CSMA)


def runs(px: pd.Series, modes) -> List[dict]:
    """Recovery-mode episodes: start, end, index move, success (new high) or failure."""
    out, start = [], None
    for i in range(1, len(modes)):
        if modes[i] == MODE_RECOVERY and modes[i - 1] != MODE_RECOVERY:
            start = i
        elif start is not None and modes[i] != MODE_RECOVERY:
            out.append({"start": px.index[start], "end": px.index[i],
                        "ret": float(px.iloc[i] / px.iloc[start] - 1),
                        "success": bool(modes[i] == MODE_NORMAL)})
            start = None
    if start is not None:
        out.append({"start": px.index[start], "end": None,
                    "ret": float(px.iloc[-1] / px.iloc[start] - 1), "success": None})
    return out


def fail_share(eps: List[dict], a: str, b: str) -> Tuple[float, int]:
    done = [e for e in eps if pd.Timestamp(a) <= e["start"] <= pd.Timestamp(b)
            and e["success"] is not None]
    return (float(np.mean([not e["success"] for e in done])) if done else float("nan"), len(done))


def index_tests(rule: Optional[str]) -> Dict:
    ndx = _fred("NASDAQ100").dropna().loc[:WIN_END]
    modes = rule_modes(ndx, rule) if rule else None
    book = R.index_book(ndx, R._rate("DTB3", ndx.index), L_REC if rule else None, modes=modes)
    eps = runs(ndx, modes) if rule else []
    long = book.loc[LONG[0]:LONG[1]]
    return {"dotcom_mdd": _mdd(book.loc[DOTCOM[0]:DOTCOM[1]]), "long_cagr": _cagr(long),
            "long_mdd": _mdd(long), "eps": eps, "end": ndx.index[-1],
            "fail": [fail_share(eps, a, b) for a, b in SEGMENTS]}


def _avg(eps: List[dict], outcome: bool) -> str:
    v = [e["ret"] for e in eps if e["success"] is outcome]
    return f"{np.mean(v):+.1%}" if v else "n/a"


def main() -> None:
    cfg = build_default_config()
    ds = cache.load_dataset()
    idx = ds["SPY"].index.union(ds["QQQ"].index)
    k = known_spread(idx)
    ds[CREDIT] = pd.DataFrame({"open": k, "high": k, "low": k, "close": k, "tr_close": k,
                               "dividend": 0.0}, index=idx)
    G, A, X = {}, {}, {}
    for rule in RULES:
        p = pulse_params(rule)
        if rule != "P":
            G[rule] = R.gate(ds, p)
        A[rule] = {"full": R.account(ds, cfg, p, FULL)}
        if rule != "P":
            A[rule]["h1"] = R.account(ds, cfg, p, HALVES[0])
            A[rule]["h2"] = R.account(ds, cfg, p, HALVES[1])
        A[rule]["full"]["crash"] = {c: _mdd(A[rule]["full"]["equity"].loc[a:b])
                                    for c, (a, b) in CRASHES.items()}
        X[rule] = index_tests(rule)
        f = A[rule]["full"]
        gate = (f"gate {'PASS' if G[rule]['pass'] else 'FAIL'} "
                f"(PULSE MaxDD {G[rule]['metrics']['PULSE']['max_drawdown']:.1%}) | "
                if rule in G else "")
        print(f"{NAME[rule]}: {gate}after-tax {f[key(PRIMARY)]:.2%} pre {f['pre']:.2%} "
              f"Sharpe {f['sharpe']:.3f} MaxDD {f['mdd']:.1%} | dot-com {X[rule]['dotcom_mdd']:.1%} | "
              f"NDX 1986+ {X[rule]['long_cagr']:.2%}/{X[rule]['long_mdd']:.1%} | fail "
              + ", ".join(f"{s:.0%} of {n}" for s, n in X[rule]["fail"]), flush=True)

    grid = {}
    for rule in CANDIDATES:
        for arm in GRID_ARM:
            for csma in GRID_CSMA:
                grid[(rule, arm, csma)] = (
                    A[rule]["full"][key(PRIMARY)] if (arm, csma) == (ARM, CSMA)
                    else R.account(ds, cfg, pulse_params(rule, arm, csma), FULL)[key(PRIMARY)])
        print(f"grid {rule}: " + ", ".join(f"{a:.3g}/{s}: {grid[(rule, a, s)]:.2%}"
                                          for a in GRID_ARM for s in GRID_CSMA), flush=True)

    b, bx = A[None], X[None]

    def checks(rule) -> Dict[str, Tuple[bool, str]]:
        g, a, x = G[rule], A[rule], X[rule]
        pm, P = g["metrics"]["PULSE"], key(PRIMARY)
        c = {}
        c["C1 gating bar"] = (g["pass"], f"admitted {g['admitted']}; PULSE MaxDD "
                                         f"{pm['max_drawdown']:.1%} (limit -45%), Sharpe {pm['sharpe']:.2f}")
        gain = a["full"][P] - b["full"][P]
        c["C2 gain >= 1.25pp"] = (gain >= 0.0125, f"{gain * 100:+.2f}pp after tax")
        hs = {h: a[h][P] - b[h][P] for h in ("h1", "h2")}
        c["C3 both halves >= +0.50pp"] = (all(v >= 0.005 for v in hs.values()), "; ".join(
            f"{a[h][P]:.2%} vs {b[h][P]:.2%} ({v * 100:+.2f}pp)" for h, v in hs.items()))
        c["C4 every bracket >= +1.00pp"] = (
            all(a["full"][key(r)] - b["full"][key(r)] >= 0.01 for r in SENS),
            "; ".join(f"{r.label}: {a['full'][key(r)]:.2%} vs {b['full'][key(r)]:.2%}" for r in SENS))
        c["C5 Sharpe >= baseline"] = (a["full"]["sharpe"] >= b["full"]["sharpe"],
                                      f"{a['full']['sharpe']:.3f} vs {b['full']['sharpe']:.3f}")
        dd = [("MaxDD", a["full"]["mdd"], b["full"]["mdd"])] + [
            (n, a["full"]["crash"][n], b["full"]["crash"][n]) for n in CRASHES]
        c["C6 drawdowns within 5pp"] = (all(v >= w - 0.05 for _, v, w in dd),
                                        ", ".join(f"{n} {v:.1%} vs {w:.1%}" for n, v, w in dd))
        c["C7 dot-com within 5pp"] = (x["dotcom_mdd"] >= bx["dotcom_mdd"] - 0.05,
                                      f"{x['dotcom_mdd']:.1%} vs {bx['dotcom_mdd']:.1%}")
        c["C8 NDX 1986+"] = (x["long_cagr"] > bx["long_cagr"] and x["long_mdd"] >= bx["long_mdd"] - 0.05,
                             f"CAGR {x['long_cagr']:.2%} vs {bx['long_cagr']:.2%}; "
                             f"MaxDD {x['long_mdd']:.1%} vs {bx['long_mdd']:.1%}")
        ref = X["P"]["fail"]
        c["C9 mechanism"] = (all(s == s and s < r for (s, _), (r, _) in zip(x["fail"], ref)), "; ".join(
            f"{lo[:4]}-{hi[:4]}: {s:.0%} of {n} failed vs price-only {r:.0%} of {m}"
            for (lo, hi), (s, n), (r, m) in zip(SEGMENTS, x["fail"], ref)))
        wins = sum(grid[(rule, a_, s_)] > b["full"][P] for a_ in GRID_ARM for s_ in GRID_CSMA)
        c["C10 robustness >= 7/9"] = (wins >= 7, f"{wins}/9 cells beat the baseline after tax")
        return c

    verdict = {r: checks(r) for r in CANDIDATES}
    passing = [r for r in CANDIDATES if all(ok for ok, _ in verdict[r].values())]
    chosen = max(passing, key=lambda r: A[r]["full"][key(PRIMARY)]) if passing else None

    # information only
    qqq = ds["QQQ"].loc[FULL[0]:FULL[1]]
    qqq_at = {r.label: _cagr(buy_and_hold_after_tax(qqq["tr_close"], qqq["close"], qqq["dividend"], r))
              for r in [PRIMARY] + SENS}
    troughs = [t for _, t in R.episodes(ds)]
    cap = {r: {n: R.capture(A[r]["full"]["equity"], troughs, n) for n in (21, 63, 126)} for r in RULES}
    cap["QQQ"] = {n: R.capture(qqq["tr_close"], troughs, n) for n in (21, 63, 126)}
    px = ds["QQQ"]["close"].loc[:WIN_END]
    kq = ds[CREDIT]["close"].reindex(px.index)
    q_eps = {r: [e for e in runs(px, rule_modes(px, r, kq)) if e["start"] >= pd.Timestamp(FULL[0])]
             for r in ("P",) + CANDIDATES}
    now = {}
    for r in ("P",) + CANDIDATES:
        t = RecoveryTracker(ARM, SMA, HYST, confirm=RULES[r], credit_sma=CSMA)
        m = t.modes(px, kq if r in CANDIDATES else None)
        now[r] = (m[-1], t.armed, t.price_ok, t.credit_ok)
    k_now, k_avg = float(kq.iloc[-1]), float(kq.iloc[-CSMA:].mean())
    sma_now, hi_now = float(px.iloc[-SMA:].mean()), float(px.iloc[-252:].max())

    md: List[str] = [
        "# Credit-confirmation study — RESULTS", "",
        "Pre-registration: [CREDIT_STUDY_PREREG.md](CREDIT_STUDY_PREREG.md), committed before this ran "
        "(f219e4b, 2026-11-04 22:19 ET; SHA-256 97624309…5e39; its date line says 22:30 ET, but the commit "
        "time is the binding record). Credit measure: FRED BAA10Y, the latest value dated before each "
        f"decision. Account window {FULL[0]} → {FULL[1]}, live execution (40% band, signal override 10%, "
        "recovery starts and failures force a rebalance) and the tax model of the tax study. "
        f"Index model on FRED NASDAQ100 through {bx['end'].date()} (the last date FRED had).", "",
        "## Account results", "",
        "| PULSE | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | turnover | ST share | after-tax 32/15 | "
        "24/15 | 37/20 | half 1 | half 2 |",
        "|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|"]
    for r in RULES:
        f = A[r]["full"]
        halves = (" | ".join(f"{A[r][h][key(PRIMARY)]:.2%}" for h in ("h1", "h2")) if "h1" in A[r]
                  else "— | —")
        md.append(f"| {NAME[r]} | {f['pre']:.2%} | {f['sharpe']:.2f} | {f['mdd']:.1%} | "
                  + " | ".join(f"{v:.1%}" for v in f["crash"].values())
                  + f" | {f['turnover']:.1f}x | {f['st_share']:.0%} | **{f[key(PRIMARY)]:.2%}** | "
                  f"{f[key(SENS[0])]:.2%} | {f[key(SENS[1])]:.2%} | {halves} |")
    md += ["", "QQQ buy-and-hold after tax: " + ", ".join(f"{k_} {v:.2%}" for k_, v in qqq_at.items())
           + f"; pre-tax {_cagr(qqq['tr_close']):.2%}.", "",
           "## Index model (NASDAQ100; no band, costs or tax; ROTATION as cash)", "",
           "| PULSE | dot-com book MaxDD 1999-06 → 2003-12 | book CAGR 1986+ | book MaxDD 1986+ | "
           "failed episodes 1986–2007 | failed episodes 2008+ |", "|---|--:|--:|--:|--:|--:|"]
    for r in RULES:
        x = X[r]
        fs = (" | ".join(f"{s:.0%} of {n}" for s, n in x["fail"]) if r else "— | —")
        md.append(f"| {NAME[r]} | {x['dotcom_mdd']:.1%} | {x['long_cagr']:.2%} | {x['long_mdd']:.1%} | {fs} |")
    md += ["", "## Original gating bar (PULSE standalone, full window)", "",
           "| PULSE | CAGR | Sharpe | MaxDD (limit −45%) | worst regime-window DD | A6 robustness | admitted |",
           "|---|--:|--:|--:|--:|---|---|"]
    for r in G:
        pm = G[r]["metrics"]["PULSE"]
        wdd = min(m.get("max_drawdown", 0.0) for _, m in G[r]["walk"]["PULSE"])
        a6 = G[r]["a6"].get("PULSE")
        md.append(f"| {NAME[r]} | {pm['cagr']:.1%} | {pm['sharpe']:.2f} | {pm['max_drawdown']:.1%} | {wdd:.1%} | "
                  f"{'PASS' if a6 and a6[0] else ('FAIL' if a6 else 'not reached')}"
                  f"{(': ' + a6[1]) if a6 else ''} | {'yes' if G[r]['pass'] else 'no'} |")
    md += ["", "## Parameter grid (C10): after-tax CAGR 32/15, full window", "",
           "| rule | arm \\ credit SMA | " + " | ".join(str(s) for s in GRID_CSMA) + " |",
           "|---|---|--:|--:|--:|"]
    for r in CANDIDATES:
        for a_ in GRID_ARM:
            md.append(f"| {r} | {a_:.1%} | " + " | ".join(f"{grid[(r, a_, s_)]:.2%}" for s_ in GRID_CSMA) + " |")
    md += ["", f"Baseline: {b['full'][key(PRIMARY)]:.2%}.", "", "## Criteria", ""]
    for r in CANDIDATES:
        md += [f"**{NAME[r]}** — {'PASSES ALL' if r in passing else 'FAILS'}", ""]
        md += [f"- {'PASS' if ok else 'FAIL'} — {k_}: {why}" for k_, (ok, why) in verdict[r].items()]
        md.append("")
    md.append(f"**Selection: {NAME[chosen]}.** Per the pre-registration this is NOT deployed automatically; "
              "the user decides." if chosen else
              "**Selection: none.** No candidate meets every criterion; PULSE stays unchanged (negative result).")
    md += ["", "## Information only (not criteria)", "",
           "Average return after the troughs of the 10%+ QQQ drawdowns in the account window:", "",
           "| | +1 month | +3 months | +6 months |", "|---|--:|--:|--:|"]
    for r in ("QQQ",) + tuple(RULES):
        md.append(f"| {r if r == 'QQQ' else NAME[r]} | " +
                  " | ".join(f"{cap[r][n]:+.1%}" for n in (21, 63, 126)) + " |")
    md += ["", "Recovery-mode episodes (index move from the signal close to the exit close):", "",
           "| rule | QQQ since 2008: episodes | new high / failed | avg success / failure | "
           "NDX since 1986: episodes | new high / failed | avg success / failure |",
           "|---|--:|--:|--:|--:|--:|--:|"]
    for r in ("P",) + CANDIDATES:
        qe, ne = q_eps[r], X[r]["eps"]
        cnt = lambda e: f"{sum(1 for v in e if v['success'])} / {sum(1 for v in e if v['success'] is False)}"
        md.append(f"| {NAME[r]} | {len(qe)} | {cnt(qe)} | {_avg(qe, True)} / {_avg(qe, False)} | "
                  f"{len(ne)} | {cnt(ne)} | {_avg(ne, True)} / {_avg(ne, False)} |")
    for title, lo, hi, src in (("Dot-com bust on NASDAQ100, 2000-03 → 2003-12", "2000-03-01", "2003-12-31", "ndx"),
                               ("QQQ since 2008", FULL[0], FULL[1], "qqq")):
        md += ["", f"{title}:", "", "| rule | start | end | move | outcome |", "|---|---|---|--:|---|"]
        for r in ("P",) + CANDIDATES:
            eps = X[r]["eps"] if src == "ndx" else q_eps[r]
            for e in eps:
                if pd.Timestamp(lo) <= e["start"] <= pd.Timestamp(hi):
                    md.append(f"| {r} | {e['start'].date()} | {e['end'].date() if e['end'] is not None else 'open'} | "
                              f"{e['ret']:+.1%} | {'new high' if e['success'] else ('failed' if e['success'] is False else 'open')} |")
    label = ["normal", "armed", "RECOVERY"]
    md += ["", f"Last completed session {px.index[-1].date()}: QQQ {px.iloc[-1]:.2f}, "
               f"{px.iloc[-1] / hi_now - 1:+.1%} from its 252-session high {hi_now:.2f}; 50-day SMA {sma_now:.2f} "
               f"(price confirms above {sma_now * (1 + HYST):.2f}). BAA10Y known at this decision {k_now:.2f}, "
               f"its 50-day average {k_avg:.3f} (credit confirms below {k_avg * (1 - HYST):.3f}, clears above "
               f"{k_avg * (1 + HYST):.3f}).", ""]
    for r in ("P",) + CANDIDATES:
        m, armed, pok, cok = now[r]
        md.append(f"- {NAME[r]}: mode {label[m]}; price confirmed {pok}"
                  + (f", credit confirmed {cok}" if r in CANDIDATES else ""))
    out = PACKAGE_DIR / "docs" / "CREDIT_STUDY.md"
    out.write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


if __name__ == "__main__":
    main()
