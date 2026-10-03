"""What Horizon emails you, and when (redesigned 2026-10-03).

Every email is written for a person deciding whether to act. Engine internals
(funding scale, ceiling scale, band config, order counts) stay in the logs.

  ACTION NEEDED  Something stopped or is wrong and the email says what to do:
                 a failed cycle, stale market data, rejected Alpaca keys, an
                 ownership conflict, an emergency flatten.
  Drawdown       One email each time the account falls another 10% below its
                 peak, with the backtested context. Re-armed at each new high.
  Daily report   After every weekday cycle: performance vs QQQ and SPY, what
                 traded and why, holdings, signals, and only the warnings that
                 apply. It doubles as a heartbeat: no report by ~9:35 AM ET on a
                 trading day means the engine did not run.
  Restart        When the service starts. Expected on deploys; otherwise it
                 means the service crashed and was restarted.

Plain text, short lines, no column alignment (mail clients render plain text
in proportional fonts). Pure functions; engine/main.py gathers the facts.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DRAWDOWN_STEPS = (0.10, 0.20, 0.30, 0.40)
BACKTEST_MAX_DD = 0.26   # account simulator 2008-2026, live config (docs/TAX_STUDY.md)
STRESS_DD = 0.42         # dot-com-style bust estimate (2026-09-25 review)
NASDAQ_WEIGHTS = {"QQQ": 1.0, "QQQM": 1.0, "QLD": 2.0}
BENCHMARKS = ("QQQ", "SPY")      # compared close-to-close on total-return closes


# --- formatting --------------------------------------------------------------

def _d(iso: str) -> str:
    """'2026-10-01' -> 'Thu Oct 1'."""
    d = date.fromisoformat(str(iso)[:10])
    return f"{d:%a %b} {d.day}"


def _money(x: float, signed: bool = False, cents: bool = False) -> str:
    body = f"${abs(x):,.2f}" if cents else f"${abs(x):,.0f}"
    if signed:
        return ("+" if x >= 0 else "-") + body
    return ("-" if x < 0 else "") + body


def _pct(x: float, digits: int = 2) -> str:
    return f"{x * 100:+.{digits}f}%"


def _bench_line(b: Dict[str, float]) -> str:
    parts = [f"{k} {_pct(b[k])}" for k in ("QQQ", "SPY") if k in b]
    return ", ".join(parts)


def _week(iso: str) -> str:
    y, w, _ = date.fromisoformat(str(iso)[:10]).isocalendar()
    return f"{y}-W{w:02d}"


# --- performance tracker (persisted in the state volume) ---------------------

def load_tracker(path: Path) -> dict:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_tracker(path: Path, st: dict) -> None:
    Path(path).write_text(json.dumps(st, indent=2), encoding="utf-8")


def _bench_ret(now: Dict[str, float], base: Dict[str, float]) -> Dict[str, float]:
    return {k: now[k] / base[k] - 1.0 for k in now if base.get(k)}


def update_tracker(st: dict, as_of: str, equity: float, net_flow: float,
                   bench: Dict[str, float]) -> Tuple[dict, dict]:
    """Advance the tracker by one report. Deposits and withdrawals (net_flow,
    since the last report) are excluded from performance and move the peak,
    so a deposit is not reported as a gain and a withdrawal is not a drawdown.

    Returns (new_state, perf). perf keys: first, chg_abs, chg_pct, since_as_of,
    bench, wtd_pct, wtd_bench, drawdown, peak, peak_as_of, new_high, crossed.
    """
    st = json.loads(json.dumps(st or {}))
    perf: dict = {"first": "last_equity" not in st, "new_high": False, "crossed": None}
    if perf["first"]:
        st.update(peak=equity, peak_as_of=as_of, dd_alerted=[], week=_week(as_of),
                  week_base_equity=equity, week_base_bench=dict(bench), week_flows=0.0)
    else:
        last_eq = float(st["last_equity"])
        st["peak"] = float(st["peak"]) + net_flow
        chg = equity - last_eq - net_flow
        perf.update(chg_abs=chg, chg_pct=(chg / last_eq) if last_eq > 0 else 0.0,
                    since_as_of=st["last_as_of"],
                    bench=_bench_ret(bench, st.get("last_bench", {})))
        if _week(as_of) != st.get("week"):
            # The week is keyed on the SESSION being reported (as_of), so the
            # Monday 9:00 report, which covers Friday, still belongs to last week.
            st.update(week=_week(as_of), week_base_equity=last_eq,
                      week_base_bench=st.get("last_bench", {}), week_flows=0.0)
        st["week_flows"] = float(st.get("week_flows", 0.0)) + net_flow
    base = float(st["week_base_equity"])
    perf["wtd_pct"] = ((equity - base - float(st["week_flows"])) / base) if base > 0 else 0.0
    perf["wtd_bench"] = _bench_ret(bench, st.get("week_base_bench", {}))
    if equity > float(st["peak"]):
        perf["new_high"] = not perf["first"]
        st.update(peak=equity, peak_as_of=as_of, dd_alerted=[])
    dd = equity / float(st["peak"]) - 1.0
    crossed = [s for s in DRAWDOWN_STEPS
               if dd <= -s + 1e-12 and s not in st.get("dd_alerted", [])]
    if crossed:
        st["dd_alerted"] = sorted(set(st.get("dd_alerted", [])) | set(crossed))
        perf["crossed"] = max(crossed)
    perf.update(drawdown=dd, peak=float(st["peak"]), peak_as_of=st["peak_as_of"])
    st.update(last_as_of=as_of, last_equity=equity, last_bench=dict(bench))
    return st, perf


# --- daily report ------------------------------------------------------------

def build_daily_report(*, as_of: str, equity: float, perf: dict, orders: List[dict],
                       trade_reason: Optional[str], drift: float, band: float,
                       holdings: Dict[str, float], cash: float, regime: str,
                       score: float, prev_regime: Optional[str], pulse_lev: float,
                       rotation: List[str], warnings: List[str]) -> Tuple[str, str]:
    """orders: [{symbol, side, notional, filled}] where filled is the filled
    dollar amount or None if not (yet) filled."""
    n = len(orders)
    act = "held" if n == 0 else f"{n} trade{'s' if n != 1 else ''}"
    if perf.get("first"):
        head = _money(equity)
    else:
        q = perf.get("bench", {}).get("QQQ")
        head = f"{_money(equity)} {_pct(perf['chg_pct'])}"
        if q is not None:
            head += f" (QQQ {_pct(q)})"
    subject = f"{head} · {act}"
    if warnings:
        subject += f" · {len(warnings)} warning{'s' if len(warnings) != 1 else ''}"

    L: List[str] = [f"Equity at {_d(as_of)} close: {_money(equity, cents=True)}"]
    if perf.get("first"):
        L.append("Performance tracking starts with this report.")
    else:
        L.append(f"Since {_d(perf['since_as_of'])} close: "
                 f"{_money(perf['chg_abs'], signed=True, cents=True)} ({_pct(perf['chg_pct'])}). "
                 f"{_bench_line(perf.get('bench', {}))}.")
        L.append(f"Week to date: {_pct(perf['wtd_pct'])}. "
                 f"{_bench_line(perf.get('wtd_bench', {}))}.")
    if perf.get("new_high"):
        L.append("From peak: new high.")
    elif perf.get("drawdown", 0.0) > -0.0005:
        L.append("From peak: at the peak.")
    else:
        L.append(f"From peak: {_pct(perf['drawdown'], 1)} "
                 f"(peak {_money(perf['peak'])} on {_d(perf['peak_as_of'])}).")

    if warnings:
        L += ["", "Warnings:"] + [f"- {w}" for w in warnings]

    L.append("")
    if n == 0:
        L.append(f"Today: no trades. Holdings are within {drift:.1%} of target; "
                 f"the engine rebalances at {band:.0%}.")
    else:
        L.append(f"Today: {n} trade{'s' if n != 1 else ''} at the open. {trade_reason or ''}".rstrip())
        for o in sorted(orders, key=lambda o: (o["side"] != "sell", -o["notional"])):
            verb = "Sold" if o["side"] == "sell" else "Bought"
            if o.get("filled") is not None:
                L.append(f"- {verb} {o['symbol']} {_money(o['filled'])}")
            else:
                L.append(f"- {verb} {o['symbol']} {_money(o['notional'])} (not filled yet)")

    total = sum(holdings.values()) + cash
    if total > 0:
        L += ["", "Holdings:"]
        for sym, mv in sorted(holdings.items(), key=lambda kv: -kv[1]):
            L.append(f"- {sym} {_money(mv)} ({mv / total:.0%})")
        L.append(f"- Cash {_money(cash)} ({cash / total:.0%})")
        nas = sum(mv * NASDAQ_WEIGHTS.get(s, 0.0) for s, mv in holdings.items()) / total
        L.append(f"Nasdaq-100 exposure: {nas:.2f}x (QLD counts double).")

    reg = f"Market regime {regime} ({score:.0f})"
    if prev_regime and prev_regime != regime:
        reg += f", changed from {prev_regime}"
    L += ["", "Signals:",
          f"- {reg}.",
          f"- PULSE leverage {pulse_lev:.2f}x.",
          f"- ROTATION holds {' + '.join(rotation) if rotation else 'T-bills'}."]
    return subject, "\n".join(L)


# --- event emails ------------------------------------------------------------

def build_drawdown_alert(step: float, perf: dict, equity: float) -> Tuple[str, str]:
    dd = perf["drawdown"]
    subject = f"Drawdown passed -{step:.0%}: now {_pct(dd, 1)} from peak"
    within = abs(dd) <= BACKTEST_MAX_DD
    body = "\n".join([
        f"Equity {_money(equity, cents=True)} at the last close is {_pct(dd, 1)} below its peak of "
        f"{_money(perf['peak'])} on {_d(perf['peak_as_of'])}.",
        "",
        f"For reference, the worst drawdown in the 2008-2026 backtest of this exact "
        f"setup was about -{BACKTEST_MAX_DD:.0%}. A dot-com-style bust is estimated "
        f"at about -{STRESS_DD:.0%}.",
        ("This is within the backtested range." if within
         else "This is beyond the worst backtested drawdown."),
        "",
        "The engine needs no action from you: PULSE cuts leverage automatically "
        "as volatility rises. This email is for your own plan. You get one at "
        "each 10% step, and the steps reset at the next new high.",
    ])
    return subject, body


def build_restart(when_et: str, equity: Optional[float], next_cycle: str,
                  live: bool) -> Tuple[str, str]:
    subject = f"Restarted · next cycle {next_cycle}"
    lines = [f"Horizon restarted at {when_et} and is {'trading live' if live else 'in DRY-RUN (no real orders)'}."]
    if equity is not None:
        lines.append(f"Equity: {_money(equity, cents=True)}.")
    lines += [f"Next cycle: {next_cycle}.", "",
              "Restarts happen on every deploy. If nothing was deployed, the "
              "service crashed and Railway restarted it; the Railway logs show why."]
    return subject, "\n".join(lines)


def build_cycle_failed(tb: str) -> Tuple[str, str]:
    last = next((ln.strip() for ln in reversed(tb.strip().splitlines()) if ln.strip()),
                "unknown error")
    body = "\n".join([
        "Today's 9:00 AM ET cycle stopped with an error.",
        "",
        "Orders submitted before the error may have executed; nothing after it "
        "was placed. The engine will not retry today. The next attempt is the "
        "next weekday's 9:00 AM ET cycle, so check the account before then.",
        "",
        f"Error: {last}",
        "",
        "Technical detail:",
        tb.strip(),
    ])
    return "Today's cycle failed", body


def build_stale_data(as_of: str, completed: str, limit_days: int) -> Tuple[str, str]:
    body = "\n".join([
        f"Horizon refused to trade today. Its market data ends on {_d(as_of)}, "
        f"but the last completed session was {_d(completed)} "
        f"(the limit is {limit_days} days).",
        "",
        "No orders were placed and positions are unchanged. It tries again at "
        "the next 9:00 AM ET cycle.",
        "",
        "Usual causes: a Polygon outage, or an invalid POLYGON_API_KEY on the "
        "horizon-live Railway service.",
    ])
    return "No trades today, market data is stale", body


def build_auth_failed(err: str) -> Tuple[str, str]:
    body = "\n".join([
        "Alpaca rejected Horizon's API keys at startup, so the engine stopped. "
        "It will not trade until this is fixed. Railway retries the start up "
        "to 10 times.",
        "",
        "Fix: check ALPACA_API_KEY and ALPACA_SECRET_KEY on the horizon-live "
        "Railway service. They must be the LIVE account keys.",
        "",
        f"Error: {err}",
    ])
    return "Horizon cannot connect to Alpaca and is not running", body


def build_conflict(conflicts) -> Tuple[str, str]:
    body = "\n".join([
        f"Two owners claim the same position: {conflicts}.",
        "",
        "Horizon blocked new buys until this is resolved; sells still run. It "
        "usually means another engine or a manual trade touched a Horizon symbol.",
    ])
    return "Ownership conflict, new buys blocked", body
