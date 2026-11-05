"""What Horizon emails you, and when.

Redesigned 2026-10-03 (content), restyled 2026-10-08 (HTML, benchmarks), moved
to after the close 2026-10-31. Every email is written for a person deciding
whether to act; engine internals stay in the logs.

  Daily report   Once per market day, 30 minutes after the close (1:30 PM ET on
                 half days): today vs QQQ and SPY, week, month and since Horizon
                 took over the account, in percent and in dollars; this
                 morning's trades with final fills; what tomorrow's open is
                 expected to do; holdings; this year's realized gains for tax;
                 signals; warnings only when they apply. It doubles as a
                 heartbeat: no report by about 5 PM ET on a market day means
                 the engine is not running. Built by engine/evening.py.
  ACTION NEEDED  Sent immediately: a failed cycle, stale data, rejected Alpaca
                 keys, an ownership conflict, an emergency flatten, a failed report.
  Drawdown       One email each time the account falls another 10% below its
                 high, with the backtested context. Re-armed at each new high.
  Restart        When the service starts (expected on deploys).

Performance is time-weighted from the account's closing equity, so deposits and
withdrawals never count as gains or losses, and it is recomputed from scratch
every day. Benchmarks use total-return closes, so their dividends count.

Every builder returns (subject, text, html). The HTML is table-based with inline
styles, the format mail clients (Gmail web and app included) render reliably.
"""

from __future__ import annotations

import html as _html
import json
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DRAWDOWN_STEPS = (0.10, 0.20, 0.30, 0.40)
BACKTEST_MAX_DD = 0.293  # account simulator 2008-2026, live config: -29.3% (docs/LEVERAGE_STUDY.md)
STRESS_DD = 0.48         # dot-com-style bust estimate at target_vol 0.275 (docs/LEVERAGE_STUDY.md)
NASDAQ_WEIGHTS = {"QQQ": 1.0, "QQQM": 1.0, "QLD": 2.0}
BENCHMARKS = ("QQQ", "SPY")

# Palette and type, all inline (mail clients strip most <style> rules).
FONT = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"
INK, MUTED, LINE, BG = "#202124", "#5f6368", "#e8eaed", "#f4f5f7"
GREEN, RED = "#137333", "#c5221f"
TONES = {"report": "#1a73e8", "action": "#c5221f", "drawdown": "#e37400", "info": "#5f6368"}

Email = Tuple[str, str, str]   # (subject, text, html)


# --- formatting --------------------------------------------------------------

def _d(iso: str) -> str:
    """'2026-10-01' -> 'Thu Oct 1'."""
    d = date.fromisoformat(str(iso)[:10])
    return f"{d:%a %b} {d.day}"


def _md(iso: str) -> str:
    """'2026-09-04' -> 'Sep 4'."""
    d = date.fromisoformat(str(iso)[:10])
    return f"{d:%b} {d.day}"


def _money(x: float, signed: bool = False, cents: bool = False) -> str:
    body = f"${abs(x):,.2f}" if cents else f"${abs(x):,.0f}"
    if signed:
        return ("+" if x >= 0 else "-") + body
    return ("-" if x < 0 else "") + body


def _pct(x: float, digits: int = 2) -> str:
    return f"{x * 100:+.{digits}f}%"


def _pts(x: float) -> str:
    return f"{x * 100:+.2f}"


def _e(s: str) -> str:
    return _html.escape(str(s), quote=True)


def _color(x: float) -> str:
    return GREEN if x >= 0 else RED


def _week(iso: str) -> str:
    y, w, _ = date.fromisoformat(str(iso)[:10]).isocalendar()
    return f"{y}-W{w:02d}"


# --- performance (stateless, from daily closes) ------------------------------

def _level(series, iso: str) -> Optional[float]:
    import pandas as pd
    s = series.loc[:pd.Timestamp(iso)]
    return float(s.iloc[-1]) if len(s) else None


def compute_performance(closes: Dict[str, float], flows: Dict[str, float],
                        bench: Dict[str, object], as_of: str,
                        inception: str) -> Optional[dict]:
    """Time-weighted performance and benchmark comparison.

    closes: {session date: account equity at that close} (Alpaca daily history)
    flows:  {date: net external cash in (+) / out (-)}; a flow dated after one
            close and on or before the next is removed from that day's return
    bench:  {"QQQ": total-return close series (pandas, date-indexed), ...}
    Returns None when the closes do not cover as_of.
    """
    dates = sorted(d for d in closes if inception <= d <= as_of and closes[d])
    if not dates or dates[-1] != as_of:
        return None
    idx = {dates[0]: 1.0}
    for prev, d in zip(dates, dates[1:]):
        f = sum(v for k, v in flows.items() if prev < k <= d)
        e_prev = closes[prev]
        idx[d] = idx[prev] * (((closes[d] - f) / e_prev) if e_prev > 0 else 1.0)

    def window(label: str, base: Optional[str]) -> Optional[dict]:
        if base is None or base == as_of:
            return None
        row = {"label": label, "base": base, "horizon": idx[as_of] / idx[base] - 1.0}
        for b, series in bench.items():
            lo, hi = _level(series, base), _level(series, as_of)
            if lo and hi:
                row[b] = hi / lo - 1.0
        return row

    prev_week = [d for d in dates if _week(d) < _week(as_of)]
    prev_month = [d for d in dates if d[:7] < as_of[:7]]
    rows = [r for r in (
        window("Last session", dates[-2] if len(dates) > 1 else None),
        window("Week to date", prev_week[-1] if prev_week else dates[0]),
        window("Month to date", prev_month[-1] if prev_month else dates[0]),
        window(f"Since {_md(dates[0])}", dates[0]),
    ) if r]

    peak_date = max(dates, key=lambda d: (idx[d], d))
    flows_since_peak = any(peak_date < k <= as_of for k in flows)
    prev = dates[-2] if len(dates) > 1 else None
    chg = (closes[as_of] - closes[prev]
           - sum(v for k, v in flows.items() if prev < k <= as_of)) if prev else 0.0
    return {
        "as_of": as_of, "equity": closes[as_of], "inception": dates[0],
        "rows": rows, "chg_abs": chg,
        "chg_pct": rows[0]["horizon"] if rows and rows[0]["label"] == "Last session" else 0.0,
        "bench_session": {b: rows[0].get(b) for b in bench} if rows else {},
        "drawdown": idx[as_of] / idx[peak_date] - 1.0,
        "peak_date": peak_date,
        "peak_equity": None if flows_since_peak else closes[peak_date],
        "new_high": peak_date == as_of and len(dates) > 1,
    }


def drawdown_steps(state: dict, perf: dict) -> Tuple[dict, Optional[float]]:
    """Which 10% drawdown step (if any) was newly crossed. Steps re-arm when a
    new high is set (the peak date changes)."""
    st = json.loads(json.dumps(state or {}))
    if st.get("dd_peak_date") != perf["peak_date"]:
        st.update(dd_peak_date=perf["peak_date"], dd_alerted=[])
    crossed = [s for s in DRAWDOWN_STEPS
               if perf["drawdown"] <= -s + 1e-12 and s not in st["dd_alerted"]]
    if crossed:
        st["dd_alerted"] = sorted(set(st["dd_alerted"]) | set(crossed))
    return st, (max(crossed) if crossed else None)


def load_state(path: Path) -> dict:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_state(path: Path, st: dict) -> None:
    Path(path).write_text(json.dumps(st, indent=2), encoding="utf-8")


# --- HTML building blocks ------------------------------------------------------

def _page(*, kicker: str, title: str, tone: str, preheader: str, content: str,
          footer: str) -> str:
    accent = TONES.get(tone, TONES["info"])
    return (
        '<!doctype html><html><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="color-scheme" content="light"><meta name="supported-color-schemes" content="light">'
        f'<title>{_e(title)}</title></head>'
        f'<body style="margin:0;padding:0;background:{BG};">'
        f'<div style="display:none;max-height:0;overflow:hidden;opacity:0;">{_e(preheader)}</div>'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{BG};">'
        '<tr><td align="center" style="padding:16px 6px;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="max-width:600px;background:#ffffff;border-radius:10px;border-top:4px solid {accent};'
        f'font-family:{FONT};color:{INK};">'
        '<tr><td style="padding:20px 20px 4px 20px;">'
        f'<div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:{MUTED};">{_e(kicker)}</div>'
        f'<div style="font-size:19px;font-weight:600;margin-top:4px;line-height:1.3;">{_e(title)}</div>'
        '</td></tr>'
        f'{content}'
        f'<tr><td style="padding:14px 20px 20px 20px;font-size:11px;line-height:1.5;color:{MUTED};'
        f'border-top:1px solid {LINE};">{footer}</td></tr>'
        '</table></td></tr></table></body></html>'
    )


def _section(title: Optional[str], inner: str) -> str:
    head = (f'<div style="font-size:12px;font-weight:600;letter-spacing:.06em;text-transform:uppercase;'
            f'color:{MUTED};margin-bottom:8px;">{_e(title)}</div>') if title else ""
    return f'<tr><td style="padding:12px 20px;">{head}{inner}</td></tr>'


def _para(text: str, size: int = 14, color: str = INK) -> str:
    return (f'<p style="margin:0 0 10px 0;font-size:{size}px;line-height:1.5;color:{color};">'
            f'{_e(text)}</p>')


def _callout(lines: List[str], tone: str = "warn") -> str:
    bg, edge = ("#fef7e0", "#f9ab00") if tone == "warn" else ("#fce8e6", RED)
    items = "".join(f'<div style="margin:2px 0;">{_e(t)}</div>' for t in lines)
    return (f'<div style="background:{bg};border-left:4px solid {edge};border-radius:4px;'
            f'padding:10px 12px;font-size:13px;line-height:1.5;">{items}</div>')


def _num(x: float, fmt) -> str:
    return f'<span style="color:{_color(x)};">{_e(fmt(x))}</span>'


# --- evening report ------------------------------------------------------------

def _table(rows: List[Tuple[str, str]], muted_first: bool = True, width: int = 150) -> str:
    cells = "".join(
        f'<tr><td style="padding:3px 0;{f"color:{MUTED};" if muted_first else ""}width:{width}px;'
        f'vertical-align:top;">{_e(k)}</td><td style="padding:3px 0;">{v}</td></tr>' for k, v in rows)
    return f'<table role="presentation" cellpadding="0" cellspacing="0" style="font-size:13px;">{cells}</table>'


def _order_rows(orders: List[dict], amount_key: str) -> List[Tuple[str, str, str]]:
    out = []
    for o in sorted(orders, key=lambda o: (o["side"] != "sell", -float(o.get("notional") or 0))):
        verb = "Sold" if o["side"] == "sell" else "Bought"
        amt = o.get(amount_key)
        out.append((verb, o["symbol"], _money(amt) if amt is not None else
                    f"{_money(o.get('notional') or 0)} (not filled)"))
    return out


def build_evening_report(*, day: str, perf: Optional[dict], equity: float,
                         dollars: Optional[dict], trades: List[dict],
                         trade_reason: Optional[str], cycle_note: Optional[str],
                         outlook: Optional[dict], holdings: Dict[str, float], cash: float,
                         signals: Optional[dict], prev_regime: Optional[str],
                         tax: Optional[dict], warnings: List[str]) -> Email:
    """The once-a-day report, sent after the close. See engine/evening.py."""
    rows = [dict(r, label="Today" if r["label"] == "Last session" else r["label"])
            for r in ((perf or {}).get("rows") or [])]
    today = rows[0] if rows and rows[0]["label"] == "Today" else None
    n = len(trades)
    act = "held" if n == 0 else f"{n} trade{'s' if n != 1 else ''}"
    warn_tag = f" · {len(warnings)} warning{'s' if len(warnings) != 1 else ''}" if warnings else ""
    if today:
        subject = (f"{_d(day)}: {_pct(today['horizon'])} · QQQ {_pct(today.get('QQQ', 0.0))} · "
                   f"SPY {_pct(today.get('SPY', 0.0))} · {act}{warn_tag}")
    else:
        subject = f"{_d(day)}: {_money(equity)} · {act}{warn_tag}"

    # ---- shared phrases ----
    if perf is None:
        dd_line = ""
    elif perf["new_high"]:
        dd_line = "A new high."
    elif perf["drawdown"] > -0.0005:
        dd_line = "At its high."
    else:
        hi = f" of {_money(perf['peak_equity'])}" if perf.get("peak_equity") else ""
        dd_line = f"{abs(perf['drawdown']):.1%} below the {_md(perf['peak_date'])} high{hi}."
    money_line = None
    if dollars:
        money_line = (f"Since {_md(dollars['since'])} in dollars: Horizon "
                      f"{_money(dollars['Horizon'], signed=True)}. The same money in QQQ: "
                      f"{_money(dollars['QQQ'], signed=True)}; in SPY: {_money(dollars['SPY'], signed=True)}.")
    if n:
        trades_line = f"{n} trade{'s' if n != 1 else ''} at the open. {trade_reason or ''}".strip()
    else:
        trades_line = cycle_note or "No trades this morning."
    trade_rows = _order_rows(trades, "filled")
    if outlook is None:
        out_line, out_rows = "Tomorrow's outlook is unavailable today.", []
    else:
        when = f"{_d(outlook['session'])}'s open" if outlook.get("session") else "the next open"
        if outlook["orders"]:
            out_line = (f"Expected at {when}: {len(outlook['orders'])} trade"
                        f"{'s' if len(outlook['orders']) != 1 else ''}. {outlook.get('reason') or ''} "
                        f"Final orders are set at 9:00 AM ET.").replace("  ", " ")
        else:
            out_line = (f"No trades expected at {when}. Holdings are {outlook['gap']:.1%} from "
                        f"target; the engine rebalances at {outlook['band']:.0%}.")
        out_rows = [(("Sell" if o["side"] == "sell" else "Buy"), o["symbol"], f"about {_money(o['notional'])}")
                    for o in sorted(outlook["orders"], key=lambda o: (o["side"] != "sell", -o["notional"]))]
    total = sum(holdings.values()) + cash
    nas = (sum(mv * NASDAQ_WEIGHTS.get(s, 0.0) for s, mv in holdings.items()) / total) if total > 0 else 0.0
    sig_rows = []
    if signals:
        reg = f"{signals['regime']} ({signals['score']:.0f})"
        if prev_regime and prev_regime != signals["regime"]:
            reg += f", changed from {prev_regime}"
        rot = " + ".join(signals["rotation"]) if signals["rotation"] else "T-bills"
        if signals.get("next_review"):
            rot += f"; next review {_d(signals['next_review'])} close"
        sig_rows = [("Market regime", reg), ("PULSE leverage", f"{signals['pulse_lev']:.2f}x target"),
                    ("ROTATION holds", rot)]
    tax_rows, tax_note = [], None
    if tax:
        st, lt = tax["st"], tax["lt"]
        est, div_tax = tax["est_tax"], tax.get("div_tax", 0.0)
        signed = lambda x: _money(x, signed=True) if abs(x) >= 0.5 else "$0"
        if st + lt > 0:
            est_txt = _money(est)
        else:
            est_txt = "none on trading (net loss so far)"
            if div_tax >= 1:
                est_txt += f"; about {_money(div_tax)} on dividends"
        tax_rows = [("Realized short-term", signed(st)),
                    ("Realized long-term", signed(lt)),
                    ("Dividends", _money(tax["dividends"])),
                    (f"Est. federal tax ({tax['rates'][0]:.0%} / {tax['rates'][1]:.0%})", est_txt),
                    ("Unrealized on holdings", _money(tax["unrealized_st"] + tax["unrealized_lt"], signed=True))]
        tax_note = ("Estimate for this account only, before losses from other accounts or prior "
                    "years and state tax. Alpaca's 1099 is the official figure.")

    # ---- plain text ----
    T = [f"Equity at {_d(day)} close: {_money(equity, cents=True)}"]
    if today:
        T.append(f"Today: {_money(perf['chg_abs'], signed=True, cents=True)} ({_pct(today['horizon'])}). {dd_line}".strip())
    if rows:
        T += ["", "Performance (Horizon / QQQ / SPY; then Horizon minus each, in points):"]
        for r in rows:
            q, s = r.get("QQQ"), r.get("SPY")
            line = (f"- {r['label']}: {_pct(r['horizon'])} / {_pct(q) if q is not None else 'n/a'} / "
                    f"{_pct(s) if s is not None else 'n/a'}")
            if q is not None and s is not None:
                line += f" (vs QQQ {_pts(r['horizon'] - q)}, vs SPY {_pts(r['horizon'] - s)})"
            T.append(line)
        if money_line:
            T.append(money_line)
    if warnings:
        T += ["", "Warnings:"] + [f"- {w}" for w in warnings]
    T += ["", f"Trades today: {trades_line}"] + [f"- {v} {s} {a}" for v, s, a in trade_rows]
    T += ["", f"Tomorrow: {out_line}"] + [f"- {v} {s} {a}" for v, s, a in out_rows]
    if total > 0:
        T += ["", "Holdings at the close:"]
        T += [f"- {s} {_money(mv)} ({mv / total:.0%})" for s, mv in sorted(holdings.items(), key=lambda kv: -kv[1])]
        T += [f"- Cash {_money(cash)} ({cash / total:.0%})", f"Nasdaq-100 exposure: {nas:.2f}x (QLD counts double)."]
    if tax_rows:
        T += ["", f"Taxes, {tax['year']} to date:"] + [f"- {k}: {v}" for k, v in tax_rows] + [tax_note]
    if sig_rows:
        T += ["", "Signals at today's close:"] + [f"- {k}: {v}" for k, v in sig_rows]
    text = "\n".join(T)

    # ---- HTML ----
    hero = (f'<div style="font-size:30px;font-weight:700;letter-spacing:-.01em;">{_e(_money(equity, cents=True))}</div>'
            f'<div style="font-size:13px;color:{MUTED};margin-top:2px;">Equity at the {_e(_d(day))} close</div>')
    if today:
        hero += (f'<div style="font-size:15px;margin-top:8px;">'
                 f'<span style="color:{_color(perf["chg_abs"])};font-weight:600;">'
                 f'{_e(_money(perf["chg_abs"], signed=True, cents=True))} ({_e(_pct(today["horizon"]))})</span>'
                 f'<span style="color:{MUTED};"> today</span></div>')
    chip_bg, chip_fg = ("#e8f0fe", "#1967d2") if n else ("#f1f3f4", MUTED)
    hero += (f'<div style="margin-top:10px;"><span style="display:inline-block;background:{chip_bg};'
             f'color:{chip_fg};font-size:12px;font-weight:600;padding:3px 10px;border-radius:12px;">'
             f'{_e("Held, no trades" if n == 0 else act.capitalize())}</span>'
             f'<span style="font-size:12px;color:{MUTED};margin-left:8px;">{_e(dd_line)}</span></div>')
    content = _section(None, hero)

    if rows:
        th = (f'style="padding:6px 4px;font-size:11px;font-weight:600;color:{MUTED};'
              f'border-bottom:1px solid {LINE};text-align:right;"')
        head = (f'<tr><th width="30%" style="padding:6px 4px;font-size:11px;font-weight:600;'
                f'color:{MUTED};border-bottom:1px solid {LINE};text-align:left;">Period</th>'
                f'<th width="20%" {th}>Horizon</th><th width="25%" {th}>vs QQQ</th>'
                f'<th width="25%" {th}>vs SPY</th></tr>')
        td = f'style="padding:8px 4px;border-bottom:1px solid {LINE};text-align:right;vertical-align:top;"'

        def vs_cell(r, b):
            v = r.get(b)
            if v is None:
                return f'<td {td}>n/a</td>'
            gap = r["horizon"] - v
            return (f'<td {td}><div style="font-weight:700;color:{_color(gap)};">{_e(_pts(gap))}</div>'
                    f'<div style="font-size:11px;color:{MUTED};margin-top:1px;white-space:nowrap;">{_e(_pct(v))}</div></td>')
        body = "".join(
            f'<tr><td style="padding:8px 4px;border-bottom:1px solid {LINE};vertical-align:top;">{_e(r["label"])}</td>'
            f'<td {td}><span style="font-weight:700;color:{_color(r["horizon"])};">{_e(_pct(r["horizon"]))}</span></td>'
            f'{vs_cell(r, "QQQ")}{vs_cell(r, "SPY")}</tr>' for r in rows)
        perf_html = (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
                     f'style="border-collapse:collapse;font-size:13px;table-layout:fixed;">{head}{body}</table>'
                     f'<div style="font-size:11px;color:{MUTED};margin-top:6px;line-height:1.4;">'
                     f'vs QQQ and vs SPY: Horizon\'s return minus the benchmark\'s, in percentage points; '
                     f'green means Horizon is ahead. The gray figure is the benchmark\'s own return.</div>')
        if dollars:
            cell = lambda label, v, bold=False: (
                f'<td style="padding:6px 4px;text-align:center;width:33%;">'
                f'<div style="font-size:11px;color:{MUTED};">{_e(label)}</div>'
                f'<div style="font-size:15px;font-weight:{700 if bold else 600};color:{_color(v)};">'
                f'{_e(_money(v, signed=True))}</div></td>')
            perf_html += (f'<div style="font-size:12px;color:{MUTED};margin:12px 0 4px 0;">'
                          f'Since {_e(_md(dollars["since"]))} in dollars: Horizon vs the same money in each</div>'
                          f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
                          f'style="background:{BG};border-radius:6px;"><tr>'
                          f'{cell("Horizon", dollars["Horizon"], True)}{cell("QQQ", dollars["QQQ"])}'
                          f'{cell("SPY", dollars["SPY"])}</tr></table>')
        content += _section("Performance vs QQQ and SPY", perf_html)

    if warnings:
        content += _section("Warnings", _callout(warnings))

    def order_table(rws, green_word):
        return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:14px;">'
                + "".join(f'<tr><td style="padding:4px 0;color:{GREEN if v == green_word else RED};font-weight:600;width:70px;">{_e(v)}</td>'
                          f'<td style="padding:4px 0;font-weight:600;">{_e(s)}</td>'
                          f'<td style="padding:4px 0;text-align:right;">{_e(a)}</td></tr>' for v, s, a in rws)
                + '</table>')
    content += _section("Trades today", _para(trades_line) + (order_table(trade_rows, "Bought") if trade_rows else ""))
    title_tom = f"Tomorrow's open" + (f" · {_d(outlook['session'])}" if outlook and outlook.get("session") else "")
    content += _section(title_tom, _para(out_line) + (order_table(out_rows, "Buy") if out_rows else ""))

    if total > 0:
        def hrow(name: str, mv: float, color: str) -> str:
            w = max(0.0, min(1.0, mv / total))
            return (f'<tr><td style="padding:5px 0;font-weight:600;width:56px;">{_e(name)}</td>'
                    f'<td style="padding:5px 8px;text-align:right;width:80px;">{_e(_money(mv))}</td>'
                    f'<td style="padding:5px 0;"><div style="background:{BG};border-radius:3px;height:8px;">'
                    f'<div style="width:{w * 100:.1f}%;background:{color};height:8px;border-radius:3px;"></div></div></td>'
                    f'<td style="padding:5px 0 5px 8px;text-align:right;width:40px;color:{MUTED};">{w:.0%}</td></tr>')
        hrows = "".join(hrow(s, mv, TONES["report"]) for s, mv in sorted(holdings.items(), key=lambda kv: -kv[1]))
        hrows += hrow("Cash", cash, "#9aa0a6")
        content += _section("Holdings at the close", (
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:13px;">{hrows}</table>'
            f'<div style="font-size:13px;margin-top:8px;">Nasdaq-100 exposure <b>{nas:.2f}x</b>'
            f'<span style="color:{MUTED};"> (QLD counts double)</span></div>'))

    if tax_rows:
        content += _section(f"Taxes, {tax['year']} to date", _table([(k, _e(v)) for k, v in tax_rows], width=190)
                            + f'<div style="font-size:11px;color:{MUTED};margin-top:6px;line-height:1.4;">{_e(tax_note)}</div>')
    if sig_rows:
        content += _section("Signals at today's close", _table([(k, _e(v)) for k, v in sig_rows], width=130))

    since = f" since {_md(perf['inception'])}, when Horizon took over the whole account" if perf else ""
    footer = (f"Sent after the close. Returns are time-weighted from the account's closing equity{_e(since)}, "
              f"so deposits and withdrawals are not gains or losses. Today's equity is priced at the same "
              f"official closes as QQQ and SPY, which include dividends. The 9:00 AM ET cycle trades at the open.")
    pre = (f"Horizon {_pct(today['horizon'])} today vs QQQ {_pct(today.get('QQQ', 0))} and SPY "
           f"{_pct(today.get('SPY', 0))}. {trades_line}") if today else trades_line
    html = _page(kicker=f"Horizon · {_d(day)} close", title="Daily report", tone="report",
                 preheader=pre, content=content, footer=footer)
    return subject, text, html


# --- event emails --------------------------------------------------------------

def _event(*, kicker: str, title: str, tone: str, paragraphs: List[str],
           callout: Optional[List[str]] = None, detail: Optional[str] = None,
           subject: Optional[str] = None) -> Email:
    content = _section(None, "".join(_para(p) for p in paragraphs)
                       + (_callout(callout, "action" if tone == "action" else "warn") if callout else ""))
    if detail:
        content += _section("Technical detail", (
            f'<pre style="margin:0;white-space:pre-wrap;word-break:break-word;font-size:11px;line-height:1.4;'
            f'background:{BG};padding:10px;border-radius:4px;color:{INK};">{_e(detail)}</pre>'))
    text = "\n\n".join(paragraphs + (["\n".join(callout)] if callout else [])
                       + ([f"Technical detail:\n{detail}"] if detail else []))
    html = _page(kicker=kicker, title=title, tone=tone, preheader=paragraphs[0] if paragraphs else title,
                 content=content, footer="Sent by Horizon on Railway.")
    return subject or title, text, html


def build_drawdown_alert(step: float, perf: dict) -> Email:
    dd = perf["drawdown"]
    within = abs(dd) <= BACKTEST_MAX_DD
    hi = f" of {_money(perf['peak_equity'])}" if perf.get("peak_equity") else ""
    return _event(
        kicker="Horizon · Drawdown", tone="drawdown",
        title=f"Down {abs(dd):.1%} from the high",
        subject=f"Drawdown passed -{step:.0%}: now {_pct(dd, 1)} from the high",
        paragraphs=[
            f"Equity of {_money(perf['equity'], cents=True)} at the {_d(perf['as_of'])} close is "
            f"{abs(dd):.1%} below the {_md(perf['peak_date'])} high{hi}, net of deposits and withdrawals.",
            f"For reference, the worst drawdown in the 2008-2026 backtest of this exact setup was "
            f"about -{BACKTEST_MAX_DD:.0%}, and a dot-com-style bust is estimated at about -{STRESS_DD:.0%}. "
            + ("This is within the backtested range." if within else "This is beyond the worst backtested drawdown."),
            "The engine needs no action from you: PULSE cuts leverage automatically as volatility rises. "
            "You get one of these at each 10% step, and the steps reset at the next new high.",
        ])


def build_report_failed(tb: str) -> Email:
    last = next((ln.strip() for ln in reversed(tb.strip().splitlines()) if ln.strip()), "unknown error")
    return _event(
        kicker="Horizon · Action needed", title="Today's report failed", tone="action",
        paragraphs=["The evening report could not be built, so today's summary is missing. "
                    "Trading is not affected: the next 9:00 AM ET cycle runs as usual.",
                    "Check the account directly in Alpaca for today's figures."],
        callout=[f"Error: {last}"], detail=tb.strip())


def build_restart(when_et: str, equity: Optional[float], next_cycle: str, live: bool) -> Email:
    paras = [f"Horizon restarted at {when_et} and is "
             f"{'trading live' if live else 'in DRY-RUN, placing no real orders'}."]
    if equity is not None:
        paras.append(f"Equity: {_money(equity, cents=True)}. Next: {next_cycle}. Trading cycles "
                     f"run at 9:00 AM ET and the daily report arrives 30 minutes after the close.")
    else:
        paras.append(f"Next: {next_cycle}.")
    paras.append("Restarts happen on every deploy. If nothing was deployed, the service crashed "
                 "and Railway restarted it; the Railway logs show why.")
    return _event(kicker="Horizon · Service", title="Restarted", tone="info", paragraphs=paras,
                  subject=f"Restarted · next: {next_cycle}")


def build_cycle_failed(tb: str) -> Email:
    last = next((ln.strip() for ln in reversed(tb.strip().splitlines()) if ln.strip()), "unknown error")
    return _event(
        kicker="Horizon · Action needed", title="Today's cycle failed", tone="action",
        paragraphs=["Today's 9:00 AM ET cycle stopped with an error.",
                    "Orders submitted before the error may have executed; nothing after it was placed. "
                    "The engine will not retry today. The next attempt is the next weekday's 9:00 AM ET "
                    "cycle, so check the account before then."],
        callout=[f"Error: {last}"], detail=tb.strip())


def build_stale_data(as_of: str, completed: str, limit_days: int) -> Email:
    return _event(
        kicker="Horizon · Action needed", title="No trades today: market data is stale", tone="action",
        subject="No trades today, market data is stale",
        paragraphs=[f"Horizon refused to trade today. Its market data ends on {_d(as_of)}, but the last "
                    f"completed session was {_d(completed)} (the limit is {limit_days} days).",
                    "No orders were placed and positions are unchanged. It tries again at the next "
                    "9:00 AM ET cycle."],
        callout=["Usual causes: a Polygon outage, or an invalid POLYGON_API_KEY on the horizon-live "
                 "Railway service."])


def build_auth_failed(err: str) -> Email:
    return _event(
        kicker="Horizon · Action needed", title="Horizon cannot connect to Alpaca and is not running",
        tone="action",
        paragraphs=["Alpaca rejected Horizon's API keys at startup, so the engine stopped. It will not "
                    "trade until this is fixed. Railway retries the start up to 10 times."],
        callout=["Fix: check ALPACA_API_KEY and ALPACA_SECRET_KEY on the horizon-live Railway service. "
                 "They must be the LIVE account keys."],
        detail=f"Error: {err}")


def build_conflict(conflicts) -> Email:
    return _event(
        kicker="Horizon · Action needed", title="Ownership conflict: new buys blocked", tone="action",
        subject="Ownership conflict, new buys blocked",
        paragraphs=[f"Two owners claim the same position: {conflicts}.",
                    "Horizon blocked new buys until this is resolved; sells still run. It usually means "
                    "another engine or a manual trade touched a Horizon symbol."])


def build_flatten(msg: str) -> Email:
    return _event(
        kicker="Horizon · Action needed", title="Emergency flatten executed", tone="action",
        paragraphs=[msg, "Trading stays halted until the kill-switch file is removed."])
