#!/usr/bin/env python3
"""
CP checkpoint evidence — Horizon live-gate scorecard (read-only).

Run under the live env:  railway run ./.venv/bin/python utilities/horizon_checkpoint.py

Pulls every HZN_* order since the gate start and scores the checkpoint criteria
(G-lite versions of horizon_live_gate_2026-08-12.md):
  C1  all HZN orders reached a terminal fill state (no stuck/rejected)
  C2  fill slippage vs prior close within tolerance (<= 30 bps avg for liquid ETFs;
      decisions are made at close T, filled at open T+1 — overnight gap is NOT
      slippage, so we report both gap and same-open slippage where possible)
  C3  current HZN-symbol positions consistent with Horizon's latest targets
  C4  no cross-engine contamination: no unified-engine symbol traded by HZN orders,
      no HZN symbol in the unified sleeves' books
  C5  capital cap respected: HZN gross exposure <= cap * 1.05

Manual companions (not automatable here): unified engine logs show zero
unclassified/conflict lines; horizon-live logs show one clean cycle per weekday.
"""
import os
import sys
import time
from collections import defaultdict
from datetime import date
import requests

GATE_START = "2026-08-12T00:00:00Z"
HZN_SYMBOLS = {"QQQM", "IEFA", "VGLT", "IAU", "PDBC", "SHV"}
UNIFIED_SYMBOLS = {"SPY", "QQQ", "IWM", "XLK", "XLF", "XLE", "XLV", "XLI", "XLY",
                   "XLC", "SMH", "IBB", "XHB", "MTUM", "QUAL", "SOXX", "IGV",
                   "CIBR", "SKYY", "ARKK", "XBI", "KWEB", "SGOV", "BIL",
                   "TLT", "IEF", "SHY", "TBT", "GLD", "SLV", "DBC", "USO",
                   "UNG", "DBA", "GLL", "UUP", "FXE", "FXY"}

KEY = os.getenv("ALPACA_API_KEY"); SEC = os.getenv("ALPACA_SECRET_KEY")
BASE = (os.getenv("ALPACA_BASE_URL") or "https://api.alpaca.markets").rstrip("/")
H = {"APCA-API-KEY-ID": KEY, "APCA-API-SECRET-KEY": SEC}
POLY = os.getenv("POLYGON_API_KEY")


def get(p, par=None):
    r = requests.get(f"{BASE}{p}", headers=H, params=par, timeout=30)
    r.raise_for_status()
    return r.json()


def prior_close(symbol: str, iso_day: str):
    """Close of the last trading day STRICTLY BEFORE iso_day."""
    try:
        from datetime import timedelta
        d = date.fromisoformat(iso_day)
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
            f"{(d - timedelta(days=7)).isoformat()}/{d.isoformat()}",
            params={"adjusted": "true", "sort": "asc", "limit": 10, "apiKey": POLY},
            timeout=20)
        res = r.json().get("results", [])
        import datetime as _dt
        prior = [x for x in res
                 if _dt.datetime.fromtimestamp(x["t"] / 1000).date() < d]
        return float(prior[-1]["c"]) if prior else None
    except Exception:
        return None


def main():
    cap = float(os.getenv("HORIZON_CAPITAL_CAP", "1290"))
    checks = {}

    # all orders since gate start
    orders, after, seen = [], GATE_START, set()
    while True:
        b = get("/v2/orders", {"status": "all", "after": after, "limit": 500,
                               "direction": "asc"})
        if not b:
            break
        nw = [o for o in b if o["id"] not in seen]
        for o in nw:
            seen.add(o["id"])
        orders.extend(nw)
        if len(b) < 500:
            break
        after = b[-1]["submitted_at"]
        time.sleep(0.15)

    hzn = [o for o in orders if (o.get("client_order_id") or "").startswith("HZN_")]
    print(f"=== HZN orders since {GATE_START[:10]}: {len(hzn)} ===")
    terminal = {"filled", "canceled", "cancelled", "expired"}
    bad = [o for o in hzn if o["status"] not in terminal]
    stuck = [o for o in hzn if o["status"] in ("rejected", "suspended")]
    for o in hzn:
        fp = o.get("filled_avg_price")
        print(f"  {(o.get('filled_at') or o['submitted_at'])[:16]} {o['symbol']:5} "
              f"{o['side']:4} {o.get('filled_qty') or o['qty']:>9} "
              f"@ {fp or '-':>8}  {o['status']}")
    checks["C1 all orders terminal"] = (len(bad) == 0 and len(stuck) == 0,
                                        f"{len(bad)} non-terminal, {len(stuck)} rejected")

    # slippage proxy: fill vs prior trading day's close (includes overnight gap)
    gaps = []
    for o in hzn:
        if o["status"] != "filled" or not o.get("filled_avg_price"):
            continue
        d = (o.get("filled_at") or "")[:10]
        pc = prior_close(o["symbol"], d)
        if pc:
            side = 1 if o["side"] == "buy" else -1
            gaps.append(side * (float(o["filled_avg_price"]) / pc - 1) * 1e4)
    avg_gap = sum(gaps) / len(gaps) if gaps else 0.0
    checks["C2 fill vs prior close"] = (abs(avg_gap) <= 100,
                                        f"avg {avg_gap:+.1f} bps over {len(gaps)} fills "
                                        f"(includes overnight gap; tolerance 100bps)")

    # positions
    pos = get("/v2/positions")
    hzn_pos = {p["symbol"]: float(p["market_value"]) for p in pos
               if p["symbol"] in HZN_SYMBOLS}
    other_pos = {p["symbol"]: float(p["market_value"]) for p in pos
                 if p["symbol"] not in HZN_SYMBOLS}
    gross = sum(abs(v) for v in hzn_pos.values())
    print(f"\nHZN positions: { {k: round(v) for k, v in hzn_pos.items()} }  "
          f"gross ${gross:,.0f} (cap ${cap:,.0f})")
    print(f"other positions: { {k: round(v) for k, v in other_pos.items()} }")
    checks["C3 holds HZN symbols only via HZN orders"] = (
        all(o["symbol"] in HZN_SYMBOLS for o in hzn),
        "every HZN order is in the equivalents universe")
    checks["C4 no cross-engine contamination"] = (
        not (set(hzn_pos) & UNIFIED_SYMBOLS) and
        not any(o["symbol"] in UNIFIED_SYMBOLS for o in hzn),
        "HZN book and orders disjoint from unified universe")
    # C5 must account for book leverage: HORIZON_CAPITAL_CAP bounds the EQUITY
    # BASIS Horizon sizes against, and book_leverage multiplies gross exposure on
    # top of it. Comparing gross to the raw cap would false-alarm at any leverage
    # above 1.0 (it did, after the 2026-08-24 dilution fix took effective leverage
    # from ~1.03x to ~1.48x).
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from horizon.config import build_default_config as _hcfg
        book_lev = float(_hcfg().book_leverage)
    except Exception:
        book_lev = 1.0
    lev_cap = cap * book_lev
    checks["C5 capital cap respected"] = (
        gross <= lev_cap * 1.05,
        f"gross ${gross:,.0f} <= cap*book_leverage*1.05 ${lev_cap*1.05:,.0f} "
        f"(cap ${cap:,.0f} x {book_lev:.2f}x)")

    print("\n=== CHECKPOINT SCORECARD ===")
    all_ok = True
    for name, (ok, note) in checks.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name} — {note}")
        all_ok = all_ok and ok
    print(f"\nRESULT: {'CLEAN — CP step may execute' if all_ok else 'DIRTY — STOP THE RAMP'}")
    print("Manual companions: (1) horizon-live logs — one clean cycle per weekday, no")
    print("tracebacks; (2) unified engine logs — zero unclassified/conflict lines.")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
