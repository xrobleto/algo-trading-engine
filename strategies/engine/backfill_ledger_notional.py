#!/usr/bin/env python3
"""
One-shot: correct notional_at_entry in the ownership ledger using actual fill
data. Fixes the qty*100 fallback that fired for new TREND/CROSSASSET positions
on rebalance days before the adapter patch.

Usage (inside Railway container):
    python3 /app/strategies/engine/backfill_ledger_notional.py
    python3 /app/strategies/engine/backfill_ledger_notional.py --dry-run
    python3 /app/strategies/engine/backfill_ledger_notional.py --path /data/data/state/engine_ownership_live.json
"""
import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_PATH = Path("/data/data/state/engine_ownership_live.json")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, default=DEFAULT_PATH)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.01,
        help="Skip entries whose old/new notional differ by less than this ($).",
    )
    args = parser.parse_args()

    if not args.path.exists():
        print(f"ERROR: ledger not found at {args.path}", file=sys.stderr)
        return 1

    raw = args.path.read_text()
    data = json.loads(raw)
    entries = data.get("entries", {})

    fixes = []
    for coid, entry in entries.items():
        if entry.get("status") != "filled":
            continue
        fill_qty = entry.get("fill_qty")
        fill_price = entry.get("fill_price")
        if not fill_qty or not fill_price:
            continue
        old = float(entry.get("notional_at_entry", 0.0))
        new = float(fill_qty) * float(fill_price)
        if abs(new - old) < args.tolerance:
            continue
        fixes.append((entry.get("symbol", "?"), entry.get("strategy_id", "?"), old, new, coid))
        if not args.dry_run:
            entry["notional_at_entry"] = new

    if not fixes:
        print("No corrections needed.")
        return 0

    print(f"{'(dry-run) ' if args.dry_run else ''}Will correct {len(fixes)} entries:")
    print(f"  {'SYMBOL':<8} {'SLEEVE':<12} {'OLD':>12} {'NEW':>12}  {'DELTA':>12}")
    sleeve_delta = {}
    for sym, sleeve, old, new, _coid in fixes:
        delta = new - old
        sleeve_delta[sleeve] = sleeve_delta.get(sleeve, 0.0) + delta
        print(f"  {sym:<8} {sleeve:<12} {old:>12.2f} {new:>12.2f}  {delta:>+12.2f}")

    print("\nNet change by sleeve:")
    for sleeve, delta in sleeve_delta.items():
        print(f"  {sleeve}: {delta:+.2f}")

    if args.dry_run:
        print("\nDry run — no file modified.")
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = args.path.with_name(args.path.name + f".bak.{ts}")
    shutil.copyfile(args.path, backup)
    print(f"\nBackup written: {backup}")

    args.path.write_text(json.dumps(data, indent=2))
    print(f"Updated: {args.path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
