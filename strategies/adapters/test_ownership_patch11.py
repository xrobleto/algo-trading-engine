"""
Regression tests for Patch 11 — ownership ledger `notional_at_entry` correction.

The bug: trend_adapter and cross_asset_adapter compute a pre-submit notional
estimate via `qty * get_open_position(symbol).current_price`. For new positions
that don't yet exist at the broker, the call raises and the adapters fall back
to `qty * 100`. That placeholder was being persisted into the ledger as
`notional_at_entry`, corrupting `get_deployed_notional()` and the sleeve %
shown in engine tick logs.

The fix: `OwnershipLedger.update_status()` now accepts a `notional` kwarg, and
both adapters call `update_status(..., notional=qty * fill_price)` post-submit
to overwrite the placeholder with the actual fill notional.

These tests pin down:
  1. The new kwarg semantics on `update_status`.
  2. The regression scenario end-to-end: register with buggy notional, advance
     to filled with corrected notional, verify get_deployed_notional reflects it.
  3. The backfill script behavior (filled-only, idempotent, leaves a backup).

Run:
    python strategies/adapters/test_ownership_patch11.py
    pytest strategies/adapters/test_ownership_patch11.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_STRATEGIES_DIR = _REPO_ROOT / "strategies"
if str(_STRATEGIES_DIR) not in sys.path:
    sys.path.insert(0, str(_STRATEGIES_DIR))

from engine.ownership import OwnershipLedger  # noqa: E402

_BACKFILL_SCRIPT = _REPO_ROOT / "strategies" / "engine" / "backfill_ledger_notional.py"


# ---------------------------------------------------------------------------
# update_status notional kwarg
# ---------------------------------------------------------------------------

def test_update_status_writes_notional() -> None:
    """Passing notional=X overwrites entry.notional_at_entry."""
    ledger = OwnershipLedger()
    ledger.register_order(
        strategy_id="TREND", symbol="SMH", side="buy", qty=2.121,
        client_order_id="COID", notional=212.10,  # the qty*100 fallback
    )
    ledger.update_status(
        "COID", "filled",
        fill_price=505.83, fill_qty=2.121,
        notional=2.121 * 505.83,
    )
    entry = ledger.entries["COID"]
    assert abs(entry.notional_at_entry - 1072.87) < 0.01, entry.notional_at_entry
    assert entry.fill_price == 505.83
    assert entry.fill_qty == 2.121
    assert entry.status == "filled"
    print("PASS: test_update_status_writes_notional")


def test_update_status_notional_omitted_recomputes_from_fill() -> None:
    """Patch 16: when caller omits notional but fill_price + fill_qty are both
    known, notional_at_entry is recomputed as fill_price * fill_qty. Tightens
    the original Patch 11 contract because the reconciler updates fill_price
    without re-supplying notional, and the stale qty*100 placeholder must not
    survive that update."""
    ledger = OwnershipLedger()
    ledger.register_order(
        strategy_id="X", symbol="Y", side="buy", qty=1.0,
        client_order_id="C", notional=999.0,
    )
    ledger.update_status("C", "filled", fill_price=10.0, fill_qty=1.0)
    # 10.0 * 1.0 = 10.0, replacing the stale 999.0 placeholder
    assert ledger.entries["C"].notional_at_entry == 10.0
    print("PASS: test_update_status_notional_omitted_recomputes_from_fill")


def test_update_status_notional_none_recomputes_from_fill() -> None:
    """Patch 16: explicit notional=None has the same self-heal semantics as
    omission — the reconciler call sites pass through optional kwargs, so
    None must not be a sentinel that disables the recompute."""
    ledger = OwnershipLedger()
    ledger.register_order(
        strategy_id="X", symbol="Y", side="buy", qty=1.0,
        client_order_id="C", notional=777.0,
    )
    ledger.update_status("C", "filled", fill_price=10.0, fill_qty=1.0, notional=None)
    assert ledger.entries["C"].notional_at_entry == 10.0
    print("PASS: test_update_status_notional_none_recomputes_from_fill")


def test_update_status_explicit_notional_wins() -> None:
    """An explicit notional= argument always overrides the auto-recompute."""
    ledger = OwnershipLedger()
    ledger.register_order(
        strategy_id="X", symbol="Y", side="buy", qty=1.0,
        client_order_id="C", notional=0.0,
    )
    # fill_price * fill_qty would be 50, but caller insists on 42
    ledger.update_status("C", "filled", fill_price=10.0, fill_qty=5.0, notional=42.0)
    assert ledger.entries["C"].notional_at_entry == 42.0
    print("PASS: test_update_status_explicit_notional_wins")


def test_update_status_recompute_skipped_when_fill_data_absent() -> None:
    """Self-heal must NOT fire when fill_price or fill_qty is unknown — that
    would zero out the registration-time estimate before the broker confirms.
    Critical for mark_closed (no fill data passed) and pending->cancelled."""
    ledger = OwnershipLedger()
    ledger.register_order(
        strategy_id="X", symbol="Y", side="buy", qty=1.0,
        client_order_id="C", notional=500.0,
    )
    # Status-only update, no fill data: must preserve registration notional
    ledger.update_status("C", "cancelled")
    assert ledger.entries["C"].notional_at_entry == 500.0
    print("PASS: test_update_status_recompute_skipped_when_fill_data_absent")


def test_mark_closed_does_not_touch_notional() -> None:
    """mark_closed routes through update_status without the notional kwarg —
    must leave notional_at_entry intact so historical entries remain auditable."""
    ledger = OwnershipLedger()
    ledger.register_order(
        strategy_id="X", symbol="Y", side="buy", qty=1.0,
        client_order_id="C", notional=500.0,
    )
    ledger.update_status("C", "filled", fill_price=50.0, fill_qty=1.0, notional=50.0)
    ledger.mark_closed("C")
    assert ledger.entries["C"].status == "closed"
    assert ledger.entries["C"].notional_at_entry == 50.0
    assert ledger.entries["C"].closed_at is not None
    print("PASS: test_mark_closed_does_not_touch_notional")


# ---------------------------------------------------------------------------
# Regression: full qty*100 → corrected lifecycle
# ---------------------------------------------------------------------------

def test_regression_qty100_fallback_corrected_post_fill() -> None:
    """Replay of 2026-04-24 TREND rebalance: SMH and SOXX registered with the
    qty*100 fallback notional, then post-submit update_status overwrites with
    the real qty*fill_price. get_deployed_notional must reflect the corrected
    sum, not the placeholder sum."""
    ledger = OwnershipLedger()

    # Register with the buggy qty*100 placeholder (mirrors trend_adapter line 336)
    ledger.register_order(
        strategy_id="TREND", symbol="SMH", side="buy", qty=2.121023,
        client_order_id="ENG_TREND_SMH", notional=2.121023 * 100,
    )
    ledger.register_order(
        strategy_id="TREND", symbol="SOXX", side="buy", qty=2.021720,
        client_order_id="ENG_TREND_SOXX", notional=2.021720 * 100,
    )

    # Pre-correction: deployed notional matches the buggy placeholder ~$414
    pre = ledger.get_deployed_notional("TREND")
    # Note: get_deployed_notional only sums status='filled' entries. The
    # registrations above leave status='pending', so pre is 0.
    assert pre == 0.0, pre

    # Advance both to filled with corrected notional (mirrors patched
    # trend_adapter.py:415-420)
    ledger.update_status(
        "ENG_TREND_SMH", "filled",
        fill_price=505.83, fill_qty=2.121023,
        notional=2.121023 * 505.83,
    )
    ledger.update_status(
        "ENG_TREND_SOXX", "filled",
        fill_price=459.92, fill_qty=2.021720,
        notional=2.021720 * 459.92,
    )

    # Post-correction: deployed notional reflects real fill values (~$2,000)
    post = ledger.get_deployed_notional("TREND")
    assert 1990 < post < 2010, (
        f"deployed_notional should be ~$2,000 (real fill value), got ${post:.2f}"
    )

    # Sanity: had we left the placeholder in place, deployed would be ~$414
    # (the value the engine tick was actually showing on 2026-04-27)
    placeholder_sum = 2.121023 * 100 + 2.021720 * 100
    assert 410 < placeholder_sum < 420
    assert abs(post - placeholder_sum) > 1500, (
        "regression: post-correction value should be ~5x larger than the "
        f"qty*100 placeholder (post=${post:.2f}, placeholder=${placeholder_sum:.2f})"
    )
    print(f"PASS: test_regression_qty100_fallback_corrected_post_fill  "
          f"(deployed corrected from ${placeholder_sum:.2f} → ${post:.2f})")


def test_regression_qty100_fallback_overestimates_small_priced() -> None:
    """The dual case: for low-priced ETFs (DBA, DBC, TBT) the qty*100
    placeholder *overestimates* notional. Fix must correct both directions.
    Replay of 2026-04-22 CROSSASSET rebalance."""
    ledger = OwnershipLedger()

    cases = [
        # (symbol, qty, fill_price, ledger_old_qty100)
        ("DBA", 8.0904, 27.2864, 809.04),
        ("DBC", 5.1686, 29.3764, 516.86),
        ("TBT", 6.3574, 34.7264, 635.74),
        ("USO", 0.4784, 127.784, 47.84),
    ]

    for sym, qty, fp, _old in cases:
        coid = f"ENG_XASSET_{sym}"
        ledger.register_order(
            strategy_id="CROSSASSET", symbol=sym, side="buy", qty=qty,
            client_order_id=coid, notional=qty * 100,
        )
        ledger.update_status(
            coid, "filled", fill_price=fp, fill_qty=qty, notional=qty * fp,
        )

    deployed = ledger.get_deployed_notional("CROSSASSET")
    placeholder_sum = sum(qty * 100 for _, qty, _, _ in cases)
    real_sum = sum(qty * fp for _, qty, fp, _ in cases)
    assert abs(deployed - real_sum) < 0.01, (deployed, real_sum)
    # In this case, placeholder OVERESTIMATES — the bug was bidirectional.
    assert placeholder_sum > real_sum, (placeholder_sum, real_sum)
    assert 650 < deployed < 660, deployed
    print(f"PASS: test_regression_qty100_fallback_overestimates_small_priced  "
          f"(deployed corrected from ${placeholder_sum:.2f} → ${deployed:.2f})")


# ---------------------------------------------------------------------------
# Backfill script — end-to-end on a fixture ledger
# ---------------------------------------------------------------------------

def _make_fixture(path: Path) -> None:
    """Write a ledger fixture matching live shape: filled entries with the
    qty*100 bug, plus a correctly-recorded SIMPLE closed entry and a TREND
    pending entry that should both be left alone."""
    fixture = {
        "entries": {
            "COID_SMH": {
                "strategy_id": "TREND", "symbol": "SMH", "status": "filled",
                "fill_qty": 2.121, "fill_price": 505.83,
                "notional_at_entry": 212.10,  # buggy qty*100
                "qty": 2.121, "side": "buy", "client_order_id": "COID_SMH",
                "registered_at": "2026-04-24T15:14:19Z",
            },
            "COID_DBA": {
                "strategy_id": "CROSSASSET", "symbol": "DBA", "status": "filled",
                "fill_qty": 8.0904, "fill_price": 27.2864,
                "notional_at_entry": 809.04,  # buggy qty*100
                "qty": 8.0904, "side": "buy", "client_order_id": "COID_DBA",
                "registered_at": "2026-04-22T14:42:52Z",
            },
            "COID_OK": {
                "strategy_id": "SIMPLE", "symbol": "OK", "status": "closed",
                "fill_qty": 10.0, "fill_price": 50.0,
                "notional_at_entry": 500.0,  # already correct
                "qty": 10.0, "side": "buy", "client_order_id": "COID_OK",
                "registered_at": "2026-04-20T00:00:00Z",
            },
            "COID_PEND": {
                "strategy_id": "TREND", "symbol": "PEND", "status": "pending",
                "qty": 1.0, "side": "buy", "client_order_id": "COID_PEND",
                "registered_at": "2026-04-27T00:00:00Z",
                "notional_at_entry": 100.0,  # placeholder, but not filled
            },
        },
        "last_reconciled_at": "2026-04-27T20:11:06Z",
        "version": 1,
    }
    path.write_text(json.dumps(fixture, indent=2))


def _run_backfill(ledger_path: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(_BACKFILL_SCRIPT), "--path", str(ledger_path), *args],
        capture_output=True, text=True,
    )


def test_backfill_dry_run_does_not_modify_file() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "ledger.json"
        _make_fixture(p)
        before = p.read_text()
        r = _run_backfill(p, "--dry-run")
        assert r.returncode == 0, r.stderr
        assert "SMH" in r.stdout and "DBA" in r.stdout
        assert "no file modified" in r.stdout.lower() or "dry run" in r.stdout.lower()
        assert p.read_text() == before, "dry-run modified the file"
        print("PASS: test_backfill_dry_run_does_not_modify_file")


def test_backfill_corrects_filled_only() -> None:
    """Backfill must correct filled entries, leave closed and pending alone,
    and write a timestamped .bak."""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "ledger.json"
        _make_fixture(p)
        r = _run_backfill(p)
        assert r.returncode == 0, r.stderr

        out = json.loads(p.read_text())["entries"]
        # Filled entries: corrected
        assert abs(out["COID_SMH"]["notional_at_entry"] - 1072.87) < 0.5, (
            out["COID_SMH"]["notional_at_entry"]
        )
        assert abs(out["COID_DBA"]["notional_at_entry"] - 220.76) < 0.5, (
            out["COID_DBA"]["notional_at_entry"]
        )
        # Closed entry: untouched (was already correct)
        assert out["COID_OK"]["notional_at_entry"] == 500.0
        # Pending entry: untouched (no fill data yet)
        assert out["COID_PEND"]["notional_at_entry"] == 100.0

        backups = [f for f in os.listdir(d) if "bak" in f]
        assert len(backups) == 1, f"expected 1 backup, found {backups}"
        print("PASS: test_backfill_corrects_filled_only")


def test_backfill_is_idempotent() -> None:
    """Running the backfill twice must produce no further changes the second
    time — guards against accidental drift on repeat runs."""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "ledger.json"
        _make_fixture(p)
        r1 = _run_backfill(p)
        assert r1.returncode == 0, r1.stderr
        first_state = p.read_text()
        r2 = _run_backfill(p)
        assert r2.returncode == 0, r2.stderr
        assert "No corrections needed" in r2.stdout, r2.stdout
        assert p.read_text() == first_state, "second run modified the file"
        print("PASS: test_backfill_is_idempotent")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> int:
    tests = [
        test_update_status_writes_notional,
        test_update_status_notional_omitted_preserves_value,
        test_update_status_notional_none_is_noop,
        test_mark_closed_does_not_touch_notional,
        test_regression_qty100_fallback_corrected_post_fill,
        test_regression_qty100_fallback_overestimates_small_priced,
        test_backfill_dry_run_does_not_modify_file,
        test_backfill_corrects_filled_only,
        test_backfill_is_idempotent,
    ]
    failures = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failures += 1
            print(f"FAIL: {t.__name__} — {e}")
        except Exception as e:
            failures += 1
            print(f"ERROR: {t.__name__} — {type(e).__name__}: {e}")
    if failures:
        print(f"\n{failures} failure(s) out of {len(tests)}")
        return 1
    print(f"\nAll {len(tests)} tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
