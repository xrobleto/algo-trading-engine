"""
Unit test for TrendAdapter Patch 7 — sleeve-scoped last_target_weights filter
in check_drift_mini_rebalance_needed.

The patch is a closure inside TrendAdapter._apply_patches, so we exercise the
filter semantics via a harness that reproduces the wrapper behavior with a real
OwnershipLedger + a stub of check_drift_mini_rebalance_needed that captures
what the inner function sees. The real adapter runs exactly this pattern.

Run:
    python strategies/adapters/test_trend_adapter_patch7.py
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
# The engine package lives at strategies/engine; sys.path needs the strategies
# dir so `from engine.ownership import OwnershipLedger` resolves.
_STRATEGIES_DIR = _REPO_ROOT / "strategies"
if str(_STRATEGIES_DIR) not in sys.path:
    sys.path.insert(0, str(_STRATEGIES_DIR))

from engine.ownership import OwnershipLedger  # noqa: E402


# ---------------------------------------------------------------------------
# Minimal fake `state` object — only the fields the drift check reads.
# ---------------------------------------------------------------------------

@dataclass
class FakeState:
    last_target_weights: Optional[Dict[str, float]] = None
    last_rebalance_date_iso: Optional[str] = "2026-04-18"
    spy_regime: str = "risk_on"
    active_substitutions: Optional[Dict[str, str]] = None


# ---------------------------------------------------------------------------
# The Patch 7 wrapper — mirrors trend_adapter.py exactly. When the adapter
# code changes, keep this in sync (or refactor the wrapper into a helper).
# ---------------------------------------------------------------------------

def make_patched_check_drift(original_check, ledger: OwnershipLedger, strategy_id: str = "TREND"):
    def patched_check_drift_mini(state, positions, total_equity, trading_client):
        orig_targets = state.last_target_weights
        if orig_targets:
            filtered_targets = {
                sym: w for sym, w in orig_targets.items()
                if not ledger.is_symbol_owned_by_other(sym, strategy_id)
            }
            state.last_target_weights = filtered_targets
            try:
                return original_check(state, positions, total_equity, trading_client)
            finally:
                state.last_target_weights = orig_targets
        return original_check(state, positions, total_equity, trading_client)
    return patched_check_drift_mini


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _capture_drift_check():
    """Return (fn, captures): fn plays the role of the original drift check and
    records the last_target_weights it observed into captures['seen']."""
    captures: Dict[str, Dict[str, float]] = {"seen": None}

    def original_check(state, positions, total_equity, trading_client):
        # Copy so we see the state AT CALL TIME, not after restoration.
        captures["seen"] = dict(state.last_target_weights) if state.last_target_weights else None
        return (False, 0.0, "captured")

    return original_check, captures


def _ledger_with(entries):
    """Build a ledger with pre-filled entries. `entries` = list of
    (strategy_id, symbol, status)."""
    ledger = OwnershipLedger()
    for i, (sid, sym, status) in enumerate(entries):
        entry = ledger.register_order(
            strategy_id=sid,
            symbol=sym,
            side="buy",
            qty=1.0,
            client_order_id=f"TEST_{sid}_{sym}_{i}",
            notional=100.0,
        )
        entry.status = status
    return ledger


def test_filters_cross_sleeve_target() -> None:
    """A symbol owned (filled) by CROSSASSET must be stripped from TREND's
    target weights before the drift comparison runs."""
    ledger = _ledger_with([("CROSSASSET", "DBC", "filled")])
    original, captures = _capture_drift_check()
    patched = make_patched_check_drift(original, ledger)

    state = FakeState(last_target_weights={"SMH": 0.30, "DBC": 0.31, "IEF": 0.10})
    patched(state, positions={}, total_equity=10_000.0, trading_client=None)

    seen = captures["seen"]
    assert seen is not None, "inner drift check was not called"
    assert "DBC" not in seen, f"DBC should be filtered — inner saw {seen}"
    assert seen == {"SMH": 0.30, "IEF": 0.10}, f"unexpected inner targets: {seen}"
    # state must be restored after the call
    assert "DBC" in state.last_target_weights, "state was not restored"
    print("PASS: test_filters_cross_sleeve_target")


def test_preserves_own_symbols() -> None:
    """TREND's own positions in the target must remain — filter only masks
    symbols owned by OTHER sleeves."""
    ledger = _ledger_with([
        ("TREND", "SMH", "filled"),
        ("CROSSASSET", "DBC", "filled"),
    ])
    original, captures = _capture_drift_check()
    patched = make_patched_check_drift(original, ledger)

    state = FakeState(last_target_weights={"SMH": 0.40, "DBC": 0.30, "XLE": 0.10})
    patched(state, positions={}, total_equity=10_000.0, trading_client=None)

    seen = captures["seen"]
    assert seen == {"SMH": 0.40, "XLE": 0.10}, f"unexpected inner targets: {seen}"
    print("PASS: test_preserves_own_symbols")


def test_pending_foreign_entries_still_block() -> None:
    """Foreign ownership with status='pending' should still filter (is_active
    covers pending + filled + partially_filled)."""
    ledger = _ledger_with([("CROSSASSET", "TLT", "pending")])
    original, captures = _capture_drift_check()
    patched = make_patched_check_drift(original, ledger)

    state = FakeState(last_target_weights={"TLT": 0.25, "QQQ": 0.20})
    patched(state, positions={}, total_equity=10_000.0, trading_client=None)

    assert captures["seen"] == {"QQQ": 0.20}, captures["seen"]
    print("PASS: test_pending_foreign_entries_still_block")


def test_closed_foreign_entries_do_not_block() -> None:
    """A symbol previously owned by another sleeve but now closed must NOT
    be filtered — TREND is free to target it again."""
    ledger = _ledger_with([("CROSSASSET", "GLD", "closed")])
    original, captures = _capture_drift_check()
    patched = make_patched_check_drift(original, ledger)

    state = FakeState(last_target_weights={"GLD": 0.15, "SPY": 0.40})
    patched(state, positions={}, total_equity=10_000.0, trading_client=None)

    assert captures["seen"] == {"GLD": 0.15, "SPY": 0.40}, captures["seen"]
    print("PASS: test_closed_foreign_entries_do_not_block")


def test_null_targets_passthrough() -> None:
    """When last_target_weights is None, the wrapper must call through
    unchanged without error."""
    ledger = _ledger_with([("CROSSASSET", "DBC", "filled")])
    original, captures = _capture_drift_check()
    patched = make_patched_check_drift(original, ledger)

    state = FakeState(last_target_weights=None)
    result = patched(state, positions={}, total_equity=10_000.0, trading_client=None)

    assert captures["seen"] is None, "inner should have seen None"
    assert result == (False, 0.0, "captured")
    print("PASS: test_null_targets_passthrough")


def test_state_restored_after_exception() -> None:
    """If the inner check raises, state.last_target_weights must be restored
    so subsequent code sees the original targets."""
    ledger = _ledger_with([("CROSSASSET", "DBC", "filled")])

    def raising_check(state, *_, **__):
        raise RuntimeError("simulated failure")

    patched = make_patched_check_drift(raising_check, ledger)

    state = FakeState(last_target_weights={"SMH": 0.30, "DBC": 0.31})
    try:
        patched(state, positions={}, total_equity=10_000.0, trading_client=None)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError")

    assert state.last_target_weights == {"SMH": 0.30, "DBC": 0.31}, (
        f"state not restored after exception: {state.last_target_weights}"
    )
    print("PASS: test_state_restored_after_exception")


# ---------------------------------------------------------------------------
# Regression: the exact 2026-04-15 phantom-drift scenario.
# ---------------------------------------------------------------------------

def test_regression_dbc_phantom_drift() -> None:
    """Replay of 2026-04-15: TREND's last_target_weights still had DBC at
    31.1% from the 04-13 Monday rebalance, but CROSSASSET bought DBC on the
    Wednesday rotation. With Patch 6 current_w[DBC]=0. Without Patch 7,
    drift = |0 - 0.311| = 31.1% → triggers DRIFT_MINI. With Patch 7, DBC is
    masked and drift falls to the real max among TREND-owned symbols."""
    ledger = _ledger_with([("CROSSASSET", "DBC", "filled")])
    captured_max_drift = {"val": None}

    def realistic_drift_check(state, positions, total_equity, trading_client):
        # Recreate the real logic at trend_bot.py:3016-3028
        current_w = {"SMH": 0.32, "SOXX": 0.15, "XLE": 0.18}  # post-Patch 6 filtered
        max_drift = 0.0
        max_sym = ""
        for sym in set(current_w.keys()) | set(state.last_target_weights.keys()):
            drift = abs(current_w.get(sym, 0.0) - state.last_target_weights.get(sym, 0.0))
            if drift > max_drift:
                max_drift = drift
                max_sym = sym
        captured_max_drift["val"] = (max_drift, max_sym)
        return (max_drift >= 0.05, max_drift, f"{max_sym} drifted {max_drift:.1%}")

    patched = make_patched_check_drift(realistic_drift_check, ledger)

    state = FakeState(last_target_weights={
        "SMH": 0.30,
        "SOXX": 0.15,
        "XLE": 0.20,
        "DBC": 0.311,   # the poison
    })

    needs_mini, max_drift, reason = patched(state, {}, 10_000.0, None)

    max_val, max_sym = captured_max_drift["val"]
    assert max_sym != "DBC", f"DBC should not be max drift; got {max_sym}"
    assert max_val < 0.05, f"max drift should be under threshold; got {max_val:.1%} at {max_sym}"
    assert not needs_mini, "drift mini should NOT trigger post-Patch-7"
    print(f"PASS: test_regression_dbc_phantom_drift  (max drift now {max_val:.1%} at {max_sym})")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> int:
    tests = [
        test_filters_cross_sleeve_target,
        test_preserves_own_symbols,
        test_pending_foreign_entries_still_block,
        test_closed_foreign_entries_do_not_block,
        test_null_targets_passthrough,
        test_state_restored_after_exception,
        test_regression_dbc_phantom_drift,
    ]
    failures = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failures += 1
            print(f"FAIL: {t.__name__}  — {e}")
        except Exception as e:
            failures += 1
            print(f"ERROR: {t.__name__}  — {type(e).__name__}: {e}")
    if failures:
        print(f"\n{failures} failure(s) out of {len(tests)}")
        return 1
    print(f"\nAll {len(tests)} tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
