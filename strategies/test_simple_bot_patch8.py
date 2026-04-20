"""
Unit test for Patch 8 — classify_fill_status (ladder-mode aware fill classifier).

Background: on 2026-04-20 a ladder-mode MP entry filled 18/18 shares in a
single OTO order but the legacy logic tagged it PARTIALLY_FILLED (because
`runner_qty > 0` and no separate runner order ever filled). The 45-second
entry-timeout flattened a fully-established position for a -$1.62 scratch.

These tests lock down the new classifier across all three entry shapes
(single-bracket, two-bracket, ladder) and the broker-authoritative fallback.

The classifier is a pure function — no mocks needed. We import it directly
from strategies.simple_bot, but we cannot import simple_bot at module load
(it pulls market_scanner + network libs). Instead we load the function via
importlib with a minimal environment so the test stays hermetic.

Run:
    python strategies/test_simple_bot_patch8.py
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from types import ModuleType


_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load_classify_fill_status():
    """Extract just the `classify_fill_status` function from simple_bot.py
    by AST-slicing the module so we don't trigger the full import chain
    (market_scanner, websocket, alpaca SDK, etc.)."""
    src_path = _REPO_ROOT / "strategies" / "simple_bot.py"
    tree = ast.parse(src_path.read_text(encoding="utf-8"))

    fn_node = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "classify_fill_status":
            fn_node = node
            break
    if fn_node is None:
        raise RuntimeError("classify_fill_status not found in simple_bot.py — Patch 8 not installed")

    # Compile and execute in a minimal namespace — typing.Optional is the only
    # external name referenced in the function signature/body.
    module_src = ast.unparse(fn_node)
    namespace: dict = {}
    exec(
        "from typing import Optional\n" + module_src,
        namespace,
    )
    return namespace["classify_fill_status"]


classify_fill_status = _load_classify_fill_status()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_mp_ladder_mode_full_fill_is_full() -> None:
    """The exact MP scenario: ladder-mode OTO, 18/18 shares filled on the
    scalp order, no separate runner order (runner is software-managed)."""
    status = classify_fill_status(
        total_qty=18,
        runner_qty=5,            # ladder runner qty (software-managed)
        runner_order_id=None,    # ← ladder-mode signature
        scalp_filled=True,
        runner_filled=False,
        scalp_qty=13,            # may be stale original split; ignored in ladder mode
        broker_position_qty=0,   # assume state-sync path (no broker query)
    )
    assert status == 'full', f"MP ladder-mode full fill should be 'full', got {status!r}"
    print("PASS: test_mp_ladder_mode_full_fill_is_full")


def test_ladder_mode_no_fill_is_none() -> None:
    """Ladder-mode entry that hasn't filled yet."""
    status = classify_fill_status(
        total_qty=18, runner_qty=5, runner_order_id=None,
        scalp_filled=False, runner_filled=False, scalp_qty=13,
    )
    assert status == 'none', f"expected 'none', got {status!r}"
    print("PASS: test_ladder_mode_no_fill_is_none")


def test_single_bracket_full_is_full() -> None:
    """Legacy single-bracket (runner disabled): 10/10 filled, no runner leg."""
    status = classify_fill_status(
        total_qty=10, runner_qty=0, runner_order_id=None,
        scalp_filled=True, runner_filled=False, scalp_qty=10,
    )
    assert status == 'full', f"expected 'full', got {status!r}"
    print("PASS: test_single_bracket_full_is_full")


def test_single_bracket_no_fill_is_none() -> None:
    status = classify_fill_status(
        total_qty=10, runner_qty=0, runner_order_id=None,
        scalp_filled=False, runner_filled=False, scalp_qty=10,
    )
    assert status == 'none'
    print("PASS: test_single_bracket_no_fill_is_none")


def test_two_bracket_both_filled_is_full() -> None:
    """Legacy two-bracket mode: both scalp and runner orders filled."""
    status = classify_fill_status(
        total_qty=18, runner_qty=5, runner_order_id="RUN-ORD-123",
        scalp_filled=True, runner_filled=True, scalp_qty=13,
    )
    assert status == 'full', f"expected 'full', got {status!r}"
    print("PASS: test_two_bracket_both_filled_is_full")


def test_two_bracket_scalp_only_is_partial() -> None:
    """Genuine partial fill: two-bracket mode, only scalp leg filled."""
    status = classify_fill_status(
        total_qty=18, runner_qty=5, runner_order_id="RUN-ORD-123",
        scalp_filled=True, runner_filled=False, scalp_qty=13,
    )
    assert status == 'partial', f"expected 'partial', got {status!r}"
    print("PASS: test_two_bracket_scalp_only_is_partial")


def test_two_bracket_runner_only_is_partial() -> None:
    """Genuine partial fill: two-bracket mode, only runner leg filled."""
    status = classify_fill_status(
        total_qty=18, runner_qty=5, runner_order_id="RUN-ORD-123",
        scalp_filled=False, runner_filled=True, scalp_qty=13,
    )
    assert status == 'partial'
    print("PASS: test_two_bracket_runner_only_is_partial")


def test_two_bracket_no_fill_is_none() -> None:
    status = classify_fill_status(
        total_qty=18, runner_qty=5, runner_order_id="RUN-ORD-123",
        scalp_filled=False, runner_filled=False, scalp_qty=13,
    )
    assert status == 'none'
    print("PASS: test_two_bracket_no_fill_is_none")


def test_broker_authoritative_overrides_flags() -> None:
    """Belt-and-suspenders: if broker reports full position qty, we trust it
    regardless of local flag state. Handles the case where order-status
    polling is lagging."""
    status = classify_fill_status(
        total_qty=18, runner_qty=5, runner_order_id=None,
        scalp_filled=False, runner_filled=False, scalp_qty=13,
        broker_position_qty=18,   # ← broker says fully in
    )
    assert status == 'full', f"broker-authoritative should give 'full', got {status!r}"
    print("PASS: test_broker_authoritative_overrides_flags")


def test_broker_partial_qty_forces_partial() -> None:
    """Safety guard: if the broker has SOME position but our flags say
    'nothing filled', we must NOT cancel orders (would leave naked shares).
    Classify as 'partial' so the caller flattens properly."""
    status = classify_fill_status(
        total_qty=18, runner_qty=0, runner_order_id=None,
        scalp_filled=False, runner_filled=False, scalp_qty=18,
        broker_position_qty=10,   # broker has 10 of 18 — naked position risk
    )
    assert status == 'partial', f"partial broker qty must force 'partial', got {status!r}"
    print("PASS: test_broker_partial_qty_forces_partial")


def test_broker_qty_trumps_ladder_flags_on_partial() -> None:
    """Even in ladder mode, a partial broker position must be treated as
    partial — not full — to preserve the flatten semantics."""
    status = classify_fill_status(
        total_qty=18, runner_qty=5, runner_order_id=None,
        scalp_filled=True, runner_filled=False, scalp_qty=13,
        broker_position_qty=12,   # ladder-mode scalp filled but broker only has 12
    )
    # Broker authority says "partial". Trust broker.
    assert status == 'partial', f"broker=12/18 must yield 'partial', got {status!r}"
    print("PASS: test_broker_qty_trumps_ladder_flags_on_partial")


def test_zero_total_qty_is_none() -> None:
    """Degenerate case — should not throw."""
    assert classify_fill_status(
        total_qty=0, runner_qty=0, runner_order_id=None,
        scalp_filled=True, runner_filled=True, scalp_qty=0,
        broker_position_qty=0,
    ) == 'none'
    print("PASS: test_zero_total_qty_is_none")


# ---------------------------------------------------------------------------
# Regression: exact 2026-04-20 MP trace, assert the new classifier returns
# 'full' for every STATE_SYNC tick between entry fill and timeout.
# ---------------------------------------------------------------------------

def test_regression_mp_2026_04_20() -> None:
    """Replay MP: entry submitted at 14:19:09 as OTO ladder (qty=18), scalp
    order fills at 14:19:09.598. STATE_SYNC ran at 14:19:15, :24, :29, :34,
    :40, 14:20:57. All of those should now classify as 'full' (not 'partial'
    as the old code did)."""
    # Snapshot of TradeIntent fields after ladder-mode entry submission + fill.
    mp_fields = dict(
        total_qty=18,
        runner_qty=5,              # ladder_runner_qty
        runner_order_id=None,      # ladder mode — single OTO
        scalp_filled=True,         # Alpaca fill event at 14:19:09.598
        runner_filled=False,       # never a separate runner order
        scalp_qty=13,              # legacy-split remnant; classifier ignores it in ladder mode
        broker_position_qty=0,     # STATE_SYNC doesn't query positions
    )

    for tick_label in ["14:19:15", "14:19:24", "14:19:29", "14:19:34", "14:19:40", "14:20:57"]:
        status = classify_fill_status(**mp_fields)
        assert status == 'full', (
            f"MP regression @ {tick_label}: expected 'full', got {status!r}. "
            f"If this fails Patch 8 is broken — the 45s timeout will flatten "
            f"the position again."
        )

    # And the timeout-handler path (which DOES query broker position):
    mp_fields["broker_position_qty"] = 18   # broker confirms position
    assert classify_fill_status(**mp_fields) == 'full'
    print("PASS: test_regression_mp_2026_04_20")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> int:
    tests = [
        test_mp_ladder_mode_full_fill_is_full,
        test_ladder_mode_no_fill_is_none,
        test_single_bracket_full_is_full,
        test_single_bracket_no_fill_is_none,
        test_two_bracket_both_filled_is_full,
        test_two_bracket_scalp_only_is_partial,
        test_two_bracket_runner_only_is_partial,
        test_two_bracket_no_fill_is_none,
        test_broker_authoritative_overrides_flags,
        test_broker_partial_qty_forces_partial,
        test_broker_qty_trumps_ladder_flags_on_partial,
        test_zero_total_qty_is_none,
        test_regression_mp_2026_04_20,
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
