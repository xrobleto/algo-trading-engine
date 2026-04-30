"""
Unit test for TrendAdapter Patch 15 — sleeve-scoped rank_by_momentum input
filter at rebalance time.

The patch is a closure inside TrendAdapter._apply_patches, so we exercise the
filter semantics via a harness that reproduces the wrapper behavior with a real
OwnershipLedger + a stub of rank_by_momentum that captures what the inner
function sees. The real adapter runs exactly this pattern.

Run:
    python strategies/adapters/test_trend_adapter_patch15.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Tuple

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_STRATEGIES_DIR = _REPO_ROOT / "strategies"
if str(_STRATEGIES_DIR) not in sys.path:
    sys.path.insert(0, str(_STRATEGIES_DIR))

from engine.ownership import OwnershipLedger  # noqa: E402


# ---------------------------------------------------------------------------
# The Patch 15 wrapper — mirrors trend_adapter.py exactly. When the adapter
# code changes, keep this in sync (or refactor the wrapper into a helper).
# ---------------------------------------------------------------------------

def make_patched_rank_by_momentum(original_rank, ledger: OwnershipLedger,
                                   strategy_id: str = "TREND"):
    def patched_rank_by_momentum(bars, symbols, trading_client, top_n=0, spy_close=None):
        if symbols:
            filtered = [
                s for s in symbols
                if not ledger.is_symbol_owned_by_other(s, strategy_id)
            ]
            symbols = filtered
        return original_rank(bars, symbols, trading_client, top_n=top_n, spy_close=spy_close)
    return patched_rank_by_momentum


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------

def _capture_rank():
    """Return (fn, captures): fn plays the role of the original rank_by_momentum
    and records the symbol list it observed into captures['seen']."""
    captures: Dict[str, List[str]] = {"seen": None}

    def original_rank(bars, symbols, trading_client, top_n=0, spy_close=None):
        captures["seen"] = list(symbols) if symbols is not None else None
        # Return a deterministic ranking so tests can assert on output shape too.
        return [(s, 1.0 - i * 0.01) for i, s in enumerate(symbols)]

    return original_rank, captures


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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_filters_cross_sleeve_owned_from_universe() -> None:
    """A symbol owned (filled) by CROSSASSET must be stripped from the
    universe before rank_by_momentum runs."""
    ledger = _ledger_with([("CROSSASSET", "DBC", "filled")])
    original, captures = _capture_rank()
    patched = make_patched_rank_by_momentum(original, ledger)

    universe = ["SMH", "DBC", "SOXX", "GLD"]
    patched(bars=None, symbols=universe, trading_client=None, top_n=0)

    seen = captures["seen"]
    assert seen is not None, "inner rank was not called"
    assert "DBC" not in seen, f"DBC should be filtered — inner saw {seen}"
    assert seen == ["SMH", "SOXX", "GLD"], f"unexpected inner symbols: {seen}"
    print("PASS: test_filters_cross_sleeve_owned_from_universe")


def test_preserves_own_symbols() -> None:
    """TREND's own positions in the universe must remain — filter only masks
    symbols owned by OTHER sleeves."""
    ledger = _ledger_with([
        ("TREND", "SMH", "filled"),
        ("CROSSASSET", "DBC", "filled"),
    ])
    original, captures = _capture_rank()
    patched = make_patched_rank_by_momentum(original, ledger)

    universe = ["SMH", "DBC", "XLE", "QQQ"]
    patched(bars=None, symbols=universe, trading_client=None, top_n=0)

    seen = captures["seen"]
    assert seen == ["SMH", "XLE", "QQQ"], f"unexpected inner symbols: {seen}"
    print("PASS: test_preserves_own_symbols")


def test_pending_foreign_entries_still_block() -> None:
    """Foreign ownership with status='pending' should still filter (is_active
    covers pending + filled + partially_filled). This prevents a race where
    SIMPLE has just submitted an order on a ticker TREND is about to rank."""
    ledger = _ledger_with([("SIMPLE", "AMZU", "pending")])
    original, captures = _capture_rank()
    patched = make_patched_rank_by_momentum(original, ledger)

    universe = ["QQQ", "AMZU", "SPY"]
    patched(bars=None, symbols=universe, trading_client=None, top_n=0)

    assert captures["seen"] == ["QQQ", "SPY"], captures["seen"]
    print("PASS: test_pending_foreign_entries_still_block")


def test_closed_foreign_entries_do_not_block() -> None:
    """A symbol previously owned by another sleeve but now closed must NOT
    be filtered — TREND is free to rank it again."""
    ledger = _ledger_with([("CROSSASSET", "GLD", "closed")])
    original, captures = _capture_rank()
    patched = make_patched_rank_by_momentum(original, ledger)

    universe = ["GLD", "SPY"]
    patched(bars=None, symbols=universe, trading_client=None, top_n=0)

    assert captures["seen"] == ["GLD", "SPY"], captures["seen"]
    print("PASS: test_closed_foreign_entries_do_not_block")


def test_empty_universe_passthrough() -> None:
    """When symbols list is empty, the wrapper must pass through unchanged
    without error. Mirrors trend_bot calling rank_by_momentum on an empty
    DEFENSIVE_TICKERS list (current Patch 10 config)."""
    ledger = _ledger_with([("CROSSASSET", "DBC", "filled")])
    original, captures = _capture_rank()
    patched = make_patched_rank_by_momentum(original, ledger)

    result = patched(bars=None, symbols=[], trading_client=None, top_n=0)

    assert captures["seen"] == [], captures["seen"]
    assert result == [], f"empty input should produce empty ranking: {result}"
    print("PASS: test_empty_universe_passthrough")


def test_top_n_and_spy_close_passed_through() -> None:
    """The wrapper must forward top_n and spy_close kwargs unchanged."""
    ledger = OwnershipLedger()
    seen_kwargs = {}

    def original_rank(bars, symbols, trading_client, top_n=0, spy_close=None):
        seen_kwargs["top_n"] = top_n
        seen_kwargs["spy_close"] = spy_close
        return []

    patched = make_patched_rank_by_momentum(original_rank, ledger)
    spy_sentinel = object()
    patched(bars=None, symbols=["SPY"], trading_client=None,
            top_n=4, spy_close=spy_sentinel)

    assert seen_kwargs["top_n"] == 4, seen_kwargs
    assert seen_kwargs["spy_close"] is spy_sentinel, seen_kwargs
    print("PASS: test_top_n_and_spy_close_passed_through")


# ---------------------------------------------------------------------------
# Regression: the exact 2026-04-24 weekly rebalance scenario.
# ---------------------------------------------------------------------------

def test_regression_dbc_blocked_underdeployment() -> None:
    """Replay of 2026-04-24: TREND ranked DEFENSIVE_TICKERS=['DBC','GLD'] and
    assigned DBC=30.33% target weight. CROSSASSET owned DBC, so the buy was
    rejected by the sleeve guard — but the 30.33% allocation was NOT
    redistributed, leaving TREND deployed at only 8.7% of its sleeve target.

    With Patch 15, DBC is filtered out of the input universe entirely. The
    ranker only sees GLD; weight allocation downstream normalizes over the
    trimmed universe; no blocked-and-lost weight."""
    ledger = _ledger_with([("CROSSASSET", "DBC", "filled")])

    # Simulate the two rank_by_momentum calls in compute_target_weights:
    # 1. equity_ranked over ALL_EQUITY (no overlap with CROSSASSET)
    # 2. defensive_ranked over DEFENSIVE_TICKERS (had 'DBC' before Patch 10)
    inner_calls: List[Tuple[List[str], List[Tuple[str, float]]]] = []

    def fake_rank(bars, symbols, trading_client, top_n=0, spy_close=None):
        # Deterministic ranking — DBC would have ranked top of defensives
        score_table = {"DBC": 0.95, "GLD": 0.40, "SMH": 0.80, "SOXX": 0.70, "XLE": 0.50}
        ranked = sorted(
            [(s, score_table.get(s, 0.10)) for s in symbols],
            key=lambda x: x[1], reverse=True,
        )
        result = ranked[:top_n] if top_n else ranked
        inner_calls.append((list(symbols), result))
        return result

    patched = make_patched_rank_by_momentum(fake_rank, ledger)

    # Pre-Patch 10 universe (worst case, with DBC still in defensives)
    all_equity = ["SMH", "SOXX", "XLE"]
    defensive = ["DBC", "GLD"]

    eq_ranked = patched(bars=None, symbols=all_equity,
                        trading_client=None, top_n=0)
    def_ranked = patched(bars=None, symbols=defensive,
                         trading_client=None, top_n=2)

    # Equity ranking unaffected (no cross-sleeve overlap)
    assert [s for s, _ in eq_ranked] == ["SMH", "SOXX", "XLE"], eq_ranked

    # Defensive ranking: DBC is gone, only GLD remains
    seen_defensive = inner_calls[1][0]
    assert "DBC" not in seen_defensive, (
        f"DBC must be filtered from defensive universe; got {seen_defensive}"
    )
    assert seen_defensive == ["GLD"], seen_defensive
    assert [s for s, _ in def_ranked] == ["GLD"], def_ranked

    # Top-N selection now picks only tradable tickers (no blocked DBC)
    print("PASS: test_regression_dbc_blocked_underdeployment")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> int:
    tests = [
        test_filters_cross_sleeve_owned_from_universe,
        test_preserves_own_symbols,
        test_pending_foreign_entries_still_block,
        test_closed_foreign_entries_do_not_block,
        test_empty_universe_passthrough,
        test_top_n_and_spy_close_passed_through,
        test_regression_dbc_blocked_underdeployment,
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
