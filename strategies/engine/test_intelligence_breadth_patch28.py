"""
Regression tests for Patch 28 — soften the SIMPLE breadth gate.

Before: when market-structure narrowness was sustained in RISK_ON / CAUTIOUS,
`_compute_entry_gate` returned entry_allowed=False for SIMPLE, so the sleeve
went fully dark (every scalp order rejected with "breadth_narrow"). This shut
SIMPLE off for ~2 weeks (2026-05-20 .. 06-02) — exactly in the narrow,
parabolic-leader markets a momentum scalper is built to exploit.

After: sustained-narrow breadth is a *size dampener*, not an entry block.
  1. `_compute_entry_gate` no longer blocks SIMPLE on breadth (CRISIS still does).
  2. `_compute_sleeve_adjustments` applies SIMPLE_BREADTH_ALLOC_MULT (0.5)
     OUTSIDE the ±ALLOCATION_SWING_PCT clamp, so the cut genuinely halves
     SIMPLE's allocation rather than being capped at ~base*0.9.

These tests pin down:
  1. SIMPLE entry is allowed under sustained-narrow CAUTIOUS / RISK_ON.
  2. SIMPLE entry is still blocked in CRISIS.
  3. The breadth dampener halves SIMPLE's adjusted allocation (below the swing
     floor) when narrow is sustained.
  4. No dampener (and full allocation) when breadth is not sustained-narrow.

Run:
    python strategies/engine/test_intelligence_breadth_patch28.py
    pytest strategies/engine/test_intelligence_breadth_patch28.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_STRATEGIES_DIR = _REPO_ROOT / "strategies"
if str(_STRATEGIES_DIR) not in sys.path:
    sys.path.insert(0, str(_STRATEGIES_DIR))

from engine import intelligence as intel  # noqa: E402
from engine.intelligence import MarketIntelligenceLayer, MarketRegime  # noqa: E402
from engine.config import build_default_config  # noqa: E402


def _narrow_ms():
    return {
        "available": True,
        "narrowness_sustained": True,
        "narrowness_score": 1.0,
        "chop_score": 0.0,
    }


def _make_layer(ms, simple_alloc=None):
    """Minimal layer instance without running the full constructor.

    `simple_alloc` overrides SIMPLE's base allocation so the dampener-MATH tests
    don't depend on the production default (SIMPLE is parked at 0.0 in prod as of
    2026-06-16; the 0.5x dampener math must still be testable on a non-zero base)."""
    layer = MarketIntelligenceLayer.__new__(MarketIntelligenceLayer)
    layer._config = build_default_config()
    if simple_alloc is not None:
        layer._config.sleeves["SIMPLE"].allocation_pct = simple_alloc
    layer._ms_cache = ms
    layer._previous_allocations = {}
    return layer


def test_entry_gate_allows_simple_on_sustained_narrow():
    for regime in (MarketRegime.CAUTIOUS, MarketRegime.RISK_ON):
        allowed, reason = MarketIntelligenceLayer._compute_entry_gate(
            regime, "SIMPLE", _narrow_ms()
        )
        assert allowed is True, f"SIMPLE should not be breadth-blocked in {regime}"
        assert reason == "ok"


def test_entry_gate_still_blocks_simple_in_crisis():
    allowed, reason = MarketIntelligenceLayer._compute_entry_gate(
        MarketRegime.CRISIS, "SIMPLE", None
    )
    assert allowed is False
    assert "CRISIS" in reason


def test_breadth_dampener_halves_simple_allocation():
    """With the gate on + sustained narrow, SIMPLE alloc ≈ 0.5× its undampened value."""
    saved_gate = intel.MARKET_STRUCTURE_GATE_ENABLED
    saved_vel = intel.MAX_ALLOCATION_VELOCITY
    try:
        # Remove velocity clamping so we measure the steady-state target directly.
        intel.MAX_ALLOCATION_VELOCITY = 1.0

        # Baseline: gate OFF → no breadth dampener. Use an explicit non-zero SIMPLE
        # base so the halving math is exercised regardless of the parked prod default.
        intel.MARKET_STRUCTURE_GATE_ENABLED = False
        base_layer = _make_layer(_narrow_ms(), simple_alloc=0.10)
        base_adj = base_layer._compute_sleeve_adjustments(MarketRegime.CAUTIOUS)
        simple_base = base_adj["SIMPLE"].adjusted_allocation

        # Dampened: gate ON + sustained narrow.
        intel.MARKET_STRUCTURE_GATE_ENABLED = True
        damp_layer = _make_layer(_narrow_ms(), simple_alloc=0.10)
        damp_adj = damp_layer._compute_sleeve_adjustments(MarketRegime.CAUTIOUS)
        simple_damp = damp_adj["SIMPLE"].adjusted_allocation

        assert simple_base > 0
        # Genuine ~half — and crucially below the ±10% swing floor (base*0.9).
        assert abs(simple_damp - 0.5 * simple_base) < 0.005, (
            f"expected ~{0.5 * simple_base:.4f}, got {simple_damp:.4f}"
        )
        assert simple_damp < 0.9 * simple_base, "dampener was clamped by the swing bound"
        assert any("breadth_dampener" in r for r in damp_adj["SIMPLE"].adjustment_reasons)
        # Entry stays allowed — softened, not blocked.
        assert damp_adj["SIMPLE"].entry_allowed is True
    finally:
        intel.MARKET_STRUCTURE_GATE_ENABLED = saved_gate
        intel.MAX_ALLOCATION_VELOCITY = saved_vel


def test_no_dampener_when_breadth_not_narrow():
    saved_gate = intel.MARKET_STRUCTURE_GATE_ENABLED
    saved_vel = intel.MAX_ALLOCATION_VELOCITY
    try:
        intel.MAX_ALLOCATION_VELOCITY = 1.0
        intel.MARKET_STRUCTURE_GATE_ENABLED = True
        ms = _narrow_ms()
        ms["narrowness_sustained"] = False
        layer = _make_layer(ms)
        adj = layer._compute_sleeve_adjustments(MarketRegime.CAUTIOUS)
        simple = adj["SIMPLE"]
        # Full base allocation, no dampener reason, entry allowed.
        assert abs(simple.adjusted_allocation - simple.base_allocation) < 0.005
        assert not any("breadth_dampener" in r for r in simple.adjustment_reasons)
        assert simple.entry_allowed is True
    finally:
        intel.MARKET_STRUCTURE_GATE_ENABLED = saved_gate
        intel.MAX_ALLOCATION_VELOCITY = saved_vel


if __name__ == "__main__":
    tests = [
        test_entry_gate_allows_simple_on_sustained_narrow,
        test_entry_gate_still_blocks_simple_in_crisis,
        test_breadth_dampener_halves_simple_allocation,
        test_no_dampener_when_breadth_not_narrow,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  FAIL  {t.__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
