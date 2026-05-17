# Backtest ↔ Live Reconciliation Plan

Gating bar **E5**: before any real-money use, the faithful backtest must be
shown to reproduce live behavior within tolerance. This document is that plan.

## 1. The structural guarantee

The strongest reconciliation is built into the architecture, not checked after
the fact: **the live engine and the backtest harness call the identical
`Strategy.decide()`**. `engine/main.py` and `backtest/harness.py` both
`build_all()` from `strategies/registry.py` and invoke the same objects. There
is no separate "live version" of any strategy.

`tests/test_single_source.py` asserts that both import paths resolve to the
same class objects. If that test passes, a decision divergence is impossible by
construction — only *data* and *execution* can differ.

## 2. The decision-equivalence check (no money at risk)

Run the engine in dry-run and confirm its decisions match a same-dated backtest:

1. Run `python -m horizon.engine.main --interval 86400` for 10+ trading days.
   Each cycle logs the regime and the orders the engine *would* place.
2. For the same dates, run the harness up to each date and read the strategies'
   target weights.
3. **The target weights must be identical.** Same code + same data (Polygon is
   the source for both) ⇒ identical decisions. Any mismatch is a data-staleness
   or wiring bug, to be fixed before proceeding.

## 3. The execution-fidelity check (small stakes)

Once running small-stakes live (see TEST_PLAN.md), reconcile fills and equity:

| Quantity | Tolerance | If exceeded |
|---|---|---|
| Decision / target weights | exact match | data or wiring bug — stop |
| Per-fill slippage vs the 2-5 bps cost model | within ~3× the modeled bps | re-estimate the cost model from real fills |
| 1-month equity path vs harness | within ~2-3% | investigate fills, timing, missed bars |
| Regime label | exact match | regime-input data bug |

Compare the live ownership ledger (`state/ledger.json`) and Alpaca's account
history against a harness run seeded with the same start date and equity.

## 4. What divergence means

- **Decisions diverge** → a code-path or data bug. The single-source-of-truth
  design makes this the highest-priority failure: fix before any further use.
- **Fills diverge modestly** → the cost model is mis-calibrated. Update
  `config.CostModel` from observed fills and re-run the validation; the
  honest expected edge shifts accordingly.
- **Equity diverges with matching decisions and fills** → a timing issue
  (orders not placed at the intended next-open) or missed dividends/bars.

## 5. Sign-off

Real-money scaling beyond the small-stakes test is gated on: section 2 passing
exactly, and section 3 holding for **at least 30 trading days**. Until then the
engine runs dry-run or at minimal stakes only. This is the user's decision —
Horizon is delivered validated and testable, not deployed.
