# Horizon Live Validation Gate (E5) — started 2026-08-12

User decisions: no paper account — validation runs on the LIVE account with real (small) capital;
risk tolerance high, profitability-first; scale-up at 1.4–1.8× book leverage if the gate passes.

## Deployment configuration
- **Service**: Railway `horizon-live` (same project), volume `/data`, start `python -m horizon.engine.main --daily --live`.
- **Capital carve-out**: HORIZON_CAPITAL_CAP=$1,290 (~19% of $6.78k; sized under the Unified Engine's
  0.20 HORIZON reserve to leave buffer for intelligence-layer allocation tilts).
- **Unified Engine**: TREND 0.60 / SIMPLE 0.00 (parked) / CROSSASSET 0.17 / HORIZON reserve 0.20 / cash 0.03.
  Reserve sleeve has no adapter — classifies `HZN_*` orders and keeps the capital unspent.
- **Symbol equivalents** (`HORIZON_SYMBOL_EQUIVALENTS=1`): QQQ→QQQM, EFA→IEFA, TLT→VGLT, GLD→IAU,
  DBC→PDBC, BIL→SHV. Same underlying indexes; zero symbol overlap with the unified sleeves, so broker
  positions never merge across engines. Backtests keep the deep-history originals.
- **Shared-account safety (verified in code)**: Horizon adopts foreign positions as UNMANAGED and never
  trades them; the Unified Engine classifies `HZN_*` via the reserve sleeve instead of kill-switching.
- **Book leverage: 1.0× during the gate** (respects the carve-out). Scale-up decision uses the validated
  frontier (1.0×→16.7% CAGR / 1.4×→19.1% / 1.8×→20.8%, maxDD −37.6%).

## Pre-registered gate criteria (evaluate ~2026-09-25, ≈30 trading days)
- **G0 Data freshness (added 2026-09-05)**: every live cycle's `as_of` equals the last completed
  session (holiday tolerance 4 calendar days). The engine refuses to trade and alerts CRITICAL
  otherwise. Evidence: `railway logs -s horizon-live | grep "cycle:"` — the line now carries
  `as_of=`. Background: horizon_stale_data_2026-09-05.md (Aug 24-Sep 4 cycles ran on frozen data).
- **G1 Decision equivalence**: each live cycle's targets match the faithful harness re-run on the same
  dates (allowing for the QQQM-vs-QQQ data basis; flag any divergence > a rebalance band).
- **G2 Execution fidelity**: realized fills within modeled costs (slippage assumption ≤ 2× modeled;
  no missed/duplicated orders; ledger↔broker reconciliation clean every cycle).
- **G3 Operational integrity**: no kill-switch trips, no ownership conflicts, no orphan mishandling,
  no unexplained equity divergence between Horizon's ledger view and the carve-out.
- **G4 No cross-engine interference**: Unified Engine reconciliation stays clean (HZN_* classified;
  zero conflicts); neither engine trades the other's symbols.
- **Pass → scale**: raise Horizon allocation (recommend: retire CROSSASSET, PF 0.41 live, redundant
  with validated ROTATION) and raise book leverage toward 1.4–1.8× per the user's risk preference.
- **Fail → report plainly** and fix or stand down; no scale-up on a failed gate.

## Staged capital migration (user-approved 2026-08-12)
Faster than the strict 30-day wait, on evidence checkpoints — any dirty checkpoint stops the ramp.
User reviews evidence at each step (quick yes/no). Freed capital parks in SGOV between steps.
| checkpoint | condition | action |
|---|---|---|
| **CP1 ~Aug 19** | 5 clean trading days: fills ≈ modeled costs, ledgers reconcile, no cross-engine issues | Retire CROSSASSET (live PF 0.41; redundant with ROTATION) → **HORIZON 0.40 / TREND 0.40** |
| **CP2 ~Aug 26–Sep 2** | still clean | **HORIZON 0.60 / TREND 0.20** |
| **CP3 ~Sep 25** | full G1–G4 pass | **HORIZON 0.75–0.80 core at 1.4× book leverage** (1.8× per user risk preference), TREND minimal/retired |
- Raise HORIZON_CAPITAL_CAP in lockstep with the reserve at each step.
- Capital injection (user's) lands after CP1+, directly into the Horizon core.
- In parallel: TREND faithful validation (harness imports production trend_bot code, walk-forward).
  Fails → retire at CP2/CP3; passes → retains a sized allocation with known parameters.

## Notes
- ROTATION's first live pick differed slightly on the equivalents basis (PDBC+QQQM vs QQQ-only on the
  originals) — equivalent ETFs have near- but not perfectly-identical momentum ranks. This is exactly
  what G1 measures; log divergences, don't hide them.
- The Aug-12 audit (engine_audit_2026-08-12.md) is the context for this pivot: no homegrown sleeve has a
  demonstrated live edge; PULSE+ROTATION are the only bar-clearing strategies in the codebase.
