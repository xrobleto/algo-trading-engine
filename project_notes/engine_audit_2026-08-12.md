# Unified Engine — Performance Audit (2026-06-16 → 2026-08-12)

Read-only audit vs live Alpaca + Railway logs. Window = since the June-16 reallocation deploy
(SIMPLE parked, TREND 0.80, CROSSASSET 0.17, correlation guard added). 39 trading days.

## Headline
| | value |
|---|---|
| Equity | $7,816 → **$6,788** (**−13.15%**) |
| Max drawdown | **−14.9%** |
| Ann. Sharpe (window) | −2.67 |
| SPY same window | **+2.70%** (maxDD −3.4%) |
| QQQ same window | −1.56% (maxDD −10.7%) |
| SMH (semis) same window | −6.99% (**maxDD −24.6%** — semis crash mid-window) |
| XBI (biotech) same window | +16.79% |

**≈16pp underperformance vs SPY.** For context, pre-change (Feb→Jun) the account rose +55.7%
(Sharpe 2.23) in a bull tape.

## Per-sleeve attribution (realized, FIFO, symbol-set)
| sleeve | since 6/16 | cumulative since go-live |
|---|---|---|
| **TREND** | **−$633.62** (94 rt, WR 34%, PF 0.27) | **−$505.85** (146 rt, PF 0.56) |
| CROSSASSET | −$53.34 (16 rt, PF 0.20) | −$41.41 (22 rt, PF 0.41) |
| SIMPLE (parked) | $0.00 — **0 orders** ✓ | −$53.45 (pre-park) |

Key implication: **TREND's June "edge" (+$128, PF 1.44) was a bull-window artifact.** It has now
given back everything and more. NO sleeve has a demonstrated positive live edge. (Same
small-sample lesson the SIMPLE R&D track learned — now confirmed at the sleeve level.)

## Failure mechanism (TREND −$634)
1. **Sector concentration → the crash we flagged.** Losses: SOXX −$261, TECL −$247, SMH −$54,
   SOXL −$28 → **semis cluster = −$590 of −$634 (93%)**. The June audit explicitly warned the book
   was "~93% US equity, dominated by semis… effectively a concentrated semiconductor long." SMH then
   drew down −24.6%. The correlation guard never fired (0 blocks) because the book stayed under the
   1.25× *aggregate-beta* cap — the guard measures total beta, **not concentration**. The risk was
   inside the cap.
2. **Leveraged-ETF churn.** TECL (3×) did **30 round-trips**; SOXL 15. Median hold **1.0 day** for a
   *weekly-rebalance* strategy; 43 fills in week W26 alone. **45/80 round-trips held ≤2 days, net
   −$157** — whipsaw + vol-decay bleed in a volatile tape.
3. **Slow de-risk.** Worst day 07-17 −$294 (−3.9% equity in a day). Book only de-risked (now 0.41×
   net risk-on, heavy SGOV/cash, rotated into XBI) after the damage.
4. **WS4 chop dampener — built for exactly this — is still feature-flagged OFF** (`INTEL_CHOP_DAMPENER=0`).

## Live bug found during audit (happening now)
**CROSSASSET DBA mini-rebalance infinite loop:** every ~5 min it detects "DBA drifted 25%", submits a
BUY of ~$1,039 against a sleeve with only ~$360 available, gets rejected (`sleeve insufficient`), trips
its circuit breaker, waits, repeats — indefinitely. Net effect: **CROSSASSET cannot rebalance at all**
(frozen book), plus log spam / wasted API calls. Root cause: mini-rebalance sizes the order to the raw
target weight without clamping to `sleeve_available`.

## What worked
- **Parking SIMPLE**: 0 orders since 6/16, exactly as designed — that sleeve's bleed is stopped.
- Parked-adapter skip: no scanner waste; ownership/reconciler clean; no kill-switch events; engine stable.
- The de-risk posture now (0.41× net risk-on) is at least not compounding the drawdown.

## Recommendations (ranked by expected value)
1. **Add a sector-cluster concentration cap to the correlation guard** (the actual −$590 failure).
   Extend `correlation_guard.py` with correlated clusters (e.g. SEMIS = {SMH, SOXX, SOXL, TECL, semi
   single-names}); enforce max ~40–50% of equity per cluster in `can_deploy` Check 4. The guard already
   classifies symbols — this is an incremental, low-risk change to code we control.
2. **Leveraged-ETF policy in TREND**: remove 3× ETFs (TECL/SOXL/TQQQ/UPRO/FAS) from TREND's universe,
   or enforce min-hold ≥5 trading days + reduced size. TECL/SOXL churn was −$275 of pure whipsaw/decay.
3. **Churn control**: per-symbol trade cooldown / wider drift bands so a weekly strategy stops doing
   1-day round-trips (56% of round-trips ≤2 days, net negative).
4. **Fix the CROSSASSET DBA loop**: clamp mini-rebalance orders to `sleeve_available` (partial fill of
   target) or skip + alert once, so the sleeve can actually rebalance again.
5. **Enable WS4 chop dampener** (`INTEL_CHOP_DAMPENER=1`): already built + tested, only *reduces* TREND
   allocation in chop; July was its exact use-case.
6. **Honest strategic view — TREND does not deserve 80% on current evidence.** Cumulative live PF 0.56;
   its backtest is a known reimplementation (unfaithful); live behavior (43 fills/week) plainly diverges
   from its "weekly rebalance" design. Either (a) give trend_bot the faithful-harness treatment before it
   keeps the allocation, and/or (b) reduce gross exposure (raise cash/SGOV floor) until any sleeve
   demonstrates a live edge — especially ahead of the planned capital injection. Items 1–3 make TREND
   safer; they do not make it profitable.

## Approval gate
Nothing has been changed. Items 1–5 are concrete code/config changes I can implement and validate
(tests + staged deploy) on approval. Item 6 is a capital-allocation decision that is yours.
