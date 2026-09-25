# Horizon live gate — Sep 25 checkpoint (G0–G4)

**Scope:** every Horizon cycle under the CP3 configuration ("Candidate B": PULSE via QLD at
target_vol 0.22, ROTATION lookbacks 47/95/189, book 1.0, HORIZON 100%). 13 cycles, fill days
2026-09-08 through 2026-09-24. Evidence pulled 2026-09-24 20:28 ET (before the Sep 25 cycle).

**Verdict: PASS.** G0, G1, G3, G4 clean. G2 passes at book level; one per-symbol breach (IEFA),
wholly caused by the two CP3 transition buys on Sep 8, documented below rather than waived.

## Scorecard (utilities/horizon_checkpoint.py, run against horizon-live env)

| Check | Result |
|---|---|
| C1 all orders terminal | PASS — 88 HZN orders since Aug 12, 0 non-terminal, 0 rejected |
| C2 fill vs prior close | PASS — avg −16.4 bps (includes overnight gap) |
| C3 HZN symbols only | PASS |
| C4 no cross-engine contamination | PASS |
| C5 no borrowing (uncapped) | PASS — gross $6,841 = 97.0% of equity $7,050; cash $209; multiplier 1x |

The script was updated for the post-CP3 book: QLD added to the Horizon universe, and C5 now checks
"never borrows" when no capital cap is set (it would otherwise have compared gross to a $0 cap).

## G0 — data freshness: PASS

All 13 cycles decided on the prior completed session. Sep 18–24 from the retained container log;
Sep 8–17 observed in earlier status checks this month. Sep 8 correctly used as_of 2026-09-04 (the
session before Labor Day). G1 corroborates independently: a stale input would not reproduce the
harness's day-by-day leverage changes (e.g. the 1.92x spike on as_of Sep 16) within 0.5pp.

## G1 — decision equivalence: PASS

Live post-fill weights (shares after each day's fills × as_of close) versus the production
`decide()` replayed on the same as_of dates:

| | Result | Band |
|---|---|---|
| Worst live-vs-harness gap, any symbol, any day | **0.73pp** | 5.00pp |
| Worst QQQ-basis vs live-ticker basis gap | **0.68pp** | 5.00pp |

PULSE leverage moved 1.37x–1.92x across the window and live tracked every change. ROTATION's
September pick is PDBC + IEFA on live tickers and DBC + EFA on the validated basis — the same
exposures, so the equivalents mapping transfers cleanly.

**Tracking, Sep 4 close to Sep 24 close:** live account +3.23%, harness (overlay off, as live)
+3.05% on live tickers, +3.04% on the QQQ basis. Live ran 0.18pp ahead of the harness.

## G2 — execution fidelity: PASS at book level, one documented per-symbol breach

Slippage measured against the official open of the fill day (positive = cost). Limit = 2× modeled.

| Symbol | Fills | Notional | Avg bps | Modeled | Limit |
|---|---|---|---|---|---|
| QQQM | 14 | $8,228 | +0.82 | 2 | 4 |
| QLD | 14 | $8,046 | +5.14 | 5 | 10 |
| PDBC | 14 | $365 | −1.66 | 5 | 10 |
| IEFA | 14 | $1,173 | **+11.80** | 5 | 10 |
| **Book** | 56 | $17,812 | **+3.45** | 3.61 blended | |

Total realized execution cost across 13 sessions: **$6.14**.

**IEFA breach, explained:** the two Sep 8 transition buys ($240 + $814, opening a new position the
morning after Labor Day) cost +13.1 bps. The other 12 IEFA fills, all maintenance orders under
$25, averaged 0.00 bps. Total IEFA cost: $1.38. Not a systematic problem; it is the cost of opening
a position at the open. It becomes relevant when a large deposit is deployed (see Notes).

**Orders:** 4 per cycle every day from Sep 9, matching each cycle's logged `orders=4`. Sep 8 shows
8 — the Labor Day cycle's queued legs plus Tuesday's cycle, the behavior fixed by commit 5b3ff07.
No missed orders, no genuine duplicates.

## G3 — operational integrity: PASS

- 0 kill-switch trips, CRITICAL alerts, ownership conflicts, tracebacks, or stale-data refusals.
- Orphans (SGOV $0.09, XLK $0.01 dust) flagged every cycle and never traded.
- No equity divergence: Horizon sizes against broker equity (no cap); the Unified Engine's HORIZON
  sleeve reads $6,842 of $6,912 (99%), consistent with Horizon's 97% gross plus drift.

## G4 — no cross-engine interference: PASS

- 86 Unified Engine reconcile snapshots in the retained log, every one "6 matched, 0 unclassified,
  0 conflicts".
- 0 non-HZN orders on the account since Sep 4.

## Notes and follow-ups (no action required to continue)

1. **Turnover.** $17,812 traded on a ~$7,050 book in 13 sessions (2.5x). About $8.4k of that is the
   one-off Sep 8 transition; the rest is PULSE rotating between QQQM and QLD as volatility moves,
   which is how the vol target is implemented. Live re-pins any $1 drift where the harness uses a 5%
   band, yet live tracked the harness +0.18pp and total cost was $6. No band change warranted at
   this account size.
2. **Capital injection.** Horizon is uncapped, so a deposit is deployed in full at the next weekday
   09:00 cycle as market orders at the open. The Sep 8 evidence says opening a new position at the
   open costs ~13 bps. On liquid ETFs that is acceptable; staging a large deposit over a few days
   is optional, not necessary.
3. **Gate status.** The ramp is complete (HORIZON 100% since CP3). This was the last scheduled
   checkpoint. Recommend moving to a monthly health check with the same scripts.
