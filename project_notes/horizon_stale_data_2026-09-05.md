# Horizon live: frozen-data incident (2026-08-12 -> 2026-09-04)

**Found** during the 2026-09-05 deep audit. **Not caused by market events.**

## What happened
`horizon/data/cache.py` computed `FETCH_END = date.today()` **at import time** and treated a
cached symbol as fresh if its last bar was within 7 days of that frozen date. In the long-running
`horizon-live` container (started 2026-08-24 14:25 UTC; the previous incarnation ran from Aug 12),
every daily cycle therefore re-read the cache written on the first cycle. The engine decided on
**2026-08-24 data for nine sessions**:

| | container (logs) | fresh data on Sep 4 |
|---|---|---|
| regime score | 75 every day | 84.1 |
| QQQM target on the $3,850 cap | $4,839 (book held $4,911) | $7,844 |
| PULSE leverage | 0.99x | 1.76x |
| ROTATION September rebalance | never fired (month key stuck at 2026-08) | due Sep 1 |

The $8-63 daily orders were drift re-pins, not decisions. Reproduced with
`asof_scan.py` (session scratchpad): slicing fresh data at each date shows the container's
numbers match Aug 24 exactly. The daily heartbeat email carried `as_of: 2026-08-24` throughout —
nobody asserted on it, including the assistant's cycle checks.

## Impact
Under-deployed during a calm melt-up (no loss; foregone gain). The dangerous direction was
untested: in a vol spike the book would not have de-levered and the regime would never have
flipped.

## Fix (commit on 2026-09-05)
- `cache.fetch_end()` / `cache.completed_through()` are evaluated **per call**; a cache is fresh
  only if it contains the last completed session; bars dated after the last completed session
  (partial pre-/intra-day bars) are dropped.
- `engine/main.py`: **G0** — `run_cycle` refuses to trade and alerts CRITICAL if the dataset is
  more than `MAX_STALE_DAYS=4` calendar days behind; the `cycle:` log line now prints `as_of=`.
- **Funding guard** (`apply_funding_guard`): the account has **no margin** (Alpaca multiplier 1).
  With fresh data the vol-targeted book wants ~$8.8k gross on a $6.8k account; the guard scales
  targets to (cash + managed positions) x multiplier x 0.97 so buys are never rejected.
- Tests: `test_cache_freshness_is_evaluated_per_call`, `test_stale_cycle_is_refused`,
  `test_funding_guard_scales_to_account_capacity`.
- Gate doc: G0 added to horizon_live_gate_2026-08-12.md.

- **Gross ceiling** `HORIZON_MAX_GROSS=5775` set on horizon-live (= cap $3,850 x 1.5, the CP2
  boundary): the fixed engine would otherwise have taken the book to ~97% of equity at once
  (fresh data: PULSE 1.76x -> QQQM $7,844 target -> funding guard clamps to $6,625). With the
  ceiling the book holds ~$5,775 and vol-targeting still de-levers on the way down. Removing the
  env var lets the guard-clamped book (~97% of equity) deploy — that is CP3 in effect.

## Follow-ups (user decisions)
1. Leverage path: enable margin on Alpaca, keep the funding clamp, or move to the QLD expression
   (VALIDATION_CANDIDATE.md).
2. Drawdown overlay: wire live (needs a Horizon NAV tracker) or document OFF (LIMITATIONS #13).
