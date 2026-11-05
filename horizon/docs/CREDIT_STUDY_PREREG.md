# Credit-confirmation study — PRE-REGISTRATION

**Date:** 2026-11-04, 22:30 ET, written before any credit-based result is computed. It is committed
before the study runs; its SHA-256 is also recorded in the session. **Requested by:** the user
("run the credit spread study"), after the recovery-mode study failed
([RECOVERY_STUDY.md](RECOVERY_STUDY.md)) and a follow-up was proposed: a rebound signal needs
information beyond price, such as whether credit markets confirm the rally, and would have to
clear a stricter bar because it is designed after that result.

## Question

After a correction, can confirmation from the corporate credit market let PULSE go to 2x in real
recoveries while avoiding the bear-market rallies that sank the price-only recovery mode?

## Disclosure: what was already seen

- **The whole recovery-mode study.** That includes the price-only rule's account, index-model and
  grid results, and its 39 QQQ episodes since 2008 with their outcomes: 19 failed, among them 5
  of 6 in 2008–09 and all 3 in 2022. This design was written knowing which episodes failed, and
  that is why the bar below is stricter than that study's R1–R10.
- **General knowledge of credit history.**
  - Corporate spreads widened through most of 2000–02 and 2007–08.
  - They peaked near the equity lows of October 2002, early 2009 and March 2020.
  - They narrowed during the mid-2022 bear-market rally.

  So this is not a blind test. Credit confirmation is not expected to catch every failed rally;
  2022 is a known counterexample.
- **Data availability, checked 2026-11-04 before this was written.**
  - FRED's ICE BofA high-yield and investment-grade OAS series now start 2023-11-06, too short to
    test.
  - Moody's Baa minus the 10-year Treasury (FRED `BAA10Y`) runs daily from 1986-01-02 to
    2026-11-03.
  - The check displayed the latest values, BAA10Y 1.54 and HY OAS 3.40 on 2026-11-03, so current
    spreads are known to be narrow. It also confirmed that the newest BAA10Y value is never more
    than 4 calendar days old at any decision since 2004.
  - Nothing else about the series was computed.
- **No credit-based variant has been run** on any data.

## Credit measure

- **Series:** FRED `BAA10Y`, the Moody's seasoned Baa corporate bond yield minus the 10-year
  Treasury constant-maturity yield, daily, in percent. It is the only daily credit spread on FRED
  whose history covers the dot-com bust (the decisive bear-market-rally test) as well as the
  account window.
- **Known at decision time:** the decision on close T uses the latest BAA10Y value dated strictly
  before T. That is a one-session publication lag, since FRED posts each day's value the next
  day. Missing days carry the previous value. Every spread average below is taken over these
  known-at-decision values.

## The rule (candidates, PULSE only; ROTATION unchanged)

**Kept from the recovery-mode rule:**
- It is computed on PULSE's risk-asset closes: QQQ in backtests; QQQM live, which tracks the same
  index.
- **Armed** when the close is at least **10%** below its 252-session high. It stays armed until a
  close at a new 252-session high, which ends the episode as a **success**.
- **In recovery mode**, PULSE leverage = max(volatility-target leverage, **2.0**), capped at 2.0x
  via the QQQ/QLD mix. Outside it, PULSE is unchanged: clip(0.25 / 20-day realized vol, 0.5, 2.0).
- **Execution:**
  - Recovery starts and failure exits force a full account rebalance (band bypassed), so the
    signal acts at the next open.
  - A success exit reverts to normal rules without forcing.
  - All other days use the live band rules: 0.40 band, 0.5% minimum trade, 0.10 signal override.

**Two confirmation states.** Each is updated only while armed, and both are cleared at a new
252-session high:
- **Price confirmed** (the recovery study's signal):
  - set when the close is above its 50-day SMA × (1 + 1.5%);
  - cleared when the close is below its 50-day SMA × (1 − 1.5%).
- **Credit confirmed:**
  - set when the spread is below its 50-day average × (1 − 1.5%), i.e. spreads are narrowing;
  - cleared when the spread is above its 50-day average × (1 + 1.5%).

**Candidates** (both at L_rec 2.0):
- **A — confirmed:**
  - Recovery mode = armed AND price confirmed AND credit confirmed.
  - It ends with a failure when either confirmation clears.
- **B — credit-led:**
  - Recovery mode = armed AND credit confirmed, with no price condition.
  - It tests whether credit turns before price, which would mean earlier entries.
  - It ends with a failure when credit confirmation clears.

**Where the values come from:**
- The 50-day window and the 1.5% band are the price rule's, applied unchanged to the spread.
- 2.0x is PULSE's cap.
- L_rec 1.5 is dropped: it was dominated in the recovery study, and one leverage level keeps the
  comparison count at two.

**Baseline:** the live configuration, with no recovery mode: target_vol 0.25 via QLD, ROTATION
lookbacks (47, 95, 189), book leverage 1.0, regime tilt and funding guard.

**Reference** (information only): the price-only rule at L_rec 2.0, re-run here.

## Evaluation

Same machinery as the recovery study.

- **Account tests:** `backtest/account_sim.py`, which calls the live `plan_orders`, funding guard
  and buy sizing, plus `backtest/tax.py` (FIFO, share-matched wash sales).
  - Full window 2008-01-02 → 2026-11-04.
  - Halves 2008-01-02 → 2017-12-29 and 2018-01-02 → 2026-11-04, each a fresh run.
- **Index model:** as in the recovery study.
  - Data: FRED NASDAQ100. PULSE leverage is set at close T and held for T+1.
  - Financing: DTB3 + 0.95% on leverage above 1.
  - Book = 64.3% PULSE + 35.7% T-bill cash. No band, costs or tax.
  - BAA10Y is aligned to the index dates with the same one-session lag.

## Pass criteria — a candidate must meet ALL

Each criterion notes the recovery study's version in brackets.

- **C1 Gating bar** (same as R1).
  - PULSE with the rule passes A1–A5 and A7 (−45% drawdown limit) on the original harness,
    2008-01-02 → 2026-11-04.
  - It also passes A6: target_vol ×0.75/×1.25 and vol_days ×0.75/×1.25, each with the rule on,
    must keep 2018+ OOS CAGR > 0 and Sharpe ≥ 0.25.
  - ROTATION stays admitted.
- **C2 Material gain.** After-tax CAGR (ST 32% / LT 15%) ≥ baseline + **1.25 pp**, full window.
  [R2: +1.00 pp]
- **C3 Both halves.** After-tax CAGR (32/15) ≥ baseline + **0.50 pp** in each half.
  [R3: ≥ baseline]
- **C4 Every bracket.** After-tax CAGR ≥ baseline + **1.00 pp** at ST 24%/LT 15% and at
  ST 37%/LT 20%. [R4: > baseline]
- **C5 Efficiency.** Pre-tax Sharpe ≥ the baseline's. [R5: up to 0.10 below] The gain must come
  from better timing, not just more leverage, which a higher target_vol already offers.
- **C6 Drawdowns.** No worse than the baseline's by more than **5 pp**, for:
  - the account MaxDD;
  - the MaxDD in each crash window on the full-period run: GFC 2008-01-02 → 2009-06-30,
    COVID 2020-02-03 → 2020-06-30, and 2022-01-03 → 2022-12-30.

  [R6: −35% absolute]
- **C7 Dot-com stress.** Index model, book MaxDD 1999-06-01 → 2003-12-31: no worse than the
  baseline model's by more than **5 pp**. [R7: −50% absolute]
- **C8 Long history.** Index model, 1986-01-02 → 2026-11-04, the credit data's span. [R8, on the
  Nasdaq Composite 1971+: MaxDD within 10 pp]
  - Book CAGR > the baseline model's.
  - Book MaxDD no worse than the baseline model's by more than **5 pp**.
- **C9 Mechanism** (new). Index model, in both segments 1986-01-02 → 2007-12-31 and
  2008-01-02 → 2026-11-04:
  - The share of the candidate's recovery-mode episodes that end in failure must be lower than
    the price-only rule's. A failure is an episode that ends without a new 252-session high.
  - An episode counts in the segment where it starts, and an episode still open at the end is
    excluded.

  This checks that the credit condition does what it is for, which is to filter out failed
  rallies.
- **C10 Robustness.** At least **7 of the 9** cells, credit SMA ∈ {20, 50, 100} × arm ∈ {7.5%,
  10%, 15%}, must beat the baseline's after-tax CAGR (32/15). The other settings stay fixed: price
  SMA 50, bands 1.5%, L_rec 2.0. Full window, account simulator. [R10: 6 of 9]

The recovery study's Japan test (R9) is dropped, because no daily Japanese credit spread with the
needed history is available. C9 takes its place as the test against rallies that keep failing.

## Selection

Among candidates meeting every criterion, choose the higher after-tax CAGR (32/15). If neither
qualifies, PULSE stays unchanged and the study is reported as a negative result.

## Reported for information (not criteria, not selectable)

- The price-only reference rule's account results.
- Recovery-mode episodes per rule, on QQQ since 2008 and on NASDAQ100 since 1986: count, failure
  share and average index move.
- Recovery capture: +1/+3/+6 months after each 10%+ trough.
- Turnover and the short-term share of gains.
- What each candidate says on the last completed session (2026-11-04): the spread, its 50-day
  average and the levels that would change the mode.

## Deployment

**Not automatic.** The result goes to the user, who decides. If approved:

- `PulseStrategy` gets the credit confirmation, off by default (the study code).
- The live engine fetches BAA10Y from FRED each cycle. If the series is missing, or its newest
  value is more than 5 sessions old, recovery mode stays off and PULSE uses the baseline rule.
- The live cycle honors the forced-rebalance flag the same way `account_sim` does.
- Then the usual tests, live dry run, deploy and verification.
