# Recovery-mode study — PRE-REGISTRATION

**Date:** 2026-11-04 (after the close), written before any candidate result is computed. Its
SHA-256 is recorded in the session before the study runs. **Requested by:** the user, after the
Oct 6 → Nov 2 drawdown: "make sure that we are set up to identify [recoveries] as soon as we get
the proper signals, and quickly move into leveraged [positions]", with high risk tolerance "as
long as our risk is based on educated predictions".

## Question

After a correction, should PULSE go to high leverage as soon as the recovery is confirmed by
trend, instead of waiting for realized volatility to fall?

## Disclosure: what was already seen

- **Baseline only.** On 2026-11-04 the live configuration was replayed with the account
  simulator (2008-01-02 → 2026-11-04). In 15 QQQ corrections of 10–20%, Horizon averaged −13.3%
  vs QQQ −12.5%. In 5 bear markets of 20% or more it averaged −20.8% vs −31.7%. In the three
  months after a bear-market trough it gained +15.5% vs +25.8%. By phase: edge vs QQQ +0.76 log
  points in normal markets, +0.70 while falling, and −0.63 while recovering (avg Nasdaq-100
  exposure 0.85x then). This diagnosis motivated the question.
- **No recovery-mode variant has been run** on any data. The rule below was written from general
  knowledge of market history (2000–02, 2008–09, 2020, 2022 and others are well known), so it is
  not blind. That is why it uses only conventional or pre-existing parameter values, and why the
  out-of-sample stress tests (1971–2007 Nasdaq history, Japan 1990–2012) are pass criteria.

## The rule (candidate, PULSE only; ROTATION unchanged)

Computed on PULSE's risk-asset closes (QQQ in backtests; QQQM live, which tracks the same index):

- **Armed** when the close is at least **10%** below its 252-session high (rolling, close-based).
  It stays armed until a close at a new 252-session high.
- **Recovery mode** starts when armed and the close is above its **50-day SMA × (1 + 1.5%)**.
  - It ends on **failure** (the rally broke): close below the 50-day SMA × (1 − 1.5%). It stays
    armed, so it may re-trigger.
  - It ends on **success**: a close at a new 252-session high, which also disarms.
- **In recovery mode,** PULSE leverage = max(volatility-target leverage, **L_rec**), capped at
  2.0x via the existing QQQ/QLD mix. **Outside it,** PULSE is unchanged:
  clip(0.25 / 20-day realized vol, 0.5, 2.0).
- **Execution.** Recovery start and failure-exit days force a full account rebalance (band
  bypassed), so the signal acts at the next open. A success exit reverts to normal rules with
  no forced trade. All other days use the live band rules (0.40 band, 0.5% minimum trade,
  0.10 signal override).

Values: 10% (a correction by convention), 50-day SMA (the standard intermediate trend), 1.5%
(PULSE's existing trend-filter hysteresis), 252 sessions (52 weeks), 2.0x (PULSE's existing cap).

**Candidates:** L_rec ∈ {2.0, 1.5}. Everything else is the live configuration: target_vol 0.25 via
QLD, ROTATION lookbacks (47, 95, 189), book leverage 1.0, regime tilt, funding guard.

## Evaluation

- **Account tests:** `backtest/account_sim.py`, which calls the live `plan_orders`, funding guard
  and buy sizing, + `backtest/tax.py` (FIFO, share-matched wash sales).
  - Full window 2008-01-02 → 2026-11-04.
  - Halves 2008-01-02 → 2017-12-29 and 2018-01-02 → 2026-11-04, each a fresh run.
  - The baseline is the live configuration, run the same way.
- **Index model** (stress and long history), extending the leverage study's L8 model:
  - Data: a FRED price index (no dividends).
  - PULSE leverage is set at close T from data through T and held for T+1.
  - PULSE return is L·r for L ≤ 1, and L·r − (L−1)·(short rate + 0.95%)/252 for L > 1.
  - Book = 64.3% PULSE + 35.7% short-rate cash. No band, costs or tax.
  - Short rate: US 3-month T-bill (DTB3) for US indices; Japan call-money rate
    (IRSTCI01JPM156N, monthly, forward-filled) for the Nikkei.

## Pass criteria — a candidate must meet ALL

- **R1 Gating bar.**
  - PULSE with recovery mode passes A1–A5 and A7 (−45% drawdown limit) on the original
    harness, 2008-01-02 → 2026-11-04.
  - It also passes A6: target_vol ×0.75/×1.25 and vol_days ×0.75/×1.25, each with recovery
    mode on, must keep 2018+ OOS CAGR > 0 and Sharpe ≥ 0.25.
  - ROTATION stays admitted.
- **R2 Material gain.** After-tax CAGR (ST 32% / LT 15%) ≥ baseline + **1.00 pp**, full window.
- **R3 Both halves.** After-tax CAGR (32/15) ≥ baseline in each half.
- **R4 Every bracket.** After-tax CAGR > baseline at ST 24%/LT 15% and at ST 37%/LT 20%.
- **R5 Efficiency.** Pre-tax Sharpe ≥ baseline − 0.10.
- **R6 Drawdowns.**
  - Account MaxDD no worse than −35%.
  - MaxDD no worse than −35% in each crash window, measured on the full-period run:
    GFC 2008-01-02 → 2009-06-30, COVID 2020-02-03 → 2020-06-30, and
    2022-01-03 → 2022-12-30.
- **R7 Dot-com stress.** Index model on NASDAQ100: book MaxDD, 1999-06-01 → 2003-12-31, no
  worse than −50%.
- **R8 Long history.** Index model on NASDAQCOM, 1971-02-05 → last date:
  - book CAGR > the baseline model's;
  - book MaxDD no worse than the baseline model's by more than 10 pp.
- **R9 Adverse regime.** Index model on NIKKEI225, 1989-12-29 → 2012-12-28 (rallies that kept
  failing):
  - book CAGR no worse than the baseline model's by more than 1.0 pp a year;
  - MaxDD no worse by more than 10 pp.
- **R10 Parameter robustness.** At the candidate's L_rec, at least **6 of the 9** cells
  arm ∈ {7.5%, 10%, 15%} × SMA ∈ {20, 50, 100} must beat the baseline's after-tax CAGR (32/15),
  full window, account simulator.

## Selection

Among candidates meeting every criterion, choose the higher after-tax CAGR (32/15). If neither
qualifies, PULSE stays unchanged and the study is reported as a negative result.

## Reported for information (not criteria, not selectable)

- Recovery capture: +1/+3/+6 months after each trough, baseline vs candidates.
- Recovery-mode episodes: count, success/failure split, average gain per episode.
- Turnover and the short-term share of gains.
- The index model with a 3x fund (TQQQ-like) as L_rec.
- What the rule says on the last completed session, and the close that would trigger it.

## Deployment

**Not automatic.** This raises risk in exactly the moments that feel worst, so the result goes to
the user, who decides. If approved:

- `PulseStrategy` gets the recovery parameters, off by default (the study code).
- The live cycle honors the forced-rebalance flag the same way `account_sim` does.
- Then the usual tests, live dry run, deploy and verification.
