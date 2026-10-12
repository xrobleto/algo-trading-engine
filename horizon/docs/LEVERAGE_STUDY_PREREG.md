# PULSE volatility-target study — PRE-REGISTRATION

**Date:** 2026-10-12, committed before any result below is computed. **Requested by:** the user
("Go, run the study"), after an Oct 6 discussion of raising leverage.

## Question

Should PULSE's annualized volatility target rise from **0.22** (live) to **0.25** or **0.275**?
A higher target means more QLD in calm markets: more return, deeper drawdowns.

## Disclosure: what was already seen

This is a robustness check, not a blind test. On 2026-10-06, exploratory runs (account simulator,
40% band, 2008-01-02 → 2026-09-04, ST 32% / LT 15%) showed for 0.275: after-tax CAGR 17.2% vs
14.4% at 0.22, MaxDD −29.3% vs −25.9%, 2022 −24.2% vs −15.8%, and a dot-com book stress of −48% vs
−42%. Also seen earlier: 0.275 passed the A6 perturbation check (OOS CAGR 25.3%, Sharpe 0.78), and
0.30 failed PULSE's A3 drawdown limit (−48.4% vs −45%).

**Not yet computed:** the full per-strategy gating bar at 0.25 and 0.275, the half-period splits,
the tax-bracket sensitivity, and anything at 0.25. The thresholds below express the user's stated
risk tolerance; they were not chosen to fit the numbers above. The −50% dot-com limit was stated
to the user on 2026-10-12, before this document.

## Setup

- **Baseline (live):** PULSE target_vol 0.22 via QLD; ROTATION lookbacks (47, 95, 189); account
  no-trade band 0.40, min trade 0.5% of equity, signal override 0.10; book leverage 1.0.
- **Candidates:** target_vol ∈ {0.25, 0.275}. Nothing else changes.
- **Window:** 2008-01-02 → 2026-10-09 (last completed session at pre-registration). Halves:
  2008-01-02 → 2017-12-29 and 2018-01-02 → 2026-10-09, each a fresh run.
- **After-tax:** `backtest/account_sim.py` + `backtest/tax.py` (FIFO, share-matched wash sales),
  the instruments used by the tax study.

## Pass criteria — a candidate must meet ALL

- **L1 Original gating bar.** At the candidate, PULSE passes A1–A5 and A7 with its −45% drawdown
  limit, and A6 (target_vol ×0.75 / ×1.25, vol_days ×0.75 / ×1.25: OOS CAGR > 0, Sharpe ≥ 0.25).
  ROTATION stays admitted.
- **L2 Material gain.** After-tax CAGR (ST 32% / LT 15%) beats the baseline by ≥ 0.50 pp.
- **L3 Both halves.** After-tax CAGR (32/15) beats the baseline in each half.
- **L4 Every bracket.** After-tax CAGR beats the baseline at ST 24%/LT 15% and ST 37%/LT 20%.
- **L5 Efficiency.** Pre-tax Sharpe ≥ baseline − 0.10.
- **L6 Full-period drawdown.** Account MaxDD no worse than −35%.
- **L7 Crash windows.** MaxDD no worse than −35% in each of GFC 2008-01-02 → 2009-06-30,
  COVID 2020-02-03 → 2020-06-30, and 2022-01-03 → 2022-12-30 (measured on the full-period run).
- **L8 Dot-com stress.** Synthetic book drawdown, 1999-06-01 → 2003-12-31, no worse than −50%.
  Model, fixed now: FRED NASDAQ100 price index (no dividends) from 1986; PULSE leverage
  L = clip(target_vol / 20-day realized vol, 0.5, 2.0) set at close T and held T+1; return
  L·r for L ≤ 1, and L·r − (L−1)·(3-month T-bill + 0.95%)/252 for L > 1 (a daily-reset 2x fund);
  book = 64.3% PULSE + 35.7% T-bills (ROTATION modeled as T-bills); no band, no costs.

## Selection

Among candidates meeting every criterion, choose the one with the highest after-tax CAGR (32/15).
If neither qualifies, live stays at 0.22 and the study is reported as a negative result.

## Deployment

**Not automatic.** This deliberately raises the risk level, so the result goes to the user, who
decides. If approved, it is a one-value change to `strategy_params["PULSE"]["target_vol"]`, with
the usual tests, live dry-run, deploy and verification.
