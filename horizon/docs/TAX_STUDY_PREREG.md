# Tax-aware execution study — PRE-REGISTRATION (written before any study result)

**Date:** 2026-09-25. **Trigger:** the user confirmed the live account is a *taxable* brokerage
account. The 2026-09-25 review estimated 90% of realized gains are short-term and that, after tax,
the book's edge over QQQ shrinks to +0.8pp or turns negative (−0.4 to −0.9pp) depending on bracket.

## Question

Can execution-layer changes raise **after-tax** return without giving up the validated pre-tax
risk profile? Strategy signals (PULSE vol target, ROTATION momentum) are not re-tuned here, with the
single exception of the ROTATION hold buffer below.

## Instrument: account-level simulator (new)

`horizon/backtest/account_sim.py` replays the *account* the way `engine/main.py: run_cycle` does:
same strategy objects and states, same regime tilt and sleeve budgets, same funding guard, same
order planner (`plan_orders`), same sells-first / buys-sized-to-cash sequencing, fills at the next
open with the harness cost model. It records every fill and dividend. `horizon/backtest/tax.py`
turns those into taxes: FIFO lots (Alpaca's default), wash-sale disallowance (±30 days, loss added
to the replacement lot's basis, no holding-period tacking), annual ST/LT netting with loss
carryforward (no $3,000 ordinary offset), dividends taxed each year (bond / T-bill / commodity fund
distributions at the ordinary rate, equity ETF dividends at the long-term rate), and full
liquidation tax at the end so the comparison with buy-and-hold QQQ is like-for-like. Taxes are
modeled as paid from the account each year. State tax is excluded.

**Fidelity requirements (must pass before any candidate is judged):**
- F1: simulator at band 0, 2008-01-02 → 2026-09-04, pre-tax CAGR within ±1.0pp of the harness
  portfolio with the overlay off (19.9%).
- F2: simulator on live tickers over the live window 2026-09-04 → 2026-09-24, starting $6,830, return
  within ±0.5pp of the live account (+3.23%).

## Candidates (the full grid — nothing is added after results are seen)

- Account-level no-trade band `b` ∈ {0 (baseline = current live), 0.02, 0.05, 0.10, 0.15, 0.20}.
  When the band triggers, deltas under 0.5% of equity are skipped (fixed, not tuned); full exits
  always trade.
- ROTATION hold buffer `retain_rank` ∈ {none (baseline), 3}.

## Metric and eligibility

- **Primary metric:** after-tax CAGR 2008-01-02 → 2026-09-04 at ST 32% / LT 15%.
- **Eligible only if all hold:**
  1. pre-tax Sharpe ≥ baseline − 0.05;
  2. pre-tax MaxDD no more than 3pp worse than baseline;
  3. after-tax CAGR beats baseline in BOTH halves (2008–2017 and 2018–2026, each run fresh);
  4. after-tax CAGR beats baseline at ST 24%/LT 15% AND ST 37%/LT 20%.

## Selection rule

- Among eligible configurations *without* the hold buffer, choose the band whose **plateau score**
  (the minimum after-tax CAGR of that band and its immediate grid neighbours) is highest. This
  prefers a flat, robust region over a single best point.
- Add the hold buffer only if, at the chosen band, it is itself eligible AND adds ≥ 0.2pp of
  after-tax CAGR. Because it changes ROTATION's decisions, it must then also pass the original
  per-strategy gating bar (A1–A7) via `run_validation`; if it fails, it is dropped.
- If nothing is eligible, live stays at band 0 and the study is reported as a negative result.

## Deployment

The user pre-authorized applying the recommendation ("Please apply your best recommendation").
Deployment follows the usual path: tests, a dry-run cycle against the live account, redeploy, and
verification of the next live cycle.
