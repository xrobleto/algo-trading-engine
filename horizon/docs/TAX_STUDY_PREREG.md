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

---

## ADDENDUM A — signal-change override (pre-registered 2026-09-25, after the main grid, before testing this variant)

**Why:** the main grid selected band 0.20. Diagnosing it (not part of the selection) showed the band
also defers ROTATION's *signal* changes: a single-slot exit is ~17% of equity, below the 20% band, so
over 2008–2026 exits waited a median 9 trading days, p90 35, max 71 (DBC held 71 extra days while
falling 10%). Aggregate criteria passed, but that is a regime-change risk the aggregates can hide.

**Variant (tested at the chosen band 0.20 only — no new band search):** the band also triggers when
any symbol is being fully entered (held ≈ 0, target > 0) or fully exited (target 0, held > 0) with
|delta| ≥ 10% of equity. 10% is fixed a priori as "half a ROTATION slot"; it is not tuned. Drift and
small PULSE leverage nudges still wait for the band.

**Deploy the override instead of the plain band only if ALL hold:**
1. it meets every main-study eligibility test against the band-0 baseline;
2. after-tax CAGR (32/15, full period) is no more than 0.30pp below plain band 0.20;
3. its maximum exit lag for |delta| ≥ 10% positions is 0 days (by construction — verified, not assumed).

Otherwise the plain band 0.20 ships, with the lag documented in LIMITATIONS.

---

## ADDENDUM B — wider bands (pre-registered 2026-09-25, before any wider-band result)

**Why:** in the main grid, after-tax CAGR rose monotonically through band 0.20, the edge of the grid.

**Baseline = current live:** band 0.20, min trade 0.5% of equity, signal override 0.10.
**Candidates:** band ∈ {0.25, 0.30, 0.35, 0.40}; min trade and override unchanged. Nothing else varies.

**New risk this addendum must rule out:** a wider band can defer PULSE's *de-leveraging* when
volatility spikes (a leverage cut from 1.9x to 1.5x moves ~26% of equity), leaving the book over-levered
into a crash. Aggregate MaxDD can hide this, so crash windows are checked separately.

**Eligible only if ALL hold, each versus the current live baseline:**
1. pre-tax Sharpe ≥ baseline − 0.05;
2. full-period MaxDD no more than 3pp worse;
3. MaxDD inside each crash window no more than 3pp worse — GFC 2008-01-02→2009-06-30, COVID
   2020-02-03→2020-06-30, 2022 bear 2022-01-03→2022-12-30 (measured on the full-period run);
4. after-tax CAGR (ST 32% / LT 15%) beats baseline in BOTH halves (fresh runs, as before);
5. after-tax CAGR beats baseline at ST 24%/LT 15% AND ST 37%/LT 20%;
6. no deferred exit of a position ≥ 10% of equity (verified, not assumed).

**Selection:** among eligible candidates, the band with the highest plateau score (minimum
after-tax CAGR, 32/15, of the band and its grid neighbours, the baseline 0.20 included as a
neighbour). Switch only if the chosen band beats the baseline by ≥ 0.20pp after tax; otherwise live
stays at 0.20. If the choice is 0.40, the grid edge, it is still taken, and the edge is reported.

**Deployment:** under the user's standing instruction for the tax work ("apply your best
recommendation"), a qualifying band is deployed as a one-value config change with the usual
verification.
