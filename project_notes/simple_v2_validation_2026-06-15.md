# SIMPLE v2 (Parabolic-Continuation ORB) — Validation Scorecard & Verdict (2026-06-15)

## Pre-registered acceptance bar (documented BEFORE testing)
To deploy, the redesign had to clear ALL of, on a FAITHFUL harness (imports prod code, no look-ahead,
realistic slippage+commissions), across multiple regimes:
- **B1** positive expectancy net of costs
- **B2** profit factor ≥ 1.3 and Sharpe ≥ 0.8
- **B3** max DD within cap, recovery-path risk overlay (no death-spiral)
- **B4** beat the SIMPLE baseline AND buy-hold QQQ/SPY over the same window
- **B5** survive walk-forward / out-of-sample across regimes
- **B6** robust to ±25% parameter perturbation
- **B7** backtest↔live reconciliation before scaling

## What was built (single source of truth, reversible)
- Pure strategy module `strategies/simple_parabolic/strategy.py` (+`__init__.py`): ORB-for-stocks-in-play
  continuation — select abnormally-active gappers, arm a resting stop at the opening-range high, ATR stop,
  scalp+trail, HARD intraday flatten. Inverts the current scorer's anti-extension bias; rewards extension.
- Faithful harness `backtest/simple_parabolic_backtest.py` — imports the strategy (asserts single-source
  fingerprint + no-look-ahead in `--selftest`), signal@T / resting-stop fill intrabar at trigger,
  8bps slippage each side, $0 commission, account-size realism (whole shares, min-notional), rolling-window
  drawdown throttle. Live engine, adapters, and `simple_bot.py` were NOT modified (pure additions).

## Results (faithful harness)
Mega-cap liquid universe (34 names), resting-stop fill:
| window | trades | ret | PF | WR | Sharpe |
|---|---|---|---|---|---|
| 2023Q1 recovery | 37 | −0.18% | 0.97 | 59% | −0.21 |
| 2024Q3 chop | 29 | −2.96% | 0.53 | 59% | −2.49 |
| 2025 vol | 20 | −1.11% | 0.80 | 45% | −0.66 |
| 2026 bull (baseline) | 23 | −5.47% | 0.30 | 39% | −5.56 |
| **TOTAL** | **109** | **net −$75.79** | — | — | **0/4 positive** |

- At a $10,000 sleeve: 0/4 positive, net −$1,582 (−7.25% in the bull window) — more capital into a losing edge.
- Low-priced mover universe (22 names: MARA/RIOT/SOFI/PLUG/HOOD/PLTR/…): 0/3 positive, net −$126.29,
  PF 0.20–0.97. Worse than mega-caps.
- 5 exit-model variants (wider stops, no scalp, let-winners-run, stricter RVOL): all negative on baseline.
- The pessimistic t+1-open fill gave the same conclusion (0/4, −$93.94) — verdict is fill-model-robust.

## Verdict — DOES NOT CLEAR THE BAR
- **FAILS B1, B2, B4, B5** decisively and consistently. PASSES the mechanical-integrity goals (0 overnight
  holds in every run; faithful single-source / no-look-ahead harness; leveraged-ETF guard).
- Failure mode (sanity-confirmed, not a bug): opening-range breakouts are predominantly FALSE — price
  triggers the stop entry, then reverses; avg loss > avg win at sub-50% win rate. No exit tweak fixes a
  no-edge entry.
- Beat the SIMPLE baseline on *mechanics* (no overnight drift, real fills) but NOT on P&L, and lost badly
  to buy-hold QQQ (+15%) / SPY (+6.5%).

## The deeper, honest finding
A **price-only, backtestable** intraday parabolic-continuation core has no demonstrable edge. This is
consistent with the brief's own hypothesis and with Horizon's analysis: the parabolic edge — if it
exists — lives in the **predictive data (options flow, dark pool, short volume, sentiment)** that has
**no backtestable history**. Our chosen "backtestable core + live overlay" decomposition requires a
non-edgeless core to scale; the core is edgeless, so an overlay would have to *be* the edge — which can
only be validated by a pre-registered FORWARD test, not a backtest.

## Recommendation (profitability-first)
1. **Do NOT deploy SIMPLE v2.** It does not clear the pre-registered bar.
2. **Retire the SIMPLE sleeve and redeploy its 0.10 allocation** to the demonstrated live edges:
   TREND (live +$127.77, PF 1.44) and/or CROSSASSET (live +$11.93, PF 4.20). Stops the bleed immediately.
3. If a parabolic sleeve is still wanted, build it as a **predictive-data-driven strategy validated by a
   pre-registered forward test** (paper/small-stakes, ~20–30 trading days) on a low-priced mover universe —
   since that edge cannot be backtested. Gate any allocation on clearing that forward bar.
