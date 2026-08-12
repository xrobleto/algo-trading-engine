# TREND — Faithful Validation Verdict (2026-08-12): FAILS THE BAR

Harness: `backtest/trend_bot_faithful_backtest.py` — imports and calls PRODUCTION
`trend_bot.spy_regime` + `compute_target_weights` (never reimplemented; confirmed via
trend_bot's own [MOMENTUM]/[TOP_N] logs). Decisions Friday-close, fills next open +5bps,
full ALL_TICKERS universe, clock patched to simulated time. Two documented divergences make
the harness **OPTIMISTIC vs live**: drift mini-rebalances (live churn: 43 fills/week observed)
are NOT simulated, and the live-VIX override is omitted.

## Walk-forward, 8 yearly windows (each $10k start)
| year | TREND | SPY | excess | Sharpe | maxDD | fills |
|---|---|---|---|---|---|---|
| 2019 | −2.10% | +28.65% | −30.8pp | −0.03 | −25.8% | 173 |
| 2020 | −7.76% | +15.09% | −22.9pp | −0.14 | −35.8% | 189 |
| 2021 | +5.17% | +28.79% | −23.6pp | 0.32 | −19.0% | 213 |
| 2022 | −20.24% | −19.95% | −0.3pp | −1.65 | −23.9% | 135 |
| 2023 | +12.73% | +24.81% | −12.1pp | 0.84 | −12.6% | 189 |
| 2024 | +16.65% | +24.00% | −7.4pp | 0.70 | −15.7% | 232 |
| 2025 | −14.60% | +16.64% | −31.2pp | −0.54 | −17.6% | 204 |
| 2026* | +15.67% | +13.19% | +2.5pp | 0.79 | −20.2% | 116 |

*2026 = Jan–Aug 08. Chained across all 8 windows: **≈ −1.6% TOTAL over 7.6 years**
(CAGR ≈ −0.2%) vs SPY ≈ +140%. Turnover 15–41× per year (135–232 fills/yr) — and the live
version churns MORE (drift minis).

## Pre-registered bar (written before results)
| gate | criterion | result |
|---|---|---|
| T1 | CAGR > 4% (cash) | **FAIL** (≈ −0.2%) |
| T2 | Sharpe ≥ 0.40 | **FAIL** (≈ 0 full-period) |
| T3 | maxDD ≤ 40% | pass (worst window −35.8%) |
| T4 | beat SPY or corr ≤ 0.85 | **FAIL** (1/8 windows beat SPY; long-equity book, high corr) |
| T5 | ≥ 60% windows positive | **FAIL** (4/8 = 50%) |

**Verdict: TREND fails 4 of 5 pre-registered criteria on an OPTIMISTIC harness.** The live
record (cumulative PF 0.56, −$506) is not bad luck — the strategy has no demonstrated edge in
any regime tested. Notably: in the 2022 bear its regime protection did NOT protect (lost more
than SPY); in 2019/2025 it missed big up-years almost entirely (whipsaw). Its one good stretch
(2026 YTD, +2.5pp) is the same kind of single-window artifact the SIMPLE R&D track learned to
distrust.

## Recommendation (feeds the CP schedule in horizon_live_gate_2026-08-12.md)
Accelerate TREND's retirement rather than waiting for CP3:
- **CP1 (~Aug 19)**: instead of HORIZON 0.40 / TREND 0.40 → **HORIZON 0.40 / TREND 0.20 /
  SGOV-parked 0.20** (retire CROSSASSET as planned).
- **CP2**: retire TREND fully → **HORIZON 0.60 / SGOV 0.37**.
- **CP3 (full gate pass)**: HORIZON 0.75–0.80 core at 1.4–1.8× leverage.
This keeps every checkpoint evidence-gated on Horizon's live behavior while removing capital
from a strategy that failed both live AND faithful validation. User decision at CP1.
