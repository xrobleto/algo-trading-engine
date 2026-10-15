# Core + overlay study — PRE-REGISTRATION

**Date:** 2026-10-15, committed before any core + overlay result is computed. **Requested by:**
the user ("Run the core and overlay study").

## Question

In a taxable account, the engine earns about QQQ's after-tax return because ~85% of its realized
gains are short-term. Does holding part of the Nasdaq-100 exposure as a **core** of QQQ shares
that is never sold (gains defer and age into long-term), with the engine trading only the
**overlay** around it, earn meaningfully more after tax than (a) the engine as it runs today and
(b) simply holding QQQ?

## Already known (disclosed, from 2026-10-15)

2008-01-02 → 2026-10-09, ST 32% / LT 15%: QQQ buy-and-hold 16.4% pre-tax, 15.4% after tax if sold
at the end, 16.3% if never sold (dividend tax only). Plain engine: 14.6% / 14.8% at target_vol
0.22; 16.4% / 16.7% at 0.25. No core + overlay figure has been computed.

## Design (fixed now)

- **Core:** at the start, buy QQQ worth `c` × starting equity. That share count is a floor: the
  account never holds fewer QQQ shares. No deposits, so the core is never topped up.
- **Overlay:** each cycle the engine computes its usual target book (PULSE + ROTATION, regime tilt,
  funding guard). Its target Nasdaq-100 exposure E = QQQ target + 2 × QLD target is then delivered
  around the core: QQQ target = max(engine QQQ target, core value); QLD target = max(0, (E − QQQ
  target) / 2). If that needs more capital than the account has, QLD and the non-Nasdaq targets
  are scaled down; QQQ never goes below the core. When the engine wants less Nasdaq exposure than
  the core provides, the account keeps the core: this is the protection the design gives up.
- **Execution and tax:** the live path unchanged (40% no-trade band, 0.5% minimum trade, 10% signal
  override, sells first, buys sized to cash), then `backtest/tax.py`. Lot relief stays FIFO, the
  live account's method: an overlay sale of QQQ above the floor relieves the oldest lots first.
- **Grid:** core c ∈ {0.25, 0.50, 0.75} × target_vol ∈ {0.22, 0.25}. References: the plain engine
  (c = 0) at each target_vol, and QQQ buy-and-hold.
- **Windows:** full 2008-01-02 → 2026-10-09; halves 2008-01-02 → 2017-12-29 and 2018-01-02 →
  2026-10-09 (fresh runs). Crash windows GFC 2008-01-02 → 2009-06-30, COVID 2020-02-03 → 2020-06-30,
  2022-01-03 → 2022-12-30, measured on the full run.
- **Dot-com stress:** the synthetic model of the leverage study (FRED NASDAQ100 price index,
  ROTATION as T-bills), extended with the core: daily Nasdaq exposure = max(0.643 × L, core
  value / equity), the remainder in T-bills, the 2x-fund financing cost on the PULSE leverage above
  1x as before. Book drawdown 1999-06-01 → 2003-12-31.
- **Tax conventions:** *sold at the end* (every position liquidated on the last day, the primary
  metric) and *never sold* (unrealized gains left untaxed), both at ST 32% / LT 15%, plus ST 24% /
  LT 15% and ST 37% / LT 20% for sensitivity.

## Criteria

A core variant is **eligible** only if, against the plain engine at the same target_vol, ALL hold:

- **K1** after-tax CAGR (sold at the end, 32/15) at least 0.50 pp higher;
- **K2** higher after tax in both halves;
- **K3** higher after tax at 24/15 and at 37/20;
- **K4** full-period MaxDD, and each crash window's MaxDD, no more than 5 pp worse;
- **K5** dot-com stress drawdown no worse than −50% (the limit used in the leverage study).

**Best configuration:** the highest after-tax CAGR (sold at the end, 32/15) among the plain engine
at 0.22, the plain engine at 0.25, and every eligible core variant.

**Verdict against QQQ:** the engine "earns its keep" in the taxable account only if the best
configuration beats QQQ buy-and-hold after tax by **at least 1.0 pp a year under both conventions**
(sold at the end vs QQQ sold at the end; never sold vs QQQ never sold). Otherwise the result is
reported as: no configuration beats QQQ by a meaningful margin after tax, and the choice comes down
to whether the engine's lower drawdowns are worth giving up that margin.

## Deployment

None from this study. Results go to the user.
