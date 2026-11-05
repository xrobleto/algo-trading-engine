# Credit-confirmation study — RESULTS

Pre-registration: [CREDIT_STUDY_PREREG.md](CREDIT_STUDY_PREREG.md), committed before this ran (f219e4b, 2026-11-04 22:19 ET; SHA-256 97624309…5e39; its date line says 22:30 ET, but the commit time is the binding record). Credit measure: FRED BAA10Y, the latest value dated before each decision. Account window 2008-01-02 → 2026-11-04, live execution (40% band, signal override 10%, recovery starts and failures force a rebalance) and the tax model of the tax study. Index model on FRED NASDAQ100 through 2026-11-03 (the last date FRED had).

## Account results

| PULSE | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | turnover | ST share | after-tax 32/15 | 24/15 | 37/20 | half 1 | half 2 |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline (live) | 20.95% | 1.06 | -28.5% | -28.5% | -22.1% | -18.1% | 5.1x | 83% | **15.83%** | 16.91% | 14.87% | 15.22% | 16.27% |
| price-only (reference) | 22.10% | 1.02 | -37.1% | -37.1% | -22.1% | -25.5% | 8.0x | 89% | **16.36%** | 17.63% | 15.32% | — | — |
| A — confirmed | 20.76% | 1.00 | -34.7% | -34.7% | -22.1% | -28.4% | 7.5x | 85% | **15.52%** | 16.67% | 14.56% | 15.40% | 15.42% |
| B — credit-led | 21.67% | 1.01 | -39.0% | -39.0% | -22.1% | -35.4% | 7.8x | 83% | **16.51%** | 17.56% | 15.51% | 16.73% | 15.83% |

QQQ buy-and-hold after tax: ST 32% / LT 15% 14.82%, ST 24% / LT 15% 14.82%, ST 37% / LT 20% 14.46%; pre-tax 15.84%.

## Index model (NASDAQ100; no band, costs or tax; ROTATION as cash)

| PULSE | dot-com book MaxDD 1999-06 → 2003-12 | book CAGR 1986+ | book MaxDD 1986+ | failed episodes 1986–2007 | failed episodes 2008+ |
|---|--:|--:|--:|--:|--:|
| baseline (live) | -45.1% | 13.61% | -45.1% | — | — |
| price-only (reference) | -65.9% | 15.50% | -65.9% | 53% of 53 | 49% of 39 |
| A — confirmed | -63.1% | 14.18% | -63.1% | 69% of 48 | 60% of 30 |
| B — credit-led | -77.4% | 14.57% | -77.4% | 74% of 57 | 59% of 29 |

## Original gating bar (PULSE standalone, full window)

| PULSE | CAGR | Sharpe | MaxDD (limit −45%) | worst regime-window DD | A6 robustness | admitted |
|---|--:|--:|--:|--:|---|---|
| baseline (live) | 22.8% | 0.76 | -42.7% | -42.7% | PASS: target_vol=0.188: CAGR 19.5%, Sharpe 0.75 OK; target_vol=0.312: CAGR 25.1%, Sharpe 0.74 OK; vol_days=15: CAGR 22.0%, Sharpe 0.71 OK; vol_days=25: CAGR 21.9%, Sharpe 0.72 OK | yes |
| A — confirmed | 23.4% | 0.74 | -52.2% | -52.2% | not reached | no |
| B — credit-led | 24.6% | 0.75 | -58.3% | -58.3% | not reached | no |

## Parameter grid (C10): after-tax CAGR 32/15, full window

| rule | arm \ credit SMA | 20 | 50 | 100 |
|---|---|--:|--:|--:|
| A | 7.5% | 15.62% | 15.57% | 15.21% |
| A | 10.0% | 15.58% | 15.52% | 15.12% |
| A | 15.0% | 16.14% | 15.79% | 15.69% |
| B | 7.5% | 16.02% | 16.73% | 17.02% |
| B | 10.0% | 15.97% | 16.51% | 16.66% |
| B | 15.0% | 15.92% | 15.92% | 15.77% |

Baseline: 15.83%.

## Criteria

**A — confirmed** — FAILS

- FAIL — C1 gating bar: admitted ['ROTATION']; PULSE MaxDD -52.2% (limit -45%), Sharpe 0.74
- FAIL — C2 gain >= 1.25pp: -0.31pp after tax
- FAIL — C3 both halves >= +0.50pp: 15.40% vs 15.22% (+0.18pp); 15.42% vs 16.27% (-0.85pp)
- FAIL — C4 every bracket >= +1.00pp: ST 24% / LT 15%: 16.67% vs 16.91%; ST 37% / LT 20%: 14.56% vs 14.87%
- FAIL — C5 Sharpe >= baseline: 1.001 vs 1.055
- FAIL — C6 drawdowns within 5pp: MaxDD -34.7% vs -28.5%, GFC -34.7% vs -28.5%, COVID -22.1% vs -22.1%, 2022 -28.4% vs -18.1%
- FAIL — C7 dot-com within 5pp: -63.1% vs -45.1%
- FAIL — C8 NDX 1986+: CAGR 14.18% vs 13.61%; MaxDD -63.1% vs -45.1%
- FAIL — C9 mechanism: 1986-2007: 69% of 48 failed vs price-only 53% of 53; 2008-2026: 60% of 30 failed vs price-only 49% of 39
- FAIL — C10 robustness >= 7/9: 1/9 cells beat the baseline after tax

**B — credit-led** — FAILS

- FAIL — C1 gating bar: admitted ['ROTATION']; PULSE MaxDD -58.3% (limit -45%), Sharpe 0.75
- FAIL — C2 gain >= 1.25pp: +0.67pp after tax
- FAIL — C3 both halves >= +0.50pp: 16.73% vs 15.22% (+1.51pp); 15.83% vs 16.27% (-0.45pp)
- FAIL — C4 every bracket >= +1.00pp: ST 24% / LT 15%: 17.56% vs 16.91%; ST 37% / LT 20%: 15.51% vs 14.87%
- FAIL — C5 Sharpe >= baseline: 1.009 vs 1.055
- FAIL — C6 drawdowns within 5pp: MaxDD -39.0% vs -28.5%, GFC -39.0% vs -28.5%, COVID -22.1% vs -22.1%, 2022 -35.4% vs -18.1%
- FAIL — C7 dot-com within 5pp: -77.4% vs -45.1%
- FAIL — C8 NDX 1986+: CAGR 14.57% vs 13.61%; MaxDD -77.4% vs -45.1%
- FAIL — C9 mechanism: 1986-2007: 74% of 57 failed vs price-only 53% of 53; 2008-2026: 59% of 29 failed vs price-only 49% of 39
- PASS — C10 robustness >= 7/9: 8/9 cells beat the baseline after tax

**Selection: none.** No candidate meets every criterion; PULSE stays unchanged (negative result).

## Information only (not criteria)

Average return after the troughs of the 10%+ QQQ drawdowns in the account window:

| | +1 month | +3 months | +6 months |
|---|--:|--:|--:|
| QQQ | +11.0% | +17.0% | +26.4% |
| baseline (live) | +7.8% | +13.0% | +23.5% |
| price-only (reference) | +7.9% | +13.7% | +24.9% |
| A — confirmed | +7.8% | +13.2% | +23.7% |
| B — credit-led | +8.4% | +14.3% | +25.3% |

Recovery-mode episodes (index move from the signal close to the exit close):

| rule | QQQ since 2008: episodes | new high / failed | avg success / failure | NDX since 1986: episodes | new high / failed | avg success / failure |
|---|--:|--:|--:|--:|--:|--:|
| price-only (reference) | 39 | 20 / 19 | +6.8% / -2.1% | 92 | 45 / 47 | +8.1% / -2.6% |
| A — confirmed | 30 | 12 / 18 | +6.1% / -1.4% | 78 | 27 / 51 | +6.9% / -1.5% |
| B — credit-led | 29 | 12 / 17 | +8.4% / -0.2% | 86 | 27 / 59 | +9.1% / -0.7% |

Dot-com bust on NASDAQ100, 2000-03 → 2003-12:

| rule | start | end | move | outcome |
|---|---|---|--:|---|
| P | 2000-04-07 | 2000-04-10 | -6.8% | failed |
| P | 2000-06-07 | 2000-07-28 | -6.9% | failed |
| P | 2000-08-17 | 2000-09-11 | -3.2% | failed |
| P | 2001-01-23 | 2001-02-02 | -9.4% | failed |
| P | 2001-04-19 | 2001-04-24 | -9.8% | failed |
| P | 2001-04-27 | 2001-06-14 | -5.5% | failed |
| P | 2001-10-24 | 2002-01-16 | +9.4% | failed |
| P | 2002-03-08 | 2002-03-13 | -4.4% | failed |
| P | 2002-08-19 | 2002-08-28 | -7.9% | failed |
| P | 2002-10-15 | 2002-12-27 | +5.0% | failed |
| P | 2003-01-06 | 2003-01-17 | -4.1% | failed |
| P | 2003-03-13 | 2003-06-04 | +19.3% | new high |
| A | 2000-06-08 | 2000-07-28 | -6.2% | failed |
| A | 2001-01-24 | 2001-02-02 | -9.3% | failed |
| A | 2001-04-23 | 2001-04-24 | -2.7% | failed |
| A | 2001-04-27 | 2001-06-14 | -5.5% | failed |
| A | 2001-11-14 | 2002-01-16 | -1.7% | failed |
| A | 2002-03-08 | 2002-03-13 | -4.4% | failed |
| A | 2002-11-15 | 2002-12-27 | -6.0% | failed |
| A | 2003-01-06 | 2003-01-17 | -4.1% | failed |
| A | 2003-03-13 | 2003-06-04 | +19.3% | new high |
| B | 2000-06-08 | 2000-08-10 | -3.0% | failed |
| B | 2000-11-06 | 2000-11-21 | -15.3% | failed |
| B | 2001-01-24 | 2001-02-13 | -19.0% | failed |
| B | 2001-04-23 | 2001-07-12 | -3.4% | failed |
| B | 2001-11-14 | 2002-02-06 | -8.8% | failed |
| B | 2002-03-08 | 2002-05-06 | -25.3% | failed |
| B | 2002-11-15 | 2003-06-04 | +15.8% | new high |

QQQ since 2008:

| rule | start | end | move | outcome |
|---|---|---|--:|---|
| P | 2008-03-25 | 2008-06-23 | +5.0% | failed |
| P | 2008-08-08 | 2008-09-03 | -4.6% | failed |
| P | 2009-01-02 | 2009-01-14 | -7.7% | failed |
| P | 2009-01-28 | 2009-02-17 | -3.7% | failed |
| P | 2009-03-18 | 2009-07-07 | +16.3% | failed |
| P | 2009-07-15 | 2009-09-21 | +15.3% | new high |
| P | 2010-07-23 | 2010-08-23 | -3.4% | failed |
| P | 2010-09-03 | 2010-10-13 | +9.8% | new high |
| P | 2011-09-15 | 2011-09-22 | -4.6% | failed |
| P | 2011-10-10 | 2011-11-21 | -2.9% | failed |
| P | 2011-12-05 | 2011-12-14 | -4.1% | failed |
| P | 2012-01-05 | 2012-01-19 | +3.9% | new high |
| P | 2012-07-03 | 2012-08-17 | +5.3% | new high |
| P | 2012-12-18 | 2012-12-28 | -4.2% | failed |
| P | 2013-01-02 | 2013-04-18 | -0.0% | failed |
| P | 2013-04-23 | 2013-04-30 | +1.8% | new high |
| P | 2015-10-15 | 2015-10-28 | +5.9% | new high |
| P | 2016-03-02 | 2016-05-03 | -0.1% | failed |
| P | 2016-05-27 | 2016-06-17 | -3.3% | failed |
| P | 2016-07-08 | 2016-07-29 | +4.5% | new high |
| P | 2018-02-15 | 2018-03-09 | +4.5% | new high |
| P | 2018-05-09 | 2018-06-04 | +3.8% | new high |
| P | 2019-01-18 | 2019-04-17 | +13.3% | new high |
| P | 2019-06-18 | 2019-07-03 | +2.7% | new high |
| P | 2020-04-14 | 2020-06-05 | +13.1% | new high |
| P | 2020-09-09 | 2020-09-18 | -4.0% | failed |
| P | 2020-10-01 | 2020-10-28 | -3.8% | failed |
| P | 2020-11-04 | 2020-12-01 | +5.8% | new high |
| P | 2021-04-05 | 2021-04-09 | +1.8% | new high |
| P | 2022-03-22 | 2022-04-11 | -4.5% | failed |
| P | 2022-07-19 | 2022-08-31 | +0.3% | failed |
| P | 2022-11-11 | 2022-12-19 | -6.3% | failed |
| P | 2023-01-17 | 2023-05-18 | +19.8% | new high |
| P | 2023-11-07 | 2023-11-16 | +3.6% | new high |
| P | 2024-08-19 | 2024-09-03 | -4.0% | failed |
| P | 2024-09-19 | 2024-11-06 | +4.6% | new high |
| P | 2025-05-02 | 2025-06-24 | +10.4% | new high |
| P | 2026-04-09 | 2026-04-15 | +4.5% | new high |
| P | 2026-08-13 | 2026-09-22 | +2.1% | new high |
| A | 2008-04-22 | 2008-06-23 | +1.5% | failed |
| A | 2009-01-08 | 2009-01-14 | -6.9% | failed |
| A | 2009-01-28 | 2009-02-17 | -3.7% | failed |
| A | 2009-04-21 | 2009-07-07 | +5.7% | failed |
| A | 2009-07-15 | 2009-09-21 | +15.3% | new high |
| A | 2010-07-28 | 2010-08-11 | -1.4% | failed |
| A | 2010-08-19 | 2010-08-23 | -0.8% | failed |
| A | 2010-09-03 | 2010-09-20 | +6.1% | failed |
| A | 2011-10-21 | 2011-11-21 | -5.2% | failed |
| A | 2012-07-23 | 2012-08-17 | +7.7% | new high |
| A | 2013-01-07 | 2013-02-27 | +0.9% | failed |
| A | 2013-04-08 | 2013-04-18 | -1.5% | failed |
| A | 2013-04-23 | 2013-04-30 | +1.8% | new high |
| A | 2016-03-09 | 2016-05-03 | +0.9% | failed |
| A | 2016-05-27 | 2016-06-17 | -3.3% | failed |
| A | 2016-07-08 | 2016-07-29 | +4.5% | new high |
| A | 2018-02-15 | 2018-03-02 | +0.2% | failed |
| A | 2019-02-06 | 2019-04-17 | +9.8% | new high |
| A | 2020-05-06 | 2020-06-05 | +9.4% | new high |
| A | 2020-10-20 | 2020-10-28 | -4.5% | failed |
| A | 2020-11-04 | 2020-12-01 | +5.8% | new high |
| A | 2021-04-05 | 2021-04-09 | +1.8% | new high |
| A | 2022-03-25 | 2022-04-11 | -5.1% | failed |
| A | 2022-08-25 | 2022-08-31 | -6.6% | failed |
| A | 2022-11-18 | 2022-12-19 | -5.3% | failed |
| A | 2023-01-17 | 2023-03-13 | +3.2% | failed |
| A | 2024-09-24 | 2024-11-06 | +4.2% | new high |
| A | 2025-05-30 | 2025-06-24 | +4.0% | new high |
| A | 2026-04-09 | 2026-04-15 | +4.5% | new high |
| A | 2026-08-31 | 2026-09-22 | +4.3% | new high |
| B | 2008-04-22 | 2008-07-07 | -3.1% | failed |
| B | 2009-01-08 | 2009-03-13 | -6.6% | failed |
| B | 2009-04-21 | 2009-09-21 | +30.3% | new high |
| B | 2010-05-07 | 2010-05-10 | +5.2% | failed |
| B | 2010-07-28 | 2010-08-11 | -1.4% | failed |
| B | 2010-08-19 | 2010-09-20 | +8.8% | failed |
| B | 2011-10-21 | 2011-12-02 | -1.2% | failed |
| B | 2012-07-23 | 2012-08-17 | +7.7% | new high |
| B | 2012-11-08 | 2012-11-30 | +4.2% | failed |
| B | 2013-01-07 | 2013-02-27 | +0.9% | failed |
| B | 2013-04-08 | 2013-04-30 | +3.7% | new high |
| B | 2016-03-09 | 2016-06-27 | -2.5% | failed |
| B | 2016-07-06 | 2016-07-29 | +6.4% | new high |
| B | 2018-02-08 | 2018-03-02 | +8.2% | failed |
| B | 2019-02-06 | 2019-04-17 | +9.8% | new high |
| B | 2020-05-06 | 2020-06-05 | +9.4% | new high |
| B | 2020-10-20 | 2020-12-01 | +6.7% | new high |
| B | 2021-03-08 | 2021-03-18 | +4.0% | failed |
| B | 2021-03-22 | 2021-04-09 | +5.8% | new high |
| B | 2022-03-25 | 2022-05-06 | -13.9% | failed |
| B | 2022-06-14 | 2022-06-17 | -0.4% | failed |
| B | 2022-08-25 | 2022-09-30 | -16.6% | failed |
| B | 2022-10-07 | 2022-10-13 | -0.1% | failed |
| B | 2022-11-03 | 2022-11-10 | +8.5% | failed |
| B | 2022-11-18 | 2023-03-13 | +2.1% | failed |
| B | 2024-09-24 | 2024-11-06 | +4.2% | new high |
| B | 2025-05-30 | 2025-06-24 | +4.0% | new high |
| B | 2026-04-07 | 2026-04-15 | +8.3% | new high |
| B | 2026-08-31 | 2026-09-22 | +4.3% | new high |

Last completed session 2026-11-04: QQQ 691.95, -8.9% from its 252-session high 759.66; 50-day SMA 723.56 (price confirms above 734.41). BAA10Y known at this decision 1.54, its 50-day average 1.486 (credit confirms below 1.463, clears above 1.508).

- price-only (reference): mode armed; price confirmed False
- A — confirmed: mode armed; price confirmed False, credit confirmed False
- B — credit-led: mode armed; price confirmed False, credit confirmed False
