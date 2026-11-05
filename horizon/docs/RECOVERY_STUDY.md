# Recovery-mode study — RESULTS

Pre-registration: [RECOVERY_STUDY_PREREG.md](RECOVERY_STUDY_PREREG.md), written before this ran (SHA-256 bfa5a62b…fe53, 2026-11-04 19:56 ET). Window 2008-01-02 → 2026-11-04. Account figures use the live execution (40% band, signal override 10%, recovery starts and failures force a rebalance) and the tax model of the tax study.

## Account results

| PULSE | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | turnover | ST share | after-tax 32/15 | 24/15 | 37/20 | half 1 | half 2 |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline (live) | 20.95% | 1.06 | -28.5% | -28.5% | -22.1% | -18.1% | 5.1x | 83% | **15.83%** | 16.91% | 14.87% | 15.22% | 16.27% |
| L_rec 2.0 | 22.10% | 1.02 | -37.1% | -37.1% | -22.1% | -25.5% | 8.0x | 89% | **16.36%** | 17.63% | 15.32% | 16.00% | 16.52% |
| L_rec 1.5 | 20.77% | 1.02 | -32.6% | -32.6% | -22.1% | -22.4% | 5.7x | 82% | **15.61%** | 16.70% | 14.64% | 14.55% | 16.57% |

QQQ buy-and-hold after tax: ST 32% / LT 15% 14.82%, ST 24% / LT 15% 14.82%, ST 37% / LT 20% 14.46%; pre-tax 15.84%.

## Index model (no band, costs or tax; ROTATION as cash)

| PULSE | dot-com book MaxDD 1999-06 → 2003-12 | NDX book CAGR 1990+ | Nasdaq Composite 1971+ CAGR / MaxDD | Nikkei 1990-2012 CAGR / MaxDD |
|---|--:|--:|--:|--:|
| baseline (live) | -45.1% | 13.03% | 10.61% / -64.6% | -5.21% / -80.5% |
| L_rec 2.0 | -65.9% | 14.87% | 11.38% / -64.8% | -4.84% / -82.2% |
| L_rec 1.5 | -59.0% | 13.71% | 10.72% / -64.6% | -4.74% / -80.1% |
| 3x (information only) | -77.7% | 17.13% | 13.71% / -71.1% | -5.46% / -87.3% |

## Original gating bar (PULSE standalone, full window)

| PULSE | CAGR | Sharpe | MaxDD (limit −45%) | worst regime-window DD | A6 robustness | admitted |
|---|--:|--:|--:|--:|---|---|
| baseline (live) | 22.8% | 0.76 | -42.7% | -42.7% | PASS: target_vol=0.188: CAGR 19.5%, Sharpe 0.75 OK; target_vol=0.312: CAGR 25.1%, Sharpe 0.74 OK; vol_days=15: CAGR 22.0%, Sharpe 0.71 OK; vol_days=25: CAGR 21.9%, Sharpe 0.72 OK | yes |
| L_rec 2.0 | 25.5% | 0.77 | -54.6% | -54.6% | not reached | no |
| L_rec 1.5 | 23.7% | 0.76 | -48.5% | -48.5% | not reached | no |

## Parameter grid (R10): after-tax CAGR 32/15, full window

| L_rec | arm \ SMA | 20 | 50 | 100 |
|---|---|--:|--:|--:|
| 2.0 | 7.5% | 16.54% | 16.51% | 16.25% |
| 2.0 | 10.0% | 16.33% | 16.36% | 16.58% |
| 2.0 | 15.0% | 17.51% | 16.90% | 16.92% |
| 1.5 | 7.5% | 15.37% | 15.60% | 15.75% |
| 1.5 | 10.0% | 15.48% | 15.61% | 16.11% |
| 1.5 | 15.0% | 16.49% | 16.19% | 16.23% |

Baseline: 15.83%.

## Criteria

**L_rec 2.0** — FAILS

- FAIL — R1 gating bar: admitted ['ROTATION']; PULSE MaxDD -54.6% (limit -45%), Sharpe 0.77
- FAIL — R2 gain >= 1.00pp: +0.52pp after tax
- PASS — R3 both halves: 16.00% vs 15.22%; 16.52% vs 16.27%
- PASS — R4 every bracket: ST 24% / LT 15%: 17.63% vs 16.91%; ST 37% / LT 20%: 15.32% vs 14.87%
- PASS — R5 Sharpe: 1.02 vs 1.06
- FAIL — R6 drawdowns >= -35%: MaxDD -37.1%; GFC -37.1%, COVID -22.1%, 2022 -25.5%
- FAIL — R7 dot-com >= -50%: -65.9%
- PASS — R8 Nasdaq 1971+: CAGR 11.38% vs 10.61%; MaxDD -64.8% vs -64.6%
- PASS — R9 Japan 1990-2012: CAGR -4.84% vs -5.21%; MaxDD -82.2% vs -80.5%
- PASS — R10 robustness >= 6/9: 9/9 cells beat the baseline after tax

**L_rec 1.5** — FAILS

- FAIL — R1 gating bar: admitted ['ROTATION']; PULSE MaxDD -48.5% (limit -45%), Sharpe 0.76
- FAIL — R2 gain >= 1.00pp: -0.23pp after tax
- FAIL — R3 both halves: 14.55% vs 15.22%; 16.57% vs 16.27%
- FAIL — R4 every bracket: ST 24% / LT 15%: 16.70% vs 16.91%; ST 37% / LT 20%: 14.64% vs 14.87%
- PASS — R5 Sharpe: 1.02 vs 1.06
- PASS — R6 drawdowns >= -35%: MaxDD -32.6%; GFC -32.6%, COVID -22.1%, 2022 -22.4%
- FAIL — R7 dot-com >= -50%: -59.0%
- PASS — R8 Nasdaq 1971+: CAGR 10.72% vs 10.61%; MaxDD -64.6% vs -64.6%
- PASS — R9 Japan 1990-2012: CAGR -4.74% vs -5.21%; MaxDD -80.1% vs -80.5%
- FAIL — R10 robustness >= 6/9: 4/9 cells beat the baseline after tax

**Selection: none.** No candidate meets every criterion; PULSE stays unchanged (negative result).

## Information only (not criteria)

Average return after the troughs of the 10%+ QQQ drawdowns in the window:

| | +1 month | +3 months | +6 months |
|---|--:|--:|--:|
| QQQ | +11.0% | +17.0% | +26.4% |
| baseline (live) | +7.8% | +13.0% | +23.5% |
| L_rec 2.0 | +7.9% | +13.7% | +24.9% |
| L_rec 1.5 | +7.9% | +13.4% | +23.8% |

Recovery-mode episodes on QQQ since 2008: 39 (20 reached a new high, 19 failed); average QQQ move while in recovery mode: successes +6.8%, failures -2.1%.

| start | end | QQQ move | outcome |
|---|---|--:|---|
| 2008-03-25 | 2008-06-23 | +5.0% | failed |
| 2008-08-08 | 2008-09-03 | -4.6% | failed |
| 2009-01-02 | 2009-01-14 | -7.7% | failed |
| 2009-01-28 | 2009-02-17 | -3.7% | failed |
| 2009-03-18 | 2009-07-07 | +16.3% | failed |
| 2009-07-15 | 2009-09-21 | +15.3% | new high |
| 2010-07-23 | 2010-08-23 | -3.4% | failed |
| 2010-09-03 | 2010-10-13 | +9.8% | new high |
| 2011-09-15 | 2011-09-22 | -4.6% | failed |
| 2011-10-10 | 2011-11-21 | -2.9% | failed |
| 2011-12-05 | 2011-12-14 | -4.1% | failed |
| 2012-01-05 | 2012-01-19 | +3.9% | new high |
| 2012-07-03 | 2012-08-17 | +5.3% | new high |
| 2012-12-18 | 2012-12-28 | -4.2% | failed |
| 2013-01-02 | 2013-04-18 | -0.0% | failed |
| 2013-04-23 | 2013-04-30 | +1.8% | new high |
| 2015-10-15 | 2015-10-28 | +5.9% | new high |
| 2016-03-02 | 2016-05-03 | -0.1% | failed |
| 2016-05-27 | 2016-06-17 | -3.3% | failed |
| 2016-07-08 | 2016-07-29 | +4.5% | new high |
| 2018-02-15 | 2018-03-09 | +4.5% | new high |
| 2018-05-09 | 2018-06-04 | +3.8% | new high |
| 2019-01-18 | 2019-04-17 | +13.3% | new high |
| 2019-06-18 | 2019-07-03 | +2.7% | new high |
| 2020-04-14 | 2020-06-05 | +13.1% | new high |
| 2020-09-09 | 2020-09-18 | -4.0% | failed |
| 2020-10-01 | 2020-10-28 | -3.8% | failed |
| 2020-11-04 | 2020-12-01 | +5.8% | new high |
| 2021-04-05 | 2021-04-09 | +1.8% | new high |
| 2022-03-22 | 2022-04-11 | -4.5% | failed |
| 2022-07-19 | 2022-08-31 | +0.3% | failed |
| 2022-11-11 | 2022-12-19 | -6.3% | failed |
| 2023-01-17 | 2023-05-18 | +19.8% | new high |
| 2023-11-07 | 2023-11-16 | +3.6% | new high |
| 2024-08-19 | 2024-09-03 | -4.0% | failed |
| 2024-09-19 | 2024-11-06 | +4.6% | new high |
| 2025-05-02 | 2025-06-24 | +10.4% | new high |
| 2026-04-09 | 2026-04-15 | +4.5% | new high |
| 2026-08-13 | 2026-09-22 | +2.1% | new high |

Last completed session 2026-11-04: QQQ 691.95, -8.9% from its 252-session high 759.66; 50-day SMA 723.56. Mode: armed (in a 10%+ drawdown, not confirmed). Recovery mode would start on a close above 734.41 (the 50-day SMA moves daily).

