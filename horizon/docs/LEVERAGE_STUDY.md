# PULSE volatility-target study — RESULTS

Pre-registration: [LEVERAGE_STUDY_PREREG.md](LEVERAGE_STUDY_PREREG.md), committed before this ran. Window 2008-01-02 → 2026-10-09. Account figures use the live execution (40% band, signal override 10%) and the tax model of the tax study.

## Account results

| target vol | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | dot-com stress | turnover | ST share | after-tax 32/15 | 24/15 | 37/20 | half 1 | half 2 |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 0.22 (live) | 19.86% | 1.09 | -25.9% | -25.9% | -20.2% | -15.8% | -41.8% | 5.3x | 86% | **14.58%** | 15.77% | 13.65% | 12.98% | 16.19% |
| 0.25 | 21.79% | 1.09 | -28.5% | -28.5% | -22.1% | -18.1% | -45.1% | 4.9x | 83% | **16.44%** | 17.57% | 15.44% | 15.22% | 17.59% |
| 0.275 | 22.68% | 1.09 | -29.3% | -29.3% | -24.6% | -24.2% | -48.4% | 5.1x | 79% | **17.45%** | 18.49% | 16.43% | 16.20% | 18.50% |

QQQ buy-and-hold after tax: ST 32% / LT 15% 15.38%, ST 24% / LT 15% 15.38%, ST 37% / LT 20% 15.00%; pre-tax 16.41%.

Dot-com stress is the synthetic model fixed in the pre-registration (price index, no dividends, ROTATION as T-bills); 1990–2026 synthetic book CAGR: 0.22 12.3%, 0.25 13.3%, 0.275 14.1%.

## Original gating bar (PULSE standalone, full window)

| target vol | CAGR | Sharpe | MaxDD (limit −45%) | worst regime-window DD | A6 robustness | admitted |
|---|--:|--:|--:|--:|---|---|
| 0.22 | 22.0% | 0.78 | -38.7% | -38.7% | PASS: target_vol=0.165: CAGR 19.0%, Sharpe 0.79 OK; target_vol=0.275: CAGR 26.1%, Sharpe 0.80 OK; vol_days=15: CAGR 22.2%, Sharpe 0.77 OK; vol_days=25: CAGR 21.9%, Sharpe 0.77 OK | yes |
| 0.25 | 23.7% | 0.79 | -42.7% | -42.7% | PASS: target_vol=0.188: CAGR 21.0%, Sharpe 0.81 OK; target_vol=0.312: CAGR 27.7%, Sharpe 0.80 OK; vol_days=15: CAGR 24.1%, Sharpe 0.77 OK; vol_days=25: CAGR 24.0%, Sharpe 0.78 OK | yes |
| 0.275 | 25.1% | 0.80 | -45.1% | -45.1% | not reached | no |

## Criteria

**target_vol 0.25** — PASSES ALL

- PASS — L1 gating bar: admitted ['PULSE', 'ROTATION']; PULSE MaxDD -42.7% (limit -45%), Sharpe 0.79
- PASS — L2 gain >= 0.50pp: +1.86pp after tax
- PASS — L3 both halves: 15.22% vs 12.98%; 17.59% vs 16.19%
- PASS — L4 every bracket: ST 24% / LT 15%: 17.57% vs 15.77%; ST 37% / LT 20%: 15.44% vs 13.65%
- PASS — L5 Sharpe: 1.09 vs 1.09
- PASS — L6 MaxDD >= -35%: -28.5%
- PASS — L7 crash windows >= -35%: GFC -28.5%, COVID -22.1%, 2022 -18.1%
- PASS — L8 dot-com >= -50%: -45.1%

**target_vol 0.275** — FAILS

- FAIL — L1 gating bar: admitted ['ROTATION']; PULSE MaxDD -45.1% (limit -45%), Sharpe 0.80
- PASS — L2 gain >= 0.50pp: +2.88pp after tax
- PASS — L3 both halves: 16.20% vs 12.98%; 18.50% vs 16.19%
- PASS — L4 every bracket: ST 24% / LT 15%: 18.49% vs 15.77%; ST 37% / LT 20%: 16.43% vs 13.65%
- PASS — L5 Sharpe: 1.09 vs 1.09
- PASS — L6 MaxDD >= -35%: -29.3%
- PASS — L7 crash windows >= -35%: GFC -29.3%, COVID -24.6%, 2022 -24.2%
- PASS — L8 dot-com >= -50%: -48.4%

**Selection: target_vol 0.25.** Per the pre-registration this is NOT deployed automatically; the user decides.
