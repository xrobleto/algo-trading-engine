# Core + overlay study — RESULTS

Pre-registration: [CORE_OVERLAY_PREREG.md](CORE_OVERLAY_PREREG.md), committed before this ran. 2008-01-02 → 2026-10-09, live execution (40% band), FIFO, ST 32% / LT 15% unless noted.

| target vol | core | after tax, sold at end | after tax, never sold | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | dot-com stress | ST share of gains | core share at end | eligible |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|
| 0.22 | 0% | **14.58%** | 14.76% | 19.86% | 1.09 | -25.9% | -25.9% | -20.2% | -15.8% | -41.8% | 86% | 0% | reference |
| 0.22 | 25% | **14.43%** | 14.61% | 19.92% | 1.09 | -25.9% | -25.9% | -20.2% | -15.8% | -42.5% | 89% | 12% | no |
| 0.22 | 50% | **15.22%** | 15.40% | 19.48% | 1.10 | -30.0% | -30.0% | -20.2% | -15.9% | -55.3% | 71% | 26% | no |
| 0.22 | 75% | **14.43%** | 14.67% | 16.80% | 0.96 | -38.2% | -38.2% | -21.3% | -23.4% | -70.5% | 30% | 60% | no |
| 0.25 | 0% | **16.44%** | 16.67% | 21.79% | 1.09 | -28.5% | -28.5% | -22.1% | -18.1% | -45.1% | 83% | 0% | reference |
| 0.25 | 25% | **16.22%** | 16.44% | 21.42% | 1.09 | -28.5% | -28.5% | -22.1% | -18.1% | -45.4% | 82% | 10% | no |
| 0.25 | 50% | **16.13%** | 16.35% | 20.57% | 1.08 | -31.5% | -31.5% | -22.1% | -18.1% | -57.1% | 72% | 22% | no |
| 0.25 | 75% | **15.13%** | 15.36% | 17.88% | 0.98 | -38.2% | -38.2% | -24.0% | -21.6% | -70.5% | 35% | 51% | no |
| QQQ buy-and-hold | 100% | **15.38%** | 16.32% | 16.41% | — | -49.4% | — | — | — | -82.9% | — | — | — |

## Criteria (each core variant vs the plain engine at the same target vol)

**target vol 0.22, core 25%** — not eligible
- FAIL — K1 +0.50pp after tax: -0.15pp
- FAIL — K2 both halves: 12.40% vs 12.98%; 16.42% vs 16.19%
- FAIL — K3 every bracket: ST 24% / LT 15% 15.72% vs 15.77%; ST 37% / LT 20% 13.49% vs 13.65%
- PASS — K4 drawdowns within 5pp: MaxDD -25.9% vs -25.9%; GFC -25.9% vs -25.9%, COVID -20.2% vs -20.2%, 2022 -15.8% vs -15.8%
- PASS — K5 dot-com >= -50%: -42.5%

**target vol 0.22, core 50%** — not eligible
- PASS — K1 +0.50pp after tax: +0.65pp
- PASS — K2 both halves: 13.11% vs 12.98%; 17.59% vs 16.19%
- PASS — K3 every bracket: ST 24% / LT 15% 16.02% vs 15.77%; ST 37% / LT 20% 14.35% vs 13.65%
- PASS — K4 drawdowns within 5pp: MaxDD -30.0% vs -25.9%; GFC -30.0% vs -25.9%, COVID -20.2% vs -20.2%, 2022 -15.9% vs -15.8%
- FAIL — K5 dot-com >= -50%: -55.3%

**target vol 0.22, core 75%** — not eligible
- FAIL — K1 +0.50pp after tax: -0.15pp
- FAIL — K2 both halves: 11.17% vs 12.98%; 17.99% vs 16.19%
- FAIL — K3 every bracket: ST 24% / LT 15% 14.68% vs 15.77%; ST 37% / LT 20% 13.81% vs 13.65%
- FAIL — K4 drawdowns within 5pp: MaxDD -38.2% vs -25.9%; GFC -38.2% vs -25.9%, COVID -21.3% vs -20.2%, 2022 -23.4% vs -15.8%
- FAIL — K5 dot-com >= -50%: -70.5%

**target vol 0.25, core 25%** — not eligible
- FAIL — K1 +0.50pp after tax: -0.23pp
- FAIL — K2 both halves: 14.81% vs 15.22%; 17.99% vs 17.59%
- FAIL — K3 every bracket: ST 24% / LT 15% 17.30% vs 17.57%; ST 37% / LT 20% 15.23% vs 15.44%
- PASS — K4 drawdowns within 5pp: MaxDD -28.5% vs -28.5%; GFC -28.5% vs -28.5%, COVID -22.1% vs -22.1%, 2022 -18.1% vs -18.1%
- PASS — K5 dot-com >= -50%: -45.4%

**target vol 0.25, core 50%** — not eligible
- FAIL — K1 +0.50pp after tax: -0.31pp
- FAIL — K2 both halves: 14.32% vs 15.22%; 17.87% vs 17.59%
- FAIL — K3 every bracket: ST 24% / LT 15% 16.94% vs 17.57%; ST 37% / LT 20% 15.21% vs 15.44%
- PASS — K4 drawdowns within 5pp: MaxDD -31.5% vs -28.5%; GFC -31.5% vs -28.5%, COVID -22.1% vs -22.1%, 2022 -18.1% vs -18.1%
- FAIL — K5 dot-com >= -50%: -57.1%

**target vol 0.25, core 75%** — not eligible
- FAIL — K1 +0.50pp after tax: -1.32pp
- FAIL — K2 both halves: 12.65% vs 15.22%; 16.98% vs 17.59%
- FAIL — K3 every bracket: ST 24% / LT 15% 15.43% vs 17.57%; ST 37% / LT 20% 14.42% vs 15.44%
- FAIL — K4 drawdowns within 5pp: MaxDD -38.2% vs -28.5%; GFC -38.2% vs -28.5%, COVID -24.0% vs -22.1%, 2022 -21.6% vs -18.1%
- FAIL — K5 dot-com >= -50%: -70.5%

## Verdict

Best configuration: target vol 0.25, core 0%: 16.44% after tax sold at the end, 16.67% never sold.
Against QQQ buy-and-hold (15.38% sold at the end, 16.32% never sold): +1.06pp and +0.35pp.

**No configuration beats QQQ by a meaningful margin after tax** (the pre-registered bar is +1.0pp a year under both conventions). The choice comes down to whether the engine's lower drawdowns are worth it.
