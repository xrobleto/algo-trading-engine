# Tax-aware execution study — RESULTS

Pre-registration: [TAX_STUDY_PREREG.md](TAX_STUDY_PREREG.md) (committed before this ran). Account-level simulator, 2008-01-02 → 2026-09-04, backtest-basis symbols, taxes per `backtest/tax.py`. Fidelity: F1 and F2 passed (see the pre-registration).

QQQ buy-and-hold after tax: ST 32% / LT 15% 15.2%, ST 24% / LT 15% 15.2%, ST 37% / LT 20% 14.8%; pre-tax 16.2%.

| band | hold buffer | pre-tax CAGR | Sharpe | MaxDD | turnover | ST share | after-tax 32/15 | 24/15 | 37/20 | half 1 | half 2 | eligible |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|
| 0.00 | — | 19.36% | 1.07 | -25.7% | 14.7x | 100% | **12.79%** | 14.39% | 11.72% | 11.82% | 13.69% | baseline |
| 0.02 | — | 19.36% | 1.07 | -26.1% | 13.2x | 100% | **13.04%** | 14.58% | 12.00% | 11.98% | 14.04% | yes |
| 0.05 | — | 19.55% | 1.08 | -26.1% | 11.6x | 97% | **13.39%** | 14.88% | 12.37% | 12.28% | 14.45% | yes |
| 0.10 | — | 19.21% | 1.05 | -25.5% | 9.2x | 94% | **13.59%** | 14.94% | 12.65% | 12.52% | 14.63% | yes |
| 0.15 | — | 19.22% | 1.05 | -25.3% | 7.9x | 93% | **13.69%** | 15.01% | 12.76% | 12.41% | 14.97% | yes |
| 0.20 | — | 19.56% | 1.07 | -26.2% | 6.4x | 93% | **14.09%** | 15.40% | 13.17% | 12.68% | 15.49% | yes |
| 0.00 | 3 | 18.08% | 1.01 | -25.7% | 14.2x | 100% | **11.91%** | 13.41% | 10.91% | 11.64% | 12.03% | no: Sharpe, half h1, half h2, ST 24% / LT 15%, ST 37% / LT 20% |
| 0.02 | 3 | 18.11% | 1.01 | -26.1% | 12.7x | 99% | **12.20%** | 13.63% | 11.23% | 11.85% | 12.41% | no: Sharpe, half h2, ST 24% / LT 15%, ST 37% / LT 20% |
| 0.05 | 3 | 18.34% | 1.02 | -26.1% | 11.1x | 97% | **12.56%** | 13.95% | 11.61% | 12.14% | 12.86% | no: Sharpe, half h2, ST 24% / LT 15%, ST 37% / LT 20% |
| 0.10 | 3 | 18.09% | 1.00 | -25.5% | 8.6x | 94% | **12.82%** | 14.07% | 11.94% | 12.42% | 13.15% | no: Sharpe, half h2, ST 24% / LT 15% |
| 0.15 | 3 | 18.07% | 1.00 | -25.3% | 7.3x | 92% | **12.96%** | 14.16% | 12.09% | 12.18% | 13.86% | no: Sharpe, ST 24% / LT 15% |
| 0.20 | 3 | 18.44% | 1.01 | -26.2% | 6.0x | 90% | **13.37%** | 14.56% | 12.50% | 12.23% | 14.61% | no: Sharpe |

## Selection (pre-registered rule)

Plateau scores (min after-tax CAGR of a band and its neighbours, no buffer): 0.00 → 12.79%, 0.02 → 12.79%, 0.05 → 13.04%, 0.10 → 13.39%, 0.15 → 13.59%, 0.20 → 13.69%.

**Chosen: band 0.20, hold buffer off.**

## Addendum A — signal-change override (pre-registered before testing; see the pre-registration)

| configuration | pre-tax CAGR | Sharpe | MaxDD | turnover | after-tax 32/15 | 24/15 | 37/20 | half 1 | half 2 | rebalance days |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline band 0 | 19.36% | 1.07 | -25.7% | 14.7x | 12.79% | 14.39% | 11.72% | 11.82% | 13.69% | 100% |
| plain band 0.20 | 19.56% | 1.07 | -26.2% | 6.4x | 14.09% | 15.40% | 13.17% | 12.68% | 15.49% | 4% |
| **band 0.20 + override 0.10** | 19.59% | 1.06 | -26.2% | 7.2x | **14.02%** | 15.35% | 13.09% | 12.46% | 15.61% | 6% |

Criteria: (1) eligible vs baseline — PASS; (2) within 0.30pp of the plain band after tax (−0.07pp) —
PASS; (3) exits of ≥10% of equity deferred on 0 days (plain band: median 9, max 71 trading days) —
PASS. **Deployed: band 0.20, min trade 0.5% of equity, signal override 0.10.**

## Honest reading

- The band is close to free before tax (19.4% → 19.6%) and worth +1.2pp a year after tax.
- It does **not** close the gap to QQQ in a taxable account at a 32% short-term rate: the engine
  earns 14.0% after tax against 15.2% for buy-and-hold QQQ, with roughly half QQQ's drawdown
  (−26% vs −49%). At a 24% short-term rate it is roughly even (15.4% vs 15.2%).
- 93% of realized gains are still short-term. The remaining drag is structural: PULSE's leverage
  swings force sales of recent lots. Wider bands kept improving through 0.20 (the grid edge), so a
  follow-up pre-registered study of 0.25–0.40 is worthwhile; it was not run here to respect the
  pre-registered grid.
- The ROTATION hold buffer was rejected: it cost 1.2–1.3pp pre-tax at every band.
- Not modeled, so real results differ: state tax, the $3,000 ordinary-income offset, collectibles
  treatment of gold funds, and Alpaca's actual lot-relief behavior.

## Addendum B — wider bands (pre-registered before any result)

Baseline = current live (band 0.20, override 0.10). Crash-window drawdowns are measured on the full-period run.

| band | pre-tax | Sharpe | MaxDD | GFC | COVID | 2022 | turnover | rebalance days | ST share | after-tax 32/15 | 24/15 | 37/20 | half 1 | half 2 | plateau | eligible |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|
| 0.20 | 19.59% | 1.06 | -26.2% | -26.2% | -20.4% | -17.6% | 7.2x | 6% | 93% | **14.02%** | 15.35% | 13.09% | 12.46% | 15.61% | 14.02% | baseline (live) |
| 0.25 | 19.72% | 1.07 | -25.9% | -25.9% | -20.2% | -15.8% | 6.3x | 5% | 92% | **14.13%** | 15.46% | 13.19% | 12.50% | 15.79% | 13.90% | yes |
| 0.30 | 19.35% | 1.05 | -25.9% | -25.9% | -20.2% | -15.8% | 6.1x | 5% | 92% | **13.90%** | 15.20% | 12.98% | 12.23% | 15.60% | 13.90% | no: half h1, half h2, 0.24/0.15, 0.37/0.20 |
| 0.35 | 19.70% | 1.07 | -25.9% | -25.9% | -20.2% | -15.8% | 5.9x | 4% | 89% | **14.35%** | 15.58% | 13.42% | 12.83% | 15.62% | 13.90% | yes |
| 0.40 | 19.58% | 1.07 | -25.9% | -25.9% | -20.2% | -15.8% | 5.4x | 4% | 86% | **14.39%** | 15.56% | 13.47% | 12.98% | 15.79% | 14.35% | yes |

**Result: switch live to band 0.40** (best eligible: 0.4; after-tax gain vs live +0.36pp; switch threshold +0.20pp).
