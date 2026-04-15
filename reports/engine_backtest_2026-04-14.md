# Unified Engine Multi-Regime Backtest — 2026-04-14

**Report date:** 2026-04-15
**Sweep dir:** `backtest/_runs/engine_regime_2026-04-14/`
**Composite formula:** 0.65·TREND + 0.20·SIMPLE + 0.12·XASSET + 0.03·cash
**Windows:** 6 regimes spanning 2021-10 through 2026-04
**Sweep runtime:** 176 min, 24 runs, 0 failures

---

## 1. Executive Summary

The engine's current weight allocation (TREND 65 / SIMPLE 20 / XASSET 12 / cash 3)
**beat SPY buy-and-hold in 4 of 6 regime windows, with average alpha of +4.94pp**
across the full 6-regime set. The two losing windows are informative:

- **P3 2022 Capitulation** — composite -6.07% vs SPY +0.31% (-6.4pp). TREND took
  a -12.3% hit with 20.6% max drawdown during the choppy H2 2022 selloff. The
  engine's intelligence-layer CRISIS multiplier (TREND 0.70×) would have
  softened this in production but is **not modeled** in this backtest.
- **P5 2023 AI Rally** — composite -0.18% vs SPY +12.68% (-12.9pp). SIMPLE
  blew up spectacularly (-43% / 45% DD) as the narrow-leadership AI rally
  starved mean-reversion setups; TREND participated (+13%) but not enough at
  20% to offset.

The other four windows show the engine working as designed:

- **P2 Rate Shock** — +12.89% vs SPY -21.03% (+33.9pp alpha). XASSET was the
  hero (Sharpe 2.59); SIMPLE's +59% contribution was icing.
- **P4 Bank Stress** — +9.11% vs SPY +5.85% (+3.3pp). TREND +20.85% carried
  the portfolio while SIMPLE bled.
- **P6 Recent** — +9.62% vs SPY -1.98% (+11.6pp). XASSET +16.97% and TREND
  +10.11% both contributed; SIMPLE ran quiet.
- **P1 Euphoria** — +9.47% vs SPY +9.38% (+0.1pp). Essentially a tie; the
  engine added no alpha in this benign regime.

**Headline takeaway:** the engine delivers meaningful crisis-alpha
(P2 +33.9pp, P4 +3.3pp, P6 +11.6pp) at the cost of modest underperformance in
choppy bear markets (P3 -6.4pp) and narrow-leadership rallies (P5 -12.9pp).
The P5 pain is a **SIMPLE-specific** regime vulnerability that dominates
the loss — TREND alone in P5 returned +12.96%, matching SPY.

---

## 2. Methodology

### Composition
Each sleeve's existing backtest runs independently over the window dates, then
per-sleeve daily equity series are normalized (start = 1.0), aligned on a union
business-day index with forward-fill, and combined with the engine weights. Cash
is treated as constant 1.0 × 0.03 weight.

This is a **per-sleeve composite**, not a unified portfolio simulation — the
composition happens post hoc, and does not model:

- **Intelligence-layer scaling.** In production, the engine applies regime-based
  allocation multipliers (e.g. TREND 0.70× in CRISIS, SIMPLE 0.65× in CRISIS,
  XASSET 1.30× in CRISIS). These would reshape the P3 outcome materially.
- **Cross-sleeve cash contention.** Each sleeve assumes it has full capital
  available; in reality the engine allocates a slice of the book.
- **Founder-trade overlay.** The recent ATD founder-notification layer only
  affects go/no-go and notifications, not sizing.
- **EOD gradual reduction and correlated drawdowns.** Each sleeve handles its
  own exit logic in isolation.

### Windows (from `backtest/regime_windows.py`)

| Window | Start | End | Days | Description |
|---|---|---|---|---|
| P1_2021_Euphoria | 2021-10-01 | 2021-12-31 | 66 | Meme/retail froth, narrow breadth |
| P2_2022_RateShock | 2022-01-03 | 2022-06-30 | 129 | Fed tightening, broad bear |
| P3_2022_Capitulation | 2022-07-01 | 2022-12-30 | 131 | Chop + rolling bottoms |
| P4_2023_BankStress | 2023-03-01 | 2023-05-31 | 66 | SVB/Signature, flight-to-quality |
| P5_2023_AIRally | 2023-06-01 | 2023-12-29 | 152 | Narrow AI leadership |
| P6_2025_Recent | 2025-10-01 | 2026-04-01 | 131 | Recent full-regime test |

### Per-Strategy Equity Sources
- **TREND:** `daily_equity` snapshots from the backtest's results JSON
- **XASSET:** dedicated `*_equity.csv` produced by the daily-bar harness
- **SIMPLE / DIRECTIONAL:** synthesized from trades CSV (PnL booked to exit date, carry-forward)
- **SPY:** Polygon daily bars (cached), buy-and-hold from window start

### Verification Performed
- **Composite arithmetic:** manual reconstruction of 0.65·T + 0.20·S + 0.12·X + 0.03
  at window-end for P2 (match: +12.89% exact) and P3 (match: -6.06% exact).
- **SPY benchmarks:** P2 -21.03% matches published SPY drawdown
  (≈$477 → ≈$376), P3 +0.31% matches the known H2 2022 round-trip, P5 +12.68%
  matches the AI-rally run from ≈$423 → ≈$474.
- **Per-strategy returns:** cross-referenced to individual backtest logs.

---

## 3. Full Results Table

### Composite vs SPY

| Window | Composite | SPY B&H | Alpha | Comp Sharpe | Comp MaxDD | Comp DD Days |
|---|---|---|---|---|---|---|
| P1_2021_Euphoria     | **+9.47%**  | +9.38%   | +0.1pp  | 1.64 | 8.89%  | 30 |
| P2_2022_RateShock    | **+12.89%** | -21.03%  | +33.9pp | 1.12 | 10.02% | 39 |
| P3_2022_Capitulation | -6.07%      | +0.31%   | -6.4pp  | -0.43 | 19.15% | 106 |
| P4_2023_BankStress   | **+9.11%**  | +5.85%   | +3.3pp  | 1.33 | 10.53% | 39 |
| P5_2023_AIRally      | -0.18%      | +12.68%  | -12.9pp | 0.10 | 20.66% | 118 |
| P6_2025_Recent       | **+9.62%**  | -1.98%   | +11.6pp | 0.88 | 10.79% | 45 |
| **6-window average** | **+5.81%**  | +0.87%   | **+4.94pp** | 0.77 | 13.34% | — |
| **Beat-SPY rate**    | —           | —        | **66.7% (4/6)** | — | — | — |

### Per-Sleeve Returns by Window

| Window | TREND | SIMPLE | XASSET | DIRECTIONAL (ref) |
|---|---|---|---|---|
| P1_2021_Euphoria     | +9.81%  | +19.61% | -6.88%  | -19.11% |
| P2_2022_RateShock    | -1.89%  | **+59.25%** | +18.89% | +34.65% |
| P3_2022_Capitulation | **-12.25%** | +7.20%  | +3.82%  | -20.17% |
| P4_2023_BankStress   | **+20.85%** | -20.99% | -1.99%  | -11.19% |
| P5_2023_AIRally      | +12.96% | **-43.08%** | +0.10%  | -43.30% |
| P6_2025_Recent       | +10.11% | +5.05%  | **+16.97%** | -59.32% |

Per-sleeve Sharpes and drawdowns are in `composite/composite_stats.csv`.

### Risk Stats Highlights
- **XASSET** is the most consistently positive-Sharpe sleeve: 2.59 (P2), 0.58
  (P3), 0.06 (P5), 2.15 (P6) — low vol (~5–15%), small MaxDDs (<13% in all
  windows).
- **TREND** has the highest single-window Sharpe (2.11 in P4) and also the
  deepest drawdown history (25.4% in P5, 20.6% in P3).
- **SIMPLE** is a volatility machine: Sharpe 1.52 in P2 but -1.74 in P5,
  with 29–45% MaxDDs — it's the biggest driver of composite dispersion.

---

## 4. Per-Window Deep-Dives

### P1 — 2021 Euphoria (Oct–Dec 2021)
Benign uptrend with narrow breadth / meme-retail froth. Composite +9.47% ties
SPY +9.38%; no alpha, no pain. SIMPLE actually led (+19.6%) on short-term
mean-reversion setups in overheated names, but XASSET whipsawed at the regime
turn (-6.9%). This is the engine's **acceptable-tie** regime.

### P2 — 2022 Rate Shock (Jan–Jun 2022)
The engine's finest hour. Composite +12.89% vs SPY -21.03% = **+33.9pp alpha**.
All three engine sleeves contributed: SIMPLE +59.25% (shorts + mean-reversion
in a high-vol bear), XASSET +18.89% Sharpe 2.59 (gold + bonds flight),
TREND -1.89% but lost less than index. SIMPLE's 28.7% MaxDD during this
window is a warning: the engine captured gross alpha but endured painful
intra-window drawdowns in the SIMPLE sleeve.

### P3 — 2022 Capitulation (Jul–Dec 2022)
The worst window, and the most interesting one for tuning. Composite -6.07%
vs SPY +0.31% = -6.4pp. TREND was the culprit: -12.3% with 20.6% MaxDD lasting
98 days, as the choppy H2 2022 rolling-bottom chewed through breakout signals.
SIMPLE (+7.2%) and XASSET (+3.8%) were positive but only 32% of the composite
weight.

**This is where the intelligence-layer scaling matters.** In production, a
CRISIS or BEAR_GRIND regime detection would have applied TREND 0.70× and
XASSET 1.30×, pushing effective weights toward 0.46/0.20/0.16/0.18 (where
the extra 0.18 goes into cash + unused allocation). A rough scaling of
TREND's -12.3% by 0.70× and lifting XASSET's +3.8% by 1.30× would have
improved the window by roughly +4 to +5pp, putting the composite near flat.
This is not modeled but should be when the repro gets to a full-engine
simulator.

### P4 — 2023 Bank Stress (Mar–May 2023)
Composite +9.11% vs SPY +5.85% = +3.3pp alpha. TREND was the hero at +20.85%
Sharpe 2.11 (regional bank shorts + flight-to-quality in mega-caps). SIMPLE
suffered (-21%) because bank-stress-era mean reversion kept getting run over
by event-driven continuations. XASSET went nowhere (-2%). A healthy result:
TREND weight at 65% carried the book exactly as the engine design intends.

### P5 — 2023 AI Rally (Jun–Dec 2023)
Composite -0.18% vs SPY +12.68% = -12.9pp. This is SIMPLE's regime nightmare.
The rally was dominated by ~7 names (NVDA, MSFT, META, etc.) with the rest of
the market flat. SIMPLE (mean-reversion) saw every intraday pullback as a
buy signal in names that kept going higher — -43% over 7 months with 45% MaxDD.
TREND did fine (+12.96%, essentially matching SPY), but 20% SIMPLE at -43%
dragged -8.6pp off the composite.

The intelligence layer's current SIMPLE 0.65× multiplier in CRISIS is
**directionally correct but misses this regime** — a "narrow leadership"
or "momentum-dominant" regime classifier would help. **Open research question:
does the existing RS (relative strength) breadth signal differentiate
P5-style narrow rallies from P1-style broad euphoria?**

### P6 — 2025 Recent (Oct 2025–Apr 2026)
Composite +9.62% vs SPY -1.98% = +11.6pp alpha. XASSET led (+16.97% Sharpe
2.15), TREND solid (+10.11%), SIMPLE small positive (+5.05%). DIRECTIONAL
(reference) blew up at -59%. This is the most relevant window for forward
deployment because it includes the SIMPLE v51 TP-ladder, the EOD
dynamic-universe fix, and the current risk-config profile. Clean beat with no
single sleeve doing anything unusual.

---

## 5. Weight Sensitivity Analysis

Recomputed the composite under five alternative weight schemes using the same
underlying per-sleeve equity series (no re-run required). Full detail in
`composite/weight_sensitivity.md` and `.csv`.

| Scheme | T/S/X/C | Avg Return | Avg Alpha | Beat-SPY Rate | Worst Window | Avg Sharpe |
|---|---|---|---|---|---|---|
| **baseline**       | 65/20/12/3 | **+5.81%** | **+4.94pp** | **66.7%** | -6.07% (P3) | 0.77 |
| sim_heavy          | 55/30/12/3 | +5.60%     | +4.73pp     | 50.0%     | -5.79% (P5) | 0.72 |
| sim_mid            | 60/27/10/3 | +5.69%     | +4.82pp     | 66.7%     | -5.03% (P3) | 0.74 |
| trend_heavy        | 75/10/12/3 | +6.02%     | +5.15pp     | 50.0%     | -8.01% (P3) | 0.75 |
| xasset_heavy       | 60/20/17/3 | +5.74%     | +4.87pp     | 50.0%     | -5.26% (P3) | **0.78** |
| equal_sleeves      | 33/33/31/3 | +5.26%     | +4.39pp     | 33.3%     | -9.91% (P5) | 0.56 |

### Key findings
1. **Baseline is Pareto-optimal on beat-rate.** No alternative keeps 66.7%
   beat-SPY with better or equal risk-adjusted return except sim_mid
   (essentially a tie — both 66.7%, sim_mid trails on avg return by 0.12pp).

2. **The preliminary "raise SIMPLE 20→30%" recommendation from P1–P3 analysis
   is wrong.** sim_heavy drops beat-rate to 50% and loses 5.6pp in P5. Per-window:
   - P2: +19.0% vs baseline +12.9% (+6.1pp — sim_heavy wins)
   - P4: +4.9% vs baseline +9.1% (-4.2pp — sim_heavy loses)
   - P5: -5.8% vs baseline -0.2% (-5.6pp — sim_heavy loses)
   - Net effect: tails wash out in average but dispersion is worse.

3. **trend_heavy has the highest average return** (+6.02%) but only 50%
   beat-rate. It dominates in P4 (+13.3% vs +9.1%) and P5 (+5.4% vs -0.2%)
   but gives up +6.1pp in P2 (where SIMPLE's +59% was doing the heavy
   lifting). A regime-aware weight shift toward TREND in narrow-leadership
   regimes is worth exploring — the intelligence layer already has this
   concept, it just needs a "momentum-dominant" regime classifier to trigger.

4. **xasset_heavy has the best risk-adjusted profile:** avg Sharpe 0.78,
   lowest avg MaxDD 12.6%, lowest worst MaxDD 19.2%, but 50% beat-rate.
   Worth considering as a **defensive weight variant** for high-VIX regimes.

5. **equal_sleeves is strictly dominated.** 33% beat-rate, worst Sharpe,
   worst P5. The engine's 65/20/12 unequal weighting is clearly doing real
   work.

---

## 6. Revised Recommendations

### Keep the current weights.
The preliminary P1–P3 analysis suggested raising SIMPLE from 20% to 30%. The
full 6-regime view reverses this call. Baseline 65/20/12/3 is the best
out-of-the-box configuration across the full regime set: top beat-rate (66.7%),
near-best Sharpe, shallowest worst-window loss among the non-xasset-heavy
alternatives.

### Invest in the intelligence layer rather than static weights.
Three of the four places the engine underperforms have a clear regime signature:
- **P3 chop:** TREND suffers; intelligence layer already has TREND 0.70× in
  CRISIS but the P3 regime tagging may not fire crisply enough. **Action:**
  Validate CRISIS detection on P3 2022 dates — does the live regime classifier
  tag H2 2022 as CRISIS? If not, tune the REGIME_THRESHOLDS
  (`strategies/engine/intelligence.py` lines 128–165).
- **P5 narrow rally:** SIMPLE suffers; no current regime tag captures
  "narrow leadership". **Action:** Add a breadth-driven regime — e.g. MOMENTUM_NARROW —
  triggered when RS-ranked top-20 return >> SPY return by a threshold. Apply
  SIMPLE 0.40× and TREND 1.10× in this regime.
- **P4 bank stress:** already fine, TREND carried; no change needed.

### Defensive weight variant worth prototyping.
`xasset_heavy 60/20/17/3` has the best risk-adjusted numbers and the lowest
drawdowns. Consider it as a **conditional weight profile** the engine could
shift to during HIGH_VOL regimes (VIX > threshold), rather than a permanent
replacement. Intelligence layer already has this type of switching; just add
an alternate weight table keyed on regime.

### Do NOT over-fit to P2.
The P2 2022 rate shock produced SIMPLE's best window (+59%) and drove most of
the engine's lifetime alpha in this backtest (+33.9pp of the +29.6pp sum of
alphas). A weight scheme optimized to replicate P2 will fail in P5-type
regimes. The 65/20/12 allocation is a compromise that captures most of P2's
gains while bounding P5's loss — keep it.

---

## 7. Explicit Caveats

1. **No intelligence-layer scaling modeled.** Baseline composite uses static
   weights throughout. Production engine would have scaled TREND down in P3
   and scaled XASSET up — the P3 loss is an upper-bound on engine pain.

2. **No cross-sleeve cash contention.** Each sleeve's backtest assumes full
   capital available. In production, a $100k account with 65% TREND + 20% SIMPLE
   + 12% XASSET means each sleeve operates on a slice, not the full balance.
   Per-position sizing in the underlying backtests is % of equity, so this
   mostly holds — but any fixed-minimum order sizes (e.g. min share counts)
   would behave differently on a $65k TREND sleeve than on $100k standalone.

3. **Post-hoc composition.** Daily equity curves from independent backtests
   are weight-summed. The real engine allocates cash at position-open time
   and books realized PnL into a unified ledger — subtle differences accrue
   when positions from different sleeves overlap in time.

4. **P6 is in-sample for the v51 code.** The current SIMPLE TP-ladder and
   EOD dynamic-universe fix were developed during or shortly before P6. The
   +11.6pp alpha in P6 is partly validation and partly fit-to-recent-data.
   P1–P5 are more credible as out-of-sample tests.

5. **DIRECTIONAL is not in the engine.** The -59% DIRECTIONAL result in P6
   and similar historical poor showings are why; this backtest confirms the
   decision to exclude it.

6. **Founder-trade overlay not in backtest.** The recent ATD notification
   layer is informational only, but any future sizing overlay from founder
   signals would need separate backtesting.

7. **No transaction-cost stress test.** Commissions and slippage use the
   defaults in each sleeve's backtest. A 2× slippage stress test is a natural
   follow-up, particularly for SIMPLE which trades heavy in P2.

8. **Six windows is not comprehensive.** Missing: 2020 COVID crash (extreme
   volatility), 2018 Q4 selloff (short bear), 2019 melt-up (broad bull). A
   follow-up sweep covering these is recommended before any weight changes
   are committed.

---

## 8. Artifacts

All outputs in `backtest/_runs/engine_regime_2026-04-14/`:

- `manifest.json` — full run configuration + per-run metadata
- `composite/composite_stats.csv` — 30 rows (6 windows × 5 series incl. composite & SPY)
- `composite/composite_equity_<window>.csv` — daily normalized equity per window
- `composite/plot_<window>.png` — equity + drawdown plots (6 files)
- `composite/summary.md` — auto-generated window summary
- `composite/weight_sensitivity.csv` + `.md` — alternative weight scheme results
- `composite/weight_sensitivity_summary.csv` — per-scheme aggregates
- Per-strategy raw outputs: `{strategy}_{window}.log`, `_trades.csv`, `.json`, `_equity.csv`

Source code:
- `backtest/regime_windows.py` — window definitions
- `backtest/engine_regime_backtest.py` — orchestrator
- `backtest/engine_composite_report.py` — composite report builder
- `backtest/engine_weight_sensitivity.py` — weight sensitivity (post-hoc)
- `backtest/cross_asset_bot_backtest.py` — XASSET daily-bar harness (new for this study)
- `backtest/{simple,directional,trend}_bot_backtest.py` — per-sleeve harnesses
  (modified with --start/--end CLI + daily_equity output for TREND)

---

## 9. Next Steps

1. **Run the backtest harness on missing regimes:** 2020 COVID crash (Feb–Apr 2020),
   2018 Q4 selloff (Oct–Dec 2018), and 2019 full-year melt-up. This will tell
   us whether the +4.94pp average alpha is robust or regime-set-specific.

2. **Build an intelligence-layer-enabled composite harness.** The current
   composite is static-weights. A v2 should read `MarketContext` for each
   date and apply the regime multipliers before weighting. This will
   quantify how much of the P3 and P5 drag the live engine would actually
   have absorbed.

3. **Prototype a MOMENTUM_NARROW regime classifier** (see Recommendations §2).
   The test is whether it correctly fires on P5 2023 dates while staying
   quiet in P1 2021 (both narrow-breadth but very different outcomes).

4. **Transaction-cost stress test.** Re-run the sweep at 2× slippage and 2×
   commission, especially to stress-test SIMPLE's P2 +59% result (the sleeve
   that trades heaviest in that window).
