# Horizon backtest↔live divergence: the book is ~30% under-deployed

Found 2026-08-24 while verifying C5 after the CP2 cap raise. This is exactly the class of
bug the G1 "decision equivalence" gate exists to catch — the live engine is NOT deploying
the book its own validation measured.

## The mechanism
`horizon/config.py` defines four sleeves, all `enabled` by default:
PULSE 0.45 · ROTATION 0.25 · **REVERT 0.20 · DRIFT 0.10**, with `book_leverage = 1.5`.

REVERT and DRIFT were **REJECTED** by validation, so `engine/main.py` only ever asks
`ADMITTED_SLEEVES = ["PULSE", "ROTATION"]` to decide. But `engine/sleeves.py:budgets()`
normalizes weights across **all enabled sleeves**:
```
raw[sid] = base_allocation * regime_mult ;  weight = raw[sid] / sum(raw)
sleeve_equity = total_equity * weight * book_leverage
```
So the 30% of weight belonging to the two rejected sleeves is computed, then silently
never deployed — it just becomes idle cash.

Meanwhile the validation composes the frontier from admitted sleeves only
(`backtest/run_validation.py:240`: `base = {s: cfg.sleeves[s].base_allocation for s in final_admitted}`),
renormalized inside `backtest/portfolio.py`. Its "book_leverage 1.5" therefore means the
**full** 1.5×.

## Measured impact (2026-08-24, cap $3,850)
| | value |
|---|---|
| PULSE sleeve (live) | $2,743 |
| ROTATION sleeve (live) | $1,275 |
| target book | QQQM $3,323 + PDBC $637 = **$3,961** |
| effective leverage | **1.03× the cap** (config says 1.5×) |
| dilution factor | 0.696 (= admitted raw weight ÷ all-sleeve raw weight) |
| capital idle vs validated config | **≈ $1,773** |

Validated frontier for reference: 1.0× → 16.7% CAGR / −26.8% maxDD · 1.4× → 19.1% / −33.0%
· 1.8× → 20.8% / −37.6%. Live is running the ~1.0× return profile while configured for ~1.5×.

## Fix (NOT applied — needs a user risk decision)
Mirror what the validation did: disable the rejected sleeves and renormalize the admitted ones.
```python
"REVERT":  SleeveConfig(..., enabled=False),
"DRIFT":   SleeveConfig(..., enabled=False),
"PULSE":   base_allocation=0.6429,   # 0.45 / 0.70
"ROTATION": base_allocation=0.3571,  # 0.25 / 0.70
```
(`_validate()` requires enabled base allocations to sum to 1.0, so the renormalization is required.)

**Consequence:** gross exposure $4,002 → ~$5,775 (+44%), funded from the $2,705 cash
(needs ≈$1,773, leaves ≈$930). Drawdown profile moves from roughly −27% toward −34%.
This is a real risk increase and must be the user's call — but it is the configuration
that was actually validated, and the user's stated stance is profitability-first with high
risk tolerance.

**Also fix regardless of the leverage decision:** `budgets()` should normalize over the
sleeves that can actually trade, so this cannot silently recur if the admitted set changes.

## Note on prior documentation
Earlier gate notes stated "book leverage 1.0× during the gate." That was an unverified
assumption — the value has been 1.5× (diluted to ~1.03× effective) since Horizon went live
on 2026-08-12. Corrected here.

---

## RESOLVED 2026-08-24 (commit 0b420ad, deployed)
User approved the fix. Applied exactly as proposed:
- `horizon/config.py`: REVERT + DRIFT `enabled=False`, `base_allocation=0.0`;
  PULSE 0.642857 / ROTATION 0.357143 (0.45:0.25 ratio preserved). Pre-gate values kept
  in comments.
- `horizon/engine/main.py`: `ADMITTED_SLEEVES` now DERIVED from the config `enabled`
  flags — config is the single source of truth for both trading set and weighting, so
  this class of drift cannot recur silently.
- `utilities/horizon_checkpoint.py`: C5 made leverage-aware (gross vs
  `cap x book_leverage x 1.05`); otherwise every checkpoint above 1.0x would have
  false-alarmed DIRTY.

Verified: horizon tests 10/10; target book at the $3,850 cap moved
$3,961 (1.03x) -> **$5,692 (1.48x)**; needs +$1,687 of $2,705 cash (no margin —
account-level exposure ~85%, ~$1,018 cash left). Scorecard re-run after deploy: 5/5 CLEAN
(C5 now reads `gross $4,003 <= $6,064 (cap $3,850 x 1.50x)`).

**The scale-up executes at the next weekday 09:00 ET cycle** (today's had already run;
the engine records the cycle date to avoid double-running). Expect QQQM ~$4,776 and
PDBC ~$916 after it converges — watch the first cycle for order count and fill quality.
