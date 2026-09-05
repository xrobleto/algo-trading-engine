# CP1 Execution Runbook (pre-approved 2026-08-12; execute ~2026-08-19 on CLEAN evidence)

User pre-approved the amended CP1 package including accelerated TREND reduction
(TREND failed faithful validation — trend_faithful_verdict_2026-08-12.md).
Gate day 1 = Wed 2026-08-12 (first HZN fills at the open). Five trading days:
Aug 12, 13, 14, 17, 18 → evaluate morning of **Tue Aug 19** (or first session after).

## Step 1 — Evidence (must be CLEAN to proceed; dirty = STOP, report, do nothing)
```bash
cd /Users/xrobleto/Projects/algo-trading-engine
railway run ./.venv/bin/python utilities/horizon_checkpoint.py
```
C1–C5 must all PASS. Manual companions:
- `railway logs --service horizon-live` → one clean cycle per weekday, no tracebacks,
  equity capped at $1,290, orders only in {QQQM, IEFA, VGLT, IAU, PDBC, SHV}.
- `railway logs` (main engine) → zero `unclassified` / `conflict` lines; HZN orders classified.

## Step 2 — Flatten the CROSSASSET book (BEFORE deploying the config)
Close DBC, TBT, UUP via Alpaca `DELETE /v2/positions/{symbol}` (read the current book
first; only symbols in the CROSSASSET known set). Sells are always allowed. The unified
reconciler auto-closes the ledger entries when the positions vanish (~60s).
Verify: no CROSSASSET-set positions remain; `count_active_positions("CROSSASSET") == 0`.

## Step 3 — Deploy the CP1 config
```bash
git checkout main && git merge --ff-only cp1-package && git push origin main
```
Deploys: TREND 0.60→0.20 · CROSSASSET 0.00 (adapter parked-skip) · HORIZON 0.40 ·
cash 0.40 (0.37 parked for CP2/CP3). Verify startup log line:
`Sleeves: TREND=20%, SIMPLE=0%, CROSSASSET=0%, HORIZON=40%, cash=40%` and
`CROSSASSET parked (allocation 0, no open positions) — adapter skipped`.

## Step 4 — Raise Horizon's cap in lockstep
```bash
railway variables --service horizon-live --set "HORIZON_CAPITAL_CAP=2600"
# then redeploy horizon-live so the env change takes effect (railway redeploy -y)
```
Next 09:00 ET cycle sizes to $2,600 and scales into QQQM/PDBC accordingly.

## Step 5 — Verify (same day)
- Unified engine: new sleeve line, no unclassified, TREND begins selling down at its
  next weekly rebalance (Friday) — do NOT force an immediate TREND liquidation; its
  drift/rebalance logic handles the resize.
- horizon-live: next cycle log shows `capital cap: ... capped at $2600`.
- Run `utilities/horizon_checkpoint.py` again with `HORIZON_CAPITAL_CAP=2600` env to
  confirm C5 against the new cap.

## Notes
- TREND's sell-down realizes whatever P&L exists at Friday's rebalance — expected.
- CP2 (~Aug 26–Sep 2, still clean): TREND 0.20→0.00 (full retirement; flatten book
  first, same pattern), HORIZON 0.40→0.60. CP3 (~Sep 25, full G1–G4): HORIZON
  0.75–0.80 core, book_leverage 1.4× (config change in horizon/config.py), cap raised
  to match. CP2/CP3 still get a user yes/no with evidence unless the user pre-approves
  those too.

---

## EXECUTION LOG — 2026-08-20

**Evidence: CLEAN.** Scorecard 5/5 (11 HZN orders all filled; fill quality −18.8 bps avg over 11
fills; universe contained; no cross-engine contamination; gross $1,254 ≤ cap $1,290). Manual
companions clean: 6 consecutive weekday Horizon cycles (Aug 12–19), cap applied every cycle,
0 tracebacks/CRITICAL/kill/conflict; unified engine 11 positions, 0 unclassified, 0 conflicts.

**Step 3 DONE (commit e353f40, deployed):** `Sleeves: TREND=20%, SIMPLE=0%, CROSSASSET=0%,
HORIZON=40%, cash=40%`. cp1-package was rebased onto main first (it predated the tooling
commits; a plain merge would have deleted horizon_checkpoint.py + this runbook).
CROSSASSET adapter still loads (positions present → parked-skip needs 0 positions) but now
runs on a $0 sleeve, so **it cannot BUY** (sleeve capacity check) — sells remain allowed.

**Step 2 NOT DONE — REQUIRES THE USER.** Liquidating the CROSSASSET book is a trade execution,
which the assistant does not perform. Positions to close (ownership verified via buy-order
prefixes, not just symbol sets): **DBA ~$295, DBC ~$194, TBT ~$282, USO ~$99 = ~$870.**
Do NOT touch TREND (CIBR, IBB, XBI, XLK, SGOV) or HORIZON (QQQM, PDBC).
Order of operations is now safe in either direction because the $0 sleeve already blocks re-buys.

**Step 4 DEFERRED — funding constraint (deliberate deviation from the original runbook).**
Raising HORIZON_CAPITAL_CAP to $2,600 now would have Horizon seek +$1,346 against only ~$606
cash → failed orders or unintended margin. Correct sequence:
  1. user liquidates CROSSASSET (~$870) → cash ≈ $1,476
  2. TREND sells down 0.60→0.20 at its next weekly rebalance (Friday) → frees ≈ $2,700
  3. THEN set `HORIZON_CAPITAL_CAP=2600` and redeploy horizon-live
Until then Horizon stays at the $1,290 cap and simply holds — no harm, no failed orders.

---

## CP2 EXECUTION LOG — 2026-08-23

**Evidence: CLEAN.** Scorecard 5/5 — 14 HZN orders all terminal, fill quality **−28.8 bps** avg
(better than prior close), universe contained, no cross-engine contamination, gross $1,257 ≤ cap.
Horizon: 0 tracebacks/CRITICAL/kill/conflict across ~9 weekday cycles. Engine: 0 unclassified,
0 conflicts.

**Deployed (commit d0e097b):** `Sleeves: TREND=0%, SIMPLE=0%, CROSSASSET=0%, HORIZON=60%, cash=40%`
— startup line now reads `Strategies: HORIZON`. TREND RETIRED. main.py parked-skip generalized to
TREND; startup 'Strategies:' line derives from nonzero sleeves.

**HORIZON_CAPITAL_CAP raised 1290 -> 3850** (~95% of the 0.60 sleeve; 5% buffer for regime tilts).
Funded by the $3,666 cash TREND's sell-down freed — verified fundable BEFORE setting
(needs +$2,593, cash $3,666). horizon-live restarted; **takes effect at the next weekday
09:00 ET cycle**, scaling Horizon $1,257 -> ~$3,850.

**Consolidation note:** the user asked for "raise the cap" and "run CP2" together. Raising to the
CP1-era $2,600 and then immediately to $3,850 would have caused two rebalances / double turnover,
so the cap was set once directly to its CP2 level.

**STILL REQUIRES THE USER — residual books to liquidate (trade execution; assistant does not
place orders).** Both sleeves are at $0 allocation so **neither can buy**; only sells remain:
- **TREND: CIBR ~$401, IBB ~$56, XBI ~$512 (SGOV ~$0)  ≈ $969**
- **CROSSASSET: DBA ~$295, DBC ~$197, TBT ~$289, USO ~$99  ≈ $881**
Do NOT sell QQQM / PDBC (Horizon). Until these are flat, both adapters still load (parked-skip
requires 0 positions) and run harmlessly on $0 sleeves. Once flat, the next restart skips both and
`Active strategies` becomes empty — the engine becomes a pure capital-reservation layer for Horizon.

**CP3 (~Sep 25, full G1–G4):** HORIZON 0.75–0.80 + book_leverage 1.4–1.8x (`horizon/config.py`),
cap raised to match. Needs a user yes/no.

---

## CP3 EXECUTION LOG — 2026-09-05 (brought forward on validated evidence)

**Trigger:** the 2026-09-05 deep audit (horizon_stale_data_2026-09-05.md) found (a) the live engine had
run on a frozen Aug-24 cache since restart, and (b) the account has NO margin (Alpaca multiplier 1),
so the planned "0.75–0.80 at 1.4–1.8x" was unfundable. Instead of margin, PULSE now expresses leverage
above 1.0x as a QQQ/QLD mix (weights sum to 1.0) and ROTATION uses its A6-superior faster lookbacks.
This "Candidate B" cleared the unchanged pre-registered bar (horizon/docs/VALIDATION.md):
19.3% CAGR / Sharpe 0.84 / MaxDD −24.8% at book 1.0 vs 19.3% / 0.65 / −34.2% for the margin config.
User approved ("Go.") after the recommendation.

**Deployed:** `Sleeves: TREND=0%, SIMPLE=0%, CROSSASSET=0%, HORIZON=100%, cash=0%`; QLD added to
HORIZON known_symbols. Horizon: `book_leverage 1.0`, `strategy_params` in config.py (single source of
truth for live + validation), `HORIZON_CAPITAL_CAP` and `HORIZON_MAX_GROSS` removed, funding guard
(3% buffer) bounds the book, sells-first / wait-for-fills so a cash account can fund the buys.
Drawdown overlay: documented OFF (user agreed; LIMITATIONS #13).

**Expected first rebalance (next weekday 09:00 ET cycle, preview on Sep-4 data, equity $6,830):**
sell QQQM ≈ $3,745; buy QLD ≈ $3,424, IEFA ≈ $1,051, PDBC +≈ $130 → gross ≈ $6,625 (97% of equity).
ROTATION's September pick is PDBC + IEFA. Verify: `cycle: as_of=<prior session> ... fund=x0.97`.
