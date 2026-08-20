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
