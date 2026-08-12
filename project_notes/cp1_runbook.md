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
