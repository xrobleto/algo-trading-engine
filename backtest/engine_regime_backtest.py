"""
Engine Multi-Regime Backtest Orchestrator
==========================================

Drives the four per-sleeve backtests (TREND, SIMPLE, DIRECTIONAL, XASSET) across the
six regime windows defined in backtest/regime_windows.py. Runs 24 backtests serially
(to avoid Polygon rate-limit collisions), collects outputs, and writes a manifest.

Downstream: backtest/engine_composite_report.py consumes the manifest to build the
engine-weighted composite equity curve and per-window stat tables.

Notes:
- DIRECTIONAL is NOT an engine sleeve; we run it for regime-level reference only.
  The composite report only combines TREND/SIMPLE/XASSET (engine weights).
- Each sub-run's stdout/stderr is captured to a per-run log file for debugging.
- Use --force to re-run and overwrite existing outputs. Default is skip-if-exists,
  which makes reruns cheap after fixing one strategy.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backtest.regime_windows import REGIME_WINDOWS, RegimeWindow  # noqa: E402


# ============================================================
# OUTPUT LAYOUT
# ============================================================

_ALGO_OUT = os.getenv("ALGO_OUTPUT_DIR")
_DEFAULT_RUN_ROOT = (
    Path(_ALGO_OUT) / "data" / "backtests"
    if _ALGO_OUT
    else _REPO_ROOT / "backtest" / "_runs"
)


# ============================================================
# STRATEGY SPECS
# ============================================================

@dataclass
class StrategySpec:
    name: str                       # short label (engine sleeve name or "DIRECTIONAL")
    script: str                     # script path relative to repo root
    output_kind: str                # "json_trend" | "csv_trades_simple" | "csv_trades_directional" | "xasset"
    engine_weight: float            # sleeve weight in composite; 0.0 for non-engine strategies


STRATEGIES: List[StrategySpec] = [
    StrategySpec("TREND",       "backtest/trend_bot_backtest.py",       "json_trend",            0.65),
    StrategySpec("SIMPLE",      "backtest/simple_bot_backtest.py",      "csv_trades_simple",     0.20),
    StrategySpec("XASSET",      "backtest/cross_asset_bot_backtest.py", "xasset",                0.12),
    StrategySpec("DIRECTIONAL", "backtest/directional_bot_backtest.py", "csv_trades_directional", 0.00),
]


# ============================================================
# RUN RECORD
# ============================================================

@dataclass
class RunRecord:
    strategy: str
    window: str
    window_start: str
    window_end: str
    engine_weight: float
    status: str                     # "success" | "failure" | "skipped"
    outputs: Dict[str, str]         # kind -> absolute path
    log_path: str
    runtime_seconds: float
    command: List[str]
    error: Optional[str] = None


# ============================================================
# SUBPROCESS RUNNER
# ============================================================

def _output_paths_for(spec: StrategySpec, window: RegimeWindow, run_dir: Path) -> Dict[str, str]:
    """Produce expected output file paths for a given (strategy, window). Orchestrator-owned."""
    prefix = f"{spec.name.lower()}_{window.label}"
    paths: Dict[str, str] = {}
    if spec.output_kind == "json_trend":
        paths["results_json"] = str(run_dir / f"{prefix}.json")
    elif spec.output_kind == "csv_trades_simple":
        paths["trades_csv"] = str(run_dir / f"{prefix}_trades.csv")
    elif spec.output_kind == "csv_trades_directional":
        paths["trades_csv"] = str(run_dir / f"{prefix}_trades.csv")
    elif spec.output_kind == "xasset":
        paths["trades_csv"] = str(run_dir / f"{prefix}_trades.csv")
        paths["equity_csv"] = str(run_dir / f"{prefix}_equity.csv")
    else:
        raise ValueError(f"Unknown output_kind: {spec.output_kind}")
    return paths


def _build_command(spec: StrategySpec, window: RegimeWindow, outputs: Dict[str, str]) -> List[str]:
    script_path = str(_REPO_ROOT / spec.script)
    cmd = [sys.executable, script_path, "--start", window.start.isoformat(), "--end", window.end.isoformat()]

    if spec.output_kind == "json_trend":
        cmd.extend(["--output", outputs["results_json"]])
    elif spec.output_kind in ("csv_trades_simple", "csv_trades_directional"):
        cmd.extend(["--output", outputs["trades_csv"]])
    elif spec.output_kind == "xasset":
        cmd.extend([
            "--output-trades", outputs["trades_csv"],
            "--output-equity", outputs["equity_csv"],
        ])
    return cmd


def _outputs_exist(outputs: Dict[str, str]) -> bool:
    return all(Path(p).exists() for p in outputs.values())


def run_one(spec: StrategySpec, window: RegimeWindow, run_dir: Path, force: bool) -> RunRecord:
    run_dir.mkdir(parents=True, exist_ok=True)
    outputs = _output_paths_for(spec, window, run_dir)
    log_path = run_dir / f"{spec.name.lower()}_{window.label}.log"
    cmd = _build_command(spec, window, outputs)

    if not force and _outputs_exist(outputs):
        print(f"  [SKIP] {spec.name} / {window.label} — outputs exist (use --force to re-run)")
        return RunRecord(
            strategy=spec.name, window=window.label,
            window_start=window.start.isoformat(), window_end=window.end.isoformat(),
            engine_weight=spec.engine_weight,
            status="skipped", outputs=outputs, log_path=str(log_path),
            runtime_seconds=0.0, command=cmd,
        )

    print(f"  [RUN ] {spec.name:12} / {window.label:22} -> {' '.join(Path(v).name for v in outputs.values())}")
    t0 = time.time()
    try:
        with open(log_path, "w", encoding="utf-8") as lf:
            proc = subprocess.run(
                cmd, cwd=str(_REPO_ROOT),
                stdout=lf, stderr=subprocess.STDOUT,
                check=False, text=True,
            )
        dt = time.time() - t0
        if proc.returncode == 0:
            print(f"         done in {dt:.1f}s")
            return RunRecord(
                strategy=spec.name, window=window.label,
                window_start=window.start.isoformat(), window_end=window.end.isoformat(),
                engine_weight=spec.engine_weight,
                status="success", outputs=outputs, log_path=str(log_path),
                runtime_seconds=dt, command=cmd,
            )
        else:
            # Read last lines of log for quick error surfacing
            try:
                with open(log_path, "r", encoding="utf-8") as lf:
                    tail = lf.readlines()[-10:]
            except Exception:
                tail = []
            err_msg = f"exit={proc.returncode}; tail=\n{''.join(tail)}"
            print(f"         FAILED: exit={proc.returncode} (see {log_path})")
            return RunRecord(
                strategy=spec.name, window=window.label,
                window_start=window.start.isoformat(), window_end=window.end.isoformat(),
                engine_weight=spec.engine_weight,
                status="failure", outputs=outputs, log_path=str(log_path),
                runtime_seconds=dt, command=cmd, error=err_msg,
            )
    except Exception as e:
        dt = time.time() - t0
        print(f"         FAILED: {e}")
        return RunRecord(
            strategy=spec.name, window=window.label,
            window_start=window.start.isoformat(), window_end=window.end.isoformat(),
            engine_weight=spec.engine_weight,
            status="failure", outputs=outputs, log_path=str(log_path),
            runtime_seconds=dt, command=cmd, error=str(e),
        )


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Run all per-sleeve backtests across all regime windows",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python backtest/engine_regime_backtest.py\n"
            "  python backtest/engine_regime_backtest.py --strategies TREND,SIMPLE --windows P6_2025_Recent\n"
            "  python backtest/engine_regime_backtest.py --force\n"
        ),
    )
    parser.add_argument(
        "--run-id", type=str,
        default=f"engine_regime_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        help="Subfolder name for this run's outputs (default: engine_regime_<utc-timestamp>)",
    )
    parser.add_argument(
        "--run-root", type=str, default=None,
        help=f"Root output folder (default: {_DEFAULT_RUN_ROOT})",
    )
    parser.add_argument(
        "--strategies", type=str, default=None,
        help="Comma-separated strategy names to run (default: all). Choices: TREND, SIMPLE, XASSET, DIRECTIONAL",
    )
    parser.add_argument(
        "--windows", type=str, default=None,
        help="Comma-separated window labels to run (default: all). See regime_windows.py for labels.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-run even if outputs already exist.",
    )
    args = parser.parse_args()

    run_root = Path(args.run_root) if args.run_root else _DEFAULT_RUN_ROOT
    run_dir = run_root / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    selected_strategies = STRATEGIES
    if args.strategies:
        want = {s.strip().upper() for s in args.strategies.split(",") if s.strip()}
        selected_strategies = [s for s in STRATEGIES if s.name in want]
        if not selected_strategies:
            print(f"[ERROR] No strategies matched {sorted(want)}")
            sys.exit(1)

    selected_windows = REGIME_WINDOWS
    if args.windows:
        want = {w.strip() for w in args.windows.split(",") if w.strip()}
        selected_windows = [w for w in REGIME_WINDOWS if w.label in want]
        if not selected_windows:
            print(f"[ERROR] No windows matched {sorted(want)}")
            sys.exit(1)

    total = len(selected_strategies) * len(selected_windows)
    print(f"[ORCHESTRATOR] Run ID: {args.run_id}")
    print(f"[ORCHESTRATOR] Output dir: {run_dir}")
    print(f"[ORCHESTRATOR] Strategies: {[s.name for s in selected_strategies]}")
    print(f"[ORCHESTRATOR] Windows:    {[w.label for w in selected_windows]}")
    print(f"[ORCHESTRATOR] Total runs: {total}")
    print()

    records: List[RunRecord] = []
    t_all = time.time()
    for wi, window in enumerate(selected_windows):
        print(f"\n=== Window {wi + 1}/{len(selected_windows)}: {window.label} ({window.start} -> {window.end}) ===")
        print(f"    {window.description}")
        for spec in selected_strategies:
            rec = run_one(spec, window, run_dir, args.force)
            records.append(rec)

    total_dt = time.time() - t_all

    # Manifest
    manifest = {
        "run_id": args.run_id,
        "run_dir": str(run_dir),
        "started_at_utc": datetime.utcnow().isoformat(),
        "total_runtime_seconds": round(total_dt, 2),
        "strategy_specs": [asdict(s) for s in selected_strategies],
        "windows": [
            {"label": w.label, "start": w.start.isoformat(), "end": w.end.isoformat(),
             "description": w.description}
            for w in selected_windows
        ],
        "runs": [asdict(r) for r in records],
    }
    manifest_path = run_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    # Summary
    n_ok = sum(1 for r in records if r.status == "success")
    n_skip = sum(1 for r in records if r.status == "skipped")
    n_fail = sum(1 for r in records if r.status == "failure")
    print("\n" + "=" * 60)
    print("ORCHESTRATOR SUMMARY")
    print("=" * 60)
    print(f"Total runs: {len(records)}")
    print(f"  success:  {n_ok}")
    print(f"  skipped:  {n_skip}")
    print(f"  failure:  {n_fail}")
    print(f"Total runtime: {total_dt / 60:.1f} min")
    print(f"Manifest: {manifest_path}")
    if n_fail > 0:
        print("\nFailures:")
        for r in records:
            if r.status == "failure":
                print(f"  - {r.strategy} / {r.window}: {r.log_path}")

    sys.exit(0 if n_fail == 0 else 2)


if __name__ == "__main__":
    main()
