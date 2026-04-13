"""
Simple Bot Parameter Optimizer
==============================

Systematically tests different parameter combinations to find optimal settings.
Uses simple_bot_backtest.py as the base engine.

Usage:
    python simple_bot_optimizer.py
"""

import subprocess
import re
import json
import os
from datetime import datetime
from itertools import product
from typing import Dict, List, Tuple
import pandas as pd

# Parameter ranges to test
PARAM_GRID = {
    # Entry filters
    "MIN_MOMENTUM_5MIN_PCT": [0.15, 0.20, 0.25, 0.30],
    "MIN_VWAP_DISTANCE_PCT": [0.20, 0.25, 0.30],

    # ADX filter bounds
    "MAX_ADX": [23.0, 25.0, 28.0],
    "MIN_ADX": [12.0, 15.0, 18.0],

    # Stop/Target configuration
    "ATR_STOP_MULTIPLIER": [5.0, 6.0, 7.0, 8.0],

    # Bracket allocation
    "SCALP_BRACKET_PCT": [0.70, 0.80, 0.90],

    # Trailing stop
    "TRAILING_STOP_ACTIVATION_R": [0.30, 0.40, 0.50],
    "TRAILING_STOP_DISTANCE_R": [0.15, 0.20, 0.25],

    # Time filters
    "NO_TRADE_LAST_MINUTES": [30, 45, 60],
}

# Quick sweep - fewer combinations for faster testing
QUICK_GRID = {
    "ATR_STOP_MULTIPLIER": [5.0, 6.0, 8.0],
    "SCALP_BRACKET_PCT": [0.70, 0.80],
    "TRAILING_STOP_ACTIVATION_R": [0.30, 0.40],
    "NO_TRADE_LAST_MINUTES": [30, 60],
}

# Single parameter sweep - test one param at a time
SINGLE_PARAM_TESTS = {
    "ATR_STOP_MULTIPLIER": [4.0, 5.0, 6.0, 7.0, 8.0, 10.0],
    "SCALP_BRACKET_PCT": [0.60, 0.70, 0.80, 0.90, 1.00],
    "TRAILING_STOP_ACTIVATION_R": [0.20, 0.30, 0.40, 0.50, 0.60],
    "MAX_ADX": [20.0, 23.0, 25.0, 28.0, 30.0],
    "MIN_ADX": [10.0, 12.0, 15.0, 18.0],
    "NO_TRADE_LAST_MINUTES": [15, 30, 45, 60, 90],
}

BACKTEST_FILE = "simple_bot_backtest.py"
RESULTS_FILE = "optimizer_results.csv"


def modify_backtest_params(params: Dict) -> str:
    """Create a modified version of the backtest file with new parameters."""
    with open(BACKTEST_FILE, "r") as f:
        content = f.read()

    modified = content
    for param, value in params.items():
        # Match parameter assignment lines
        pattern = rf"^{param}\s*=\s*[\d.]+.*$"
        replacement = f"{param} = {value}"
        modified = re.sub(pattern, replacement, modified, flags=re.MULTILINE)

    # Write to temp file
    temp_file = "simple_bot_backtest_temp.py"
    with open(temp_file, "w") as f:
        f.write(modified)

    return temp_file


def run_backtest(temp_file: str) -> Dict:
    """Run backtest and parse results."""
    try:
        result = subprocess.run(
            ["python", temp_file],
            capture_output=True,
            text=True,
            timeout=300
        )
        output = result.stdout + result.stderr

        # Parse results from output
        metrics = {
            "total_trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "net_pnl": 0.0,
            "avg_winner": 0.0,
            "avg_loser": 0.0,
            "avg_r": 0.0,
            "max_drawdown": 0.0,
            "return_pct": 0.0,
        }

        # Extract metrics using regex
        patterns = {
            "total_trades": r"Total Trades:\s*(\d+)",
            "win_rate": r"Win Rate:\s*([\d.]+)%",
            "profit_factor": r"Profit Factor:\s*([\d.]+)",
            "net_pnl": r"Net P&L:\s*\$?([-\d,.]+)",
            "avg_winner": r"Avg Winner:\s*\$?([\d,.]+)",
            "avg_loser": r"Avg Loser:\s*\$?([\d,.]+)",
            "avg_r": r"Avg R-Multiple:\s*([-\d.]+)R",
            "max_drawdown": r"Max Drawdown:\s*([\d.]+)%",
            "return_pct": r"Return:\s*([-\d.]+)%",
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, output)
            if match:
                val = match.group(1).replace(",", "")
                metrics[key] = float(val)

        return metrics

    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    except Exception as e:
        return {"error": str(e)}


def calculate_score(metrics: Dict) -> float:
    """
    Calculate composite score for optimization.

    Priorities:
    1. Profit Factor (most important for sustainability)
    2. Win Rate (psychological comfort)
    3. Net P&L (absolute returns)
    4. Max Drawdown penalty (risk management)
    """
    if "error" in metrics or metrics["total_trades"] < 20:
        return -999.0

    # Weighted composite score
    pf_score = metrics["profit_factor"] * 40  # Max ~80 points
    wr_score = metrics["win_rate"] * 0.5      # Max ~40 points
    pnl_score = min(20, metrics["net_pnl"] / 500)  # Max 20 points
    dd_penalty = metrics["max_drawdown"] * 2   # Penalty for drawdown

    # Bonus for balanced avg winner/loser ratio
    if metrics["avg_loser"] > 0:
        rr_ratio = metrics["avg_winner"] / metrics["avg_loser"]
        rr_bonus = min(10, rr_ratio * 5)  # Max 10 bonus points
    else:
        rr_bonus = 0

    score = pf_score + wr_score + pnl_score + rr_bonus - dd_penalty
    return score


def run_single_param_sweep(param_name: str, values: List) -> pd.DataFrame:
    """Test a single parameter across multiple values."""
    print(f"\n{'='*60}")
    print(f"SINGLE PARAMETER SWEEP: {param_name}")
    print(f"{'='*60}")

    results = []

    for value in values:
        params = {param_name: value}
        print(f"\nTesting {param_name} = {value}...")

        temp_file = modify_backtest_params(params)
        metrics = run_backtest(temp_file)

        if "error" not in metrics:
            score = calculate_score(metrics)
            result = {
                "param": param_name,
                "value": value,
                **metrics,
                "score": score
            }
            results.append(result)

            print(f"  Trades: {metrics['total_trades']}, "
                  f"WR: {metrics['win_rate']:.1f}%, "
                  f"PF: {metrics['profit_factor']:.2f}, "
                  f"P&L: ${metrics['net_pnl']:.2f}, "
                  f"Score: {score:.1f}")
        else:
            print(f"  ERROR: {metrics['error']}")

        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values("score", ascending=False)
        print(f"\nBest {param_name} = {df.iloc[0]['value']} (score: {df.iloc[0]['score']:.1f})")

    return df


def run_grid_search(param_grid: Dict, max_combinations: int = 50) -> pd.DataFrame:
    """Run grid search over parameter combinations."""
    print(f"\n{'='*60}")
    print("GRID SEARCH OPTIMIZATION")
    print(f"{'='*60}")

    # Generate all combinations
    param_names = list(param_grid.keys())
    param_values = list(param_grid.values())
    combinations = list(product(*param_values))

    print(f"Total combinations: {len(combinations)}")
    if len(combinations) > max_combinations:
        print(f"Limiting to first {max_combinations} combinations")
        combinations = combinations[:max_combinations]

    results = []

    for i, combo in enumerate(combinations, 1):
        params = dict(zip(param_names, combo))
        print(f"\n[{i}/{len(combinations)}] Testing: {params}")

        temp_file = modify_backtest_params(params)
        metrics = run_backtest(temp_file)

        if "error" not in metrics:
            score = calculate_score(metrics)
            result = {**params, **metrics, "score": score}
            results.append(result)

            print(f"  WR: {metrics['win_rate']:.1f}%, "
                  f"PF: {metrics['profit_factor']:.2f}, "
                  f"P&L: ${metrics['net_pnl']:.2f}, "
                  f"Score: {score:.1f}")
        else:
            print(f"  ERROR: {metrics['error']}")

        # Clean up
        if os.path.exists(temp_file):
            os.remove(temp_file)

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values("score", ascending=False)
        print(f"\n{'='*60}")
        print("TOP 5 CONFIGURATIONS:")
        print(f"{'='*60}")
        print(df.head(5).to_string())

    return df


def main():
    import sys

    print("""
============================================================
         SIMPLE BOT PARAMETER OPTIMIZER
============================================================
  Options:
    1. Single Parameter Sweep (test one param at a time)
    2. Quick Grid Search (limited combinations)
    3. Full Grid Search (comprehensive)
============================================================
    """)

    # Accept command line argument or prompt
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = input("Select mode (1/2/3): ").strip()

    all_results = []

    if choice == "1":
        # Single parameter sweeps
        for param, values in SINGLE_PARAM_TESTS.items():
            df = run_single_param_sweep(param, values)
            if not df.empty:
                all_results.append(df)

    elif choice == "2":
        # Quick grid search
        df = run_grid_search(QUICK_GRID, max_combinations=30)
        if not df.empty:
            all_results.append(df)

    elif choice == "3":
        # Full grid search
        df = run_grid_search(PARAM_GRID, max_combinations=100)
        if not df.empty:
            all_results.append(df)
    else:
        print("Invalid choice")
        return

    # Save all results
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        combined.to_csv(RESULTS_FILE, index=False)
        print(f"\nResults saved to {RESULTS_FILE}")

        # Print summary of best configurations
        print(f"\n{'='*60}")
        print("OPTIMIZATION COMPLETE - BEST CONFIGURATIONS")
        print(f"{'='*60}")

        best = combined.nlargest(10, "score")
        for i, row in best.iterrows():
            print(f"\nRank {best.index.get_loc(i)+1}:")
            print(f"  Score: {row['score']:.1f}")
            print(f"  Win Rate: {row['win_rate']:.1f}%")
            print(f"  Profit Factor: {row['profit_factor']:.2f}")
            print(f"  Net P&L: ${row['net_pnl']:.2f}")
            print(f"  Max Drawdown: {row['max_drawdown']:.1f}%")


if __name__ == "__main__":
    main()
