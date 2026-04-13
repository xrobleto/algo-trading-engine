"""
HYSA Phase 1 - Risk Level Configuration Helper

This script applies risk level presets (1-3) to your config.yaml.
Run this after changing risk_level in config.yaml to update all thresholds.

Usage:
    python apply_risk_level.py

The script will:
1. Read risk_level from config.yaml
2. Apply the appropriate preset values
3. Update config.yaml with new thresholds
4. Show a summary of what changed
"""

import yaml
from pathlib import Path
from typing import Dict, Any

# =============================================================================
# RISK LEVEL PRESETS
# =============================================================================

RISK_PRESETS = {
    1: {  # Conservative - WOW-only
        "analysis": {
            "rsi_overbought": 74,
            "rsi_very_overbought": 82,
            "extend_over_ema20_pct": 0.10,
            "drawdown_from_20d_high_pct": 0.12,
            "min_unrealized_gain_pct_for_strength_sell": 0.12,
            "ticker_alert_cooldown_minutes": 1440,
            "max_positions_in_email": 3,
            "min_score_to_alert_strength": 75,
            "min_score_to_alert_riskoff": 65,
        },
        "buy": {
            "max_positions_in_email": 3,
            "ticker_alert_cooldown_minutes": 1440,
            "min_score_to_alert_breakout": 75,
            "min_score_to_alert_pullback": 70,
            "min_score_to_alert_meanrevert": 90,
            "require_uptrend_for_pullback": True,
            "require_uptrend_for_breakout": True,
            "pullback_to_ema20_min_pct": -0.03,
            "pullback_to_ema20_max_pct": 0.00,
            "max_extension_above_ema20_pct": 0.06,
            "breakout_near_20d_high_buffer_pct": 0.002,
            "breakout_volume_ratio_min": 2.5,
            "meanrevert_rsi_max": 25,
            "risk_per_trade_usd": 200,
            "max_position_usd": 3000,
            "stop_atr_mult_pullback": 1.5,
            "stop_atr_mult_breakout": 1.7,
            "stop_atr_mult_meanrevert": 1.8,
        }
    },
    2: {  # Moderate - Balanced
        "analysis": {
            "rsi_overbought": 70,
            "rsi_very_overbought": 78,
            "extend_over_ema20_pct": 0.08,
            "drawdown_from_20d_high_pct": 0.10,
            "min_unrealized_gain_pct_for_strength_sell": 0.08,
            "ticker_alert_cooldown_minutes": 720,
            "max_positions_in_email": 5,
            "min_score_to_alert_strength": 65,
            "min_score_to_alert_riskoff": 55,
        },
        "buy": {
            "max_positions_in_email": 5,
            "ticker_alert_cooldown_minutes": 720,
            "min_score_to_alert_breakout": 65,
            "min_score_to_alert_pullback": 60,
            "min_score_to_alert_meanrevert": 80,
            "require_uptrend_for_pullback": True,
            "require_uptrend_for_breakout": True,
            "pullback_to_ema20_min_pct": -0.05,
            "pullback_to_ema20_max_pct": 0.00,
            "max_extension_above_ema20_pct": 0.08,
            "breakout_near_20d_high_buffer_pct": 0.005,
            "breakout_volume_ratio_min": 2.0,
            "meanrevert_rsi_max": 28,
            "risk_per_trade_usd": 300,
            "max_position_usd": 4500,
            "stop_atr_mult_pullback": 1.8,
            "stop_atr_mult_breakout": 2.0,
            "stop_atr_mult_meanrevert": 2.2,
        }
    },
    3: {  # Aggressive - High frequency
        "analysis": {
            "rsi_overbought": 68,
            "rsi_very_overbought": 75,
            "extend_over_ema20_pct": 0.06,
            "drawdown_from_20d_high_pct": 0.08,
            "min_unrealized_gain_pct_for_strength_sell": 0.05,
            "ticker_alert_cooldown_minutes": 360,
            "max_positions_in_email": 8,
            "min_score_to_alert_strength": 55,
            "min_score_to_alert_riskoff": 45,
        },
        "buy": {
            "max_positions_in_email": 8,
            "ticker_alert_cooldown_minutes": 360,
            "min_score_to_alert_breakout": 55,
            "min_score_to_alert_pullback": 50,
            "min_score_to_alert_meanrevert": 70,
            "require_uptrend_for_pullback": False,
            "require_uptrend_for_breakout": True,
            "pullback_to_ema20_min_pct": -0.07,
            "pullback_to_ema20_max_pct": 0.02,
            "max_extension_above_ema20_pct": 0.10,
            "breakout_near_20d_high_buffer_pct": 0.010,
            "breakout_volume_ratio_min": 1.5,
            "meanrevert_rsi_max": 30,
            "risk_per_trade_usd": 400,
            "max_position_usd": 6000,
            "stop_atr_mult_pullback": 2.0,
            "stop_atr_mult_breakout": 2.3,
            "stop_atr_mult_meanrevert": 2.5,
        }
    }
}

RISK_LEVEL_NAMES = {
    1: "Conservative (WOW-only)",
    2: "Moderate (Balanced)",
    3: "Aggressive (High frequency)"
}


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load YAML config file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def save_config(config: Dict[str, Any], config_path: Path):
    """Save YAML config file"""
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def apply_risk_level(config: Dict[str, Any], risk_level: int) -> Dict[str, Any]:
    """
    Apply risk level preset to config.

    Args:
        config: Current config dict
        risk_level: 1 (Conservative), 2 (Moderate), or 3 (Aggressive)

    Returns:
        Updated config dict
    """
    if risk_level not in RISK_PRESETS:
        raise ValueError(f"Invalid risk_level: {risk_level}. Must be 1, 2, or 3.")

    preset = RISK_PRESETS[risk_level]

    # Apply analysis (sell) settings
    if "analysis" in config:
        for key, value in preset["analysis"].items():
            config["analysis"][key] = value

    # Apply buy settings
    if "buy" in config:
        for key, value in preset["buy"].items():
            config["buy"][key] = value

    return config


def show_summary(risk_level: int):
    """Print summary of applied risk level"""
    print("\n" + "=" * 80)
    print(f"RISK LEVEL {risk_level} APPLIED: {RISK_LEVEL_NAMES[risk_level]}")
    print("=" * 80)

    preset = RISK_PRESETS[risk_level]

    print("\nSELL ALERT SETTINGS:")
    print(f"  RSI Overbought:              {preset['analysis']['rsi_overbought']}")
    print(f"  RSI Very Overbought:         {preset['analysis']['rsi_very_overbought']}")
    print(f"  Extension over EMA20:        {preset['analysis']['extend_over_ema20_pct']*100:.1f}%")
    print(f"  Drawdown from 20D high:      {preset['analysis']['drawdown_from_20d_high_pct']*100:.1f}%")
    print(f"  Min gain for strength sell:  {preset['analysis']['min_unrealized_gain_pct_for_strength_sell']*100:.1f}%")
    print(f"  Alert cooldown:              {preset['analysis']['ticker_alert_cooldown_minutes']//60} hours")
    print(f"  Max positions per email:     {preset['analysis']['max_positions_in_email']}")
    print(f"  Min score (strength/riskoff): {preset['analysis']['min_score_to_alert_strength']}/{preset['analysis']['min_score_to_alert_riskoff']}")

    print("\nBUY ALERT SETTINGS:")
    print(f"  Max positions per email:     {preset['buy']['max_positions_in_email']}")
    print(f"  Alert cooldown:              {preset['buy']['ticker_alert_cooldown_minutes']//60} hours")
    print(f"  Min score (BO/PB/MR):        {preset['buy']['min_score_to_alert_breakout']}/{preset['buy']['min_score_to_alert_pullback']}/{preset['buy']['min_score_to_alert_meanrevert']}")
    print(f"  Pullback range:              {preset['buy']['pullback_to_ema20_min_pct']*100:.1f}% to {preset['buy']['pullback_to_ema20_max_pct']*100:.1f}%")
    print(f"  Max extension above EMA20:   {preset['buy']['max_extension_above_ema20_pct']*100:.1f}%")
    print(f"  Breakout proximity to 20D:   {preset['buy']['breakout_near_20d_high_buffer_pct']*100:.2f}%")
    print(f"  Breakout volume ratio:       {preset['buy']['breakout_volume_ratio_min']:.1f}x")
    print(f"  Mean reversion RSI max:      {preset['buy']['meanrevert_rsi_max']}")
    print(f"  Risk per trade:              ${preset['buy']['risk_per_trade_usd']:,}")
    print(f"  Max position size:           ${preset['buy']['max_position_usd']:,}")
    print(f"  Stop ATR mult (PB/BO/MR):    {preset['buy']['stop_atr_mult_pullback']}/{preset['buy']['stop_atr_mult_breakout']}/{preset['buy']['stop_atr_mult_meanrevert']}")
    print(f"  Require uptrend (pullback):  {preset['buy']['require_uptrend_for_pullback']}")
    print(f"  Require uptrend (breakout):  {preset['buy']['require_uptrend_for_breakout']}")

    print("\n" + "=" * 80)


def main():
    """Main execution"""
    config_path = Path(__file__).parent / "config.yaml"

    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        return

    print(f"Loading config from: {config_path}")
    config = load_config(config_path)

    # Get current risk level
    risk_level = config.get("risk_level")
    if risk_level is None:
        print("ERROR: 'risk_level' not found in config.yaml")
        print("Please add 'risk_level: 1' (or 2, 3) to the top of your config.yaml")
        return

    if risk_level not in [1, 2, 3]:
        print(f"ERROR: Invalid risk_level: {risk_level}")
        print("Must be 1 (Conservative), 2 (Moderate), or 3 (Aggressive)")
        return

    print(f"Current risk_level: {risk_level} ({RISK_LEVEL_NAMES[risk_level]})")

    # Apply risk level
    print("\nApplying preset values...")
    config = apply_risk_level(config, risk_level)

    # Save updated config
    print(f"Saving updated config to: {config_path}")
    save_config(config, config_path)

    # Show summary
    show_summary(risk_level)

    print("\n✅ Config updated successfully!")
    print("\nNOTE: The comments in config.yaml show all preset values for reference,")
    print("but this script has updated the actual values to match your risk_level.")


if __name__ == "__main__":
    main()
