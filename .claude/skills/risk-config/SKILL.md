---
name: risk-config
description: Adjust risk levels and trading configuration across strategies and alerts. Use when the user wants to change risk levels, thresholds, or trading parameters.
---

# Risk Configuration — Algo Trading

Adjust risk levels and trading parameters across the system.

## Risk Level System

The system uses a 3-tier risk level in `config/alerts_config.yaml`:

```yaml
risk_level: 1  # 1=Conservative, 2=Moderate, 3=Aggressive
```

| Level | Alert Frequency | Quality | Use Case |
|-------|----------------|---------|----------|
| 1 (Conservative) | ~1-3/week | WOW-only, highest quality | Capital preservation |
| 2 (Moderate) | ~3-8/week | Balanced quality vs frequency | Recommended default |
| 3 (Aggressive) | ~8-15/week | More frequent, lower bar | Active trading |

## Configuration files (in `config/`)

| File | Controls |
|------|----------|
| `alerts_config.yaml` | Alert thresholds, risk levels, watchlist, HYSA goal tracking |
| `master_bot_config.yaml` | Multi-strategy orchestrator settings |
| `trend_bot.env` | Trend bot specific settings |
| `directional_bot.env` | Directional bot settings |
| `smallcap_scanner.env` | Scanner settings |

## Utility for bulk risk changes

`utilities/apply_risk_level.py` — programmatically adjusts config thresholds based on risk level.

## Key thresholds affected by risk level (alerts_config.yaml)

**Sell signals** (examples by level):
- RSI overbought: Level 1: 74/82 | Level 2: 70/78 | Level 3: 68/75
- Extension and breakdown thresholds scale similarly

**Buy signals:**
- RSI oversold thresholds
- Support test proximity
- Volume confirmation requirements

## When adjusting risk

1. Ask which risk level the user wants (1, 2, or 3) if not specified in `$ARGUMENTS`
2. Read the current `config/alerts_config.yaml` to show current settings
3. Either:
   - Run `python utilities/apply_risk_level.py` to apply the preset, OR
   - Manually edit `config/alerts_config.yaml` for fine-tuned control
4. Show the user what changed (before → after)
5. Warn if switching to Level 3 (aggressive) — more trades, lower quality bar
6. If the user wants to adjust individual thresholds beyond the presets, edit `alerts_config.yaml` directly

## HYSA Goal Context

The config tracks a savings goal:
```yaml
hysa:
  goal_usd: 100000
  quit_date: "2026-03-20"
  current_hysa_usd: 76000
```

When adjusting risk, consider progress toward this goal and time remaining.
