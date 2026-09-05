"""Horizon Engine configuration.

Static configuration only. Dynamic per-cycle state lives in the ledger and in
each strategy's StrategyState; market-driven adjustments come from the
intelligence layer at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SleeveConfig:
    """Per-strategy capital sleeve."""

    strategy_id: str
    order_prefix: str          # client_order_id tag — ownership is prefix-based
    base_allocation: float     # fraction of total equity before regime scaling
    max_positions: int
    enabled: bool = True


@dataclass
class CostModel:
    """Realistic execution-cost assumptions used by the backtest harness.

    These are deliberately conservative. The harness exposes them so a
    sensitivity sweep can show how results degrade as costs rise.
    """

    broad_etf_slippage_bps: float = 2.0     # SPY/QQQ/IWM/DIA-class liquidity
    sector_etf_slippage_bps: float = 5.0    # sector / thematic ETFs
    overnight_slippage_bps: float = 4.0     # DRIFT close & open auction legs
    commission_per_trade: float = 0.0       # Alpaca equities are commission-free
    margin_rate_annual: float = 0.065       # borrow cost on leveraged dollars
    cash_is_bil: bool = True                # idle cash earns the real BIL return


@dataclass
class WithdrawalConfig:
    """High-water-mark monthly profit skim (the user's income mechanism)."""

    enabled: bool = True
    skim_fraction: float = 0.50      # share of gain above the HWM to withdraw
    min_withdrawal: float = 50.0     # skip trivially small skims
    buffer_pct: float = 0.02         # only skim once equity is 2% above the HWM


@dataclass
class EngineConfig:
    starting_equity: float = 7400.0
    benchmark: str = "QQQ"
    # Book-level leverage on the diversified sleeve blend. 2026-09-05 (CP3):
    # 1.0 — the live account has no margin (Alpaca multiplier 1) and PULSE now
    # expresses leverage through QLD with weights summing to 1.0, so a book
    # multiplier above 1.0 would request dollars the account cannot borrow.
    # Validated frontier: docs/VALIDATION.md.
    book_leverage: float = 1.0
    # 2008-01 start: BIL (T-bills ETF, the cash leg) launched 2007-05, so by
    # 2008 it has the history PULSE/ROTATION need. The window still spans the
    # 2008 GFC, 2011, 2015-16, 2018, 2020 crash, 2022 bear and 2023-26.
    backtest_start: str = "2008-01-02"
    backtest_end: str = "2026-05-15"
    risk_free_annual: float = 0.04           # for Sharpe; the harness also uses BIL

    sleeves: Dict[str, SleeveConfig] = field(default_factory=dict)
    # Constructor kwargs per strategy — THE single source of truth for the
    # parameters that trade live. strategies/registry.py applies them, and
    # backtest/run_validation.py validates exactly these (BASELINE).
    strategy_params: Dict[str, dict] = field(default_factory=dict)
    cost: CostModel = field(default_factory=CostModel)
    withdrawal: WithdrawalConfig = field(default_factory=WithdrawalConfig)

    # client_order_id namespace for the live engine
    order_namespace: str = "HZN"


def build_default_config() -> EngineConfig:
    """The canonical Horizon configuration.

    Base allocations are fixed a priori from risk-budget reasoning, not fitted:
    PULSE is the return engine and carries the most capital; ROTATION, REVERT
    and DRIFT are diversifiers. After validation, only sleeves that clear the
    gating bar receive capital and the weights renormalize across them.
    """
    # 2026-08-24: REVERT and DRIFT were REJECTED by validation, but were left
    # `enabled` with 30% of the base allocation between them. Because
    # engine/sleeves.py:budgets() normalizes weights across every ENABLED sleeve
    # while only the admitted sleeves actually trade, that 30% was computed and
    # then never deployed — silently diluting book_leverage 1.5 to ~1.03x
    # effective and leaving ~$1,773 idle (see
    # project_notes/horizon_leverage_divergence_2026-08-24.md).
    #
    # The fix mirrors what the validation itself did (run_validation.py composes
    # the frontier from admitted sleeves only, renormalized): disable the rejected
    # sleeves and renormalize the admitted pair, preserving their 0.45:0.25 ratio.
    #   PULSE    0.45 / 0.70 = 0.642857
    #   ROTATION 0.25 / 0.70 = 0.357143
    # Their post-rejection base allocations are kept in comments so the original
    # risk-budget reasoning is not lost if either is ever revalidated.
    sleeves = {
        "PULSE": SleeveConfig("PULSE", "HZN_PULSE_", base_allocation=0.642857,
                              max_positions=1),                      # pre-gate: 0.45
        "ROTATION": SleeveConfig("ROTATION", "HZN_ROT_", base_allocation=0.357143,
                                 max_positions=2),                   # pre-gate: 0.25
        "REVERT": SleeveConfig("REVERT", "HZN_REV_", base_allocation=0.0,
                               max_positions=4, enabled=False),      # REJECTED (A2: Sharpe 0.06); pre-gate 0.20
        "DRIFT": SleeveConfig("DRIFT", "HZN_DRIFT_", base_allocation=0.0,
                              max_positions=1, enabled=False),       # REJECTED (A1/A2/A3); pre-gate 0.10
    }
    # 2026-09-05 (CP3, "Candidate B" — docs/VALIDATION.md): PULSE expresses
    # leverage above 1.0x as a QQQ/QLD mix instead of margin; ROTATION uses the
    # faster lookbacks that its own A6 robustness test had flagged as superior
    # in-sample AND out-of-sample. Both cleared the unchanged pre-registered
    # bar: engine 19.3% CAGR / Sharpe 0.84 / MaxDD -24.8% at book 1.0, versus
    # 19.3% / 0.65 / -34.2% for the margin config at 1.5x — same return, a
    # third less drawdown, no borrowing.
    strategy_params = {
        "PULSE": {"target_vol": 0.22, "leverage_via": "levered_etf"},
        "ROTATION": {"lookbacks": (47, 95, 189)},
    }
    cfg = EngineConfig(sleeves=sleeves, strategy_params=strategy_params)
    _validate(cfg)
    return cfg


def _validate(cfg: EngineConfig) -> None:
    total = sum(s.base_allocation for s in cfg.sleeves.values() if s.enabled)
    if abs(total - 1.0) > 1e-6:
        raise ValueError(
            f"Enabled sleeve base allocations must sum to 1.0, got {total:.4f}"
        )
    prefixes = [s.order_prefix for s in cfg.sleeves.values()]
    if len(prefixes) != len(set(prefixes)):
        raise ValueError("Sleeve order_prefix values must be unique")
