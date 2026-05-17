"""Execution-cost model.

Every fill pays slippage; leveraged dollars pay daily borrow interest. Equity
commissions are zero (Alpaca). The model is deliberately conservative and lives
in one place so the validation can sweep it for sensitivity.
"""

from __future__ import annotations

from ..config import CostModel
from ..data import universe


def slippage_bps(symbol: str, cost: CostModel) -> float:
    bucket = universe.slippage_bucket(symbol)
    return (cost.broad_etf_slippage_bps if bucket == "broad"
            else cost.sector_etf_slippage_bps)


def buy_fill(mid_price: float, symbol: str, cost: CostModel) -> float:
    """Price actually paid on a buy — worse (higher) than the quoted price."""
    return mid_price * (1.0 + slippage_bps(symbol, cost) / 10_000.0)


def sell_fill(mid_price: float, symbol: str, cost: CostModel) -> float:
    """Price actually received on a sell — worse (lower) than quoted."""
    return mid_price * (1.0 - slippage_bps(symbol, cost) / 10_000.0)


def daily_borrow_rate(cost: CostModel) -> float:
    """Per-day margin interest applied to a negative (borrowed) cash balance."""
    return cost.margin_rate_annual / 252.0


def overnight_slippage(cost: CostModel) -> float:
    """One-leg slippage fraction for DRIFT's close/open auction fills."""
    return cost.overnight_slippage_bps / 10_000.0
