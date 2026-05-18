"""Tradable universe and reference-symbol definitions.

Every symbol here is a liquid, long-lived US ETF (or the VIX index). The
universe is deliberately small and fixed: ETF survivorship bias is negligible,
and a fixed universe keeps the backtest honest and reproducible.
"""

from __future__ import annotations

from typing import List

# --- Strategy-traded symbols -------------------------------------------------

# PULSE: leveraged growth core. Cash leg is BIL (1-3mo T-bills).
PULSE_RISK_ASSET = "QQQ"
PULSE_CASH_ASSET = "BIL"

# ROTATION: cross-asset dual momentum. BIL is the absolute-momentum cash floor.
ROTATION_ASSETS = ["QQQ", "EFA", "TLT", "GLD", "DBC"]
ROTATION_CASH_ASSET = "BIL"

# REVERT: mean-reversion swing on liquid index / sector ETFs.
REVERT_UNIVERSE = [
    "SPY", "QQQ", "IWM", "DIA",
    "XLK", "XLF", "XLV", "XLY", "XLE", "XLI", "SMH",
]

# DRIFT: overnight seasonality on the Nasdaq proxy.
DRIFT_ASSET = "QQQ"

# --- Reference symbols (intelligence / regime layer) -------------------------

REGIME_TREND = ["SPY", "QQQ"]
REGIME_BREADTH = ["RSP", "SPY"]      # equal-weight vs cap-weight = breadth proxy
REGIME_CREDIT = ["HYG", "LQD", "IEF"]  # high-yield/credit vs treasuries
# Volatility regime uses SPY realized volatility (fully backtestable, and
# computable live from price). Polygon index history (I:VIX) is only ~2 years
# deep on this plan, too shallow to backtest faithfully — so VIX is not a
# dataset dependency. The live engine may still read current VIX as enrichment.
REGIME_VOL_SYMBOL = "SPY"

BENCHMARK = "QQQ"

# Sector ETFs (used for the broad/sector slippage split).
SECTOR_ETFS = {"XLK", "XLF", "XLV", "XLY", "XLE", "XLI", "SMH", "DBC", "GLD",
               "EFA", "TLT", "HYG", "LQD", "RSP"}
BROAD_ETFS = {"SPY", "QQQ", "IWM", "DIA", "IEF", "BIL"}


def all_symbols() -> List[str]:
    """Every symbol Horizon needs to fetch, deduplicated and sorted."""
    syms = set()
    syms.add(PULSE_RISK_ASSET)
    syms.add(PULSE_CASH_ASSET)
    syms.update(ROTATION_ASSETS)
    syms.add(ROTATION_CASH_ASSET)
    syms.update(REVERT_UNIVERSE)
    syms.add(DRIFT_ASSET)
    syms.update(REGIME_TREND)
    syms.update(REGIME_BREADTH)
    syms.update(REGIME_CREDIT)
    syms.add(BENCHMARK)
    return sorted(syms)


def slippage_bucket(symbol: str) -> str:
    """Classify a symbol for the cost model: 'broad' or 'sector'."""
    return "broad" if symbol in BROAD_ETFS else "sector"
