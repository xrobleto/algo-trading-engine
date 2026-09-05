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
# Optional (PulseStrategy(leverage_via="levered_etf")): leverage above 1.0x is
# expressed as a QQQ/QLD mix whose weights sum to 1.0 — no margin borrowing.
# QLD = ProShares Ultra QQQ (2x daily), inception 2006-06. Same live ticker.
PULSE_LEVERED_ASSET = "QLD"

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
               "EFA", "TLT", "HYG", "LQD", "RSP",
               # live-equivalents (see below)
               "IEFA", "VGLT", "IAU", "PDBC"}
BROAD_ETFS = {"SPY", "QQQ", "IWM", "DIA", "IEF", "BIL", "QQQM", "SHV"}

# --- Live symbol equivalents (HORIZON_SYMBOL_EQUIVALENTS=1) ------------------
# When Horizon trades the SAME live account as the Unified Engine, the two
# engines must never trade the same tickers — broker positions merge at the
# account level and one engine would sell the other's shares. Deployed live,
# Horizon therefore trades index-equivalent ETFs the Unified Engine never
# touches (same underlying index/exposure, so the validation transfers):
#   QQQ -> QQQM (same Nasdaq-100 index)   EFA -> IEFA (same developed-intl)
#   TLT -> VGLT (long treasuries)          GLD -> IAU  (gold)
#   DBC -> PDBC (broad commodities)        BIL -> SHV  (T-bill cash leg;
#                                                TREND already uses SGOV/BIL)
# Reference/regime symbols (SPY, RSP, HYG, ...) are read-only market data —
# never traded — and stay unchanged. Backtests run with the flag OFF and keep
# the deep-history originals.
import os as _os
if _os.getenv("HORIZON_SYMBOL_EQUIVALENTS", "0") == "1":
    PULSE_RISK_ASSET = "QQQM"
    PULSE_CASH_ASSET = "SHV"
    ROTATION_ASSETS = ["QQQM", "IEFA", "VGLT", "IAU", "PDBC"]
    ROTATION_CASH_ASSET = "SHV"


def all_symbols() -> List[str]:
    """Every symbol Horizon needs to fetch, deduplicated and sorted."""
    syms = set()
    syms.add(PULSE_RISK_ASSET)
    syms.add(PULSE_CASH_ASSET)
    syms.add(PULSE_LEVERED_ASSET)
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
