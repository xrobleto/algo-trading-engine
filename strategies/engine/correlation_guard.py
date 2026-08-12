"""
Cross-sleeve correlation / aggregate-beta guard.
================================================

The engine already prevents two sleeves from holding the SAME symbol
(`OwnershipLedger.has_conflicts`). It does NOT prevent them from holding
DIFFERENT-but-correlated symbols that are really the same bet — e.g. TREND long
QQQ/SMH (equity beta ~1.2) + CROSSASSET long USO/DBC (risk-on commodities) +
SIMPLE long a high-beta name, all at once. That is hidden concentration: three
sleeves, one trade.

This module estimates each position's signed exposure to a small set of risk
factors and caps the book's NET risk-on exposure across all sleeves. It is pure,
deterministic, needs no live data feed (a static factor/beta map), and is meant to
be wired as one extra check in `SleeveManager.can_deploy`.

Design choices:
- A static factor + beta-to-risk-on map (extensible). Unknown equities default to
  a high-beta long. Leveraged ETFs carry their leverage factor. Bonds/USD are
  risk-OFF (negative risk-on beta). Gold is a low-beta diversifier.
- "risk-on exposure" = sum(beta_i * signed_notional_i) / total_equity. Long adds,
  short subtracts. The guard blocks a new order if it would push net risk-on
  exposure beyond `cap` (a multiple of total equity).
- A floor concept is unnecessary here: the guard only ever BLOCKS new entries; it
  never forces exits, so it cannot create a death-spiral.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

# factor labels (informational); the load-bearing number is the signed risk-on beta
US_EQUITY, CRYPTO, COMMODITY, RATES, USD = "US_EQUITY", "CRYPTO", "COMMODITY", "RATES", "USD"

# symbol -> (factor, risk_on_beta). Positive beta = moves WITH a risk-on day.
SYMBOL_BETA: Dict[str, Tuple[str, float]] = {
    # broad equity
    "SPY": (US_EQUITY, 1.0), "QQQ": (US_EQUITY, 1.1), "IWM": (US_EQUITY, 1.1), "DIA": (US_EQUITY, 0.95),
    "MTUM": (US_EQUITY, 1.05), "QUAL": (US_EQUITY, 1.0), "RSP": (US_EQUITY, 1.0),
    # sectors / high-beta equity
    "XLK": (US_EQUITY, 1.15), "XLY": (US_EQUITY, 1.1), "XLF": (US_EQUITY, 1.05), "XLI": (US_EQUITY, 1.0),
    "XLV": (US_EQUITY, 0.8), "XLE": (US_EQUITY, 0.9), "XLC": (US_EQUITY, 1.05),
    "SMH": (US_EQUITY, 1.4), "SOXX": (US_EQUITY, 1.4), "IGV": (US_EQUITY, 1.25), "CIBR": (US_EQUITY, 1.2),
    "SKYY": (US_EQUITY, 1.2), "ARKK": (US_EQUITY, 1.5), "XBI": (US_EQUITY, 1.2), "IBB": (US_EQUITY, 0.9),
    "KWEB": (US_EQUITY, 1.3), "XHB": (US_EQUITY, 1.1),
    # leveraged equity (carry their multiple)
    "TQQQ": (US_EQUITY, 3.3), "UPRO": (US_EQUITY, 3.0), "SOXL": (US_EQUITY, 4.2), "TECL": (US_EQUITY, 3.5),
    "FAS": (US_EQUITY, 3.1), "SPXL": (US_EQUITY, 3.0), "TNA": (US_EQUITY, 3.3),
    # inverse equity (risk-off)
    "SQQQ": (US_EQUITY, -3.3), "SPXU": (US_EQUITY, -3.0), "SOXS": (US_EQUITY, -4.2), "SPXS": (US_EQUITY, -3.0),
    # crypto-linked (risk-on, high beta)
    "BTC": (CRYPTO, 1.8), "BTCUSD": (CRYPTO, 1.8), "MSTR": (CRYPTO, 2.2), "COIN": (CRYPTO, 1.9),
    "MARA": (CRYPTO, 2.3), "RIOT": (CRYPTO, 2.2), "CLSK": (CRYPTO, 2.3), "WULF": (CRYPTO, 2.2),
    # commodities
    "GLD": (COMMODITY, 0.1), "SLV": (COMMODITY, 0.35), "DBC": (COMMODITY, 0.45), "USO": (COMMODITY, 0.55),
    "UNG": (COMMODITY, 0.25), "DBA": (COMMODITY, 0.2), "GLL": (COMMODITY, -0.1),
    # rates (long bonds = risk-off)
    "TLT": (RATES, -0.35), "IEF": (RATES, -0.2), "SHY": (RATES, -0.05), "SGOV": (RATES, 0.0),
    "BIL": (RATES, 0.0), "TBT": (RATES, 0.35),
    # USD / FX
    "UUP": (USD, -0.2), "FXE": (USD, 0.1), "FXY": (USD, -0.15),
}

DEFAULT_EQUITY_BETA = 1.2   # unknown single-name equity: assume high-beta long-only candidate

# ---------------------------------------------------------------------------
# Correlated clusters — symbols that are effectively THE SAME TRADE.
# Aug-2026 audit: TREND lost $590 concentrated in the semis cluster (SOXX/TECL/
# SMH/SOXL) while staying UNDER the aggregate-beta cap — net beta measures how
# much total market risk the book carries, not whether it is all one bet.
# Cluster exposure = sum(|beta_i| * |notional_i|) / equity per cluster, capped.
# ---------------------------------------------------------------------------
CLUSTERS: Dict[str, str] = {
    # semiconductors (incl. leveraged and single names)
    "SMH": "SEMIS", "SOXX": "SEMIS", "SOXL": "SEMIS", "SOXS": "SEMIS",
    "NVDA": "SEMIS", "AMD": "SEMIS", "AVGO": "SEMIS", "MU": "SEMIS",
    "SMCI": "SEMIS", "TSM": "SEMIS", "ARM": "SEMIS", "ON": "SEMIS",
    "QCOM": "SEMIS", "TXN": "SEMIS", "INTC": "SEMIS", "TECL": "SEMIS",
    "TECS": "SEMIS",  # TECL/TECS are tech-3x but trade as the semis/tech beta cluster
    # big tech / growth
    "QQQ": "BIGTECH", "TQQQ": "BIGTECH", "SQQQ": "BIGTECH", "XLK": "BIGTECH",
    "IGV": "BIGTECH", "SKYY": "BIGTECH", "ARKK": "BIGTECH",
    "AAPL": "BIGTECH", "MSFT": "BIGTECH", "GOOGL": "BIGTECH", "AMZN": "BIGTECH",
    "META": "BIGTECH", "TSLA": "BIGTECH", "NFLX": "BIGTECH", "CRM": "BIGTECH",
    # crypto-linked
    "MSTR": "CRYPTO", "COIN": "CRYPTO", "MARA": "CRYPTO", "RIOT": "CRYPTO",
    "CLSK": "CRYPTO", "WULF": "CRYPTO", "BTC": "CRYPTO", "BTCUSD": "CRYPTO",
    # biotech
    "XBI": "BIOTECH", "IBB": "BIOTECH", "LABU": "BIOTECH", "LABD": "BIOTECH",
    # financials
    "XLF": "FINANCIALS", "FAS": "FINANCIALS", "FAZ": "FINANCIALS",
    "JPM": "FINANCIALS", "GS": "FINANCIALS", "BAC": "FINANCIALS", "MS": "FINANCIALS",
    # energy/commodity
    "XLE": "ENERGY", "USO": "ENERGY", "UNG": "ENERGY", "XOM": "ENERGY", "CVX": "ENERGY",
}
DEFAULT_CLUSTER_CAP = 0.50   # max gross beta-weighted exposure per cluster (x equity)


def cluster_of(symbol: str) -> str:
    s = symbol.upper()
    if s in CLUSTERS:
        return CLUSTERS[s]
    factor, _ = classify(s)
    return factor  # fall back to the broad factor as its own cluster


def cluster_exposures(positions: List["Position"], total_equity: float) -> Dict[str, float]:
    """Gross beta-weighted exposure per correlated cluster, as a fraction of equity.
    Uses |beta| * |notional| — a 3x ETF counts at 3x, shorts count toward the same
    cluster's gross (they are still concentration in one theme)."""
    out: Dict[str, float] = {}
    for p in positions:
        _, beta = classify(p.symbol)
        c = cluster_of(p.symbol)
        out[c] = out.get(c, 0.0) + abs(beta) * abs(p.notional)
    if total_equity > 0:
        out = {k: v / total_equity for k, v in out.items()}
    return out


@dataclass
class Position:
    symbol: str
    notional: float    # signed: + long, - short (absolute dollar exposure)
    strategy_id: str = ""


def classify(symbol: str) -> Tuple[str, float]:
    s = symbol.upper()
    if s in SYMBOL_BETA:
        return SYMBOL_BETA[s]
    return (US_EQUITY, DEFAULT_EQUITY_BETA)


def net_risk_on(positions: List[Position], total_equity: float) -> float:
    """Net risk-on exposure as a fraction of total equity (1.0 = 100% net long beta)."""
    if total_equity <= 0:
        return 0.0
    dollar_beta = sum(classify(p.symbol)[1] * p.notional for p in positions)
    return dollar_beta / total_equity


def factor_breakdown(positions: List[Position], total_equity: float) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for p in positions:
        factor, beta = classify(p.symbol)
        out[factor] = out.get(factor, 0.0) + beta * p.notional
    if total_equity > 0:
        out = {k: v / total_equity for k, v in out.items()}
    return out


def would_breach(
    new_symbol: str, new_notional: float, side: str,
    open_positions: List[Position], total_equity: float, cap: float = 1.25,
    cluster_cap: float = DEFAULT_CLUSTER_CAP,
) -> Tuple[bool, str]:
    """Would adding this order breach either exposure limit?

    Check A — NET risk-on beta cap (`cap` x equity): blocks only risk-INCREASING
    orders; hedges/exits always pass.
    Check B — CLUSTER concentration cap (`cluster_cap` x equity, gross beta-weighted):
    blocks any new BUY that would push one correlated cluster (e.g. SEMIS) past the
    cap. This is the Aug-2026 lesson: the semis loss happened entirely UNDER the net
    beta cap because four tickers were one trade."""
    factor, beta = classify(new_symbol)
    signed = new_notional if side.lower() == "buy" else -new_notional
    current = net_risk_on(open_positions, total_equity)
    proposed = current + (beta * signed) / max(total_equity, 1e-9)
    # A: only block if we're increasing magnitude AND exceeding the cap
    if abs(proposed) > cap and abs(proposed) > abs(current):
        return True, (f"cross-sleeve risk-on cap: net beta {current:+.2f}x -> {proposed:+.2f}x "
                      f"would exceed {cap:.2f}x (adding {beta:+.1f}-beta {new_symbol})")
    # B: cluster concentration (buys only — sells reduce cluster gross)
    if side.lower() == "buy" and cluster_cap > 0:
        c = cluster_of(new_symbol)
        cur_cl = cluster_exposures(open_positions, total_equity).get(c, 0.0)
        prop_cl = cur_cl + abs(beta) * abs(new_notional) / max(total_equity, 1e-9)
        if prop_cl > cluster_cap and prop_cl > cur_cl:
            return True, (f"cluster concentration cap: {c} {cur_cl:.2f}x -> {prop_cl:.2f}x "
                          f"would exceed {cluster_cap:.2f}x equity (adding {new_symbol})")
    return False, f"ok (net risk-on {current:+.2f}x -> {proposed:+.2f}x, cap {cap:.2f}x)"


def _selftest():
    eq = 10_000.0
    book = [Position("QQQ", 6000, "TREND"), Position("SMH", 3000, "TREND"),
            Position("USO", 1500, "CROSSASSET")]
    print("net risk-on:", round(net_risk_on(book, eq), 3), "x equity")
    print("factors:", {k: round(v, 3) for k, v in factor_breakdown(book, eq).items()})
    b, why = would_breach("NVDA", 2000, "buy", book, eq, cap=1.25)
    print("add NVDA long ->", b, "|", why)
    b2, why2 = would_breach("TLT", 2000, "buy", book, eq, cap=1.25)
    print("add TLT (hedge) ->", b2, "|", why2)
    assert b is True and b2 is False

    # Cluster cap — reproduce the Aug-2026 semis concentration under the beta cap.
    # $7.8k account, SMH $2.1k + SOXX $1.7k (1.4-beta) => SEMIS gross ~0.68x. Net
    # beta is only ~0.68x (well under 1.25), but adding TECL (3.5-beta) $550 pushes
    # SEMIS to ~0.93x gross — past the 0.50 cluster cap -> must BLOCK.
    eq2 = 7_800.0
    book2 = [Position("SMH", 2100, "TREND"), Position("SOXX", 1700, "TREND")]
    print("clusters:", {k: round(v, 2) for k, v in cluster_exposures(book2, eq2).items()})
    b3, why3 = would_breach("TECL", 550, "buy", book2, eq2, cap=1.25, cluster_cap=0.50)
    print("add TECL (semis stack) ->", b3, "|", why3)
    b4, why4 = would_breach("XBI", 1500, "buy", book2, eq2, cap=1.25, cluster_cap=0.50)
    print("add XBI (diversifier) ->", b4, "|", why4)
    assert b3 is True and "cluster" in why3 and b4 is False
    # sells never trip the cluster cap
    b5, _ = would_breach("SMH", 1000, "sell", book2, eq2, cap=1.25, cluster_cap=0.50)
    assert b5 is False
    print("OK")


if __name__ == "__main__":
    _selftest()
