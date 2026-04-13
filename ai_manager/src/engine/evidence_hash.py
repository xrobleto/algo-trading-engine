"""
Evidence Hash Module

Pure functions for computing deterministic hashes of portfolio evidence.
Used for deduplication and change detection.
"""

import hashlib
import json
from typing import Any, Dict, List


def compute_evidence_hash(
    alert_type: str,
    risk_score: float,
    opportunity_score: float,
    recommendations: List[Any],
    top_news: List[Dict[str, Any]],
    concentration_flags: List[str],
) -> str:
    """
    Compute hash of EVIDENCE (catalysts + signals).

    This changes when new news/catalysts arrive, even if portfolio
    structure is unchanged. Used for per-ticker action tracking.

    Args:
        alert_type: Type of alert ("risk", "opportunity", "none")
        risk_score: Current risk score (0-100)
        opportunity_score: Current opportunity score (0-100)
        recommendations: List of ActionRecommendation objects
        top_news: List of news dicts with url/id/title/published_at
        concentration_flags: List of concentration warning strings

    Returns:
        16-character hex hash string
    """
    # Sort inputs for order stability
    # News: sort by (published_at desc, url/id) so newest first, stable order
    sorted_news = sorted(
        top_news[:5],
        key=lambda n: (n.get("published_at", "") or "", _news_key(n)),
        reverse=True,
    )

    # Recommendations: sort by (urgency desc, confidence desc, ticker)
    # so highest priority actions come first, stable order
    urgency_order = {"HIGH": 3, "MED": 2, "LOW": 1}
    sorted_recs = sorted(
        recommendations[:5],
        key=lambda r: (
            -urgency_order.get(r.urgency.value if hasattr(r.urgency, 'value') else r.urgency, 0),
            -(r.confidence if hasattr(r, 'confidence') else 0),
            r.ticker,
        ),
    )

    # Flags: sort alphabetically
    sorted_flags = sorted(concentration_flags[:3])

    evidence_data = {
        # Alert type and scores (10-point buckets reduce noise)
        "alert_type": alert_type,
        "risk_score_bucket": int(risk_score // 10) * 10,
        "opp_score_bucket": int(opportunity_score // 10) * 10,
        # Top recommendations (sorted)
        "top_actions": [
            {
                "ticker": r.ticker,
                "action": r.action.value if hasattr(r.action, 'value') else r.action,
                "urgency": r.urgency.value if hasattr(r.urgency, 'value') else r.urgency,
            }
            for r in sorted_recs
        ],
        # Top news IDs (catalysts) - use stable identifiers, sorted
        "top_news": [_news_key(n) for n in sorted_news],
        # Key risk flags (sorted)
        "risk_flags": sorted_flags,
    }
    evidence_json = json.dumps(evidence_data, sort_keys=True)
    return hashlib.sha256(evidence_json.encode()).hexdigest()[:16]


def _news_key(news_item: Dict[str, Any]) -> str:
    """
    Extract stable identifier from news item.

    Preference order: url > id > (published_at + title[:30])
    """
    if news_item.get("url"):
        return news_item["url"]
    if news_item.get("id"):
        return news_item["id"]
    # Fallback: combine published_at with truncated title
    return f"{news_item.get('published_at', '')}:{news_item.get('title', '')[:30]}"


def compute_structure_hash(holdings: List[Any]) -> str:
    """
    Compute hash of portfolio STRUCTURE (not values).

    This hashes {symbol: shares} so that normal price movements
    don't change the hash and defeat deduplication.

    Args:
        holdings: List of Holding objects with symbol and quantity

    Returns:
        16-character hex hash string
    """
    # Sort by symbol for deterministic hash
    # Note: Holding uses 'shares' not 'quantity'
    structure = {
        h.symbol: str(h.shares) for h in sorted(holdings, key=lambda x: x.symbol)
    }
    structure_json = json.dumps(structure, sort_keys=True)
    return hashlib.sha256(structure_json.encode()).hexdigest()[:16]
