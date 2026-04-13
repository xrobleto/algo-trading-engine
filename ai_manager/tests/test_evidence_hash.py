"""Tests for evidence hash materiality and stability."""

import pytest

from src.engine.evidence_hash import compute_evidence_hash
from src.utils.typing import (
    ActionRecommendation,
    ActionType,
    Urgency,
)


def make_recommendation(
    ticker: str,
    action: ActionType = ActionType.HOLD,
    urgency: Urgency = Urgency.LOW,
    confidence: int = 75,
):
    """Create a sample recommendation."""
    return ActionRecommendation(
        ticker=ticker,
        action=action,
        urgency=urgency,
        confidence=confidence,
        time_horizon_days=30,
        rationale_bullets=["Test rationale"],
        risks=["Test risk"],
        key_levels={"support": 100.0, "resistance": 120.0, "stop": None, "target": None},
    )


class TestEvidenceHashMateriality:
    """Tests verifying evidence hash changes when it should."""

    def test_same_inputs_same_hash(self):
        """Verify deterministic: same inputs always produce same hash."""
        hash1 = compute_evidence_hash(
            alert_type="risk",
            risk_score=70.0,
            opportunity_score=60.0,
            recommendations=[make_recommendation("AAPL", ActionType.TRIM, Urgency.HIGH)],
            top_news=[{"url": "https://news.com/1", "title": "Apple stock drops"}],
            concentration_flags=["Tech sector > 40%"],
        )
        hash2 = compute_evidence_hash(
            alert_type="risk",
            risk_score=70.0,
            opportunity_score=60.0,
            recommendations=[make_recommendation("AAPL", ActionType.TRIM, Urgency.HIGH)],
            top_news=[{"url": "https://news.com/1", "title": "Apple stock drops"}],
            concentration_flags=["Tech sector > 40%"],
        )

        assert hash1 == hash2, "Same inputs should produce identical hash"

    def test_new_headline_changes_hash(self):
        """Verify: same portfolio + new headline -> different hash."""
        hash_old = compute_evidence_hash(
            alert_type="none",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/old", "title": "Old news"}],
            concentration_flags=[],
        )
        hash_new = compute_evidence_hash(
            alert_type="none",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/new", "title": "Breaking: New catalyst"}],
            concentration_flags=[],
        )

        assert hash_old != hash_new, "New headline should change evidence hash"

    def test_score_bucket_change_changes_hash(self):
        """Verify: same headlines + score bucket change -> different hash."""
        # Score in 60-69 bucket
        hash_low = compute_evidence_hash(
            alert_type="none",
            risk_score=65.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/1", "title": "Same news"}],
            concentration_flags=[],
        )
        # Score in 70-79 bucket
        hash_high = compute_evidence_hash(
            alert_type="none",
            risk_score=72.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/1", "title": "Same news"}],
            concentration_flags=[],
        )

        assert hash_low != hash_high, "Score bucket change should change evidence hash"

    def test_score_within_bucket_same_hash(self):
        """Verify: score changes within same 10-point bucket -> same hash."""
        # Both scores in 60-69 bucket
        hash_a = compute_evidence_hash(
            alert_type="none",
            risk_score=61.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/1", "title": "Same news"}],
            concentration_flags=[],
        )
        hash_b = compute_evidence_hash(
            alert_type="none",
            risk_score=68.5,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/1", "title": "Same news"}],
            concentration_flags=[],
        )

        assert hash_a == hash_b, "Score changes within same bucket should NOT change hash"

    def test_recommendation_action_change_changes_hash(self):
        """Verify: same ticker but different action -> different hash."""
        hash_hold = compute_evidence_hash(
            alert_type="none",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL", ActionType.HOLD)],
            top_news=[{"url": "https://news.com/1"}],
            concentration_flags=[],
        )
        hash_trim = compute_evidence_hash(
            alert_type="none",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL", ActionType.TRIM)],
            top_news=[{"url": "https://news.com/1"}],
            concentration_flags=[],
        )

        assert hash_hold != hash_trim, "Action change should change evidence hash"

    def test_urgency_change_changes_hash(self):
        """Verify: same action but different urgency -> different hash."""
        hash_low = compute_evidence_hash(
            alert_type="none",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL", ActionType.TRIM, Urgency.LOW)],
            top_news=[{"url": "https://news.com/1"}],
            concentration_flags=[],
        )
        hash_high = compute_evidence_hash(
            alert_type="none",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL", ActionType.TRIM, Urgency.HIGH)],
            top_news=[{"url": "https://news.com/1"}],
            concentration_flags=[],
        )

        assert hash_low != hash_high, "Urgency change should change evidence hash"

    def test_concentration_flag_change_changes_hash(self):
        """Verify: new concentration flag -> different hash."""
        hash_no_flag = compute_evidence_hash(
            alert_type="none",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/1"}],
            concentration_flags=[],
        )
        hash_with_flag = compute_evidence_hash(
            alert_type="none",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/1"}],
            concentration_flags=["AAPL > 25% of portfolio"],
        )

        assert hash_no_flag != hash_with_flag, "New concentration flag should change hash"

    def test_alert_type_change_changes_hash(self):
        """Verify: different alert type -> different hash."""
        hash_risk = compute_evidence_hash(
            alert_type="risk",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/1"}],
            concentration_flags=[],
        )
        hash_opp = compute_evidence_hash(
            alert_type="opportunity",
            risk_score=70.0,
            opportunity_score=50.0,
            recommendations=[make_recommendation("AAPL")],
            top_news=[{"url": "https://news.com/1"}],
            concentration_flags=[],
        )

        assert hash_risk != hash_opp, "Alert type change should change hash"


class TestNewsHashStability:
    """Tests verifying news hashing uses stable identifiers."""

    def test_url_preferred_over_title(self):
        """Verify URL is used when available, making hash stable to title changes."""
        # Same URL, different title (e.g., provider reformatted)
        hash_a = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[{"url": "https://news.com/article-123", "title": "Stock drops 5%"}],
            concentration_flags=[],
        )
        hash_b = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[{"url": "https://news.com/article-123", "title": "Stock drops 5% today!"}],
            concentration_flags=[],
        )

        assert hash_a == hash_b, "Same URL should produce same hash regardless of title"

    def test_id_used_when_no_url(self):
        """Verify ID is used as fallback when URL is missing."""
        hash1 = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[{"id": "news-12345", "title": "Breaking news"}],
            concentration_flags=[],
        )
        # Same ID, different title
        hash2 = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[{"id": "news-12345", "title": "BREAKING: News"}],
            concentration_flags=[],
        )

        assert hash1 == hash2, "Same ID should produce same hash regardless of title"

    def test_fallback_to_published_plus_title(self):
        """Verify fallback uses published_at + title[:30] when no URL/ID."""
        hash1 = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[{"published_at": "2024-01-15T10:00:00Z", "title": "Market analysis for today"}],
            concentration_flags=[],
        )
        # Different published_at should change hash
        hash2 = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[{"published_at": "2024-01-16T10:00:00Z", "title": "Market analysis for today"}],
            concentration_flags=[],
        )

        assert hash1 != hash2, "Different published_at should change hash when no URL/ID"

    def test_title_truncation_stability(self):
        """Verify title truncation at 30 chars for stability."""
        # Titles that differ only after character 30
        title_base = "Apple announces new product li"  # 30 chars
        assert len(title_base) == 30, "Test setup: title_base must be exactly 30 chars"

        title_a = title_base + "ne for consumers"
        title_b = title_base + "neup at event"

        hash_a = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[{"published_at": "2024-01-15T10:00:00Z", "title": title_a}],
            concentration_flags=[],
        )
        hash_b = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[{"published_at": "2024-01-15T10:00:00Z", "title": title_b}],
            concentration_flags=[],
        )

        assert hash_a == hash_b, "Titles differing only after char 30 should produce same hash"


class TestOrderStability:
    """Tests verifying hash is stable regardless of input ordering.

    The production function sorts inputs before hashing, so reordering
    equally-relevant items should NOT change the hash.
    """

    def test_news_order_stability(self):
        """Verify: reordering news items with same published_at -> same hash.

        News is sorted by (published_at desc, url/id), so items with
        different URLs but same timestamp may reorder. Hash should be stable.
        """
        news_a = {"url": "https://news.com/a", "published_at": "2024-01-15T10:00:00Z"}
        news_b = {"url": "https://news.com/b", "published_at": "2024-01-15T10:00:00Z"}

        hash_ab = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[news_a, news_b],
            concentration_flags=[],
        )
        hash_ba = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[news_b, news_a],
            concentration_flags=[],
        )

        assert hash_ab == hash_ba, "News order should not affect hash (sorted by published_at, url)"

    def test_recommendation_order_stability(self):
        """Verify: reordering recommendations with same urgency/confidence -> same hash.

        Recommendations are sorted by (urgency desc, confidence desc, ticker),
        so items with equal priority should produce stable hash.
        """
        rec_aapl = make_recommendation("AAPL", ActionType.TRIM, Urgency.HIGH, confidence=80)
        rec_msft = make_recommendation("MSFT", ActionType.TRIM, Urgency.HIGH, confidence=80)

        hash_am = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[rec_aapl, rec_msft],
            top_news=[],
            concentration_flags=[],
        )
        hash_ma = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[rec_msft, rec_aapl],
            top_news=[],
            concentration_flags=[],
        )

        assert hash_am == hash_ma, "Recommendation order should not affect hash (sorted by urgency, confidence, ticker)"

    def test_concentration_flags_order_stability(self):
        """Verify: reordering concentration flags -> same hash.

        Flags are sorted alphabetically before hashing.
        """
        hash_ab = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[],
            concentration_flags=["AAPL > 25%", "Tech > 40%"],
        )
        hash_ba = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[],
            concentration_flags=["Tech > 40%", "AAPL > 25%"],
        )

        assert hash_ab == hash_ba, "Flag order should not affect hash (sorted alphabetically)"

    def test_priority_sorting_for_recommendations(self):
        """Verify: higher urgency/confidence recommendations are prioritized.

        When only top 5 are taken, the sort order determines which make it in.
        """
        low_priority = make_recommendation("LOW", ActionType.HOLD, Urgency.LOW, confidence=50)
        high_priority = make_recommendation("HIGH", ActionType.TRIM, Urgency.HIGH, confidence=90)

        # Even if low_priority is first in list, high_priority should come first after sorting
        hash1 = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[low_priority, high_priority],
            top_news=[],
            concentration_flags=[],
        )
        hash2 = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[high_priority, low_priority],
            top_news=[],
            concentration_flags=[],
        )

        assert hash1 == hash2, "Priority sorting should produce same hash regardless of input order"

    def test_news_sorted_by_recency(self):
        """Verify: newer news comes first after sorting.

        This ensures the most recent catalysts are included in the hash.
        """
        old_news = {"url": "https://news.com/old", "published_at": "2024-01-10T10:00:00Z"}
        new_news = {"url": "https://news.com/new", "published_at": "2024-01-15T10:00:00Z"}

        # Even if old_news is first in list, new_news should come first after sorting
        hash1 = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[old_news, new_news],
            concentration_flags=[],
        )
        hash2 = compute_evidence_hash(
            alert_type="none",
            risk_score=50.0,
            opportunity_score=50.0,
            recommendations=[],
            top_news=[new_news, old_news],
            concentration_flags=[],
        )

        assert hash1 == hash2, "News sorting by recency should produce same hash regardless of input order"
