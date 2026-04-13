"""Tests for email renderer module (smoke tests)."""

import pytest
from decimal import Decimal
from datetime import datetime
from pathlib import Path

from src.reporting.email_renderer import (
    EmailRenderer,
    render_email_html,
    generate_subject_line,
)
from src.utils.typing import (
    ActionRecommendation,
    ActionType,
    Urgency,
    EvidencePacket,
)


class TestEmailRenderer:
    """Tests for EmailRenderer class."""

    @pytest.fixture
    def renderer(self):
        """Create EmailRenderer instance."""
        # Check if template exists
        template_path = Path("templates/email.html.j2")
        if not template_path.exists():
            pytest.skip("Email template not found")
        return EmailRenderer(template_dir="templates")

    @pytest.fixture
    def sample_evidence(self):
        """Create sample evidence packet."""
        return EvidencePacket(
            timestamp=datetime.now(),
            total_value=Decimal("50000.00"),
            risk_alert_score=72.5,
            opportunity_score=65.0,
            top_holdings=[
                {
                    "symbol": "AAPL",
                    "value": 15000.0,
                    "pct": 30.0,
                    "pnl_pct": 12.5,
                },
                {
                    "symbol": "MSFT",
                    "value": 12000.0,
                    "pct": 24.0,
                    "pnl_pct": 8.2,
                },
            ],
            concentration_flags=[
                "Technology sector at 54% (above 40% threshold)",
                "Top 3 holdings represent 64% of portfolio",
            ],
            ticker_signals={
                "AAPL": {
                    "rsi_14": 65.0,
                    "above_sma_200": True,
                    "volatility_20d": 25.0,
                },
                "MSFT": {
                    "rsi_14": 58.0,
                    "above_sma_200": True,
                    "volatility_20d": 22.0,
                },
            },
            top_news=[
                {
                    "title": "Apple Reports Strong Q4 Earnings",
                    "url": "https://example.com/apple-earnings",
                    "ticker": "AAPL",
                    "sentiment": 0.7,
                    "published_at": "2024-01-15T10:30:00Z",
                },
            ],
            macro_indicators={
                "fear_greed_index": 45,
                "vix": 18.5,
                "treasury_10y": 4.25,
            },
            data_timestamps={
                "massive": "2024-01-15T12:00:00Z",
                "alpaca": "2024-01-15T11:55:00Z",
                "fred": "2024-01-14T00:00:00Z",
            },
        )

    @pytest.fixture
    def sample_recommendations(self):
        """Create sample recommendations."""
        return [
            ActionRecommendation(
                ticker="AAPL",
                action=ActionType.HOLD,
                urgency=Urgency.LOW,
                confidence=75,
                rationale_bullets=[
                    "Strong technical position above all major MAs",
                    "Positive earnings sentiment",
                ],
                key_levels={
                    "support": 170.00,
                    "resistance": 195.00,
                    "stop": 165.00,
                },
                risks=["High valuation", "China exposure"],
                time_horizon_days=30,
            ),
            ActionRecommendation(
                ticker="MSFT",
                action=ActionType.ADD,
                urgency=Urgency.MED,
                confidence=68,
                rationale_bullets=[
                    "AI tailwinds driving growth",
                    "Pullback to support level",
                ],
                key_levels={
                    "support": 380.00,
                    "resistance": 420.00,
                },
                risks=["Cloud competition"],
                time_horizon_days=60,
            ),
        ]

    def test_renderer_init(self, renderer):
        """Test renderer initialization."""
        assert renderer is not None
        assert renderer.template is not None

    def test_render_basic(self, renderer, sample_evidence, sample_recommendations):
        """Test basic rendering produces HTML."""
        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=sample_recommendations,
            alert_type="RISK",
        )

        assert html is not None
        assert len(html) > 0
        assert "<html" in html.lower()
        assert "</html>" in html.lower()

    def test_render_contains_scores(
        self, renderer, sample_evidence, sample_recommendations
    ):
        """Test that rendered HTML contains score information."""
        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=sample_recommendations,
            alert_type="RISK",
        )

        # Should contain risk score
        assert "72" in html or "73" in html  # Rounded score
        # Should contain opportunity score
        assert "65" in html

    def test_render_contains_tickers(
        self, renderer, sample_evidence, sample_recommendations
    ):
        """Test that rendered HTML contains ticker symbols."""
        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=sample_recommendations,
            alert_type="RISK",
        )

        assert "AAPL" in html
        assert "MSFT" in html

    def test_render_contains_actions(
        self, renderer, sample_evidence, sample_recommendations
    ):
        """Test that rendered HTML contains action types."""
        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=sample_recommendations,
            alert_type="OPPORTUNITY",
        )

        assert "HOLD" in html or "Hold" in html.lower()
        assert "ADD" in html or "Add" in html.lower()

    def test_render_with_no_recommendations(self, renderer, sample_evidence):
        """Test rendering with empty recommendations."""
        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=[],
            alert_type="RISK",
        )

        assert html is not None
        assert "<html" in html.lower()

    def test_render_with_charts(
        self, renderer, sample_evidence, sample_recommendations
    ):
        """Test rendering with chart data."""
        charts = [
            {
                "title": "Portfolio Allocation",
                "data_url": "data:image/png;base64,iVBORw0KGgo=",
            },
        ]

        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=sample_recommendations,
            alert_type="RISK",
            charts=charts,
        )

        assert "Portfolio Allocation" in html
        assert "data:image/png" in html


class TestSubjectLineGeneration:
    """Tests for email subject line generation."""

    def test_risk_alert_subject(self):
        """Test risk alert subject line."""
        subject = generate_subject_line(
            alert_type="RISK",
            risk_score=78.5,
            opportunity_score=45.0,
            top_ticker="AAPL",
        )

        assert "Risk" in subject or "risk" in subject.lower()
        assert "78" in subject or "79" in subject

    def test_opportunity_alert_subject(self):
        """Test opportunity alert subject line."""
        subject = generate_subject_line(
            alert_type="OPPORTUNITY",
            risk_score=45.0,
            opportunity_score=82.0,
            top_ticker="MSFT",
        )

        assert "Opportunity" in subject or "opportunity" in subject.lower()
        assert "82" in subject

    def test_both_alert_subject(self):
        """Test combined alert subject line."""
        subject = generate_subject_line(
            alert_type="BOTH",
            risk_score=75.0,
            opportunity_score=80.0,
            top_ticker="NVDA",
        )

        # Should mention both or indicate combined alert
        assert len(subject) > 10

    def test_subject_line_length(self):
        """Test that subject line is reasonable length."""
        subject = generate_subject_line(
            alert_type="RISK",
            risk_score=85.0,
            opportunity_score=40.0,
            top_ticker="AAPL",
        )

        # Subject should be reasonable for email clients
        assert len(subject) <= 100


class TestHtmlSanitization:
    """Tests for HTML safety."""

    @pytest.fixture
    def renderer(self):
        template_path = Path("templates/email.html.j2")
        if not template_path.exists():
            pytest.skip("Email template not found")
        return EmailRenderer(template_dir="templates")

    def test_xss_prevention_in_ticker(self, renderer):
        """Test that ticker names are properly escaped."""
        evidence = EvidencePacket(
            timestamp=datetime.now(),
            total_value=Decimal("50000"),
            risk_alert_score=70.0,
            opportunity_score=60.0,
            top_holdings=[],
            concentration_flags=[],
            ticker_signals={},
            top_news=[],
            macro_indicators={},
            data_timestamps={},
        )

        # Malicious ticker name
        malicious_rec = ActionRecommendation(
            ticker='<script>alert("xss")</script>',
            action=ActionType.REVIEW,
            urgency=Urgency.LOW,
            confidence=50,
            rationale_bullets=["Test"],
            key_levels={},
            risks=[],
            time_horizon_days=30,
        )

        html = renderer.render(
            evidence_packet=evidence,
            recommendations=[malicious_rec],
            alert_type="RISK",
        )

        # Script tags should be escaped
        assert "<script>" not in html
        assert "&lt;script&gt;" in html or "script" not in html.lower()

    def test_xss_prevention_in_rationale(self, renderer):
        """Test that rationale text is properly escaped."""
        evidence = EvidencePacket(
            timestamp=datetime.now(),
            total_value=Decimal("50000"),
            risk_alert_score=70.0,
            opportunity_score=60.0,
            top_holdings=[],
            concentration_flags=[],
            ticker_signals={},
            top_news=[],
            macro_indicators={},
            data_timestamps={},
        )

        malicious_rec = ActionRecommendation(
            ticker="AAPL",
            action=ActionType.REVIEW,
            urgency=Urgency.LOW,
            confidence=50,
            rationale_bullets=['<img src=x onerror="alert(1)">'],
            key_levels={},
            risks=[],
            time_horizon_days=30,
        )

        html = renderer.render(
            evidence_packet=evidence,
            recommendations=[malicious_rec],
            alert_type="RISK",
        )

        # onerror should be escaped
        assert 'onerror="alert' not in html


class TestEmailAccessibility:
    """Tests for email accessibility and compatibility."""

    @pytest.fixture
    def renderer(self):
        template_path = Path("templates/email.html.j2")
        if not template_path.exists():
            pytest.skip("Email template not found")
        return EmailRenderer(template_dir="templates")

    @pytest.fixture
    def sample_evidence(self):
        return EvidencePacket(
            timestamp=datetime.now(),
            total_value=Decimal("50000"),
            risk_alert_score=70.0,
            opportunity_score=60.0,
            top_holdings=[],
            concentration_flags=[],
            ticker_signals={},
            top_news=[],
            macro_indicators={},
            data_timestamps={},
        )

    def test_has_doctype(self, renderer, sample_evidence):
        """Test that HTML has doctype."""
        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=[],
            alert_type="RISK",
        )

        assert "<!DOCTYPE" in html or "<!doctype" in html.lower()

    def test_has_charset(self, renderer, sample_evidence):
        """Test that HTML specifies charset."""
        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=[],
            alert_type="RISK",
        )

        assert "charset" in html.lower()
        assert "utf-8" in html.lower()

    def test_has_viewport(self, renderer, sample_evidence):
        """Test that HTML has viewport meta for mobile."""
        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=[],
            alert_type="RISK",
        )

        assert "viewport" in html.lower()

    def test_images_have_alt(self, renderer, sample_evidence):
        """Test that images have alt attributes."""
        charts = [
            {
                "title": "Test Chart",
                "data_url": "data:image/png;base64,iVBORw0KGgo=",
            },
        ]

        html = renderer.render(
            evidence_packet=sample_evidence,
            recommendations=[],
            alert_type="RISK",
            charts=charts,
        )

        # All img tags should have alt
        import re
        img_tags = re.findall(r'<img[^>]*>', html)
        for img in img_tags:
            assert 'alt=' in img
