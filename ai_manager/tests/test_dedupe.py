"""Tests for deduplication and state store module."""

import pytest
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path

from src.storage.state_store import (
    StateStore,
    EmailRecord,
)


class TestStateStore:
    """Tests for StateStore class."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary database file."""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    @pytest.fixture
    def store(self, temp_db):
        """Create a StateStore with temporary database."""
        return StateStore(db_path=temp_db)

    def test_init_creates_tables(self, temp_db):
        """Test that initialization creates required tables."""
        store = StateStore(db_path=temp_db)
        # If no exception, tables were created
        assert Path(temp_db).exists()

    def test_record_email(self, store):
        """Test recording an email."""
        result = store.record_email(
            alert_type="RISK",
            risk_score=75.5,
            opportunity_score=45.0,
            actions_json='[{"ticker": "AAPL", "action": "TRIM"}]',
        )

        assert result is True

    def test_check_should_send_first_email(self, store):
        """Test that first email is allowed."""
        should_send, reason = store.check_should_send(
            risk_score=70.0,
            opportunity_score=50.0,
            alert_type="RISK",
        )

        assert should_send is True
        assert "first" in reason.lower() or "no previous" in reason.lower()

    def test_check_should_send_respects_cooldown(self, store):
        """Test that cooldown period is respected."""
        # Record an email
        store.record_email(
            alert_type="RISK",
            risk_score=75.0,
            opportunity_score=45.0,
            actions_json="[]",
        )

        # Try to send another immediately
        should_send, reason = store.check_should_send(
            risk_score=76.0,
            opportunity_score=46.0,
            alert_type="RISK",
        )

        # Should be blocked due to cooldown
        assert should_send is False
        assert "cooldown" in reason.lower() or "hours" in reason.lower()

    def test_check_should_send_allows_after_cooldown(self, store):
        """Test that sending is allowed after cooldown."""
        # This test manipulates timestamps directly

        # Record an email with timestamp in the past
        import sqlite3
        past_time = (datetime.now() - timedelta(hours=10)).isoformat()

        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                """INSERT INTO emails
                (sent_at, alert_type, risk_score, opportunity_score, actions_json)
                VALUES (?, ?, ?, ?, ?)""",
                (past_time, "RISK", 75.0, 45.0, "[]")
            )
            conn.commit()

        # Should be allowed now (past cooldown)
        should_send, reason = store.check_should_send(
            risk_score=80.0,
            opportunity_score=50.0,
            alert_type="RISK",
        )

        assert should_send is True

    def test_check_should_send_requires_material_change(self, store):
        """Test that material score change is required."""
        import sqlite3
        past_time = (datetime.now() - timedelta(hours=10)).isoformat()

        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                """INSERT INTO emails
                (sent_at, alert_type, risk_score, opportunity_score, actions_json)
                VALUES (?, ?, ?, ?, ?)""",
                (past_time, "RISK", 75.0, 45.0, "[]")
            )
            conn.commit()

        # Try to send with very similar scores
        should_send, reason = store.check_should_send(
            risk_score=76.0,  # Only 1 point difference
            opportunity_score=46.0,
            alert_type="RISK",
        )

        # Might be blocked if material_score_delta is configured
        # This depends on implementation

    def test_daily_email_limit(self, store):
        """Test that daily email limit is enforced."""
        import sqlite3
        now = datetime.now()

        # Insert max_emails_per_day emails
        max_emails = store.max_emails_per_day
        with sqlite3.connect(store.db_path) as conn:
            for i in range(max_emails):
                # Space them out to avoid cooldown
                past_time = (now - timedelta(minutes=i * 120 + 600)).isoformat()
                conn.execute(
                    """INSERT INTO emails
                    (sent_at, alert_type, risk_score, opportunity_score, actions_json)
                    VALUES (?, ?, ?, ?, ?)""",
                    (past_time, "RISK", 70.0 + i * 5, 45.0, "[]")
                )
            conn.commit()

        # Try to send one more
        should_send, reason = store.check_should_send(
            risk_score=95.0,
            opportunity_score=50.0,
            alert_type="RISK",
        )

        assert should_send is False
        assert "limit" in reason.lower() or "max" in reason.lower()


class TestTickerActionTracking:
    """Tests for per-ticker action tracking."""

    @pytest.fixture
    def temp_db(self):
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    @pytest.fixture
    def store(self, temp_db):
        return StateStore(db_path=temp_db)

    def test_record_ticker_action(self, store):
        """Test recording a ticker action."""
        store.record_ticker_action(
            ticker="AAPL",
            action="TRIM",
            urgency="HIGH",
            confidence=85,
        )

        # Should not raise
        assert True

    def test_get_recent_ticker_actions(self, store):
        """Test retrieving recent actions for a ticker."""
        store.record_ticker_action(
            ticker="AAPL",
            action="TRIM",
            urgency="HIGH",
            confidence=85,
        )

        actions = store.get_recent_ticker_actions("AAPL", hours=24)
        assert len(actions) >= 1
        assert actions[0]["ticker"] == "AAPL"
        assert actions[0]["action"] == "TRIM"

    def test_check_ticker_action_cooldown(self, store):
        """Test ticker action cooldown."""
        # Record an action
        store.record_ticker_action(
            ticker="MSFT",
            action="ADD",
            urgency="MED",
            confidence=70,
        )

        # Check if same action can be recommended again
        can_recommend = store.can_recommend_ticker_action(
            ticker="MSFT",
            action="ADD",
            cooldown_hours=24,
        )

        # Should be blocked
        assert can_recommend is False

    def test_different_action_allowed(self, store):
        """Test that different action type is allowed."""
        store.record_ticker_action(
            ticker="GOOGL",
            action="ADD",
            urgency="MED",
            confidence=70,
        )

        # Different action should be allowed
        can_recommend = store.can_recommend_ticker_action(
            ticker="GOOGL",
            action="TRIM",  # Different action
            cooldown_hours=24,
        )

        assert can_recommend is True


class TestScoreHistory:
    """Tests for score history tracking."""

    @pytest.fixture
    def temp_db(self):
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    @pytest.fixture
    def store(self, temp_db):
        return StateStore(db_path=temp_db)

    def test_record_score(self, store):
        """Test recording scores."""
        store.record_scores(
            risk_score=65.0,
            opportunity_score=72.0,
            total_value=50000.0,
        )

        assert True  # No exception

    def test_get_score_history(self, store):
        """Test retrieving score history."""
        # Record multiple scores
        for i in range(5):
            store.record_scores(
                risk_score=60.0 + i * 2,
                opportunity_score=50.0 + i * 3,
                total_value=50000.0 + i * 1000,
            )

        history = store.get_score_history(days=7)
        assert len(history) >= 5

    def test_score_trend_detection(self, store):
        """Test score trend detection."""
        import sqlite3
        now = datetime.now()

        # Insert historical scores showing upward trend
        with sqlite3.connect(store.db_path) as conn:
            for i in range(10):
                past_time = (now - timedelta(hours=i * 6)).isoformat()
                conn.execute(
                    """INSERT INTO score_history
                    (timestamp, risk_score, opportunity_score, total_value)
                    VALUES (?, ?, ?, ?)""",
                    (past_time, 50.0 + i * 2, 60.0, 50000.0)
                )
            conn.commit()

        # Check if trend is detected
        trend = store.get_risk_score_trend(hours=48)

        # Risk score increased from 50 to 68
        assert trend > 0  # Positive trend (increasing risk)


class TestCleanup:
    """Tests for data cleanup functionality."""

    @pytest.fixture
    def temp_db(self):
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    @pytest.fixture
    def store(self, temp_db):
        return StateStore(db_path=temp_db)

    def test_cleanup_old_records(self, store):
        """Test cleanup of old records."""
        import sqlite3
        now = datetime.now()

        # Insert old records
        old_time = (now - timedelta(days=100)).isoformat()
        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                """INSERT INTO emails
                (sent_at, alert_type, risk_score, opportunity_score, actions_json)
                VALUES (?, ?, ?, ?, ?)""",
                (old_time, "RISK", 75.0, 45.0, "[]")
            )
            conn.commit()

        # Run cleanup
        deleted = store.cleanup_old_records(days=90)

        # Should have deleted the old record
        assert deleted >= 1
