"""
State Storage Module

SQLite-based state store for deduplication and tracking.
Prevents email spam by tracking sent alerts and changes.
"""

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

from ..utils.logging import get_logger
from ..utils.time import now_utc, format_timestamp

logger = get_logger(__name__)


@dataclass
class DedupeResult:
    """Result of deduplication check."""
    should_send: bool
    reason: str
    last_sent: Optional[datetime] = None
    emails_today: int = 0
    score_delta: float = 0.0


class StateStore:
    """
    SQLite state store for tracking sent alerts and preventing spam.

    Tracks:
    - Portfolio hash (detect changes)
    - Email timestamps
    - Per-ticker actions
    - Score history
    """

    def __init__(self, db_path: str = "data/state.db"):
        """
        Initialize state store.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            # Email tracking table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS emails (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sent_at TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    risk_score REAL,
                    opportunity_score REAL,
                    recipients TEXT,
                    portfolio_hash TEXT,
                    subject TEXT
                )
            """)

            # Per-ticker action tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ticker_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    action TEXT NOT NULL,
                    urgency TEXT,
                    confidence INTEGER,
                    sent_at TEXT NOT NULL,
                    evidence_hash TEXT
                )
            """)

            # Score history
            conn.execute("""
                CREATE TABLE IF NOT EXISTS score_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    risk_score REAL NOT NULL,
                    opportunity_score REAL NOT NULL,
                    portfolio_value REAL,
                    portfolio_hash TEXT
                )
            """)

            # Create indexes
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_emails_sent_at ON emails(sent_at)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticker_actions_ticker ON ticker_actions(ticker, sent_at)
            """)

            conn.commit()

    # =========================================================
    # DEDUPLICATION
    # =========================================================

    def check_should_send(
        self,
        risk_score: float,
        opportunity_score: float,
        portfolio_hash: str,
        max_emails_per_day: int = 3,
        min_hours_between: int = 4,
        material_score_delta: float = 10,
    ) -> DedupeResult:
        """
        Check if an email should be sent based on deduplication rules.

        Args:
            risk_score: Current risk alert score
            opportunity_score: Current opportunity score
            portfolio_hash: Hash of current portfolio state
            max_emails_per_day: Maximum emails per day
            min_hours_between: Minimum hours between emails
            material_score_delta: Minimum score change for "material"

        Returns:
            DedupeResult with decision and reason
        """
        now = now_utc()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        min_time = now - timedelta(hours=min_hours_between)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Count emails today
            cursor = conn.execute(
                "SELECT COUNT(*) as count FROM emails WHERE sent_at >= ?",
                (format_timestamp(today_start),)
            )
            emails_today = cursor.fetchone()["count"]

            # Check max emails per day
            if max_emails_per_day > 0 and emails_today >= max_emails_per_day:
                return DedupeResult(
                    should_send=False,
                    reason=f"Daily limit reached ({emails_today}/{max_emails_per_day})",
                    emails_today=emails_today
                )

            # Get last email
            cursor = conn.execute(
                "SELECT * FROM emails ORDER BY sent_at DESC LIMIT 1"
            )
            last_email = cursor.fetchone()

            if last_email:
                last_sent = datetime.fromisoformat(last_email["sent_at"])
                last_risk = last_email["risk_score"] or 0
                last_opp = last_email["opportunity_score"] or 0
                last_hash = last_email["portfolio_hash"]

                # Check minimum time between emails
                if last_sent > min_time:
                    hours_since = (now - last_sent).total_seconds() / 3600
                    return DedupeResult(
                        should_send=False,
                        reason=f"Too soon ({hours_since:.1f}h < {min_hours_between}h min)",
                        last_sent=last_sent,
                        emails_today=emails_today
                    )

                # Check for material change
                risk_delta = abs(risk_score - last_risk)
                opp_delta = abs(opportunity_score - last_opp)
                max_delta = max(risk_delta, opp_delta)

                if max_delta < material_score_delta and portfolio_hash == last_hash:
                    return DedupeResult(
                        should_send=False,
                        reason=f"No material change (delta: {max_delta:.1f} < {material_score_delta})",
                        last_sent=last_sent,
                        emails_today=emails_today,
                        score_delta=max_delta
                    )

                return DedupeResult(
                    should_send=True,
                    reason=f"Material change detected (delta: {max_delta:.1f})",
                    last_sent=last_sent,
                    emails_today=emails_today,
                    score_delta=max_delta
                )

            # No previous emails - allow send
            return DedupeResult(
                should_send=True,
                reason="First email",
                emails_today=emails_today
            )

    def check_ticker_action_sent(
        self,
        ticker: str,
        action: str,
        hours_lookback: int = 24,
        evidence_hash: Optional[str] = None
    ) -> bool:
        """
        Check if a specific ticker action was already sent recently.

        Args:
            ticker: Stock symbol
            action: Action type (ADD, TRIM, etc.)
            hours_lookback: Hours to look back
            evidence_hash: Hash of evidence (for exact match)

        Returns:
            True if action was already sent
        """
        cutoff = now_utc() - timedelta(hours=hours_lookback)

        with sqlite3.connect(self.db_path) as conn:
            if evidence_hash:
                cursor = conn.execute(
                    """SELECT COUNT(*) as count FROM ticker_actions
                    WHERE ticker = ? AND action = ? AND sent_at >= ? AND evidence_hash = ?""",
                    (ticker.upper(), action, format_timestamp(cutoff), evidence_hash)
                )
            else:
                cursor = conn.execute(
                    """SELECT COUNT(*) as count FROM ticker_actions
                    WHERE ticker = ? AND action = ? AND sent_at >= ?""",
                    (ticker.upper(), action, format_timestamp(cutoff))
                )

            return cursor.fetchone()[0] > 0

    # =========================================================
    # RECORDING
    # =========================================================

    def record_email_sent(
        self,
        alert_type: str,
        risk_score: float,
        opportunity_score: float,
        portfolio_hash: str,
        recipients: List[str],
        subject: str,
    ) -> int:
        """
        Record that an email was sent.

        Args:
            alert_type: Type of alert (risk, opportunity, both)
            risk_score: Risk score at time of send
            opportunity_score: Opportunity score at time of send
            portfolio_hash: Portfolio hash
            recipients: Email recipients
            subject: Email subject

        Returns:
            ID of inserted record
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """INSERT INTO emails
                (sent_at, alert_type, risk_score, opportunity_score, portfolio_hash, recipients, subject)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    format_timestamp(now_utc()),
                    alert_type,
                    risk_score,
                    opportunity_score,
                    portfolio_hash,
                    json.dumps(recipients),
                    subject,
                )
            )
            conn.commit()
            return cursor.lastrowid

    def record_ticker_action(
        self,
        ticker: str,
        action: str,
        urgency: str,
        confidence: int,
        evidence_hash: str,
    ) -> int:
        """
        Record a ticker action that was sent.

        Args:
            ticker: Stock symbol
            action: Action type
            urgency: Urgency level
            confidence: Confidence percentage
            evidence_hash: Hash of supporting evidence

        Returns:
            ID of inserted record
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """INSERT INTO ticker_actions
                (ticker, action, urgency, confidence, sent_at, evidence_hash)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    ticker.upper(),
                    action,
                    urgency,
                    confidence,
                    format_timestamp(now_utc()),
                    evidence_hash,
                )
            )
            conn.commit()
            return cursor.lastrowid

    def record_scores(
        self,
        risk_score: float,
        opportunity_score: float,
        portfolio_value: float,
        portfolio_hash: str,
    ) -> int:
        """
        Record score snapshot for history.

        Args:
            risk_score: Risk alert score
            opportunity_score: Opportunity score
            portfolio_value: Total portfolio value
            portfolio_hash: Portfolio hash

        Returns:
            ID of inserted record
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """INSERT INTO score_history
                (timestamp, risk_score, opportunity_score, portfolio_value, portfolio_hash)
                VALUES (?, ?, ?, ?, ?)""",
                (
                    format_timestamp(now_utc()),
                    risk_score,
                    opportunity_score,
                    portfolio_value,
                    portfolio_hash,
                )
            )
            conn.commit()
            return cursor.lastrowid

    # =========================================================
    # QUERIES
    # =========================================================

    def get_email_history(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get email history for last N days."""
        cutoff = now_utc() - timedelta(days=days)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """SELECT * FROM emails WHERE sent_at >= ? ORDER BY sent_at DESC""",
                (format_timestamp(cutoff),)
            )

            return [dict(row) for row in cursor.fetchall()]

    def get_score_history(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get score history for last N days."""
        cutoff = now_utc() - timedelta(days=days)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """SELECT * FROM score_history WHERE timestamp >= ? ORDER BY timestamp DESC""",
                (format_timestamp(cutoff),)
            )

            return [dict(row) for row in cursor.fetchall()]

    def get_ticker_action_history(
        self,
        ticker: str,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """Get action history for a specific ticker."""
        cutoff = now_utc() - timedelta(days=days)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """SELECT * FROM ticker_actions
                WHERE ticker = ? AND sent_at >= ?
                ORDER BY sent_at DESC""",
                (ticker.upper(), format_timestamp(cutoff))
            )

            return [dict(row) for row in cursor.fetchall()]

    # =========================================================
    # UTILITIES
    # =========================================================

    def compute_portfolio_hash(self, holdings: Dict[str, float]) -> str:
        """
        Compute hash of portfolio state.

        Args:
            holdings: Dict mapping symbol to value

        Returns:
            SHA256 hash string
        """
        # Sort for deterministic hashing
        sorted_holdings = sorted(holdings.items())
        content = json.dumps(sorted_holdings)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def cleanup_old_records(self, days: int = 90) -> Dict[str, int]:
        """
        Delete records older than N days.

        Args:
            days: Delete records older than this

        Returns:
            Dict with counts of deleted records per table
        """
        cutoff = now_utc() - timedelta(days=days)
        cutoff_str = format_timestamp(cutoff)

        deleted = {}

        with sqlite3.connect(self.db_path) as conn:
            for table in ["emails", "ticker_actions", "score_history"]:
                timestamp_col = "sent_at" if table != "score_history" else "timestamp"
                cursor = conn.execute(
                    f"DELETE FROM {table} WHERE {timestamp_col} < ?",
                    (cutoff_str,)
                )
                deleted[table] = cursor.rowcount

            conn.commit()

        logger.info(f"Cleaned up old records: {deleted}")
        return deleted

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the state store."""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}

            for table in ["emails", "ticker_actions", "score_history"]:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                stats[f"{table}_count"] = cursor.fetchone()[0]

            # Get last email time
            cursor = conn.execute(
                "SELECT sent_at FROM emails ORDER BY sent_at DESC LIMIT 1"
            )
            row = cursor.fetchone()
            stats["last_email"] = row[0] if row else None

            # Get today's email count
            today_start = now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
            cursor = conn.execute(
                "SELECT COUNT(*) FROM emails WHERE sent_at >= ?",
                (format_timestamp(today_start),)
            )
            stats["emails_today"] = cursor.fetchone()[0]

            return stats
