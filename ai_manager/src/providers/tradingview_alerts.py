"""
TradingView Alerts Integration

Receives alerts from TradingView via webhook (FastAPI server).
No scraping - uses only TradingView's official webhook alert feature.

To use:
1. Create a TradingView alert
2. Set webhook URL to: http://your-server:8000/tv-alert
3. Use JSON message format with ticker, price, etc.
"""

import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass

from ..utils.logging import get_logger
from ..utils.time import now_utc, format_timestamp

logger = get_logger(__name__)


@dataclass
class TradingViewAlert:
    """A TradingView alert received via webhook."""
    id: str
    ticker: str
    exchange: Optional[str]
    price: Optional[float]
    alert_name: str
    message: Optional[str]
    tags: List[str]
    raw_payload: str
    received_at: datetime
    processed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "ticker": self.ticker,
            "exchange": self.exchange,
            "price": self.price,
            "alert_name": self.alert_name,
            "message": self.message,
            "tags": self.tags,
            "raw_payload": self.raw_payload,
            "received_at": format_timestamp(self.received_at),
            "processed": self.processed,
        }


class TradingViewAlertStore:
    """
    SQLite store for TradingView alerts.

    Stores alerts received via webhook for use in scoring.
    """

    def __init__(self, db_path: str = "data/tv_alerts.db"):
        """
        Initialize alert store.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    exchange TEXT,
                    price REAL,
                    alert_name TEXT NOT NULL,
                    message TEXT,
                    tags TEXT,
                    raw_payload TEXT,
                    received_at TEXT NOT NULL,
                    processed INTEGER DEFAULT 0,
                    payload_hash TEXT UNIQUE
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_alerts_ticker ON alerts(ticker)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_alerts_received ON alerts(received_at)
            """)
            conn.commit()

    def store_alert(self, alert: TradingViewAlert) -> bool:
        """
        Store an alert in the database.

        Args:
            alert: Alert to store

        Returns:
            True if stored, False if duplicate
        """
        payload_hash = hashlib.sha256(alert.raw_payload.encode()).hexdigest()[:32]

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO alerts
                    (id, ticker, exchange, price, alert_name, message, tags,
                     raw_payload, received_at, processed, payload_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    alert.id,
                    alert.ticker,
                    alert.exchange,
                    alert.price,
                    alert.alert_name,
                    alert.message,
                    json.dumps(alert.tags),
                    alert.raw_payload,
                    format_timestamp(alert.received_at),
                    int(alert.processed),
                    payload_hash,
                ))
                conn.commit()
                logger.info(f"Stored TradingView alert: {alert.ticker} - {alert.alert_name}")
                return True

        except sqlite3.IntegrityError:
            logger.debug(f"Duplicate alert ignored: {alert.ticker}")
            return False

    def get_recent_alerts(
        self,
        ticker: Optional[str] = None,
        hours: int = 24,
        unprocessed_only: bool = False
    ) -> List[TradingViewAlert]:
        """
        Get recent alerts.

        Args:
            ticker: Filter by ticker symbol
            hours: Get alerts from last N hours
            unprocessed_only: Only return unprocessed alerts

        Returns:
            List of TradingViewAlert objects
        """
        cutoff = now_utc() - timedelta(hours=hours)
        cutoff_str = format_timestamp(cutoff)

        query = "SELECT * FROM alerts WHERE received_at >= ?"
        params: List[Any] = [cutoff_str]

        if ticker:
            query += " AND ticker = ?"
            params.append(ticker.upper())

        if unprocessed_only:
            query += " AND processed = 0"

        query += " ORDER BY received_at DESC"

        alerts = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)

            for row in cursor:
                alerts.append(TradingViewAlert(
                    id=row["id"],
                    ticker=row["ticker"],
                    exchange=row["exchange"],
                    price=row["price"],
                    alert_name=row["alert_name"],
                    message=row["message"],
                    tags=json.loads(row["tags"]) if row["tags"] else [],
                    raw_payload=row["raw_payload"],
                    received_at=datetime.fromisoformat(row["received_at"]),
                    processed=bool(row["processed"]),
                ))

        return alerts

    def mark_processed(self, alert_ids: List[str]) -> int:
        """
        Mark alerts as processed.

        Args:
            alert_ids: List of alert IDs to mark

        Returns:
            Number of alerts updated
        """
        if not alert_ids:
            return 0

        placeholders = ",".join("?" * len(alert_ids))
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                f"UPDATE alerts SET processed = 1 WHERE id IN ({placeholders})",
                alert_ids
            )
            conn.commit()
            return cursor.rowcount

    def cleanup_old_alerts(self, days: int = 30) -> int:
        """
        Delete alerts older than N days.

        Args:
            days: Delete alerts older than this

        Returns:
            Number of alerts deleted
        """
        cutoff = now_utc() - timedelta(days=days)
        cutoff_str = format_timestamp(cutoff)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM alerts WHERE received_at < ?",
                (cutoff_str,)
            )
            conn.commit()
            return cursor.rowcount

    def get_alert_count(self, ticker: Optional[str] = None, hours: int = 24) -> int:
        """
        Get count of recent alerts.

        Args:
            ticker: Filter by ticker
            hours: Count from last N hours

        Returns:
            Alert count
        """
        cutoff = now_utc() - timedelta(hours=hours)
        cutoff_str = format_timestamp(cutoff)

        query = "SELECT COUNT(*) FROM alerts WHERE received_at >= ?"
        params: List[Any] = [cutoff_str]

        if ticker:
            query += " AND ticker = ?"
            params.append(ticker.upper())

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchone()[0]


def parse_tv_alert_payload(payload: Union[str, Dict[str, Any]]) -> TradingViewAlert:
    """
    Parse a TradingView webhook payload into an alert object.

    Expected payload format:
    {
        "ticker": "AAPL",
        "exchange": "NASDAQ",
        "price": 150.25,
        "alert_name": "My Alert",
        "message": "Price crossed above SMA",
        "tags": ["bullish", "breakout"]
    }

    Args:
        payload: JSON string or dict

    Returns:
        TradingViewAlert object
    """
    if isinstance(payload, str):
        try:
            data = json.loads(payload)
            raw_payload = payload
        except json.JSONDecodeError:
            # Treat as simple message
            data = {"message": payload}
            raw_payload = payload
    else:
        data = payload
        raw_payload = json.dumps(payload)

    # Generate unique ID
    alert_id = hashlib.sha256(
        f"{data.get('ticker', '')}:{data.get('alert_name', '')}:{now_utc().isoformat()}".encode()
    ).hexdigest()[:16]

    # Extract ticker (handle various formats)
    ticker = data.get("ticker", "") or data.get("symbol", "") or ""
    ticker = ticker.upper().strip()

    # Extract tags from message if not provided
    tags = data.get("tags", [])
    if isinstance(tags, str):
        tags = [tags]

    # Try to extract tags from message
    message = data.get("message", "") or data.get("comment", "") or ""
    if message and not tags:
        # Common tag patterns
        if re.search(r'\bbuy\b|\bbullish\b|\blong\b', message, re.I):
            tags.append("bullish")
        if re.search(r'\bsell\b|\bbearish\b|\bshort\b', message, re.I):
            tags.append("bearish")
        if re.search(r'\bbreakout\b|\bbreak\b', message, re.I):
            tags.append("breakout")
        if re.search(r'\bsupport\b', message, re.I):
            tags.append("support")
        if re.search(r'\bresistance\b', message, re.I):
            tags.append("resistance")

    # Extract price
    price = data.get("price") or data.get("close")
    if isinstance(price, str):
        try:
            price = float(price.replace("$", "").replace(",", ""))
        except (ValueError, TypeError):
            price = None

    return TradingViewAlert(
        id=alert_id,
        ticker=ticker,
        exchange=data.get("exchange"),
        price=price,
        alert_name=data.get("alert_name", "") or data.get("alertName", "") or "Unknown",
        message=message,
        tags=tags,
        raw_payload=raw_payload,
        received_at=now_utc(),
        processed=False,
    )


# ============================================================
# FASTAPI WEBHOOK SERVER
# ============================================================

def create_webhook_app(
    alert_store: TradingViewAlertStore,
    webhook_secret: Optional[str] = None
):
    """
    Create FastAPI app for receiving TradingView webhooks.

    Args:
        alert_store: Alert store instance
        webhook_secret: Optional secret for validating requests

    Returns:
        FastAPI app instance
    """
    try:
        from fastapi import FastAPI, HTTPException, Header, Request
        from fastapi.responses import JSONResponse
    except ImportError:
        logger.error("FastAPI not installed - webhook server unavailable")
        return None

    app = FastAPI(title="TradingView Alert Receiver")

    @app.post("/tv-alert")
    async def receive_alert(
        request: Request,
        x_webhook_secret: Optional[str] = Header(None)
    ):
        """
        Receive TradingView webhook alert.

        Validates webhook secret if configured.
        """
        # Validate secret if configured
        if webhook_secret:
            if x_webhook_secret != webhook_secret:
                logger.warning("Invalid webhook secret received")
                raise HTTPException(status_code=401, detail="Invalid webhook secret")

        # Parse request body
        try:
            body = await request.body()
            payload = body.decode("utf-8")
        except Exception as e:
            logger.error(f"Failed to read request body: {e}")
            raise HTTPException(status_code=400, detail="Invalid request body")

        # Parse alert
        try:
            alert = parse_tv_alert_payload(payload)
        except Exception as e:
            logger.error(f"Failed to parse alert payload: {e}")
            raise HTTPException(status_code=400, detail="Invalid alert format")

        # Validate required fields
        if not alert.ticker:
            raise HTTPException(status_code=400, detail="Missing ticker in payload")

        # Store alert
        stored = alert_store.store_alert(alert)

        return JSONResponse({
            "status": "ok" if stored else "duplicate",
            "alert_id": alert.id,
            "ticker": alert.ticker,
        })

    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {"status": "healthy"}

    @app.get("/alerts")
    async def get_alerts(
        ticker: Optional[str] = None,
        hours: int = 24,
        limit: int = 50
    ):
        """Get recent alerts."""
        alerts = alert_store.get_recent_alerts(ticker=ticker, hours=hours)
        return {
            "count": len(alerts),
            "alerts": [a.to_dict() for a in alerts[:limit]],
        }

    return app


def run_webhook_server(
    alert_store: TradingViewAlertStore,
    host: str = "0.0.0.0",
    port: int = 8000,
    webhook_secret: Optional[str] = None
):
    """
    Run the webhook server.

    Args:
        alert_store: Alert store instance
        host: Bind host
        port: Bind port
        webhook_secret: Optional secret for validation
    """
    try:
        import uvicorn
    except ImportError:
        logger.error("uvicorn not installed - cannot run webhook server")
        return

    app = create_webhook_app(alert_store, webhook_secret)
    if app:
        logger.info(f"Starting TradingView webhook server on {host}:{port}")
        uvicorn.run(app, host=host, port=port)


# CLI entry point for running the webhook server
if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    # Load environment
    from dotenv import load_dotenv
    load_dotenv()

    # Create store and run server
    store = TradingViewAlertStore()
    secret = os.environ.get("TV_WEBHOOK_SECRET")
    port = int(os.environ.get("TV_WEBHOOK_PORT", 8000))

    run_webhook_server(store, port=port, webhook_secret=secret)
