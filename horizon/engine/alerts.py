"""Engine alerting — email notifications for failures, kill-switch trips, and
a daily heartbeat.

Resilient by design: a failed send is logged, never raised — alerting must
never crash the engine. SMTP gets a hard 10s timeout so a blocked port cannot
stall a cycle (a lesson carried over from the Unified Engine).

Configuration is read from the environment / .env:
  SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD
  ALERT_EMAIL_FROM (optional, defaults to SMTP_USERNAME)
  HORIZON_ALERT_EMAIL or ALERT_EMAIL_TO  — recipient
  HORIZON_ALERTS_ENABLED — set to "0" to force log-only (used in tests)
If SMTP is not configured, the Alerter degrades to log-only automatically.
"""

from __future__ import annotations

import logging
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Dict, Optional

from ..paths import get_secret

log = logging.getLogger("horizon.alerts")

SMTP_TIMEOUT = 10  # seconds — a blocked port must never stall the engine


class Alerter:
    """Sends engine alerts by email; falls back to logging if unconfigured."""

    def __init__(self):
        self.to = get_secret("HORIZON_ALERT_EMAIL") or get_secret("ALERT_EMAIL_TO")
        self.smtp_server = get_secret("SMTP_SERVER")
        self.smtp_port = int(get_secret("SMTP_PORT", "587") or 587)
        self.smtp_user = get_secret("SMTP_USERNAME")
        self.smtp_pass = get_secret("SMTP_PASSWORD")
        self.sender = get_secret("ALERT_EMAIL_FROM") or self.smtp_user
        configured = all([self.to, self.smtp_server, self.smtp_user,
                          self.smtp_pass])
        self.enabled = (configured
                        and get_secret("HORIZON_ALERTS_ENABLED", "1") != "0")
        self._recent: Dict[str, datetime] = {}
        self._last_heartbeat_date = None
        if not self.enabled:
            log.info("alerting is log-only (SMTP not configured or disabled)")

    def send(self, subject: str, body: str, level: str = "INFO",
             dedup_minutes: int = 240) -> None:
        """Send an alert. Always logs; emails if enabled and not a duplicate."""
        logger = {"CRITICAL": log.critical, "WARNING": log.warning}.get(
            level, log.info)
        logger("ALERT[%s] %s", level, subject)
        if not self.enabled:
            return
        now = datetime.now(timezone.utc)
        last = self._recent.get(subject)
        if last is not None and (now - last) < timedelta(minutes=dedup_minutes):
            return  # suppress a repeating alert
        self._recent[subject] = now
        try:
            msg = EmailMessage()
            msg["Subject"] = f"[Horizon {level}] {subject}"
            msg["From"] = self.sender
            msg["To"] = self.to
            msg.set_content(body or subject)
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port,
                              timeout=SMTP_TIMEOUT) as server:
                server.starttls(context=context)
                server.login(self.smtp_user, self.smtp_pass)
                server.send_message(msg)
            log.info("alert emailed: %s", subject)
        except Exception as exc:  # noqa: BLE001 — alerting must not crash the engine
            log.warning("alert email failed (%s): %s", subject, exc)

    def critical(self, subject: str, body: str = "") -> None:
        self.send(subject, body, level="CRITICAL")

    def warning(self, subject: str, body: str = "") -> None:
        self.send(subject, body, level="WARNING")

    def heartbeat(self, summary: Dict[str, object]) -> None:
        """Send a once-per-day INFO heartbeat so silence means something broke."""
        today = datetime.now(timezone.utc).date()
        if self._last_heartbeat_date == today:
            return
        self._last_heartbeat_date = today
        body = "\n".join(f"{k}: {v}" for k, v in summary.items())
        self.send(f"daily heartbeat {today}", body, level="INFO",
                  dedup_minutes=0)
