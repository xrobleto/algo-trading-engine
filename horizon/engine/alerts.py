"""Engine alerting — email notifications for failures, kill-switch trips, and
a daily heartbeat.

Resilient by design: a failed send is logged, never raised — alerting must
never crash the engine. Every transport gets a hard 10s timeout so a blocked
network path cannot stall a cycle.

Transports, in order of preference:
  1. Resend HTTPS API — RESEND_API_KEY + RESEND_FROM (an address on a domain
     verified in Resend). Required on Railway: outbound SMTP is blocked there,
     and every SMTP heartbeat from 2026-09-25 to 2026-10-02 failed with
     "[Errno 101] Network is unreachable". HTTPS egress works.
  2. SMTP — SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD,
     ALERT_EMAIL_FROM (optional). Useful when running locally.
Common: HORIZON_ALERT_EMAIL or ALERT_EMAIL_TO — recipient(s), comma-separated.
HORIZON_ALERTS_ENABLED — set to "0" to force log-only (used in tests).
With neither transport configured the Alerter degrades to log-only.

A configured transport is NOT proof of delivery: confirm with an
"alert emailed via ..." log line (or the provider's delivery status).
"""

from __future__ import annotations

import logging
import smtplib
import ssl

import requests
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Dict, Optional

from ..paths import get_secret

log = logging.getLogger("horizon.alerts")

SMTP_TIMEOUT = 10  # seconds — a blocked port must never stall the engine
HTTP_TIMEOUT = 10
RESEND_URL = "https://api.resend.com/emails"


class Alerter:
    """Sends engine alerts by email; falls back to logging if unconfigured."""

    def __init__(self):
        self.to = get_secret("HORIZON_ALERT_EMAIL") or get_secret("ALERT_EMAIL_TO")
        self.smtp_server = get_secret("SMTP_SERVER")
        self.smtp_port = int(get_secret("SMTP_PORT", "587") or 587)
        self.smtp_user = get_secret("SMTP_USERNAME")
        self.smtp_pass = get_secret("SMTP_PASSWORD")
        self.sender = get_secret("ALERT_EMAIL_FROM") or self.smtp_user
        self.resend_key = get_secret("RESEND_API_KEY")
        self.resend_from = get_secret("RESEND_FROM")
        smtp_ok = all([self.to, self.smtp_server, self.smtp_user, self.smtp_pass])
        resend_ok = all([self.to, self.resend_key, self.resend_from])
        self.transport: Optional[str] = ("resend" if resend_ok
                                         else "smtp" if smtp_ok else None)
        self.enabled = (self.transport is not None
                        and get_secret("HORIZON_ALERTS_ENABLED", "1") != "0")
        self._recent: Dict[str, datetime] = {}
        self._last_heartbeat_date = None
        if not self.enabled:
            log.info("alerting is log-only (no transport configured, or disabled)")
        elif self.transport == "resend":
            log.info("alerting: email via Resend API from %s (configured; delivery "
                     "is confirmed per send)", self.resend_from)
        else:
            log.info("alerting: email via SMTP %s:%s (configured; delivery is "
                     "confirmed per send)", self.smtp_server, self.smtp_port)

    def _recipients(self):
        return [a.strip() for a in (self.to or "").split(",") if a.strip()]

    def _send_resend(self, subject: str, body: str) -> str:
        resp = requests.post(
            RESEND_URL,
            headers={"Authorization": f"Bearer {self.resend_key}",
                     "Content-Type": "application/json"},
            json={"from": self.resend_from, "to": self._recipients(),
                  "subject": subject, "text": body},
            timeout=HTTP_TIMEOUT)
        if resp.status_code >= 300:
            # Never log the key; the response body carries Resend's reason.
            raise RuntimeError(f"Resend HTTP {resp.status_code}: {resp.text[:200]}")
        return str((resp.json() or {}).get("id", "?"))

    def _send_smtp(self, subject: str, body: str) -> str:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self._recipients())
        msg.set_content(body)
        context = ssl.create_default_context()
        with smtplib.SMTP(self.smtp_server, self.smtp_port,
                          timeout=SMTP_TIMEOUT) as server:
            server.starttls(context=context)
            server.login(self.smtp_user, self.smtp_pass)
            server.send_message(msg)
        return "smtp"

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
        full_subject = f"[Horizon {level}] {subject}"
        try:
            if self.transport == "resend":
                ref = self._send_resend(full_subject, body or subject)
            else:
                ref = self._send_smtp(full_subject, body or subject)
            log.info("alert emailed via %s (%s): %s", self.transport, ref, subject)
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
