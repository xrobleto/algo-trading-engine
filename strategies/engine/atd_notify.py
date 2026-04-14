"""
Alpha Trader Daily (ATD) — fire-and-forget trade notifications.

Pushes TREND_FOLLOWING entry/exit events to the ATD founder-trade API
so trades appear on the Alpha Trader Daily public dashboard.

Non-blocking: each POST runs in a daemon thread. Failures are logged
but never affect the trading engine.

Env vars:
  ATD_API_URL         — endpoint (defaults to production Railway URL)
  ATD_INTERNAL_KEY    — X-Internal-Key header (required, else notifications are skipped)
"""

import logging
import os
import threading
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger("Engine")
ET = ZoneInfo("America/New_York")

_DEFAULT_URL = (
    "https://alpha-trader-daily-production.up.railway.app"
    "/api/engagement/founder-trade"
)


# ---------------------------------------------------------------------------
# Internal POST helper
# ---------------------------------------------------------------------------

def _post(payload: dict) -> None:
    """POST payload to ATD in a daemon thread (fire-and-forget)."""
    url = os.getenv("ATD_API_URL", _DEFAULT_URL)
    key = os.getenv("ATD_INTERNAL_KEY", "")

    if not key:
        log.debug("[ATD] Skipping notification — ATD_INTERNAL_KEY not set")
        return

    def _send():
        try:
            resp = requests.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Internal-Key": key,
                },
                timeout=10,
            )
            if resp.ok:
                log.info(
                    f"[ATD] Sent {payload['status']} "
                    f"{payload['symbol']} ({resp.status_code})"
                )
            else:
                log.warning(
                    f"[ATD] API error {resp.status_code}: "
                    f"{resp.text[:200]}"
                )
        except Exception as e:
            log.warning(f"[ATD] POST failed: {e}")

    threading.Thread(target=_send, daemon=True, name="atd-notify").start()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def notify_entry(
    symbol: str,
    side: str,
    entry_price: float,
    quantity: float,
    entry_time: Optional[datetime] = None,
) -> None:
    """Notify ATD of a new TREND position entry."""
    if entry_time is None:
        entry_time = datetime.now(ET)

    _post({
        "symbol": symbol,
        "side": side.upper(),
        "strategy": "Trend Following",
        "entry_price": round(entry_price, 2),
        "quantity": round(quantity, 4),
        "entry_time": entry_time.isoformat(),
        "status": "open",
    })


def notify_exit(
    symbol: str,
    side: str,
    entry_price: float,
    exit_price: float,
    quantity: float,
    pnl: float,
    entry_time: Optional[str] = None,
    exit_time: Optional[datetime] = None,
    r_multiple: Optional[float] = None,
) -> None:
    """Notify ATD of a TREND position exit."""
    if exit_time is None:
        exit_time = datetime.now(ET)

    # Convert entry_time from UTC to ET if it's an ISO string
    if isinstance(entry_time, str):
        try:
            dt = datetime.fromisoformat(entry_time)
            entry_time = dt.astimezone(ET).isoformat()
        except (ValueError, TypeError):
            pass  # keep as-is

    _post({
        "symbol": symbol,
        "side": side.upper(),
        "strategy": "Trend Following",
        "entry_price": round(entry_price, 2),
        "exit_price": round(exit_price, 2),
        "quantity": round(quantity, 4),
        "pnl": round(pnl, 2),
        "r_multiple": round(r_multiple, 2) if r_multiple is not None else None,
        "entry_time": entry_time or datetime.now(ET).isoformat(),
        "exit_time": exit_time.isoformat(),
        "status": "closed",
    })
