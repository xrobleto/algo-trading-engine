"""Time and timezone utilities for AI Investment Manager."""

import re
from datetime import datetime, date, time as dt_time, timedelta
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

# Timezone constants
ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
CT = ZoneInfo("America/Chicago")

# Market hours (Eastern Time)
MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(16, 0)
EXTENDED_OPEN = dt_time(4, 0)
EXTENDED_CLOSE = dt_time(20, 0)

# US Market Holidays (2024-2026)
US_MARKET_HOLIDAYS = {
    # 2024
    date(2024, 1, 1),    # New Year's Day
    date(2024, 1, 15),   # MLK Day
    date(2024, 2, 19),   # Presidents Day
    date(2024, 3, 29),   # Good Friday
    date(2024, 5, 27),   # Memorial Day
    date(2024, 6, 19),   # Juneteenth
    date(2024, 7, 4),    # Independence Day
    date(2024, 9, 2),    # Labor Day
    date(2024, 11, 28),  # Thanksgiving
    date(2024, 12, 25),  # Christmas

    # 2025
    date(2025, 1, 1),    # New Year's Day
    date(2025, 1, 20),   # MLK Day
    date(2025, 2, 17),   # Presidents Day
    date(2025, 4, 18),   # Good Friday
    date(2025, 5, 26),   # Memorial Day
    date(2025, 6, 19),   # Juneteenth
    date(2025, 7, 4),    # Independence Day
    date(2025, 9, 1),    # Labor Day
    date(2025, 11, 27),  # Thanksgiving
    date(2025, 12, 25),  # Christmas

    # 2026
    date(2026, 1, 1),    # New Year's Day
    date(2026, 1, 19),   # MLK Day
    date(2026, 2, 16),   # Presidents Day
    date(2026, 4, 3),    # Good Friday
    date(2026, 5, 25),   # Memorial Day
    date(2026, 6, 19),   # Juneteenth
    date(2026, 7, 3),    # Independence Day (observed)
    date(2026, 9, 7),    # Labor Day
    date(2026, 11, 26),  # Thanksgiving
    date(2026, 12, 25),  # Christmas
}


def now_utc() -> datetime:
    """Get current time in UTC."""
    return datetime.now(UTC)


def now_et() -> datetime:
    """Get current time in Eastern Time."""
    return datetime.now(ET)


def to_et(dt: datetime) -> datetime:
    """Convert datetime to Eastern Time."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(ET)


def to_utc(dt: datetime) -> datetime:
    """Convert datetime to UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(UTC)


def is_market_holiday(d: date) -> bool:
    """Check if date is a US market holiday."""
    return d in US_MARKET_HOLIDAYS


def is_weekend(d: date) -> bool:
    """Check if date is a weekend."""
    return d.weekday() >= 5


def is_trading_day(d: date) -> bool:
    """Check if date is a trading day."""
    return not is_weekend(d) and not is_market_holiday(d)


def is_market_hours(dt: Optional[datetime] = None, include_extended: bool = False) -> bool:
    """
    Check if given datetime is during market hours.

    Args:
        dt: Datetime to check (defaults to now)
        include_extended: Include extended/pre-market hours

    Returns:
        True if market is open
    """
    if dt is None:
        dt = now_et()
    else:
        dt = to_et(dt)

    # Check if trading day
    if not is_trading_day(dt.date()):
        return False

    current_time = dt.time()

    if include_extended:
        return EXTENDED_OPEN <= current_time < EXTENDED_CLOSE
    else:
        return MARKET_OPEN <= current_time < MARKET_CLOSE


def get_market_open_close(d: Optional[date] = None) -> Tuple[datetime, datetime]:
    """
    Get market open and close times for a given date.

    Args:
        d: Date (defaults to today)

    Returns:
        Tuple of (market_open, market_close) as datetime objects in ET
    """
    if d is None:
        d = now_et().date()

    market_open = datetime.combine(d, MARKET_OPEN, tzinfo=ET)
    market_close = datetime.combine(d, MARKET_CLOSE, tzinfo=ET)

    return market_open, market_close


def next_market_open() -> datetime:
    """Get the next market open time."""
    now = now_et()
    current_date = now.date()

    # Check if we're before market open today
    market_open, _ = get_market_open_close(current_date)
    if now < market_open and is_trading_day(current_date):
        return market_open

    # Find next trading day
    next_day = current_date + timedelta(days=1)
    while not is_trading_day(next_day):
        next_day += timedelta(days=1)

    return datetime.combine(next_day, MARKET_OPEN, tzinfo=ET)


def parse_date(date_str: str, fmt: str = "%m/%d/%Y") -> Optional[date]:
    """
    Parse a date string.

    Args:
        date_str: Date string to parse
        fmt: Expected format (default: MM/DD/YYYY for Robinhood)

    Returns:
        Parsed date or None if invalid
    """
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()

    try:
        return datetime.strptime(date_str, fmt).date()
    except ValueError:
        pass

    # Try alternative formats
    alt_formats = [
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]

    for alt_fmt in alt_formats:
        try:
            return datetime.strptime(date_str, alt_fmt).date()
        except ValueError:
            continue

    return None


def parse_datetime(dt_str: str) -> Optional[datetime]:
    """
    Parse a datetime string (ISO-8601 or common formats).

    Args:
        dt_str: Datetime string to parse

    Returns:
        Parsed datetime or None if invalid
    """
    if not dt_str or not dt_str.strip():
        return None

    dt_str = dt_str.strip()

    # Try ISO-8601 with timezone
    try:
        # Handle 'Z' suffix
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        return datetime.fromisoformat(dt_str)
    except ValueError:
        pass

    # Try common formats
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(dt_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            continue

    return None


def format_timestamp(dt: datetime, include_tz: bool = True) -> str:
    """Format datetime as ISO-8601 string."""
    if include_tz:
        return dt.isoformat()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def time_ago_str(dt: datetime) -> str:
    """
    Get human-readable time ago string.

    Args:
        dt: Datetime to compare against now

    Returns:
        String like "2 hours ago", "3 days ago", etc.
    """
    now = now_utc()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(UTC)

    delta = now - dt

    seconds = delta.total_seconds()
    if seconds < 0:
        return "in the future"

    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    elif seconds < 604800:
        days = int(seconds / 86400)
        return f"{days} day{'s' if days != 1 else ''} ago"
    else:
        weeks = int(seconds / 604800)
        return f"{weeks} week{'s' if weeks != 1 else ''} ago"


def get_trading_days_between(start: date, end: date) -> int:
    """Count trading days between two dates (exclusive of end)."""
    count = 0
    current = start
    while current < end:
        if is_trading_day(current):
            count += 1
        current += timedelta(days=1)
    return count


def get_previous_trading_day(d: Optional[date] = None) -> date:
    """Get the previous trading day."""
    if d is None:
        d = now_et().date()

    prev = d - timedelta(days=1)
    while not is_trading_day(prev):
        prev -= timedelta(days=1)
    return prev


def get_market_session_str() -> str:
    """Get current market session as string."""
    now = now_et()

    if not is_trading_day(now.date()):
        return "CLOSED (Weekend/Holiday)"

    current_time = now.time()

    if current_time < EXTENDED_OPEN:
        return "CLOSED"
    elif current_time < MARKET_OPEN:
        return "PRE-MARKET"
    elif current_time < MARKET_CLOSE:
        return "MARKET OPEN"
    elif current_time < EXTENDED_CLOSE:
        return "AFTER-HOURS"
    else:
        return "CLOSED"
