"""
FRED API Client

Provides macroeconomic data from the Federal Reserve Economic Data API.
Used for tracking macro indicators like VIX, Fed Funds Rate, yield curves, etc.
"""

import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any

import requests

from ..utils.logging import get_logger
from ..utils.time import now_utc, format_timestamp
from ..utils.retry import retry_with_backoff, RateLimiter, TTLCache
from ..utils.typing import MacroIndicator

logger = get_logger(__name__)


class FREDClient:
    """
    Client for FRED (Federal Reserve Economic Data) API.

    Features:
        - Economic time series data
        - Rate limiting (120 requests/minute for FRED)
        - Response caching with longer TTL (macro data changes slowly)
        - Graceful degradation
    """

    BASE_URL = "https://api.stlouisfed.org/fred"

    # Common macro series IDs
    SERIES_INFO = {
        "VIXCLS": {"name": "VIX (Volatility Index)", "frequency": "daily"},
        "DFF": {"name": "Fed Funds Rate", "frequency": "daily"},
        "T10Y2Y": {"name": "10Y-2Y Treasury Spread", "frequency": "daily"},
        "T10YIE": {"name": "10Y Breakeven Inflation", "frequency": "daily"},
        "UNRATE": {"name": "Unemployment Rate", "frequency": "monthly"},
        "CPIAUCSL": {"name": "Consumer Price Index", "frequency": "monthly"},
        "GDP": {"name": "Gross Domestic Product", "frequency": "quarterly"},
        "HOUST": {"name": "Housing Starts", "frequency": "monthly"},
        "INDPRO": {"name": "Industrial Production", "frequency": "monthly"},
        "RSXFS": {"name": "Retail Sales", "frequency": "monthly"},
        "DTWEXBGS": {"name": "Trade-Weighted Dollar Index", "frequency": "daily"},
        "BAMLH0A0HYM2": {"name": "High Yield Bond Spread", "frequency": "daily"},
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        rate_limit_per_min: int = 100,
        cache_ttl_seconds: int = 3600  # 1 hour cache for macro data
    ):
        """
        Initialize FRED client.

        Args:
            api_key: FRED API key (or from FRED_API_KEY env var)
            rate_limit_per_min: Max API calls per minute
            cache_ttl_seconds: Cache TTL (default 1 hour)
        """
        self.api_key = api_key or os.environ.get("FRED_API_KEY")
        if not self.api_key:
            logger.warning("No FRED API key configured - client will be disabled")

        self.rate_limiter = RateLimiter(rate_limit_per_min, name="fred")
        self.cache = TTLCache(cache_ttl_seconds)
        self.last_successful_call: Optional[datetime] = None
        self._enabled = bool(self.api_key)

    @property
    def is_available(self) -> bool:
        """Check if client is available and configured."""
        return self._enabled

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        cache_key: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Make an API request with rate limiting and caching.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            cache_key: Optional cache key

        Returns:
            JSON response or None on error
        """
        if not self._enabled:
            return None

        # Check cache first
        if cache_key:
            cached = self.cache.get(cache_key)
            if cached is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached

        # Rate limit
        self.rate_limiter.acquire()

        url = f"{self.BASE_URL}{endpoint}"
        params = params or {}
        params["api_key"] = self.api_key
        params["file_type"] = "json"

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            self.last_successful_call = now_utc()

            # Cache successful response
            if cache_key:
                self.cache.set(cache_key, data)

            return data

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.warning("FRED rate limit exceeded")
            elif response.status_code == 400:
                logger.error(f"FRED bad request: {e}")
            else:
                logger.error(f"FRED HTTP error: {e}")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"FRED request failed: {e}")
            return None

    # =========================================================
    # SERIES DATA
    # =========================================================

    @retry_with_backoff(max_retries=2, base_delay=1.0, exceptions=(requests.exceptions.RequestException,))
    def get_series(
        self,
        series_id: str,
        observation_start: Optional[date] = None,
        observation_end: Optional[date] = None,
        limit: int = 100
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get observations for a FRED series.

        Args:
            series_id: FRED series ID (e.g., "VIXCLS", "DFF")
            observation_start: Start date
            observation_end: End date
            limit: Max observations

        Returns:
            List of observation dicts or None
        """
        if observation_end is None:
            observation_end = date.today()
        if observation_start is None:
            observation_start = observation_end - timedelta(days=365)

        cache_key = f"fred_series:{series_id}:{observation_start}:{observation_end}"

        params = {
            "series_id": series_id,
            "observation_start": observation_start.isoformat(),
            "observation_end": observation_end.isoformat(),
            "sort_order": "desc",
            "limit": limit,
        }

        data = self._make_request("/series/observations", params=params, cache_key=cache_key)

        if not data or "observations" not in data:
            return None

        observations = []
        for obs in data["observations"]:
            value_str = obs.get("value", ".")
            if value_str == ".":
                continue  # Skip missing values

            try:
                value = float(value_str)
            except (ValueError, TypeError):
                continue

            observations.append({
                "date": obs.get("date"),
                "value": value,
            })

        return observations

    def get_latest_value(self, series_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest observation for a series.

        Args:
            series_id: FRED series ID

        Returns:
            Dict with date and value, or None
        """
        observations = self.get_series(series_id, limit=5)
        if not observations:
            return None

        # Return most recent non-null value
        return observations[0] if observations else None

    def get_macro_indicator(
        self,
        series_id: str,
        name: Optional[str] = None,
        alert_threshold_high: Optional[float] = None,
        alert_threshold_low: Optional[float] = None
    ) -> Optional[MacroIndicator]:
        """
        Get a macro indicator with change tracking and alerts.

        Args:
            series_id: FRED series ID
            name: Display name (defaults to series info)
            alert_threshold_high: Alert if value exceeds this
            alert_threshold_low: Alert if value falls below this

        Returns:
            MacroIndicator object or None
        """
        # Get recent observations
        observations = self.get_series(series_id, limit=10)
        if not observations or len(observations) < 1:
            return None

        latest = observations[0]
        current_value = latest["value"]
        current_date = datetime.strptime(latest["date"], "%Y-%m-%d").date()

        # Get previous value for change calculation
        previous_value = None
        change = None
        if len(observations) >= 2:
            previous_value = observations[1]["value"]
            change = current_value - previous_value

        # Check alert thresholds
        alert_triggered = False
        alert_reason = None

        if alert_threshold_high is not None and current_value > alert_threshold_high:
            alert_triggered = True
            alert_reason = f"Above threshold ({current_value:.2f} > {alert_threshold_high})"
        elif alert_threshold_low is not None and current_value < alert_threshold_low:
            alert_triggered = True
            alert_reason = f"Below threshold ({current_value:.2f} < {alert_threshold_low})"

        # Use provided name or look up from series info
        display_name = name or self.SERIES_INFO.get(series_id, {}).get("name", series_id)

        return MacroIndicator(
            series_id=series_id,
            name=display_name,
            value=current_value,
            date=current_date,
            previous_value=previous_value,
            change=change,
            alert_triggered=alert_triggered,
            alert_reason=alert_reason,
        )

    def get_multiple_indicators(
        self,
        series_configs: List[Dict[str, Any]]
    ) -> List[MacroIndicator]:
        """
        Get multiple macro indicators.

        Args:
            series_configs: List of dicts with series_id, name, thresholds

        Returns:
            List of MacroIndicator objects
        """
        indicators = []

        for config in series_configs:
            indicator = self.get_macro_indicator(
                series_id=config.get("series_id"),
                name=config.get("name"),
                alert_threshold_high=config.get("alert_threshold_high"),
                alert_threshold_low=config.get("alert_threshold_low"),
            )
            if indicator:
                indicators.append(indicator)

        return indicators

    # =========================================================
    # SERIES METADATA
    # =========================================================

    def get_series_info(self, series_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata about a FRED series.

        Args:
            series_id: FRED series ID

        Returns:
            Dict with series metadata or None
        """
        cache_key = f"fred_info:{series_id}"

        data = self._make_request(
            "/series",
            params={"series_id": series_id},
            cache_key=cache_key
        )

        if not data or "seriess" not in data or not data["seriess"]:
            return None

        series = data["seriess"][0]
        return {
            "id": series.get("id"),
            "title": series.get("title"),
            "frequency": series.get("frequency"),
            "units": series.get("units"),
            "seasonal_adjustment": series.get("seasonal_adjustment"),
            "last_updated": series.get("last_updated"),
            "observation_start": series.get("observation_start"),
            "observation_end": series.get("observation_end"),
        }

    # =========================================================
    # CONVENIENCE METHODS
    # =========================================================

    def get_vix(self) -> Optional[MacroIndicator]:
        """Get current VIX with standard thresholds."""
        return self.get_macro_indicator(
            series_id="VIXCLS",
            name="VIX (Volatility Index)",
            alert_threshold_high=25,
            alert_threshold_low=12,
        )

    def get_fed_funds_rate(self) -> Optional[MacroIndicator]:
        """Get current Fed Funds Rate."""
        return self.get_macro_indicator(
            series_id="DFF",
            name="Fed Funds Rate",
        )

    def get_yield_curve_spread(self) -> Optional[MacroIndicator]:
        """Get 10Y-2Y Treasury spread (yield curve indicator)."""
        return self.get_macro_indicator(
            series_id="T10Y2Y",
            name="10Y-2Y Treasury Spread",
            alert_threshold_low=-0.5,  # Inversion warning
        )

    def get_unemployment_rate(self) -> Optional[MacroIndicator]:
        """Get unemployment rate."""
        return self.get_macro_indicator(
            series_id="UNRATE",
            name="Unemployment Rate",
        )

    # =========================================================
    # STATUS
    # =========================================================

    def get_status(self) -> Dict[str, Any]:
        """Get client status for monitoring."""
        return {
            "provider": "fred",
            "enabled": self._enabled,
            "last_successful_call": format_timestamp(self.last_successful_call) if self.last_successful_call else None,
            "rate_limiter_usage": f"{self.rate_limiter.current_usage}/{self.rate_limiter.calls_per_minute}",
            "cache_status": "active",
        }
