"""
market_scanner.py — Dynamic Market Scanner

Discovers and ranks the best momentum trading candidates in real-time.
Designed to replace simple_bot's discover_dynamic_universe() with a smarter,
multi-factor approach.

Architecture:
  Pre-market (9:00-9:29 AM): Scan for gap-up stocks with unusual volume
  Intraday (9:30 AM - 4:00 PM): Track top gainers + RVOL movers + volume acceleration
  Both phases: Rank candidates by composite quality score (0-100)

Usage:
  Standalone:  python strategies/market_scanner.py
  Integration: from market_scanner import MarketScanner

Key improvements over simple_bot's discover_dynamic_universe():
  1. Pre-market gap scanning (highest probability momentum trades)
  2. Gainers endpoint (pre-filtered by Polygon, more efficient)
  3. Time-adjusted RVOL (meaningful at any time of day, not just afternoon)
  4. Volume acceleration (is volume increasing or fading?)
  5. Catalyst/news awareness (follow-through predictor)
  6. Multi-factor scoring (not just RVOL ranking)
"""

import os
import sys
import time
import logging
import requests
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dt_time, date
from typing import List, Dict, Optional, Tuple, Set
from enum import Enum
from collections import defaultdict

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
logger = logging.getLogger("market_scanner")


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

POLYGON_REST_BASE = "https://api.polygon.io"

# --- Pre-Market Gap Scanner ---
GAP_MIN_PCT = 2.0                   # Min gap up %
GAP_MAX_PCT = 15.0                  # Max gap up % (avoid parabolic/squeeze)
GAP_MIN_VOLUME = 50_000             # Min pre-market volume (shares)
GAP_MIN_DOLLAR_VOL = 5_000_000      # Min pre-market dollar volume

# --- Intraday Mover Scanner ---
MOVER_MIN_CHANGE_PCT = 0.5          # Min intraday % change (catch early movers)
MOVER_MAX_CHANGE_PCT = 10.0         # Max change % (avoid chasing extended moves)
MOVER_MIN_RVOL = 1.5                # Min time-adjusted relative volume
MOVER_MIN_DOLLAR_VOL = 10_000_000   # Min daily dollar volume

# --- General Filters ---
SCAN_MIN_PRICE = 5.0                # Min stock price
SCAN_MAX_PRICE = 500.0              # Max stock price
SCAN_MIN_PREV_CLOSE = 3.0           # Min previous close (filter pennies)
SCAN_MIN_PREV_VOLUME = 200_000      # Min previous day volume (baseline liquidity)

# --- Scoring Weights (total = 100) ---
W_CHANGE_PCT = 20                   # Intraday price change / gap %
W_RVOL = 25                         # Relative volume (strongest single predictor)
W_VOL_ACCEL = 15                    # Volume acceleration (trend confirmation)
W_CATALYST = 20                     # News/catalyst present (follow-through predictor)
W_SPREAD = 10                       # Liquidity (tight spread = easier execution)
W_DOLLAR_VOL = 10                   # Absolute liquidity

# --- Watchlist Settings ---
MAX_WATCHLIST_SIZE = 25             # Max candidates in active watchlist
MAX_PREMARKET_GAPPERS = 15          # Max pre-market candidates
NEWS_CHECK_TOP_N = 15               # Only check news for top N candidates (API rate limit)
NEWS_CACHE_MINUTES = 30             # Cache news results for 30 min
SCAN_INTERVAL_SEC = 300             # Rescan every 5 minutes (intraday)
VOL_HISTORY_SIZE = 6                # Track last 6 volume snapshots (30 min at 5-min intervals)

# --- API Settings ---
API_TIMEOUT = 15                    # Request timeout (seconds)
API_MAX_RETRIES = 3                 # Max retry attempts
API_RATE_LIMIT_DELAY = 0.5          # Delay between news checks (seconds)


# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

class CandidateSource(Enum):
    PREMARKET_GAP = "PREMARKET_GAP"
    TOP_GAINER = "TOP_GAINER"
    RVOL_MOVER = "RVOL_MOVER"


@dataclass
class ScanCandidate:
    """Raw candidate from initial scan, before scoring."""
    symbol: str
    price: float
    prev_close: float
    change_pct: float
    day_volume: int
    prev_day_volume: int
    rvol: float                 # Time-adjusted relative volume
    dollar_volume: float
    spread_bps: float = 0.0
    source: CandidateSource = CandidateSource.RVOL_MOVER


@dataclass
class WatchlistEntry:
    """Scored and ranked candidate on the active watchlist."""
    symbol: str
    price: float
    change_pct: float
    rvol: float
    dollar_volume: float
    spread_bps: float
    quality_score: float        # 0-100 composite score
    has_catalyst: bool = False
    catalyst_headline: str = ""
    vol_acceleration: float = 1.0
    source: CandidateSource = CandidateSource.RVOL_MOVER
    discovered_at: str = ""

    def __repr__(self):
        cat = " [NEWS]" if self.has_catalyst else ""
        return (f"{self.symbol:6s} ${self.price:>8.2f} | "
                f"Chg={self.change_pct:+.1f}% | RVOL={self.rvol:.1f}x | "
                f"$Vol={self.dollar_volume/1e6:.0f}M | "
                f"Score={self.quality_score:.0f}{cat}")


# ═══════════════════════════════════════════════════════════════
# POLYGON API CLIENT
# ═══════════════════════════════════════════════════════════════

class PolygonAPI:
    """Lightweight Polygon REST client for scanner use."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def _request(self, path: str, params: dict = None) -> dict:
        url = f"{POLYGON_REST_BASE}{path}"
        if params is None:
            params = {}
        params["apiKey"] = self.api_key

        for attempt in range(API_MAX_RETRIES):
            try:
                resp = self.session.get(url, params=params, timeout=API_TIMEOUT)
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"[SCANNER API] Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                elif resp.status_code >= 500:
                    wait = 2 ** attempt
                    logger.warning(f"[SCANNER API] Server error {resp.status_code}, retry {attempt+1}")
                    time.sleep(wait)
                else:
                    logger.warning(f"[SCANNER API] {resp.status_code} for {path}")
                    return {}
            except requests.exceptions.Timeout:
                logger.warning(f"[SCANNER API] Timeout for {path} (attempt {attempt+1})")
            except requests.exceptions.RequestException as e:
                logger.warning(f"[SCANNER API] Error: {e}")
                if attempt < API_MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
        return {}

    def get_all_snapshots(self) -> List[dict]:
        """Fetch snapshots for ALL US stock tickers (single API call)."""
        data = self._request("/v2/snapshot/locale/us/markets/stocks/tickers")
        return data.get("tickers", [])

    def get_gainers(self) -> List[dict]:
        """Fetch top gainers (pre-filtered by Polygon)."""
        data = self._request("/v2/snapshot/locale/us/markets/stocks/gainers")
        return data.get("tickers", [])

    def get_news(self, symbol: str, limit: int = 5) -> List[dict]:
        """Fetch recent news articles for a symbol."""
        data = self._request("/v2/reference/news", {
            "ticker": symbol,
            "limit": str(limit),
            "order": "desc",
            "sort": "published_utc"
        })
        return data.get("results", [])


# ═══════════════════════════════════════════════════════════════
# VOLUME TRACKER
# ═══════════════════════════════════════════════════════════════

class VolumeTracker:
    """Tracks volume over time to compute acceleration.

    Volume acceleration measures whether volume is increasing or decreasing.
    Rising volume during a move = strong conviction, fading volume = potential reversal.

    Compares the volume-per-minute rate in the recent half of the history window
    vs the earlier half. Ratio > 1.0 means volume is accelerating.
    """

    def __init__(self, history_size: int = VOL_HISTORY_SIZE):
        self.history_size = history_size
        self._history: Dict[str, List[Tuple[float, int]]] = defaultdict(list)

    def record(self, symbol: str, volume: int, timestamp: float = None):
        if timestamp is None:
            timestamp = time.time()
        history = self._history[symbol]
        history.append((timestamp, volume))
        if len(history) > self.history_size:
            self._history[symbol] = history[-self.history_size:]

    def get_acceleration(self, symbol: str) -> float:
        """Ratio of recent volume rate to earlier volume rate. >1.0 = accelerating."""
        history = self._history.get(symbol, [])
        if len(history) < 3:
            return 1.0

        mid = len(history) // 2

        early_vol_delta = history[mid][1] - history[0][1]
        early_time_delta = history[mid][0] - history[0][0]

        recent_vol_delta = history[-1][1] - history[mid][1]
        recent_time_delta = history[-1][0] - history[mid][0]

        if early_time_delta <= 0 or recent_time_delta <= 0 or early_vol_delta <= 0:
            return 1.0

        early_rate = early_vol_delta / early_time_delta
        recent_rate = recent_vol_delta / recent_time_delta

        return recent_rate / early_rate if early_rate > 0 else 1.0

    def clear_all(self):
        self._history.clear()


# ═══════════════════════════════════════════════════════════════
# CANDIDATE SCORER
# ═══════════════════════════════════════════════════════════════

class CandidateScorer:
    """Multi-factor scoring engine (0-100) for ranking momentum candidates.

    Factors:
      - Price momentum (change %): sweet spot 2-6% gap, 1-4% intraday
      - Relative volume: sweet spot 2-8x (time-adjusted)
      - Volume acceleration: >1.0 means fresh buying
      - Catalyst: binary bonus for today's news
      - Spread: tighter = better execution
      - Dollar volume: higher = more liquid
    """

    @staticmethod
    def score(candidate: ScanCandidate,
              vol_acceleration: float = 1.0,
              has_catalyst: bool = False) -> float:
        total = (
            CandidateScorer._score_change(candidate.change_pct, candidate.source) * (W_CHANGE_PCT / 100) +
            CandidateScorer._score_rvol(candidate.rvol) * (W_RVOL / 100) +
            CandidateScorer._score_acceleration(vol_acceleration) * (W_VOL_ACCEL / 100) +
            CandidateScorer._score_catalyst(has_catalyst) * (W_CATALYST / 100) +
            CandidateScorer._score_spread(candidate.spread_bps) * (W_SPREAD / 100) +
            CandidateScorer._score_dollar_volume(candidate.dollar_volume) * (W_DOLLAR_VOL / 100)
        )
        return min(100.0, max(0.0, total))

    @staticmethod
    def _score_change(change_pct: float, source: CandidateSource) -> float:
        pct = abs(change_pct)
        if source == CandidateSource.PREMARKET_GAP:
            # Gaps: 2-4% good, 4-8% great, 8-15% ok (may be overextended)
            if pct < 2:     return 20
            elif pct < 4:   return 60 + (pct - 2) * 15      # 60-90
            elif pct < 8:   return 90 + (pct - 4) * 2.5      # 90-100
            elif pct < 15:  return 100 - (pct - 8) * 5        # 100-65
            else:           return max(20, 65 - (pct - 15) * 5)
        else:
            # Intraday: 1-3% good, 3-5% great, >5% extended
            if pct < 0.5:   return 10
            elif pct < 1:   return 30 + (pct - 0.5) * 40     # 30-50
            elif pct < 3:   return 50 + (pct - 1) * 20        # 50-90
            elif pct < 5:   return 90 + (pct - 3) * 5         # 90-100
            elif pct < 8:   return 100 - (pct - 5) * 8        # 100-76
            else:           return max(20, 76 - (pct - 8) * 8)

    @staticmethod
    def _score_rvol(rvol: float) -> float:
        # Sweet spot: 2-8x. Above 12x may be blow-off/exhaustion.
        if rvol < 1.0:     return 0
        elif rvol < 1.5:   return 20 + (rvol - 1.0) * 40     # 20-40
        elif rvol < 2.0:   return 40 + (rvol - 1.5) * 30     # 40-55
        elif rvol < 3.0:   return 55 + (rvol - 2.0) * 20     # 55-75
        elif rvol < 5.0:   return 75 + (rvol - 3.0) * 10     # 75-95
        elif rvol < 8.0:   return 95 + (rvol - 5.0) * 1.67   # 95-100
        elif rvol < 12:    return 100 - (rvol - 8.0) * 5      # 100-80
        else:              return max(40, 80 - (rvol - 12) * 3)

    @staticmethod
    def _score_acceleration(accel: float) -> float:
        if accel <= 0.5:    return 10
        elif accel < 0.8:   return 30 + (accel - 0.5) * 50   # 30-45
        elif accel < 1.0:   return 45 + (accel - 0.8) * 50   # 45-55
        elif accel < 1.3:   return 55 + (accel - 1.0) * 100  # 55-85
        elif accel < 1.8:   return 85 + (accel - 1.3) * 30   # 85-100
        elif accel < 3.0:   return 100
        else:               return 90

    @staticmethod
    def _score_catalyst(has_catalyst: bool) -> float:
        # No catalyst isn't zero — stock can move without news — just much lower probability
        return 100 if has_catalyst else 15

    @staticmethod
    def _score_spread(spread_bps: float) -> float:
        if spread_bps <= 0:     return 50   # Unknown, neutral
        elif spread_bps < 3:    return 100
        elif spread_bps < 8:    return 90 - (spread_bps - 3) * 4     # 90-70
        elif spread_bps < 15:   return 70 - (spread_bps - 8) * 4     # 70-42
        elif spread_bps < 30:   return 42 - (spread_bps - 15) * 2    # 42-12
        else:                   return max(0, 12 - (spread_bps - 30) * 0.4)

    @staticmethod
    def _score_dollar_volume(dollar_vol: float) -> float:
        vol_m = dollar_vol / 1_000_000
        if vol_m < 5:       return 10
        elif vol_m < 20:    return 30 + (vol_m - 5) * 2      # 30-60
        elif vol_m < 50:    return 60 + (vol_m - 20) * 1     # 60-90
        elif vol_m < 100:   return 90 + (vol_m - 50) * 0.2   # 90-100
        else:               return 100


# ═══════════════════════════════════════════════════════════════
# MARKET SCANNER
# ═══════════════════════════════════════════════════════════════

class MarketScanner:
    """Dynamic market scanner that discovers and ranks momentum candidates.

    Usage:
        scanner = MarketScanner(polygon_api_key="KEY")

        # Pre-market (9:00-9:29 AM)
        gappers = scanner.scan_premarket()

        # Intraday (every 5 min)
        watchlist = scanner.scan_intraday()

        # For simple_bot integration
        symbols = scanner.get_symbols()
    """

    def __init__(self, polygon_api_key: str,
                 excluded_symbols: Set[str] = None,
                 core_symbols: Set[str] = None):
        self.api = PolygonAPI(polygon_api_key)
        self.excluded_symbols = excluded_symbols or set()
        self.core_symbols = core_symbols or set()
        self.scorer = CandidateScorer()
        self.vol_tracker = VolumeTracker()

        self._watchlist: List[WatchlistEntry] = []
        self._premarket_gappers: List[WatchlistEntry] = []
        self._news_cache: Dict[str, Tuple[float, bool, str]] = {}
        self._scan_date: Optional[date] = None

        logger.info(f"[SCANNER] Initialized | excluded={len(self.excluded_symbols)} | "
                    f"core={len(self.core_symbols)}")

    # ─── Public API ────────────────────────────────────────────

    def scan(self) -> List[WatchlistEntry]:
        """Auto-detect phase (pre-market vs intraday) and run appropriate scan."""
        now = datetime.now(ET)
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)

        if now < market_open:
            return self.scan_premarket()
        else:
            return self.scan_intraday()

    def scan_premarket(self) -> List[WatchlistEntry]:
        """Scan for pre-market gappers. Call between 9:00-9:29 AM ET."""
        self._reset_daily_state()
        logger.info("[SCANNER] Pre-market gap scan starting...")
        t0 = time.time()

        try:
            snapshots = self.api.get_all_snapshots()
            if not snapshots:
                logger.warning("[SCANNER] No snapshot data for pre-market scan")
                return []

            candidates = []
            for td in snapshots:
                c = self._evaluate_premarket(td)
                if c:
                    candidates.append(c)

            elapsed = time.time() - t0
            logger.info(f"[SCANNER] Pre-market: {len(candidates)} gappers from "
                       f"{len(snapshots)} tickers ({elapsed:.1f}s)")

            # Sort by raw strength, take top batch for news checking
            candidates.sort(key=lambda c: abs(c.change_pct) * max(c.rvol, 1.0), reverse=True)

            watchlist = self._score_candidates(candidates[:MAX_PREMARKET_GAPPERS * 2])

            watchlist.sort(key=lambda w: w.quality_score, reverse=True)
            watchlist = watchlist[:MAX_PREMARKET_GAPPERS]

            self._premarket_gappers = watchlist
            self._watchlist = watchlist

            self._log_watchlist(watchlist, "Pre-market")
            return watchlist

        except Exception as e:
            logger.error(f"[SCANNER] Pre-market scan failed: {e}")
            return []

    def scan_intraday(self) -> List[WatchlistEntry]:
        """Scan for intraday momentum candidates. Call every 5 min during RTH."""
        today = datetime.now(ET).date()
        if self._scan_date != today:
            self._reset_daily_state()

        logger.info("[SCANNER] Intraday scan starting...")
        t0 = time.time()
        now_ts = time.time()

        try:
            # Source 1: Top gainers (pre-filtered by Polygon — efficient)
            gainers = self.api.get_gainers()
            gainer_candidates = []
            for td in gainers:
                c = self._evaluate_intraday(td, CandidateSource.TOP_GAINER)
                if c:
                    gainer_candidates.append(c)

            # Source 2: All tickers (catches RVOL movers not in gainers)
            all_snaps = self.api.get_all_snapshots()
            rvol_candidates = []
            for td in all_snaps:
                c = self._evaluate_intraday(td, CandidateSource.RVOL_MOVER)
                if c:
                    rvol_candidates.append(c)

            # Merge and deduplicate (prefer gainer source)
            seen = set()
            all_candidates = []
            for c in gainer_candidates:
                if c.symbol not in seen:
                    seen.add(c.symbol)
                    all_candidates.append(c)
            for c in rvol_candidates:
                if c.symbol not in seen:
                    seen.add(c.symbol)
                    all_candidates.append(c)

            elapsed = time.time() - t0
            logger.info(f"[SCANNER] Intraday: {len(gainer_candidates)} gainers + "
                       f"{len(rvol_candidates)} RVOL = {len(all_candidates)} unique ({elapsed:.1f}s)")

            # Record volume for acceleration tracking
            for c in all_candidates:
                self.vol_tracker.record(c.symbol, c.day_volume, now_ts)

            # Sort by raw strength, score top batch
            all_candidates.sort(key=lambda c: c.rvol * abs(c.change_pct), reverse=True)
            watchlist = self._score_candidates(all_candidates[:MAX_WATCHLIST_SIZE * 2])

            # Merge with pre-market gappers (persistent priority)
            existing = {w.symbol for w in watchlist}
            for gapper in self._premarket_gappers:
                if gapper.symbol not in existing:
                    gapper.quality_score *= 0.85  # Slight decay if not in intraday scan
                    watchlist.append(gapper)

            watchlist.sort(key=lambda w: w.quality_score, reverse=True)
            watchlist = watchlist[:MAX_WATCHLIST_SIZE]

            self._watchlist = watchlist
            self._log_watchlist(watchlist, "Intraday")
            return watchlist

        except Exception as e:
            logger.error(f"[SCANNER] Intraday scan failed: {e}")
            return self._watchlist  # Return stale on failure

    def get_watchlist(self) -> List[WatchlistEntry]:
        """Return current ranked watchlist."""
        return list(self._watchlist)

    def get_symbols(self) -> List[str]:
        """Return just the symbol list (for simple_bot integration)."""
        return [w.symbol for w in self._watchlist]

    def get_premarket_gappers(self) -> List[WatchlistEntry]:
        """Return pre-market gapper list."""
        return list(self._premarket_gappers)

    def get_candidate_info(self, symbol: str) -> Optional[WatchlistEntry]:
        """Look up a specific symbol's watchlist entry."""
        for w in self._watchlist:
            if w.symbol == symbol:
                return w
        return None

    # ─── Private: State Management ─────────────────────────────

    def _reset_daily_state(self):
        today = datetime.now(ET).date()
        if self._scan_date != today:
            self._scan_date = today
            self._watchlist = []
            self._premarket_gappers = []
            self._news_cache = {}
            self.vol_tracker.clear_all()
            logger.info(f"[SCANNER] New trading day: {today}")

    # ─── Private: Filtering ────────────────────────────────────

    def _is_excluded(self, symbol: str) -> bool:
        if not symbol or len(symbol) > 5:
            return True
        if symbol in self.excluded_symbols:
            return True
        if '.' in symbol or '-' in symbol or ' ' in symbol:
            return True
        # Skip warrants, units, rights
        if symbol.endswith('W') and len(symbol) >= 5:
            return True
        return False

    def _expected_volume_fraction(self) -> float:
        """Estimate fraction of daily volume that should have occurred by now.

        Used for time-adjusted RVOL: a stock with 50% of prev-day volume by
        11:00 AM (when only ~40% should have traded) has RVOL ~1.25x.
        Without this adjustment, raw RVOL is meaningless before noon.

        Volume distribution is U-shaped (heavy at open and close):
          9:30-10:30: ~25%
          10:30-12:00: ~20%
          12:00-2:00:  ~20%
          2:00-4:00:   ~35%
        """
        now = datetime.now(ET)
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)

        if now <= market_open:
            return 0.05  # Pre-market: tiny fraction

        mins = (now - market_open).total_seconds() / 60
        total = 390.0  # 6.5 hours

        if mins >= total:
            return 1.0

        # Piecewise approximation of cumulative volume curve
        if mins <= 60:
            # First hour: 25% of daily volume (front-loaded)
            return 0.25 * (mins / 60)
        elif mins <= 150:
            # 10:30 - 12:00: 20% over 90 min
            return 0.25 + 0.20 * ((mins - 60) / 90)
        elif mins <= 270:
            # 12:00 - 2:00: 20% over 120 min (lunch lull)
            return 0.45 + 0.20 * ((mins - 150) / 120)
        else:
            # 2:00 - 4:00: 35% over 120 min (closing surge)
            return 0.65 + 0.35 * ((mins - 270) / 120)

    def _compute_time_adjusted_rvol(self, day_volume: int, prev_day_volume: int) -> float:
        """Compute RVOL adjusted for time of day.

        Raw RVOL = day_volume / prev_day_volume.
        At 10:00 AM, only ~25% of volume should have occurred.
        Time-adjusted RVOL = raw_rvol / expected_fraction.

        A stock with 0.5x raw RVOL at 10:00 AM → 0.5 / 0.25 = 2.0x time-adjusted.
        """
        if prev_day_volume <= 0:
            return 0.0
        raw_rvol = day_volume / prev_day_volume
        expected = self._expected_volume_fraction()
        if expected <= 0.01:
            return raw_rvol  # Pre-market: return raw
        return raw_rvol / expected

    def _get_spread_bps(self, ticker_data: dict) -> float:
        """Extract bid-ask spread in basis points from snapshot."""
        quote = ticker_data.get("lastQuote", {})
        bid = quote.get("p", 0)    # lowercase p = bid
        ask = quote.get("P", 0)    # uppercase P = ask
        if bid > 0 and ask > 0 and ask > bid:
            mid = (bid + ask) / 2
            return ((ask - bid) / mid) * 10000
        return 0.0

    # ─── Private: Evaluation ───────────────────────────────────

    def _evaluate_premarket(self, ticker_data: dict) -> Optional[ScanCandidate]:
        """Evaluate a single ticker for pre-market gap candidacy."""
        try:
            symbol = ticker_data.get("ticker", "")
            if self._is_excluded(symbol):
                return None

            prev_day = ticker_data.get("prevDay", {})
            day_data = ticker_data.get("day", {})
            last_trade = ticker_data.get("lastTrade", {})

            prev_close = prev_day.get("c", 0)
            if prev_close < SCAN_MIN_PREV_CLOSE:
                return None

            prev_volume = prev_day.get("v", 0)
            if prev_volume < SCAN_MIN_PREV_VOLUME:
                return None

            current_price = last_trade.get("p") or day_data.get("c") or 0
            if current_price < SCAN_MIN_PRICE or current_price > SCAN_MAX_PRICE:
                return None

            gap_pct = ((current_price - prev_close) / prev_close) * 100
            if gap_pct < GAP_MIN_PCT or gap_pct > GAP_MAX_PCT:
                return None

            pm_volume = day_data.get("v", 0)
            if pm_volume < GAP_MIN_VOLUME:
                return None

            dollar_vol = current_price * pm_volume
            if dollar_vol < GAP_MIN_DOLLAR_VOL:
                return None

            # Pre-market RVOL: floor at 1.0 since raw ratio is inherently low
            raw_rvol = pm_volume / prev_volume if prev_volume > 0 else 0
            rvol = max(raw_rvol, 1.0)

            return ScanCandidate(
                symbol=symbol,
                price=current_price,
                prev_close=prev_close,
                change_pct=gap_pct,
                day_volume=pm_volume,
                prev_day_volume=prev_volume,
                rvol=rvol,
                dollar_volume=dollar_vol,
                spread_bps=self._get_spread_bps(ticker_data),
                source=CandidateSource.PREMARKET_GAP,
            )
        except Exception:
            return None

    def _evaluate_intraday(self, ticker_data: dict,
                           source: CandidateSource) -> Optional[ScanCandidate]:
        """Evaluate a single ticker for intraday momentum candidacy."""
        try:
            symbol = ticker_data.get("ticker", "")
            if self._is_excluded(symbol):
                return None

            day_data = ticker_data.get("day", {})
            prev_day = ticker_data.get("prevDay", {})
            last_trade = ticker_data.get("lastTrade", {})

            prev_close = prev_day.get("c", 0)
            if prev_close < SCAN_MIN_PREV_CLOSE:
                return None

            prev_volume = prev_day.get("v", 0)
            if prev_volume < SCAN_MIN_PREV_VOLUME:
                return None

            current_price = day_data.get("c") or last_trade.get("p") or 0
            if current_price < SCAN_MIN_PRICE or current_price > SCAN_MAX_PRICE:
                return None

            change_pct = ((current_price - prev_close) / prev_close) * 100
            if change_pct < MOVER_MIN_CHANGE_PCT or change_pct > MOVER_MAX_CHANGE_PCT:
                return None

            day_volume = day_data.get("v", 0)
            if day_volume <= 0:
                return None

            # Time-adjusted RVOL (the key improvement)
            rvol = self._compute_time_adjusted_rvol(day_volume, prev_volume)
            if rvol < MOVER_MIN_RVOL:
                return None

            dollar_vol = current_price * day_volume
            if dollar_vol < MOVER_MIN_DOLLAR_VOL:
                return None

            return ScanCandidate(
                symbol=symbol,
                price=current_price,
                prev_close=prev_close,
                change_pct=change_pct,
                day_volume=day_volume,
                prev_day_volume=prev_volume,
                rvol=rvol,
                dollar_volume=dollar_vol,
                spread_bps=self._get_spread_bps(ticker_data),
                source=source,
            )
        except Exception:
            return None

    # ─── Private: Scoring & News ───────────────────────────────

    def _score_candidates(self, candidates: List[ScanCandidate]) -> List[WatchlistEntry]:
        """Score a batch of candidates, checking news for top N."""
        now_str = datetime.now(ET).strftime("%H:%M:%S")
        watchlist = []

        for i, candidate in enumerate(candidates):
            vol_accel = self.vol_tracker.get_acceleration(candidate.symbol)

            has_news = False
            headline = ""
            if i < NEWS_CHECK_TOP_N:
                has_news, headline = self._check_catalyst(candidate.symbol)

            score = self.scorer.score(candidate, vol_accel, has_news)

            watchlist.append(WatchlistEntry(
                symbol=candidate.symbol,
                price=candidate.price,
                change_pct=candidate.change_pct,
                rvol=candidate.rvol,
                dollar_volume=candidate.dollar_volume,
                spread_bps=candidate.spread_bps,
                quality_score=score,
                has_catalyst=has_news,
                catalyst_headline=headline,
                vol_acceleration=vol_accel,
                source=candidate.source,
                discovered_at=now_str,
            ))

        return watchlist

    def _check_catalyst(self, symbol: str) -> Tuple[bool, str]:
        """Check if symbol has news today. Uses 30-min cache to reduce API calls."""
        now_ts = time.time()

        if symbol in self._news_cache:
            cached_ts, has_news, headline = self._news_cache[symbol]
            if (now_ts - cached_ts) < NEWS_CACHE_MINUTES * 60:
                return has_news, headline

        time.sleep(API_RATE_LIMIT_DELAY)

        try:
            articles = self.api.get_news(symbol, limit=5)
            if not articles:
                self._news_cache[symbol] = (now_ts, False, "")
                return False, ""

            today = datetime.now(ET).date()
            yesterday = today - timedelta(days=1)

            for article in articles:
                pub_utc = article.get("published_utc", "")
                if not pub_utc:
                    continue
                try:
                    pub_date = datetime.fromisoformat(pub_utc.replace("Z", "+00:00"))
                    pub_et = pub_date.astimezone(ET)
                    if pub_et.date() >= yesterday:
                        headline = article.get("title", "")[:120]
                        self._news_cache[symbol] = (now_ts, True, headline)
                        return True, headline
                except (ValueError, TypeError):
                    continue

            self._news_cache[symbol] = (now_ts, False, "")
            return False, ""

        except Exception as e:
            logger.debug(f"[SCANNER] News check failed for {symbol}: {e}")
            self._news_cache[symbol] = (now_ts, False, "")
            return False, ""

    # ─── Private: Logging ──────────────────────────────────────

    def _log_watchlist(self, watchlist: List[WatchlistEntry], phase: str):
        if not watchlist:
            logger.info(f"[SCANNER] {phase}: No candidates found")
            return

        logger.info(f"[SCANNER] {phase} watchlist ({len(watchlist)} stocks):")
        for i, entry in enumerate(watchlist[:10], 1):
            accel = (f"vol_up" if entry.vol_acceleration > 1.1
                     else f"vol_dn" if entry.vol_acceleration < 0.9
                     else "steady")
            cat = " [NEWS]" if entry.has_catalyst else ""
            logger.info(
                f"[SCANNER]   #{i:2d} {entry.symbol:6s} ${entry.price:>8.2f} | "
                f"chg={entry.change_pct:+.1f}% | RVOL={entry.rvol:.1f}x | "
                f"$vol={entry.dollar_volume/1e6:.0f}M | {accel} | "
                f"score={entry.quality_score:.0f}{cat}"
            )
        if len(watchlist) > 10:
            logger.info(f"[SCANNER]   ... and {len(watchlist) - 10} more")


# ═══════════════════════════════════════════════════════════════
# STANDALONE RUNNER
# ═══════════════════════════════════════════════════════════════

def print_table(watchlist: List[WatchlistEntry], title: str = "WATCHLIST"):
    """Print a formatted watchlist table to stdout."""
    if not watchlist:
        print(f"\n  {title}: No candidates found")
        return

    now_str = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")
    print(f"\n{'='*95}")
    print(f"  {title} ({len(watchlist)} candidates) | {now_str}")
    print(f"{'='*95}")
    print(f"  {'#':>3} {'Symbol':6s} {'Price':>9} {'Chg%':>7} {'RVOL':>6} "
          f"{'$Vol':>8} {'Sprd':>5} {'Accel':>6} {'Score':>6} {'Cat':3s} {'Source':12s}")
    print(f"  {'-'*3} {'-'*6} {'-'*9} {'-'*7} {'-'*6} "
          f"{'-'*8} {'-'*5} {'-'*6} {'-'*6} {'-'*3} {'-'*12}")

    for i, e in enumerate(watchlist, 1):
        cat = "YES" if e.has_catalyst else " - "
        if e.vol_acceleration > 1.1:
            accel = f"^{e.vol_acceleration:.1f}"
        elif e.vol_acceleration < 0.9:
            accel = f"v{e.vol_acceleration:.1f}"
        else:
            accel = "  -  "
        dvol = f"{e.dollar_volume/1e6:.0f}M"
        marker = "*" if e.quality_score >= 70 else "+" if e.quality_score >= 50 else " "

        print(f"  {i:3d} {e.symbol:6s} ${e.price:>8.2f} {e.change_pct:>+6.1f}% "
              f"{e.rvol:>5.1f}x {dvol:>8s} {e.spread_bps:>4.0f} {accel:>6s} "
              f"{marker}{e.quality_score:>5.0f} {cat:3s} {e.source.value:12s}")

        if e.has_catalyst and e.catalyst_headline:
            hl = e.catalyst_headline[:75]
            print(f"        -> {hl}")

    print(f"{'='*95}")


def main():
    """Standalone mode: run scanner and print live watchlist updates."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    # Load API key from env
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None

    env_paths = [
        os.path.join(os.path.dirname(__file__), "..", "config", "momentum_bot.env"),
        os.path.join(os.path.dirname(__file__), "..", "config", "directional_bot.env"),
        os.path.join(os.path.dirname(__file__), "..", ".env"),
    ]
    if load_dotenv:
        for path in env_paths:
            if os.path.exists(path):
                load_dotenv(path)
                break

    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        print("ERROR: POLYGON_API_KEY not found in environment.")
        print("Set via .env file or: set POLYGON_API_KEY=your_key")
        sys.exit(1)

    # Same exclusions as simple_bot
    excluded = {
        "MSTR", "SHOP", "CRWD",
        # Trend bot symbols
        "SPY", "QQQ", "IWM", "EFA", "EEM",
        "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLB", "XLC",
        "SMH", "IBB", "XHB", "MTUM", "QUAL", "VLUE",
        "IEF", "TLT", "GLD", "DBC", "SGOV",
    }

    scanner = MarketScanner(
        polygon_api_key=api_key,
        excluded_symbols=excluded,
    )

    print(f"\n{'='*60}")
    print(f"  MARKET SCANNER — Standalone Mode")
    print(f"  Scan interval: {SCAN_INTERVAL_SEC}s | Excluded: {len(excluded)}")
    print(f"{'='*60}")

    now = datetime.now(ET)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    # Pre-market phase
    if now < market_open:
        print(f"\n  Pre-market scan (market opens 9:30 AM ET)...")
        gappers = scanner.scan_premarket()
        print_table(gappers, "PRE-MARKET GAPPERS")

        while datetime.now(ET) < market_open:
            remaining = (market_open - datetime.now(ET)).total_seconds()
            if remaining > 120:
                print(f"\n  {remaining/60:.0f} min until open. Re-scanning in 2 min...")
                time.sleep(120)
                gappers = scanner.scan_premarket()
                print_table(gappers, "PRE-MARKET GAPPERS (Updated)")
            else:
                print(f"\n  Market opens in {remaining:.0f}s...")
                time.sleep(remaining)
                break

    # Intraday loop
    print(f"\n  Intraday scanning (every {SCAN_INTERVAL_SEC}s). Ctrl+C to stop.\n")

    try:
        while True:
            if datetime.now(ET) > market_close:
                print(f"\n  Market closed.")
                print_table(scanner.get_watchlist(), "FINAL WATCHLIST")
                break

            watchlist = scanner.scan_intraday()
            print_table(watchlist, "INTRADAY WATCHLIST")

            # Highlight volume accelerators
            accel = [w for w in watchlist if w.vol_acceleration > 1.3]
            if accel:
                print(f"\n  VOLUME ACCELERATING ({len(accel)}):")
                for w in accel[:5]:
                    print(f"    {w.symbol}: accel={w.vol_acceleration:.2f}x | "
                          f"chg={w.change_pct:+.1f}% | RVOL={w.rvol:.1f}x")

            time.sleep(SCAN_INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\n\n  Scanner stopped.")
        print_table(scanner.get_watchlist(), "FINAL WATCHLIST")


if __name__ == "__main__":
    main()
