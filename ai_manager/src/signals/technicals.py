"""
Technical Analysis Module

Computes technical indicators and signals for stocks.
All calculations are deterministic - no LLM involved.
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
import math

from ..utils.logging import get_logger
from ..utils.typing import TechnicalSignal, SignalStrength, Holding
from ..providers.massive_client import MassiveClient
from ..providers.alpaca_client import AlpacaClient

logger = get_logger(__name__)


def compute_sma(prices: List[float], period: int) -> Optional[float]:
    """Compute Simple Moving Average."""
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def compute_ema(prices: List[float], period: int) -> Optional[float]:
    """Compute Exponential Moving Average."""
    if len(prices) < period:
        return None

    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period  # SMA as starting point

    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


def compute_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """Compute Relative Strength Index."""
    if len(prices) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    if len(gains) < period:
        return None

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def compute_macd(
    prices: List[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Compute MACD (Moving Average Convergence Divergence).

    Returns:
        Tuple of (macd_line, signal_line, histogram)
    """
    if len(prices) < slow_period + signal_period:
        return None, None, None

    fast_ema = compute_ema(prices, fast_period)
    slow_ema = compute_ema(prices, slow_period)

    if fast_ema is None or slow_ema is None:
        return None, None, None

    macd_line = fast_ema - slow_ema

    # Compute MACD values for signal line
    macd_values = []
    for i in range(slow_period, len(prices)):
        fast = compute_ema(prices[:i + 1], fast_period)
        slow = compute_ema(prices[:i + 1], slow_period)
        if fast and slow:
            macd_values.append(fast - slow)

    if len(macd_values) < signal_period:
        return macd_line, None, None

    signal_line = compute_ema(macd_values, signal_period)
    histogram = macd_line - signal_line if signal_line else None

    return macd_line, signal_line, histogram


def compute_atr(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14
) -> Optional[float]:
    """Compute Average True Range."""
    if len(highs) < period + 1:
        return None

    true_ranges = []

    for i in range(1, len(highs)):
        tr1 = highs[i] - lows[i]
        tr2 = abs(highs[i] - closes[i - 1])
        tr3 = abs(lows[i] - closes[i - 1])
        true_ranges.append(max(tr1, tr2, tr3))

    if len(true_ranges) < period:
        return None

    return sum(true_ranges[-period:]) / period


def compute_volatility(prices: List[float], period: int = 20) -> Optional[float]:
    """Compute realized volatility (standard deviation of returns)."""
    if len(prices) < period + 1:
        return None

    returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            returns.append((prices[i] - prices[i - 1]) / prices[i - 1])

    if len(returns) < period:
        return None

    recent_returns = returns[-period:]
    mean_return = sum(recent_returns) / len(recent_returns)
    variance = sum((r - mean_return) ** 2 for r in recent_returns) / len(recent_returns)

    # Annualized volatility (assuming daily data)
    return math.sqrt(variance) * math.sqrt(252) * 100


def find_support_resistance(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    lookback: int = 20
) -> Tuple[Optional[float], Optional[float]]:
    """
    Find support and resistance levels from swing highs/lows.

    Returns:
        Tuple of (support, resistance)
    """
    if len(highs) < lookback:
        return None, None

    recent_highs = highs[-lookback:]
    recent_lows = lows[-lookback:]

    # Find local maxima (resistance) and minima (support)
    swing_highs = []
    swing_lows = []

    for i in range(1, len(recent_highs) - 1):
        if recent_highs[i] > recent_highs[i - 1] and recent_highs[i] > recent_highs[i + 1]:
            swing_highs.append(recent_highs[i])
        if recent_lows[i] < recent_lows[i - 1] and recent_lows[i] < recent_lows[i + 1]:
            swing_lows.append(recent_lows[i])

    current_price = closes[-1] if closes else None

    # Find nearest support (below current price)
    support = None
    if swing_lows and current_price:
        below = [l for l in swing_lows if l < current_price]
        if below:
            support = max(below)

    # Find nearest resistance (above current price)
    resistance = None
    if swing_highs and current_price:
        above = [h for h in swing_highs if h > current_price]
        if above:
            resistance = min(above)

    return support, resistance


def compute_sma_slope(sma_values: List[float], period: int = 5) -> Optional[float]:
    """Compute the slope of an SMA (rate of change)."""
    if len(sma_values) < period:
        return None

    recent = sma_values[-period:]
    if recent[0] == 0:
        return None

    return ((recent[-1] - recent[0]) / recent[0]) * 100


class TechnicalAnalyzer:
    """
    Technical analysis engine.

    Computes indicators and generates signals for holdings.
    All calculations are deterministic.
    """

    def __init__(
        self,
        massive_client: Optional[MassiveClient] = None,
        alpaca_client: Optional[AlpacaClient] = None,
        sma_periods: List[int] = None,
        ema_periods: List[int] = None,
        rsi_period: int = 14,
        atr_period: int = 14,
        volatility_lookback: int = 20
    ):
        """
        Initialize analyzer.

        Args:
            massive_client: Polygon client for data
            alpaca_client: Alpaca client (fallback)
            sma_periods: SMA periods to compute
            ema_periods: EMA periods to compute
            rsi_period: RSI period
            atr_period: ATR period
            volatility_lookback: Days for volatility calc
        """
        self.massive = massive_client
        self.alpaca = alpaca_client
        self.sma_periods = sma_periods or [20, 50, 200]
        self.ema_periods = ema_periods or [9, 21]
        self.rsi_period = rsi_period
        self.atr_period = atr_period
        self.volatility_lookback = volatility_lookback

    def analyze(self, symbol: str) -> Optional[TechnicalSignal]:
        """
        Perform full technical analysis on a symbol.

        Args:
            symbol: Stock symbol

        Returns:
            TechnicalSignal object or None if data unavailable
        """
        # Get historical bars
        bars = self._get_bars(symbol, days=250)  # ~1 year for 200 SMA
        if not bars or len(bars) < 50:
            logger.warning(f"Insufficient bar data for {symbol}")
            return None

        # Extract price arrays
        closes = [b["close"] for b in bars if b.get("close")]
        highs = [b["high"] for b in bars if b.get("high")]
        lows = [b["low"] for b in bars if b.get("low")]

        if len(closes) < 50:
            return None

        current_price = closes[-1]

        # Compute price changes
        change_1d = self._compute_change(closes, 1)
        change_5d = self._compute_change(closes, 5)
        change_20d = self._compute_change(closes, 20)

        # Compute SMAs
        sma_20 = compute_sma(closes, 20)
        sma_50 = compute_sma(closes, 50)
        sma_200 = compute_sma(closes, 200)

        # Compute EMAs
        ema_9 = compute_ema(closes, 9)
        ema_21 = compute_ema(closes, 21)

        # Trend signals
        above_sma_20 = current_price > sma_20 if sma_20 else None
        above_sma_50 = current_price > sma_50 if sma_50 else None
        above_sma_200 = current_price > sma_200 if sma_200 else None

        # SMA slope (trend direction)
        sma_20_values = [compute_sma(closes[:i + 1], 20) for i in range(19, len(closes))]
        sma_20_values = [v for v in sma_20_values if v is not None]
        sma_20_slope = compute_sma_slope(sma_20_values) if len(sma_20_values) >= 5 else None

        # Golden/death cross detection
        golden_cross = False
        death_cross = False
        if len(closes) >= 55:
            prev_sma_50 = compute_sma(closes[:-1], 50)
            prev_sma_200 = compute_sma(closes[:-1], 200)
            if sma_50 and sma_200 and prev_sma_50 and prev_sma_200:
                if sma_50 > sma_200 and prev_sma_50 <= prev_sma_200:
                    golden_cross = True
                elif sma_50 < sma_200 and prev_sma_50 >= prev_sma_200:
                    death_cross = True

        # Momentum
        rsi = compute_rsi(closes, self.rsi_period)
        macd, macd_signal, macd_hist = compute_macd(closes)

        # Volatility
        atr = compute_atr(highs, lows, closes, self.atr_period)
        volatility = compute_volatility(closes, self.volatility_lookback)

        # Support/Resistance
        support, resistance = find_support_resistance(highs, lows, closes)

        # Recent high/low
        recent_high = max(highs[-20:]) if len(highs) >= 20 else None
        recent_low = min(lows[-20:]) if len(lows) >= 20 else None

        # Compute signal strength and score
        signal_strength, signal_score = self._compute_signal_strength(
            price=current_price,
            sma_20=sma_20,
            sma_50=sma_50,
            sma_200=sma_200,
            rsi=rsi,
            macd_hist=macd_hist,
            sma_20_slope=sma_20_slope,
            change_5d=change_5d,
        )

        return TechnicalSignal(
            symbol=symbol,
            timestamp=datetime.now(),
            price=current_price,
            change_1d_pct=change_1d or 0,
            change_5d_pct=change_5d or 0,
            change_20d_pct=change_20d or 0,
            sma_20=sma_20,
            sma_50=sma_50,
            sma_200=sma_200,
            ema_9=ema_9,
            ema_21=ema_21,
            above_sma_20=above_sma_20,
            above_sma_50=above_sma_50,
            above_sma_200=above_sma_200,
            sma_20_slope=sma_20_slope,
            golden_cross=golden_cross,
            death_cross=death_cross,
            rsi_14=rsi,
            macd=macd,
            macd_signal=macd_signal,
            macd_histogram=macd_hist,
            atr_14=atr,
            volatility_20d=volatility,
            support_level=support,
            resistance_level=resistance,
            recent_high=recent_high,
            recent_low=recent_low,
            signal_strength=signal_strength,
            signal_score=signal_score,
        )

    def _get_bars(self, symbol: str, days: int) -> Optional[List[Dict[str, Any]]]:
        """Get historical bars from available provider."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        # Try Polygon first
        if self.massive and self.massive.is_available:
            bars = self.massive.get_bars(symbol, from_date=start_date, to_date=end_date, limit=days)
            if bars:
                return bars

        # Fall back to Alpaca
        if self.alpaca and self.alpaca.is_available:
            bars = self.alpaca.get_bars(symbol, start=start_date, end=end_date, limit=days)
            if bars:
                return bars

        return None

    @staticmethod
    def _compute_change(prices: List[float], days: int) -> Optional[float]:
        """Compute percentage change over N days."""
        if len(prices) <= days:
            return None

        current = prices[-1]
        past = prices[-(days + 1)]

        if past == 0:
            return None

        return ((current - past) / past) * 100

    @staticmethod
    def _compute_signal_strength(
        price: float,
        sma_20: Optional[float],
        sma_50: Optional[float],
        sma_200: Optional[float],
        rsi: Optional[float],
        macd_hist: Optional[float],
        sma_20_slope: Optional[float],
        change_5d: Optional[float],
    ) -> Tuple[SignalStrength, float]:
        """
        Compute overall signal strength and score.

        Returns:
            Tuple of (SignalStrength enum, score 0-100)
        """
        score = 50.0  # Neutral baseline
        factors = 0

        # Trend factor (price vs SMAs)
        if sma_20 and sma_50 and sma_200:
            if price > sma_20 > sma_50 > sma_200:
                score += 15  # Strong uptrend
                factors += 1
            elif price < sma_20 < sma_50 < sma_200:
                score -= 15  # Strong downtrend
                factors += 1
            elif price > sma_20 and price > sma_50:
                score += 8
                factors += 1
            elif price < sma_20 and price < sma_50:
                score -= 8
                factors += 1

        # RSI factor
        if rsi is not None:
            if rsi < 30:
                score += 10  # Oversold (potential bounce)
                factors += 1
            elif rsi > 70:
                score -= 10  # Overbought (potential pullback)
                factors += 1
            elif 40 <= rsi <= 60:
                factors += 1  # Neutral, no change

        # MACD factor
        if macd_hist is not None:
            if macd_hist > 0:
                score += 8
            else:
                score -= 8
            factors += 1

        # Trend slope factor
        if sma_20_slope is not None:
            if sma_20_slope > 1:
                score += 5
            elif sma_20_slope < -1:
                score -= 5
            factors += 1

        # Recent momentum
        if change_5d is not None:
            if change_5d > 5:
                score += 7
            elif change_5d < -5:
                score -= 7
            factors += 1

        # Clamp score
        score = max(0, min(100, score))

        # Determine signal strength
        if score >= 75:
            strength = SignalStrength.STRONG_BULLISH
        elif score >= 60:
            strength = SignalStrength.BULLISH
        elif score <= 25:
            strength = SignalStrength.STRONG_BEARISH
        elif score <= 40:
            strength = SignalStrength.BEARISH
        else:
            strength = SignalStrength.NEUTRAL

        return strength, score


def compute_technical_signals(
    holdings: List[Holding],
    massive_client: Optional[MassiveClient] = None,
    alpaca_client: Optional[AlpacaClient] = None
) -> Dict[str, TechnicalSignal]:
    """
    Compute technical signals for all holdings.

    Args:
        holdings: List of holdings to analyze
        massive_client: Polygon client
        alpaca_client: Alpaca client

    Returns:
        Dict mapping symbol to TechnicalSignal
    """
    analyzer = TechnicalAnalyzer(
        massive_client=massive_client,
        alpaca_client=alpaca_client
    )

    signals = {}
    for holding in holdings:
        signal = analyzer.analyze(holding.symbol)
        if signal:
            signals[holding.symbol] = signal

            # Update holding with technical data
            holding.price_change_1d = signal.change_1d_pct
            holding.price_change_5d = signal.change_5d_pct
            holding.price_change_20d = signal.change_20d_pct
            holding.sma_20 = signal.sma_20
            holding.sma_50 = signal.sma_50
            holding.sma_200 = signal.sma_200
            holding.rsi_14 = signal.rsi_14
            holding.atr_14 = signal.atr_14
            holding.volatility_20d = signal.volatility_20d

    logger.info(f"Computed technical signals for {len(signals)}/{len(holdings)} holdings")
    return signals


def generate_holding_recommendations(
    holdings: List[Holding],
    technical_signals: Dict[str, TechnicalSignal],
    total_portfolio_value: float,
    max_position_pct: float = 20.0
) -> None:
    """
    Generate Buy/Hold/Sell recommendations for each holding.

    Updates holdings in-place with recommendation and recommendation_reasons.

    Args:
        holdings: List of holdings to analyze
        technical_signals: Dict of technical signals by symbol
        total_portfolio_value: Total portfolio value for weight calculations
        max_position_pct: Max position % before triggering TRIM
    """
    for holding in holdings:
        symbol = holding.symbol
        signal = technical_signals.get(symbol)
        reasons = []

        # Default to HOLD
        recommendation = "HOLD"
        buy_score = 0
        sell_score = 0

        # === Position Size Check ===
        weight = holding.weight_pct(total_portfolio_value)
        if weight > max_position_pct:
            sell_score += 2
            reasons.append(f"Position overweight at {weight:.1f}% (max {max_position_pct}%)")

        # === Technical Analysis ===
        if signal:
            # RSI signals
            if signal.rsi_14 is not None:
                if signal.rsi_14 < 30:
                    buy_score += 2
                    reasons.append(f"RSI oversold ({signal.rsi_14:.0f})")
                elif signal.rsi_14 > 70:
                    sell_score += 2
                    reasons.append(f"RSI overbought ({signal.rsi_14:.0f})")
                elif signal.rsi_14 < 40:
                    buy_score += 1
                    reasons.append(f"RSI approaching oversold ({signal.rsi_14:.0f})")
                elif signal.rsi_14 > 60:
                    sell_score += 1
                    reasons.append(f"RSI elevated ({signal.rsi_14:.0f})")

            # Moving average trend
            price = holding.current_price
            if price and signal.sma_20 and signal.sma_50:
                if price > signal.sma_20 > signal.sma_50:
                    buy_score += 1
                    reasons.append("Price above rising moving averages")
                elif price < signal.sma_20 < signal.sma_50:
                    sell_score += 1
                    reasons.append("Price below falling moving averages")

            # 200 SMA (long-term trend)
            if price and signal.sma_200:
                if price > signal.sma_200 * 1.05:
                    buy_score += 1
                    reasons.append("Above 200 SMA (uptrend)")
                elif price < signal.sma_200 * 0.95:
                    sell_score += 1
                    reasons.append("Below 200 SMA (downtrend)")

            # Momentum
            if signal.change_5d_pct is not None:
                if signal.change_5d_pct > 5:
                    buy_score += 1
                    reasons.append(f"Strong 5d momentum (+{signal.change_5d_pct:.1f}%)")
                elif signal.change_5d_pct < -5:
                    sell_score += 1
                    reasons.append(f"Weak 5d momentum ({signal.change_5d_pct:.1f}%)")

            # Golden/Death cross signals
            if signal.golden_cross:
                buy_score += 2
                reasons.append("Golden cross (bullish trend)")
            elif signal.death_cross:
                sell_score += 2
                reasons.append("Death cross (bearish trend)")

        # === P&L Considerations ===
        if holding.unrealized_pnl_pct is not None:
            if holding.unrealized_pnl_pct > 50:
                sell_score += 1
                reasons.append(f"Large unrealized gain (+{holding.unrealized_pnl_pct:.0f}%)")
            elif holding.unrealized_pnl_pct < -20:
                # Could be buy (averaging down) or sell (cutting losses)
                reasons.append(f"Significant unrealized loss ({holding.unrealized_pnl_pct:.0f}%)")

        # === News Sentiment ===
        if holding.news_sentiment:
            sent = holding.news_sentiment.value if hasattr(holding.news_sentiment, 'value') else str(holding.news_sentiment)
            if "POSITIVE" in sent.upper():
                buy_score += 1
                reasons.append(f"Positive news sentiment")
            elif "NEGATIVE" in sent.upper():
                sell_score += 1
                reasons.append(f"Negative news sentiment")

        # === Determine Final Recommendation ===
        if sell_score >= 4:
            recommendation = "SELL"
        elif sell_score >= 2 and sell_score > buy_score:
            recommendation = "TRIM"
        elif buy_score >= 3 and buy_score > sell_score:
            recommendation = "BUY"
        else:
            recommendation = "HOLD"

        # If no strong signals, default reason
        if not reasons:
            reasons.append("No strong signals - maintain current position")

        holding.recommendation = recommendation
        holding.recommendation_reasons = reasons[:4]  # Limit to top 4 reasons

    logger.info(f"Generated recommendations for {len(holdings)} holdings")
