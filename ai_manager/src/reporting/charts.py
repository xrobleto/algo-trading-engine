"""
Chart Generation Module

Creates charts for email reports using matplotlib.
Charts are embedded as base64 data URLs.
"""

import base64
import io
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

from ..utils.logging import get_logger
from ..utils.typing import PortfolioSnapshot, Holding, TechnicalSignal

logger = get_logger(__name__)

# Try to import matplotlib
try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.figure import Figure
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    logger.warning("matplotlib not installed - charts disabled")

# Color palette
COLORS = {
    "primary": "#2563eb",      # Blue
    "secondary": "#7c3aed",    # Purple
    "success": "#10b981",      # Green
    "danger": "#ef4444",       # Red
    "warning": "#f59e0b",      # Yellow
    "neutral": "#6b7280",      # Gray
    "background": "#ffffff",
    "text": "#1f2937",
}

SECTOR_COLORS = [
    "#2563eb", "#7c3aed", "#10b981", "#f59e0b", "#ef4444",
    "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#8b5cf6",
]


class ChartGenerator:
    """Generates charts for email reports."""

    def __init__(self, style: str = "default", dpi: int = 100):
        """
        Initialize chart generator.

        Args:
            style: Matplotlib style
            dpi: Resolution for output images
        """
        self.dpi = dpi
        self.style = style

        if MATPLOTLIB_AVAILABLE:
            plt.style.use('seaborn-v0_8-whitegrid')

    def generate_allocation_pie(
        self,
        snapshot: PortfolioSnapshot,
        max_slices: int = 8
    ) -> Optional[str]:
        """
        Generate portfolio allocation pie chart.

        Args:
            snapshot: Portfolio snapshot
            max_slices: Max slices before grouping as "Other"

        Returns:
            Base64-encoded PNG string or None
        """
        if not MATPLOTLIB_AVAILABLE or not snapshot.holdings:
            return None

        try:
            # Prepare data
            holdings_sorted = sorted(
                snapshot.holdings,
                key=lambda h: h.current_value or 0,
                reverse=True
            )

            labels = []
            sizes = []
            colors = []

            total = snapshot.total_value
            other_value = 0

            for i, h in enumerate(holdings_sorted):
                if h.current_value and total > 0:
                    pct = (h.current_value / total) * 100

                    if i < max_slices - 1:
                        labels.append(f"{h.symbol}\n{pct:.1f}%")
                        sizes.append(h.current_value)
                        colors.append(SECTOR_COLORS[i % len(SECTOR_COLORS)])
                    else:
                        other_value += h.current_value

            if other_value > 0:
                other_pct = (other_value / total) * 100
                labels.append(f"Other\n{other_pct:.1f}%")
                sizes.append(other_value)
                colors.append(COLORS["neutral"])

            # Create chart
            fig, ax = plt.subplots(figsize=(6, 6), dpi=self.dpi)

            wedges, texts = ax.pie(
                sizes,
                labels=labels,
                colors=colors,
                startangle=90,
                textprops={'fontsize': 9}
            )

            ax.set_title("Portfolio Allocation", fontsize=12, fontweight='bold')

            # Convert to base64
            return self._fig_to_base64(fig)

        except Exception as e:
            logger.error(f"Failed to generate allocation pie: {e}")
            return None

    def generate_sector_allocation(
        self,
        snapshot: PortfolioSnapshot
    ) -> Optional[str]:
        """
        Generate sector allocation bar chart.

        Args:
            snapshot: Portfolio snapshot

        Returns:
            Base64-encoded PNG string or None
        """
        if not MATPLOTLIB_AVAILABLE or not snapshot.sector_allocations:
            return None

        try:
            # Sort sectors by allocation
            sorted_sectors = sorted(
                snapshot.sector_allocations.items(),
                key=lambda x: x[1],
                reverse=True
            )

            sectors = [s[0] for s in sorted_sectors]
            allocations = [s[1] for s in sorted_sectors]
            colors = [SECTOR_COLORS[i % len(SECTOR_COLORS)] for i in range(len(sectors))]

            # Create chart
            fig, ax = plt.subplots(figsize=(8, 4), dpi=self.dpi)

            bars = ax.barh(sectors, allocations, color=colors)

            # Add value labels
            for bar, alloc in zip(bars, allocations):
                ax.text(
                    bar.get_width() + 0.5,
                    bar.get_y() + bar.get_height() / 2,
                    f'{alloc:.1f}%',
                    va='center',
                    fontsize=9
                )

            ax.set_xlabel('Allocation %')
            ax.set_title('Sector Allocation', fontsize=12, fontweight='bold')
            ax.set_xlim(0, max(allocations) * 1.15)

            plt.tight_layout()

            return self._fig_to_base64(fig)

        except Exception as e:
            logger.error(f"Failed to generate sector chart: {e}")
            return None

    def generate_price_chart(
        self,
        symbol: str,
        bars: List[Dict[str, Any]],
        signal: Optional[TechnicalSignal] = None,
        include_mas: bool = True
    ) -> Optional[str]:
        """
        Generate price chart with optional moving averages.

        Args:
            symbol: Stock symbol
            bars: Historical price bars
            signal: Technical signal with levels
            include_mas: Include moving averages

        Returns:
            Base64-encoded PNG string or None
        """
        if not MATPLOTLIB_AVAILABLE or not bars:
            return None

        try:
            # Extract data
            dates = [b.get("timestamp") for b in bars if b.get("timestamp")]
            closes = [b.get("close") for b in bars if b.get("close")]

            if len(closes) < 10:
                return None

            # Create chart
            fig, ax = plt.subplots(figsize=(8, 4), dpi=self.dpi)

            # Plot price
            ax.plot(dates[-60:], closes[-60:], color=COLORS["primary"], linewidth=1.5, label="Price")

            # Plot MAs if signal available
            if signal and include_mas:
                if signal.sma_20:
                    ax.axhline(y=signal.sma_20, color=COLORS["warning"], linestyle='--',
                              linewidth=1, alpha=0.7, label=f"SMA 20: {signal.sma_20:.2f}")
                if signal.sma_50:
                    ax.axhline(y=signal.sma_50, color=COLORS["secondary"], linestyle='--',
                              linewidth=1, alpha=0.7, label=f"SMA 50: {signal.sma_50:.2f}")

            # Plot support/resistance if available
            if signal:
                if signal.support_level:
                    ax.axhline(y=signal.support_level, color=COLORS["success"], linestyle=':',
                              linewidth=1.5, label=f"Support: {signal.support_level:.2f}")
                if signal.resistance_level:
                    ax.axhline(y=signal.resistance_level, color=COLORS["danger"], linestyle=':',
                              linewidth=1.5, label=f"Resistance: {signal.resistance_level:.2f}")

            ax.set_title(f"{symbol} Price Chart", fontsize=12, fontweight='bold')
            ax.set_ylabel("Price ($)")
            ax.legend(loc='upper left', fontsize=8)

            # Format x-axis
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            plt.xticks(rotation=45)

            plt.tight_layout()

            return self._fig_to_base64(fig)

        except Exception as e:
            logger.error(f"Failed to generate price chart for {symbol}: {e}")
            return None

    def generate_pnl_waterfall(
        self,
        holdings: List[Holding],
        max_items: int = 10
    ) -> Optional[str]:
        """
        Generate P&L waterfall chart.

        Args:
            holdings: List of holdings
            max_items: Max items to show

        Returns:
            Base64-encoded PNG string or None
        """
        if not MATPLOTLIB_AVAILABLE or not holdings:
            return None

        try:
            # Filter holdings with P&L
            holdings_with_pnl = [h for h in holdings if h.unrealized_pnl is not None]
            holdings_sorted = sorted(
                holdings_with_pnl,
                key=lambda h: abs(h.unrealized_pnl or 0),
                reverse=True
            )[:max_items]

            if not holdings_sorted:
                return None

            symbols = [h.symbol for h in holdings_sorted]
            pnls = [h.unrealized_pnl or 0 for h in holdings_sorted]
            colors = [COLORS["success"] if p >= 0 else COLORS["danger"] for p in pnls]

            # Create chart
            fig, ax = plt.subplots(figsize=(8, 5), dpi=self.dpi)

            bars = ax.bar(symbols, pnls, color=colors)

            # Add value labels
            for bar, pnl in zip(bars, pnls):
                height = bar.get_height()
                label = f"${pnl:+,.0f}"
                y_pos = height + (50 if height >= 0 else -50)
                va = 'bottom' if height >= 0 else 'top'
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    y_pos,
                    label,
                    ha='center',
                    va=va,
                    fontsize=8
                )

            ax.axhline(y=0, color=COLORS["neutral"], linewidth=0.5)
            ax.set_ylabel("Unrealized P&L ($)")
            ax.set_title("Position P&L", fontsize=12, fontweight='bold')

            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()

            return self._fig_to_base64(fig)

        except Exception as e:
            logger.error(f"Failed to generate P&L chart: {e}")
            return None

    def generate_score_gauge(
        self,
        risk_score: float,
        opportunity_score: float
    ) -> Optional[str]:
        """
        Generate score gauge visualization.

        Args:
            risk_score: Risk alert score (0-100)
            opportunity_score: Opportunity score (0-100)

        Returns:
            Base64-encoded PNG string or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        try:
            fig, axes = plt.subplots(1, 2, figsize=(8, 3), dpi=self.dpi)

            # Risk score gauge
            self._draw_gauge(
                axes[0],
                risk_score,
                "Risk Alert Score",
                color_low=COLORS["success"],
                color_high=COLORS["danger"]
            )

            # Opportunity score gauge
            self._draw_gauge(
                axes[1],
                opportunity_score,
                "Opportunity Score",
                color_low=COLORS["neutral"],
                color_high=COLORS["success"]
            )

            plt.tight_layout()

            return self._fig_to_base64(fig)

        except Exception as e:
            logger.error(f"Failed to generate score gauge: {e}")
            return None

    def _draw_gauge(
        self,
        ax,
        value: float,
        title: str,
        color_low: str,
        color_high: str
    ):
        """Draw a simple gauge on an axis."""
        # Create a semi-circle background
        theta = 180 - (value / 100 * 180)

        # Background arc
        ax.barh(0, 100, color='#e5e7eb', height=0.3)
        # Value arc
        ax.barh(0, value, color=color_high if value > 50 else color_low, height=0.3)

        ax.set_xlim(0, 100)
        ax.set_ylim(-0.5, 0.5)
        ax.set_title(f"{title}: {value:.0f}", fontsize=10, fontweight='bold')
        ax.set_yticks([])

        # Add threshold markers
        ax.axvline(x=70, color=COLORS["warning"], linestyle='--', alpha=0.5)

    def _fig_to_base64(self, fig: Figure) -> str:
        """Convert matplotlib figure to base64 string."""
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', facecolor='white')
        plt.close(fig)
        buf.seek(0)

        b64 = base64.b64encode(buf.read()).decode('utf-8')
        return f"data:image/png;base64,{b64}"


def generate_portfolio_charts(
    snapshot: PortfolioSnapshot,
    technical_signals: Optional[Dict[str, TechnicalSignal]] = None,
    risk_score: float = 50,
    opportunity_score: float = 50,
) -> List[Dict[str, str]]:
    """
    Generate all portfolio charts.

    Args:
        snapshot: Portfolio snapshot
        technical_signals: Technical signals for holdings
        risk_score: Risk alert score
        opportunity_score: Opportunity score

    Returns:
        List of chart dicts with 'title' and 'data_url' keys for template rendering
    """
    generator = ChartGenerator()

    # Generate charts with titles
    chart_configs = [
        ("Portfolio Allocation", generator.generate_allocation_pie(snapshot)),
        ("Sector Allocation", generator.generate_sector_allocation(snapshot)),
        ("Position P&L", generator.generate_pnl_waterfall(snapshot.holdings)),
        ("Risk & Opportunity Scores", generator.generate_score_gauge(risk_score, opportunity_score)),
    ]

    # Build list of chart objects for template (only include successful charts)
    charts = []
    for title, data_url in chart_configs:
        if data_url:
            charts.append({
                "title": title,
                "data_url": data_url
            })

    # Generate price charts for top 3 holdings (if bar data available)
    for i, holding in enumerate(snapshot.holdings[:3]):
        signal = technical_signals.get(holding.symbol) if technical_signals else None
        # Note: Would need bar data here - simplified for this implementation
        # price_chart = generator.generate_price_chart(holding.symbol, bars, signal)
        # if price_chart:
        #     charts.append({"title": f"{holding.symbol} Price Chart", "data_url": price_chart})

    logger.info(f"Generated {len(charts)} charts")

    return charts
