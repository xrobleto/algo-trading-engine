"""Tests for scoring engine module."""

import pytest
from decimal import Decimal
from datetime import datetime

from src.engine.scoring import (
    ScoringEngine,
    ScoreResult,
    compute_scores,
)
from src.signals.risk import RiskAssessment
from src.signals.macro import MacroContext
from src.utils.typing import (
    Holding,
    PortfolioSnapshot,
    TechnicalSignal,
)


class TestScoringEngine:
    """Tests for the main ScoringEngine class."""

    @pytest.fixture
    def engine(self):
        return ScoringEngine(
            risk_score_min=65,
            opportunity_score_min=75,
        )

    @pytest.fixture
    def sample_holdings(self):
        """Create sample holdings for testing."""
        return [
            Holding(
                symbol="AAPL",
                quantity=Decimal("100"),
                avg_cost_basis=Decimal("150.00"),
                current_price=Decimal("175.00"),
                current_value=Decimal("17500.00"),
                unrealized_pnl=Decimal("2500.00"),
                unrealized_pnl_pct=Decimal("16.67"),
                sector="Technology",
                description="Apple Inc",
            ),
            Holding(
                symbol="MSFT",
                quantity=Decimal("50"),
                avg_cost_basis=Decimal("350.00"),
                current_price=Decimal("400.00"),
                current_value=Decimal("20000.00"),
                unrealized_pnl=Decimal("2500.00"),
                unrealized_pnl_pct=Decimal("14.29"),
                sector="Technology",
                description="Microsoft Corp",
            ),
            Holding(
                symbol="JNJ",
                quantity=Decimal("30"),
                avg_cost_basis=Decimal("160.00"),
                current_price=Decimal("155.00"),
                current_value=Decimal("4650.00"),
                unrealized_pnl=Decimal("-150.00"),
                unrealized_pnl_pct=Decimal("-3.13"),
                sector="Healthcare",
                description="Johnson & Johnson",
            ),
        ]

    @pytest.fixture
    def sample_snapshot(self, sample_holdings):
        """Create sample portfolio snapshot."""
        total_value = sum(h.current_value for h in sample_holdings)
        total_cost = sum(h.avg_cost_basis * h.quantity for h in sample_holdings)
        total_pnl = sum(h.unrealized_pnl for h in sample_holdings)

        # Calculate sector allocations
        sector_values = {}
        for h in sample_holdings:
            sector = h.sector or "Unknown"
            sector_values[sector] = sector_values.get(sector, Decimal("0")) + (h.current_value or Decimal("0"))

        sector_allocations = {
            sector: float(value / total_value * 100)
            for sector, value in sector_values.items()
        }

        # Calculate top holding percentages
        sorted_holdings = sorted(sample_holdings, key=lambda h: h.current_value or 0, reverse=True)
        top_holding_pct = float((sorted_holdings[0].current_value or 0) / total_value * 100)
        top_3_pct = float(sum(h.current_value or 0 for h in sorted_holdings[:3]) / total_value * 100)

        return PortfolioSnapshot(
            timestamp=datetime.now(),
            holdings=sample_holdings,
            total_value=total_value,
            total_cost_basis=total_cost,
            total_unrealized_pnl=total_pnl,
            total_unrealized_pnl_pct=(total_pnl / total_cost * 100) if total_cost else Decimal("0"),
            cash_balance=Decimal("5000.00"),
            sector_allocations=sector_allocations,
            top_holding_pct=top_holding_pct,
            top_3_holdings_pct=top_3_pct,
        )

    @pytest.fixture
    def sample_technical_signals(self):
        """Create sample technical signals."""
        return {
            "AAPL": TechnicalSignal(
                symbol="AAPL",
                timestamp=datetime.now(),
                price=175.00,
                sma_20=170.00,
                sma_50=165.00,
                sma_200=155.00,
                rsi_14=65.0,
                above_sma_20=True,
                above_sma_50=True,
                above_sma_200=True,
                golden_cross=True,
                death_cross=False,
                volatility_20d=25.0,
                change_1d_pct=1.5,
                change_5d_pct=3.2,
                signal_score=70.0,
            ),
            "MSFT": TechnicalSignal(
                symbol="MSFT",
                timestamp=datetime.now(),
                price=400.00,
                sma_20=395.00,
                sma_50=380.00,
                sma_200=350.00,
                rsi_14=72.0,
                above_sma_20=True,
                above_sma_50=True,
                above_sma_200=True,
                golden_cross=False,
                death_cross=False,
                volatility_20d=22.0,
                change_1d_pct=0.8,
                change_5d_pct=2.1,
                signal_score=65.0,
            ),
            "JNJ": TechnicalSignal(
                symbol="JNJ",
                timestamp=datetime.now(),
                price=155.00,
                sma_20=158.00,
                sma_50=160.00,
                sma_200=162.00,
                rsi_14=35.0,
                above_sma_20=False,
                above_sma_50=False,
                above_sma_200=False,
                golden_cross=False,
                death_cross=True,
                volatility_20d=18.0,
                change_1d_pct=-0.5,
                change_5d_pct=-2.8,
                signal_score=35.0,
            ),
        }

    @pytest.fixture
    def sample_risk_assessment(self):
        """Create sample risk assessment."""
        return RiskAssessment(
            overall_risk_score=55.0,
            concentration_score=45.0,
            volatility_score=50.0,
            correlation_score=40.0,
            technical_risk_score=55.0,
            drawdown_score=35.0,
            concentration_warning=False,
            sector_concentration_warning=True,
            high_volatility_warning=False,
            high_correlation_warning=False,
            drawdown_warning=False,
            risk_factors=["Technology sector concentration at 89%"],
            recommendations=["Consider diversifying away from Technology"],
            timestamp=datetime.now(),
        )

    def test_compute_scores_basic(
        self, engine, sample_snapshot, sample_technical_signals, sample_risk_assessment
    ):
        """Test basic score computation."""
        result = engine.compute(
            snapshot=sample_snapshot,
            technical_signals=sample_technical_signals,
            news_analyses={},
            risk_assessment=sample_risk_assessment,
            macro_context=None,
            tv_alerts=None,
        )

        assert isinstance(result, ScoreResult)
        assert 0 <= result.risk_alert_score <= 100
        assert 0 <= result.opportunity_score <= 100
        assert isinstance(result.should_alert, bool)
        assert result.alert_type in ("risk", "opportunity", "both", "none")

    def test_score_components(
        self, engine, sample_snapshot, sample_technical_signals, sample_risk_assessment
    ):
        """Test that all score components are computed."""
        result = engine.compute(
            snapshot=sample_snapshot,
            technical_signals=sample_technical_signals,
            news_analyses={},
            risk_assessment=sample_risk_assessment,
        )

        assert "concentration" in result.risk_components
        assert "technical_breakdown" in result.risk_components
        assert "volatility" in result.risk_components
        assert "technical" in result.opportunity_components

    def test_alert_threshold_risk(self, sample_snapshot, sample_risk_assessment):
        """Test that risk alert triggers at threshold."""
        # Create engine with low threshold
        engine = ScoringEngine(risk_score_min=50, opportunity_score_min=90)

        # Create signals that should trigger high risk
        bearish_signals = {
            "AAPL": TechnicalSignal(
                symbol="AAPL",
                timestamp=datetime.now(),
                price=175.00,
                sma_20=180.00,
                sma_50=185.00,
                sma_200=190.00,
                rsi_14=25.0,
                above_sma_20=False,
                above_sma_50=False,
                above_sma_200=False,
                golden_cross=False,
                death_cross=True,
                volatility_20d=45.0,
                change_1d_pct=-5.0,
                change_5d_pct=-15.0,
                signal_score=25.0,
            ),
        }

        result = engine.compute(
            snapshot=sample_snapshot,
            technical_signals=bearish_signals,
            news_analyses={},
            risk_assessment=sample_risk_assessment,
        )

        # With bearish signals, risk should be elevated
        assert result.risk_alert_score > 40

    def test_empty_portfolio(self, engine, sample_risk_assessment):
        """Test scoring with empty portfolio."""
        empty_snapshot = PortfolioSnapshot(
            timestamp=datetime.now(),
            holdings=[],
            total_value=Decimal("0"),
            total_cost_basis=Decimal("0"),
            total_unrealized_pnl=Decimal("0"),
            total_unrealized_pnl_pct=Decimal("0"),
            cash_balance=Decimal("10000"),
            sector_allocations={},
            top_holding_pct=0.0,
            top_3_holdings_pct=0.0,
        )

        result = engine.compute(
            snapshot=empty_snapshot,
            technical_signals={},
            news_analyses={},
            risk_assessment=sample_risk_assessment,
        )

        assert isinstance(result, ScoreResult)
        assert not result.should_alert or result.alert_type in ("risk", "opportunity", "both", "none")

    def test_ticker_scores_computed(
        self, engine, sample_snapshot, sample_technical_signals, sample_risk_assessment
    ):
        """Test that per-ticker scores are computed."""
        result = engine.compute(
            snapshot=sample_snapshot,
            technical_signals=sample_technical_signals,
            news_analyses={},
            risk_assessment=sample_risk_assessment,
        )

        assert "AAPL" in result.ticker_scores
        assert "MSFT" in result.ticker_scores
        assert "JNJ" in result.ticker_scores

    def test_top_opportunities_identified(
        self, engine, sample_snapshot, sample_technical_signals, sample_risk_assessment
    ):
        """Test that top opportunities are identified."""
        result = engine.compute(
            snapshot=sample_snapshot,
            technical_signals=sample_technical_signals,
            news_analyses={},
            risk_assessment=sample_risk_assessment,
        )

        assert isinstance(result.top_opportunities, list)
        assert isinstance(result.top_risks, list)


class TestConcentrationRisk:
    """Tests for concentration risk calculation."""

    @pytest.fixture
    def engine(self):
        return ScoringEngine(max_single_position_pct=20, max_sector_pct=40)

    def test_high_concentration(self, engine):
        """Test high concentration detection."""
        holdings = [
            Holding(
                symbol="AAPL",
                quantity=Decimal("100"),
                avg_cost_basis=Decimal("100"),
                current_value=Decimal("9000"),  # 90% of portfolio
                sector="Technology",
            ),
            Holding(
                symbol="MSFT",
                quantity=Decimal("10"),
                avg_cost_basis=Decimal("100"),
                current_value=Decimal("1000"),  # 10% of portfolio
                sector="Technology",
            ),
        ]

        snapshot = PortfolioSnapshot(
            timestamp=datetime.now(),
            holdings=holdings,
            total_value=Decimal("10000"),
            total_cost_basis=Decimal("9000"),
            total_unrealized_pnl=Decimal("1000"),
            total_unrealized_pnl_pct=Decimal("11.11"),
            cash_balance=Decimal("0"),
            sector_allocations={"Technology": 100.0},
            top_holding_pct=90.0,
            top_3_holdings_pct=100.0,
        )

        score = engine._score_concentration_risk(snapshot)
        assert score > 70  # High concentration penalty

    def test_diversified_portfolio(self, engine):
        """Test well-diversified portfolio."""
        holdings = [
            Holding(
                symbol=f"STOCK{i}",
                quantity=Decimal("10"),
                avg_cost_basis=Decimal("100"),
                current_value=Decimal("1000"),  # 10% each
                sector=f"Sector{i % 5}",
            )
            for i in range(10)
        ]

        sector_allocs = {f"Sector{i}": 20.0 for i in range(5)}

        snapshot = PortfolioSnapshot(
            timestamp=datetime.now(),
            holdings=holdings,
            total_value=Decimal("10000"),
            total_cost_basis=Decimal("10000"),
            total_unrealized_pnl=Decimal("0"),
            total_unrealized_pnl_pct=Decimal("0"),
            cash_balance=Decimal("0"),
            sector_allocations=sector_allocs,
            top_holding_pct=10.0,
            top_3_holdings_pct=30.0,
        )

        score = engine._score_concentration_risk(snapshot)
        assert score < 50  # Low concentration risk


class TestTechnicalBreakdownScore:
    """Tests for technical breakdown scoring."""

    @pytest.fixture
    def engine(self):
        return ScoringEngine()

    @pytest.fixture
    def simple_snapshot(self):
        """Create a simple snapshot with one holding."""
        holdings = [
            Holding(
                symbol="TEST",
                quantity=Decimal("100"),
                avg_cost_basis=Decimal("100"),
                current_value=Decimal("10000"),
                sector="Technology",
            )
        ]
        return PortfolioSnapshot(
            timestamp=datetime.now(),
            holdings=holdings,
            total_value=Decimal("10000"),
            total_cost_basis=Decimal("10000"),
            total_unrealized_pnl=Decimal("0"),
            total_unrealized_pnl_pct=Decimal("0"),
            cash_balance=Decimal("0"),
            sector_allocations={"Technology": 100.0},
            top_holding_pct=100.0,
            top_3_holdings_pct=100.0,
        )

    def test_bullish_signals(self, engine, simple_snapshot):
        """Test bullish technical signals produce low risk."""
        signal = TechnicalSignal(
            symbol="TEST",
            timestamp=datetime.now(),
            price=100.0,
            sma_20=95.0,
            sma_50=90.0,
            sma_200=85.0,
            rsi_14=60.0,
            above_sma_20=True,
            above_sma_50=True,
            above_sma_200=True,
            golden_cross=True,
            death_cross=False,
            volatility_20d=20.0,
            change_1d_pct=2.0,
            change_5d_pct=5.0,
            signal_score=75.0,
        )

        score = engine._score_technical_breakdown_risk({"TEST": signal}, simple_snapshot)
        assert score < 50  # Bullish = low risk

    def test_bearish_signals(self, engine, simple_snapshot):
        """Test bearish technical signals produce high risk."""
        signal = TechnicalSignal(
            symbol="TEST",
            timestamp=datetime.now(),
            price=100.0,
            sma_20=105.0,
            sma_50=110.0,
            sma_200=115.0,
            rsi_14=25.0,
            above_sma_20=False,
            above_sma_50=False,
            above_sma_200=False,
            golden_cross=False,
            death_cross=True,
            volatility_20d=40.0,
            change_1d_pct=-3.0,
            change_5d_pct=-10.0,
            signal_score=25.0,
        )

        score = engine._score_technical_breakdown_risk({"TEST": signal}, simple_snapshot)
        assert score > 50  # Bearish = high risk


class TestOpportunityScoring:
    """Tests for opportunity score calculation."""

    @pytest.fixture
    def engine(self):
        return ScoringEngine()

    @pytest.fixture
    def simple_snapshot(self):
        holdings = [
            Holding(
                symbol="TEST",
                quantity=Decimal("100"),
                avg_cost_basis=Decimal("100"),
                current_value=Decimal("10000"),
                sector="Technology",
            )
        ]
        return PortfolioSnapshot(
            timestamp=datetime.now(),
            holdings=holdings,
            total_value=Decimal("10000"),
            total_cost_basis=Decimal("10000"),
            total_unrealized_pnl=Decimal("0"),
            total_unrealized_pnl_pct=Decimal("0"),
            cash_balance=Decimal("0"),
            sector_allocations={"Technology": 100.0},
            top_holding_pct=100.0,
            top_3_holdings_pct=100.0,
        )

    def test_high_opportunity(self, engine, simple_snapshot):
        """Test high opportunity detection."""
        signal = TechnicalSignal(
            symbol="TEST",
            timestamp=datetime.now(),
            price=100.0,
            sma_20=95.0,
            sma_50=92.0,
            sma_200=88.0,
            rsi_14=35.0,  # Oversold but recovering
            above_sma_20=True,
            above_sma_50=True,
            above_sma_200=True,
            golden_cross=True,
            death_cross=False,
            volatility_20d=25.0,
            change_1d_pct=3.5,
            change_5d_pct=8.0,
            signal_score=80.0,
        )

        score = engine._score_technical_opportunity({"TEST": signal}, simple_snapshot)
        assert score > 50

    def test_low_opportunity(self, engine, simple_snapshot):
        """Test low opportunity detection."""
        signal = TechnicalSignal(
            symbol="TEST",
            timestamp=datetime.now(),
            price=100.0,
            sma_20=102.0,
            sma_50=105.0,
            sma_200=108.0,
            rsi_14=75.0,  # Overbought
            above_sma_20=False,
            above_sma_50=False,
            above_sma_200=False,
            golden_cross=False,
            death_cross=True,
            volatility_20d=15.0,
            change_1d_pct=-1.0,
            change_5d_pct=-3.0,
            signal_score=30.0,
        )

        score = engine._score_technical_opportunity({"TEST": signal}, simple_snapshot)
        assert score < 60


class TestConvenienceFunction:
    """Tests for the compute_scores convenience function."""

    @pytest.fixture
    def sample_snapshot(self):
        holdings = [
            Holding(
                symbol="AAPL",
                quantity=Decimal("100"),
                avg_cost_basis=Decimal("150.00"),
                current_price=Decimal("175.00"),
                current_value=Decimal("17500.00"),
                unrealized_pnl=Decimal("2500.00"),
                sector="Technology",
            ),
        ]
        return PortfolioSnapshot(
            timestamp=datetime.now(),
            holdings=holdings,
            total_value=Decimal("17500"),
            total_cost_basis=Decimal("15000"),
            total_unrealized_pnl=Decimal("2500"),
            total_unrealized_pnl_pct=Decimal("16.67"),
            cash_balance=Decimal("5000"),
            sector_allocations={"Technology": 100.0},
            top_holding_pct=100.0,
            top_3_holdings_pct=100.0,
        )

    @pytest.fixture
    def sample_risk_assessment(self):
        return RiskAssessment(
            overall_risk_score=50.0,
            concentration_score=45.0,
            volatility_score=50.0,
            correlation_score=40.0,
            technical_risk_score=50.0,
            drawdown_score=35.0,
            concentration_warning=False,
            sector_concentration_warning=False,
            high_volatility_warning=False,
            high_correlation_warning=False,
            drawdown_warning=False,
            risk_factors=[],
            recommendations=[],
            timestamp=datetime.now(),
        )

    def test_convenience_function(self, sample_snapshot, sample_risk_assessment):
        """Test the compute_scores convenience function."""
        result = compute_scores(
            snapshot=sample_snapshot,
            technical_signals={},
            news_analyses={},
            risk_assessment=sample_risk_assessment,
        )

        assert isinstance(result, ScoreResult)
        assert result.timestamp is not None
