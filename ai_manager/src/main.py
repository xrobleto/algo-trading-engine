"""
AI Investment Manager - Main CLI Entrypoint

Professional portfolio monitoring and recommendation system.
"""

import argparse
import os
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, Any, Optional

import yaml
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logging import setup_logging, get_logger
from src.utils.time import now_et, is_market_hours, get_market_session_str, format_timestamp
from src.utils.typing import PortfolioSnapshot, Holding

from src.ingestion.robinhood_csv import RobinhoodCSVParser, build_portfolio_snapshot
from src.providers.massive_client import MassiveClient
from src.providers.alpaca_client import AlpacaClient
from src.providers.fred_client import FREDClient
from src.providers.tradingview_alerts import TradingViewAlertStore

from src.signals.technicals import compute_technical_signals, generate_holding_recommendations
from src.signals.news import aggregate_news_sentiment
from src.signals.macro import get_macro_context
from src.signals.risk import compute_portfolio_risk

from src.engine.scoring import compute_scores
from src.engine.recommendations import generate_recommendations
from src.engine.evidence_hash import compute_evidence_hash, compute_structure_hash

from src.llm.claude_client import ClaudeClient, generate_fallback_content
from src.reporting.email_renderer import render_email, send_email
from src.reporting.charts import generate_portfolio_charts

from src.storage.state_store import StateStore
from src.scheduling.runner import ScheduledRunner


logger = get_logger(__name__)


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        logger.warning(f"Config file not found: {config_path}, using defaults")
        return {}

    with open(path, 'r') as f:
        return yaml.safe_load(f) or {}


class InvestmentManager:
    """
    Main investment manager orchestrator.

    Coordinates all components for portfolio analysis.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize investment manager.

        Args:
            config: Configuration dictionary
        """
        self.config = config

        # Initialize providers
        self.massive = MassiveClient(
            rate_limit_per_min=config.get("providers", {}).get("massive_rate_limit_per_min", 5)
        ) if config.get("providers", {}).get("massive_enabled", True) else None

        self.alpaca = AlpacaClient(
            paper=config.get("providers", {}).get("alpaca_paper", True)
        ) if config.get("providers", {}).get("alpaca_enabled", True) else None

        self.fred = FREDClient() if config.get("providers", {}).get("fred_enabled", True) else None

        # Initialize TV alert store
        self.tv_store = TradingViewAlertStore() if config.get("providers", {}).get("tradingview_alerts_enabled", False) else None

        # Initialize state store
        state_db_path = config.get("paths", {}).get("state_db_path", "data/state.db")
        self.state_store = StateStore(state_db_path)

        # Initialize LLM client
        self.llm_client = ClaudeClient(
            model=config.get("llm", {}).get("model", "claude-sonnet-4-20250514"),
            temperature=config.get("llm", {}).get("temperature", 0.2),
            max_tokens=config.get("llm", {}).get("max_tokens", 2000),
        )

        # Initialize CSV parser
        recon_config = config.get("reconstruction", {})

        # Parse starting balances from config: {symbol: [shares, avg_cost]} -> {symbol: (shares, avg_cost)}
        raw_starting = recon_config.get("starting_balances", {})
        starting_balances = {}
        for symbol, val in raw_starting.items():
            if isinstance(val, (list, tuple)) and len(val) >= 2:
                starting_balances[symbol] = (float(val[0]), float(val[1]))

        self.csv_parser = RobinhoodCSVParser(
            buy_codes=recon_config.get("buy_codes"),
            sell_codes=recon_config.get("sell_codes"),
            dividend_codes=recon_config.get("dividend_codes"),
            fee_codes=recon_config.get("fee_codes"),
            cost_basis_method=recon_config.get("cost_basis_method", "average"),
            starting_balances=starting_balances if starting_balances else None
        )

        # Sector map
        self.sector_map = config.get("sector_overrides", {})

        # Thresholds
        self.thresholds = config.get("thresholds", {})

        logger.info("Investment Manager initialized")

    def run_analysis(self, dry_run: bool = False, suppress_email: bool = False, force_email: bool = False) -> Dict[str, Any]:
        """
        Run complete portfolio analysis.

        Args:
            dry_run: If True, render email to file instead of sending
            suppress_email: If True, skip email entirely (for --explain mode)

        Returns:
            Dict with analysis results
        """
        start_time = now_et()
        logger.info(f"Starting analysis at {start_time} ({get_market_session_str()})")

        # === STEP 1: Load portfolio from CSV ===
        csv_path = self.config.get("paths", {}).get("portfolio_csv_path")
        if not csv_path or not Path(csv_path).exists():
            logger.error(f"Portfolio CSV not found: {csv_path}")
            return {"error": "CSV not found"}

        parse_result = self.csv_parser.parse_file(csv_path)
        holdings = parse_result.holdings

        if not holdings:
            logger.warning("No holdings found in CSV")
            return {"error": "No holdings"}

        logger.info(f"Loaded {len(holdings)} holdings from CSV")

        # === STEP 2: Enrich with current prices ===
        symbols = list(holdings.keys())
        prices = self._get_current_prices(symbols)

        # Build portfolio snapshot
        snapshot_data = build_portfolio_snapshot(holdings, prices, self.sector_map)
        snapshot = PortfolioSnapshot(
            timestamp=now_et(),
            holdings=snapshot_data["holdings"],
            total_value=snapshot_data["total_value"],
            cash=parse_result.cash_balance,
            top_holding_pct=snapshot_data["top_1_holding_pct"],
            top_3_holdings_pct=snapshot_data["top_3_holdings_pct"],
            top_5_holdings_pct=snapshot_data["top_5_holdings_pct"],
            sector_allocations=snapshot_data["sector_allocations"],
            total_cost_basis=snapshot_data["total_cost"],
            total_unrealized_pnl=snapshot_data["total_unrealized_pnl"],
            total_unrealized_pnl_pct=(
                (snapshot_data["total_unrealized_pnl"] / snapshot_data["total_cost"] * 100)
                if snapshot_data["total_cost"] > 0 else 0
            ),
            total_dividends=float(parse_result.total_dividends),
        )

        logger.info(f"Portfolio value: ${snapshot.total_value:,.2f}")

        # === STEP 3: Compute signals ===
        # Technical signals
        tech_signals = compute_technical_signals(
            snapshot.holdings,
            massive_client=self.massive,
            alpaca_client=self.alpaca
        )

        # News analysis
        news_analyses = aggregate_news_sentiment(
            snapshot.holdings,
            massive_client=self.massive,
            alpaca_client=self.alpaca,
            max_articles_per_ticker=self.config.get("news", {}).get("max_articles_per_ticker", 10),
            max_news_age_hours=self.config.get("news", {}).get("max_news_age_hours", 72)
        )

        # Macro context
        macro_context = get_macro_context(
            fred_client=self.fred,
            indicator_configs=self.config.get("macro", {}).get("indicators")
        )

        # Risk assessment
        risk_assessment = compute_portfolio_risk(
            snapshot,
            technical_signals=tech_signals,
            max_single_position_pct=self.config.get("portfolio_risk_policy", {}).get("max_single_position_pct", 20),
            max_sector_pct=self.config.get("portfolio_risk_policy", {}).get("max_sector_pct", 40),
        )

        # Generate Buy/Hold/Sell recommendations for each holding
        generate_holding_recommendations(
            holdings=snapshot.holdings,
            technical_signals=tech_signals,
            total_portfolio_value=snapshot.total_value,
            max_position_pct=self.config.get("portfolio_risk_policy", {}).get("max_single_position_pct", 20),
        )

        # === STEP 4: Compute scores ===
        # Get TradingView alerts if enabled
        tv_alerts = []
        if self.tv_store:
            tv_alerts = self.tv_store.get_recent_alerts(hours=24)

        score_result = compute_scores(
            snapshot=snapshot,
            technical_signals=tech_signals,
            news_analyses=news_analyses,
            risk_assessment=risk_assessment,
            macro_context=macro_context,
            tv_alerts=tv_alerts,
            weights=self.config.get("signals_weights"),
            risk_score_min=self.thresholds.get("risk_score_min", 70),
            opportunity_score_min=self.thresholds.get("action_score_min", 75),
        )

        logger.info(f"Scores - Risk: {score_result.risk_alert_score:.1f}, "
                   f"Opportunity: {score_result.opportunity_score:.1f}")

        # === STEP 5: Generate recommendations ===
        rec_result = generate_recommendations(
            snapshot=snapshot,
            score_result=score_result,
            technical_signals=tech_signals,
            news_analyses=news_analyses,
            risk_assessment=risk_assessment,
            macro_context=macro_context,
        )

        logger.info(f"Generated {len(rec_result.recommendations)} recommendations")

        # === STEP 6: Record scores ===
        # FIX: Hash portfolio STRUCTURE (shares) not VALUES (which change with prices)
        # This prevents spam when prices fluctuate but portfolio structure is unchanged
        portfolio_hash = compute_structure_hash(snapshot.holdings)
        self.state_store.record_scores(
            risk_score=score_result.risk_alert_score,
            opportunity_score=score_result.opportunity_score,
            portfolio_value=snapshot.total_value,
            portfolio_hash=portfolio_hash,
        )

        # === STEP 7: Check deduplication ===
        dedupe_result = self.state_store.check_should_send(
            risk_score=score_result.risk_alert_score,
            opportunity_score=score_result.opportunity_score,
            portfolio_hash=portfolio_hash,
            max_emails_per_day=self.thresholds.get("max_emails_per_day", 3),
            min_hours_between=self.thresholds.get("min_hours_between_emails", 4),
            material_score_delta=self.thresholds.get("material_score_delta", 10),
        )

        should_email = score_result.should_alert and dedupe_result.should_send

        # Force email bypasses both deduplication AND score thresholds
        if force_email:
            should_email = True
            logger.info(f"Force email enabled - bypassing deduplication and score thresholds")

        logger.info(f"Should alert: {score_result.should_alert}, "
                   f"Dedupe allows: {dedupe_result.should_send} ({dedupe_result.reason})")

        # === STEP 8: Generate email if needed ===
        email_sent = False
        # suppress_email=True skips email entirely (for --explain mode)
        if suppress_email:
            should_email = False
        if should_email or dry_run:
            email_sent = self._send_email(
                snapshot=snapshot,
                score_result=score_result,
                rec_result=rec_result,
                portfolio_hash=portfolio_hash,
                dry_run=dry_run,
            )

        # === COMPLETE ===
        duration = (now_et() - start_time).total_seconds()
        logger.info(f"Analysis complete in {duration:.1f}s. Email sent: {email_sent}")

        return {
            "success": True,
            "duration_seconds": duration,
            "portfolio_value": snapshot.total_value,
            "risk_score": score_result.risk_alert_score,
            "opportunity_score": score_result.opportunity_score,
            "num_recommendations": len(rec_result.recommendations),
            "should_alert": score_result.should_alert,
            "email_sent": email_sent,
            "dedupe_reason": dedupe_result.reason,
        }

    def _get_current_prices(self, symbols: list) -> Dict[str, float]:
        """Get current prices for symbols from available providers."""
        prices = {}

        # Try Polygon first
        if self.massive and self.massive.is_available:
            snapshots = self.massive.get_multiple_snapshots(symbols)
            for symbol, data in snapshots.items():
                if data and data.get("price"):
                    prices[symbol] = data["price"]

        # Fall back to Alpaca for missing
        missing = [s for s in symbols if s not in prices]
        if missing and self.alpaca and self.alpaca.is_available:
            snapshots = self.alpaca.get_multiple_snapshots(missing)
            for symbol, data in snapshots.items():
                if data and data.get("price"):
                    prices[symbol] = data["price"]

        logger.info(f"Got prices for {len(prices)}/{len(symbols)} symbols")
        return prices

    def _get_provider_health(self) -> Dict[str, Dict[str, Any]]:
        """Get health status of all data providers."""
        health = {}

        # Polygon/Massive
        if self.massive:
            health["polygon"] = {
                "available": self.massive.is_available,
                "api_key_set": bool(os.environ.get("POLYGON_API_KEY")),
            }
        else:
            health["polygon"] = {"available": False, "reason": "disabled in config"}

        # Alpaca
        if self.alpaca:
            health["alpaca"] = {
                "available": self.alpaca.is_available,
                "api_key_set": bool(os.environ.get("ALPACA_API_KEY")),
            }
        else:
            health["alpaca"] = {"available": False, "reason": "disabled in config"}

        # FRED
        if self.fred:
            health["fred"] = {
                "available": self.fred.is_available if hasattr(self.fred, 'is_available') else True,
                "api_key_set": bool(os.environ.get("FRED_API_KEY")),
            }
        else:
            health["fred"] = {"available": False, "reason": "disabled in config"}

        # LLM
        health["llm"] = {
            "available": self.llm_client.is_available,
            "api_key_set": bool(os.environ.get("ANTHROPIC_API_KEY")),
            "model": self.config.get("llm", {}).get("model", "claude-sonnet-4-20250514"),
        }

        return health

    def _send_email(
        self,
        snapshot: PortfolioSnapshot,
        score_result,
        rec_result,
        portfolio_hash: str,
        dry_run: bool = False,
    ) -> bool:
        """Generate and send email."""
        try:
            # Check if we should use LLM
            use_llm = self.llm_client.is_available
            score_proximity = self.config.get("llm", {}).get("score_proximity_for_llm", 15)

            # Only use LLM if scores are near threshold (cost control)
            risk_near = abs(score_result.risk_alert_score - self.thresholds.get("risk_score_min", 70)) <= score_proximity
            opp_near = abs(score_result.opportunity_score - self.thresholds.get("action_score_min", 75)) <= score_proximity

            llm_response = None
            if use_llm and (risk_near or opp_near or score_result.should_alert):
                llm_response = self.llm_client.generate_response(
                    rec_result.evidence_packet,
                    rec_result.recommendations
                )

            # FIX: Generate fallback content and USE IT when LLM fails
            fallback_content = None
            if not llm_response:
                logger.info("Using fallback content (LLM unavailable or failed)")
                fallback_content = generate_fallback_content(
                    rec_result.evidence_packet,
                    rec_result.recommendations
                )

            # FIX: Generate charts BEFORE render_email
            charts = generate_portfolio_charts(
                snapshot,
                risk_score=score_result.risk_alert_score,
                opportunity_score=score_result.opportunity_score,
            )

            # FIX: Pass charts AND fallback_content to render_email
            html_content = render_email(
                snapshot=snapshot,
                recommendations=rec_result.recommendations,
                evidence_packet=rec_result.evidence_packet,
                llm_response=llm_response,
                fallback_content=fallback_content,
                charts=charts,
            )

            # Determine subject
            if score_result.alert_type == "risk":
                subject = f"Risk Alert - Portfolio Risk Score: {score_result.risk_alert_score:.0f}"
            elif score_result.alert_type == "opportunity":
                subject = f"Opportunity Alert - Score: {score_result.opportunity_score:.0f}"
            else:
                subject = f"Portfolio Alert - Risk: {score_result.risk_alert_score:.0f}, Opp: {score_result.opportunity_score:.0f}"

            # DRY RUN: Save HTML to file instead of sending
            if dry_run:
                output_dir = Path(self.config.get("paths", {}).get("output_dir", "output"))
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / f"email_preview_{now_et().strftime('%Y%m%d_%H%M%S')}.html"
                output_file.write_text(html_content)
                logger.info(f"Dry run: Email saved to {output_file}")
                return True

            # Get email config
            email_config = self.config.get("email", {})
            env_var_name = email_config.get("smtp_password_env_var", "SMTP_PASSWORD")
            smtp_password = os.environ.get(env_var_name)

            if not smtp_password:
                logger.error(f"SMTP password not configured (env var: {env_var_name})")
                return False

            logger.debug(f"SMTP password loaded from {env_var_name}: {'*' * len(smtp_password)}")

            # Send email
            success = send_email(
                html_content=html_content,
                subject=subject,
                to_emails=email_config.get("to_emails", []),
                from_email=email_config.get("from_email", ""),
                smtp_host=email_config.get("smtp_host", "smtp.gmail.com"),
                smtp_port=email_config.get("smtp_port", 587),
                smtp_user=email_config.get("smtp_user", ""),
                smtp_password=smtp_password,
                cc_emails=email_config.get("cc_emails"),
            )

            if success:
                # Record email sent
                self.state_store.record_email_sent(
                    alert_type=score_result.alert_type,
                    risk_score=score_result.risk_alert_score,
                    opportunity_score=score_result.opportunity_score,
                    portfolio_hash=portfolio_hash,
                    recipients=email_config.get("to_emails", []),
                    subject=subject,
                )

                # FIX: Use evidence_hash (not portfolio_hash) for ticker actions
                # This changes when new catalysts/news arrive, enabling "same portfolio
                # but new catalyst" detection
                evidence_hash = compute_evidence_hash(
                    alert_type=score_result.alert_type,
                    risk_score=score_result.risk_alert_score,
                    opportunity_score=score_result.opportunity_score,
                    recommendations=rec_result.recommendations,
                    top_news=rec_result.evidence_packet.top_news,
                    concentration_flags=rec_result.evidence_packet.concentration_flags,
                )

                # Record per-ticker actions
                for rec in rec_result.recommendations:
                    self.state_store.record_ticker_action(
                        ticker=rec.ticker,
                        action=rec.action.value,
                        urgency=rec.urgency.value,
                        confidence=rec.confidence,
                        evidence_hash=evidence_hash,
                    )

            return success

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def explain(self) -> str:
        """
        Generate explanation of current state without sending email.

        Returns:
            Human-readable explanation string
        """
        # Get provider health first (before analysis might fail)
        health = self._get_provider_health()

        # FIX: suppress_email=True prevents accidental email sends from --explain
        result = self.run_analysis(suppress_email=True)

        lines = [
            "=" * 60,
            "AI Investment Manager - Portfolio Status",
            "=" * 60,
            f"Time: {now_et().strftime('%Y-%m-%d %H:%M:%S %Z')} ({get_market_session_str()})",
            "",
        ]

        # Provider health section
        lines.append("Provider Health:")
        for provider, status in health.items():
            avail = "OK" if status.get("available") else "UNAVAILABLE"
            key_status = "key set" if status.get("api_key_set") else "NO KEY"
            lines.append(f"  {provider:10}: {avail:12} ({key_status})")
        lines.append("")

        if "error" in result:
            lines.append(f"Analysis Error: {result['error']}")
            lines.append("=" * 60)
            return "\n".join(lines)

        lines.extend([
            f"Portfolio Value: ${result['portfolio_value']:,.2f}",
            "",
            "Scores:",
            f"  Risk Alert Score:  {result['risk_score']:.1f}/100",
            f"  Opportunity Score: {result['opportunity_score']:.1f}/100",
            "",
            f"Should Alert: {result['should_alert']}",
            f"Recommendations: {result['num_recommendations']}",
            f"Dedupe Result: {result['dedupe_reason']}",
            "",
            f"Email Sent: {result['email_sent']}",
            "=" * 60,
        ])

        return "\n".join(lines)

    def test_email(self) -> bool:
        """
        Send a test email without checking thresholds.

        Returns:
            True if email sent successfully
        """
        logger.info("Sending test email...")

        # Run analysis to get real data
        csv_path = self.config.get("paths", {}).get("portfolio_csv_path")
        if not csv_path or not Path(csv_path).exists():
            logger.error("Portfolio CSV not found")
            return False

        parse_result = self.csv_parser.parse_file(csv_path)
        holdings = parse_result.holdings

        if not holdings:
            # Create dummy holding for test
            holdings = {
                "TEST": Holding(
                    symbol="TEST",
                    quantity=Decimal("100"),
                    avg_cost_basis=Decimal("50"),
                    total_cost=Decimal("5000"),
                    current_price=Decimal("55"),
                    current_value=Decimal("5500"),
                    unrealized_pnl=Decimal("500"),
                    unrealized_pnl_pct=Decimal("10.0"),
                    sector="Technology",
                )
            }

        # Build minimal snapshot
        # FIX: Use current_value (not total_cost) for total_value
        holdings_list = list(holdings.values()) if isinstance(holdings, dict) else holdings
        snapshot = PortfolioSnapshot(
            timestamp=now_et(),
            holdings=holdings_list,
            total_value=sum(float(h.current_value or h.total_cost or 0) for h in holdings_list),
        )

        # Send test email
        email_config = self.config.get("email", {})
        smtp_password = os.environ.get(email_config.get("smtp_password_env_var", "SMTP_PASSWORD"))

        if not smtp_password:
            logger.error("SMTP password not configured")
            return False

        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
            <h1 style="color: #2563eb;">AI Investment Manager - Test Email</h1>
            <p>This is a test email to verify your configuration.</p>
            <p>Time: {now_et().strftime('%Y-%m-%d %H:%M:%S %Z')}</p>
            <p>Holdings loaded: {len(holdings)}</p>
            <hr>
            <p style="font-size: 12px; color: #6b7280;">
                If you received this email, your SMTP configuration is working correctly.
            </p>
        </body>
        </html>
        """

        return send_email(
            html_content=html_content,
            subject="AI Investment Manager - Test Email",
            to_emails=email_config.get("to_emails", []),
            from_email=email_config.get("from_email", ""),
            smtp_host=email_config.get("smtp_host", "smtp.gmail.com"),
            smtp_port=email_config.get("smtp_port", 587),
            smtp_user=email_config.get("smtp_user", ""),
            smtp_password=smtp_password,
        )


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="AI Investment Manager - Professional portfolio monitoring"
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run analysis once and exit"
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run in daemon mode (continuous monitoring)"
    )
    parser.add_argument(
        "--test-email",
        action="store_true",
        help="Send a test email to verify configuration"
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Print current portfolio status without sending email"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run analysis and render email to HTML file without sending"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--force-email",
        action="store_true",
        help="Force send email, bypassing deduplication checks"
    )

    args = parser.parse_args()

    # Load environment variables from project root .env file
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(env_path)

    # Load configuration
    config = load_config(args.config)

    # Setup logging
    log_config = config.get("logging", {})
    setup_logging(
        log_dir=config.get("paths", {}).get("log_dir", "logs"),
        level=log_config.get("level", "INFO"),
        json_format=log_config.get("json_format", False),
    )

    logger.info("AI Investment Manager starting...")

    # Create manager
    manager = InvestmentManager(config)

    # Execute requested action
    if args.test_email:
        success = manager.test_email()
        sys.exit(0 if success else 1)

    elif args.explain:
        explanation = manager.explain()
        print(explanation)
        sys.exit(0)

    elif args.dry_run:
        # Run analysis with dry_run flag - renders email to file
        result = manager.run_analysis(dry_run=True)
        if "error" in result:
            logger.error(f"Analysis failed: {result['error']}")
            sys.exit(1)
        print(f"Dry run complete. Check output directory for email preview.")
        sys.exit(0)

    elif args.once or args.force_email:
        # --once always sends email (user explicitly requested a run)
        result = manager.run_analysis(force_email=True)
        if "error" in result:
            logger.error(f"Analysis failed: {result['error']}")
            sys.exit(1)
        sys.exit(0)

    elif args.daemon:
        runner = ScheduledRunner(
            analysis_callback=manager.run_analysis,
            config=config
        )
        runner.run_daemon()

    else:
        # Default: run once
        result = manager.run_analysis()
        if "error" in result:
            logger.error(f"Analysis failed: {result['error']}")
            sys.exit(1)


if __name__ == "__main__":
    main()
