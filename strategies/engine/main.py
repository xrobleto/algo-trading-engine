"""
Unified Multi-Strategy Trading Engine — main entry point.

Runs Trend Bot and Simple Bot in the same Alpaca account with:
- Capital sleeve isolation
- Position ownership tracking
- Conflict prevention
- Portfolio-level kill switch
- Startup reconciliation

Usage:
  python -m engine.main                    # Normal continuous operation
  python -m engine.main --trend-only       # Trend adapter only (fallback mode)
  python -m engine.main --status           # Show engine state and exit
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
import traceback
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from zoneinfo import ZoneInfo

# Ensure the strategies directory is on the path
SCRIPT_DIR = Path(__file__).resolve().parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from engine.config import build_default_config, validate_config, EngineConfig
from engine.broker import create_broker_from_env, BrokerFacade
from engine.ownership import OwnershipLedger
from engine.sleeves import SleeveManager
from engine.reconciler import reconcile
from engine.portfolio_kill_switch import PortfolioKillSwitch
from adapters.base import TickResult

log = logging.getLogger("Engine")

ET = ZoneInfo("America/New_York")


# =============================================================================
# MARKET SESSION
# =============================================================================

class MarketSession(str, Enum):
    MARKET_OPEN = "MARKET_OPEN"    # 9:30-16:00 ET weekdays
    PRE_MARKET = "PRE_MARKET"      # 8:00-9:30 ET weekdays
    OFF_HOURS = "OFF_HOURS"        # evenings, weekends, holidays


# Tick intervals per session
SESSION_TICK_SEC = {
    MarketSession.MARKET_OPEN: 5,     # 5 seconds — full speed
    MarketSession.PRE_MARKET: 60,     # 1 minute — prep mode
    MarketSession.OFF_HOURS: 300,     # 5 minutes — idle
}


def get_market_session() -> MarketSession:
    """Classify current time into a market session (ET-based)."""
    now = datetime.now(ET)

    # Weekends
    if now.weekday() >= 5:
        return MarketSession.OFF_HOURS

    hm = (now.hour, now.minute)

    if (9, 30) <= hm < (16, 0):
        return MarketSession.MARKET_OPEN
    elif (8, 0) <= hm < (9, 30):
        return MarketSession.PRE_MARKET
    else:
        return MarketSession.OFF_HOURS


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(log_level: str = "INFO") -> None:
    """Configure engine logging with console + file output."""
    root_logger = logging.getLogger("Engine")
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%H:%M:%S'
    ))
    root_logger.addHandler(console)

    # File handler (rotating)
    from engine.platform import get_data_dir
    log_dir = get_data_dir() / "logs"

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "engine.log"

    from logging.handlers import RotatingFileHandler
    file_handler = RotatingFileHandler(
        str(log_file), maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    root_logger.addHandler(file_handler)

    log.info(f"Logging to {log_file}")


# =============================================================================
# ENGINE HEARTBEAT
# =============================================================================

def log_heartbeat(
    sleeves: SleeveManager,
    ledger: OwnershipLedger,
    adapters: dict,
    tick_count: int,
) -> None:
    """Log engine heartbeat with sleeve usage."""
    summary = sleeves.get_summary(ledger)

    parts = [f"[ENGINE] tick={tick_count} | equity=${summary['total_equity']:,.0f}"]
    for sid, sleeve_info in summary["sleeves"].items():
        parts.append(
            f"{sid}: ${sleeve_info['used']:,.0f}/${sleeve_info['equity']:,.0f} "
            f"({sleeve_info['utilization_pct']:.0%}, {sleeve_info['positions']}pos)"
        )
    parts.append(f"cash_reserve=${summary['cash_reserve']:,.0f}")

    log.info(" | ".join(parts))


# =============================================================================
# MAIN ENGINE
# =============================================================================

def engine_main(trend_only: bool = False) -> None:
    """
    Main engine loop.

    Args:
        trend_only: If True, only run the Trend adapter (fallback mode).
    """
    # Load .env file if present (no-op if env vars already set by launcher/Railway)
    try:
        from dotenv import load_dotenv
        load_dotenv(override=False)
    except ImportError:
        pass  # python-dotenv not installed — env must be set externally

    setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    log.info("=" * 70)
    log.info("UNIFIED MULTI-STRATEGY TRADING ENGINE")
    log.info("=" * 70)

    # -------------------------------------------------------------------------
    # 1. Load configuration
    # -------------------------------------------------------------------------
    config = build_default_config()
    validate_config(config)

    mode = "LIVE" if config.live_trading else "PAPER"
    strategies = "TREND only" if trend_only else "TREND + SIMPLE + CROSSASSET"
    log.info(f"Mode: {mode} | Strategies: {strategies}")
    log.info(f"Sleeves: " + ", ".join(
        f"{sid}={sc.allocation_pct:.0%}" for sid, sc in config.sleeves.items()
    ) + f", cash={config.cash_reserve_pct:.0%}")
    log.info(f"State dir: {config.state_dir}")

    # -------------------------------------------------------------------------
    # 2. Initialize broker
    # -------------------------------------------------------------------------
    broker = create_broker_from_env()
    account = broker.get_account()
    total_equity = account["equity"]
    log.info(
        f"Account: {account['id']} | Equity: ${total_equity:,.2f} | "
        f"Status: {account['status']}"
    )

    # -------------------------------------------------------------------------
    # 3. Load ownership ledger
    # -------------------------------------------------------------------------
    ledger = OwnershipLedger.load(config.ownership_path)
    ledger.prune_terminal(max_age_days=7)

    # -------------------------------------------------------------------------
    # 4. Initialize sleeve manager
    # -------------------------------------------------------------------------
    sleeves = SleeveManager(config)
    sleeves.refresh(total_equity, ledger)

    # -------------------------------------------------------------------------
    # 5. Portfolio kill switch
    # -------------------------------------------------------------------------
    portfolio_ks = PortfolioKillSwitch(
        kill_switch_path=config.kill_switch_path,
        env_var="KILL_SWITCH"
    )

    # Pre-check kill switch
    ks_triggered, ks_reason = portfolio_ks.is_triggered()
    if ks_triggered:
        log.critical(f"Portfolio kill switch active: {ks_reason}")
        log.critical("Clear the kill switch file/env var and restart.")
        return

    # -------------------------------------------------------------------------
    # 5b. Market intelligence layer
    # -------------------------------------------------------------------------
    from engine.intelligence import MarketIntelligenceLayer
    intelligence = MarketIntelligenceLayer(engine_config=config)
    market_ctx = None
    try:
        market_ctx = intelligence.refresh()
        log.info(
            f"[INTELLIGENCE] Initial regime: {market_ctx.regime.value} "
            f"(score={market_ctx.regime_score:.1f})"
        )
    except Exception as e:
        log.warning(f"[INTELLIGENCE] Initial refresh failed: {e} — running without intelligence")

    # -------------------------------------------------------------------------
    # 6. Startup reconciliation
    # -------------------------------------------------------------------------
    if config.reconcile_on_startup:
        log.info("[STARTUP] Running broker state reconciliation...")
        recon_result = reconcile(broker, ledger, config)

        if recon_result.conflicts:
            log.critical(f"OWNERSHIP CONFLICTS: {recon_result.conflicts}")
            portfolio_ks.trigger(f"ownership conflicts: {recon_result.conflicts}")
            return

        if recon_result.unclassified_positions:
            log.warning(
                f"Unclassified positions: {recon_result.unclassified_positions}. "
                f"These may be manual trades. Blocking new entries."
            )
            portfolio_ks.trigger(
                f"unclassified positions: {recon_result.unclassified_positions}"
            )
            return

        ledger = recon_result.ownership_snapshot
        ledger.save(config.ownership_path)
        log.info("[STARTUP] Reconciliation complete — ownership state clean")

    # -------------------------------------------------------------------------
    # 7. Initialize strategy adapters
    # -------------------------------------------------------------------------
    adapters = {}

    # Trend adapter (always active)
    from adapters.trend_adapter import TrendAdapter
    trend_adapter = TrendAdapter(
        config=config.sleeves["TREND"],
        ledger=ledger,
        sleeve_manager=sleeves,
    )
    trend_ctx = sleeves.get_context("TREND", ledger)
    trend_adapter.initialize(trend_ctx)
    adapters["TREND"] = trend_adapter

    # Simple adapter (unless trend-only mode)
    if not trend_only:
        try:
            from adapters.simple_adapter import SimpleAdapter
            simple_adapter = SimpleAdapter(
                config=config.sleeves["SIMPLE"],
                ledger=ledger,
                sleeve_manager=sleeves,
            )
            simple_ctx = sleeves.get_context("SIMPLE", ledger)
            simple_adapter.initialize(simple_ctx)
            adapters["SIMPLE"] = simple_adapter
            log.info("[STARTUP] Simple adapter initialized (probation mode)")
        except ImportError:
            log.warning("[STARTUP] Simple adapter not available — running trend-only")
        except Exception as e:
            log.error(f"[STARTUP] Failed to initialize Simple adapter: {e}")
            log.warning("[STARTUP] Continuing with trend-only")
            traceback.print_exc()

    # Cross-Asset adapter (unless trend-only mode)
    if not trend_only:
        try:
            from adapters.cross_asset_adapter import CrossAssetAdapter
            crossasset_adapter = CrossAssetAdapter(
                config=config.sleeves["CROSSASSET"],
                ledger=ledger,
                sleeve_manager=sleeves,
            )
            crossasset_ctx = sleeves.get_context("CROSSASSET", ledger)
            crossasset_adapter.initialize(crossasset_ctx)
            adapters["CROSSASSET"] = crossasset_adapter
            log.info("[STARTUP] Cross-Asset adapter initialized")
        except ImportError:
            log.warning("[STARTUP] Cross-Asset adapter not available — running without it")
        except Exception as e:
            log.error(f"[STARTUP] Failed to initialize Cross-Asset adapter: {e}")
            log.warning("[STARTUP] Continuing without Cross-Asset")
            traceback.print_exc()

    log.info(f"[STARTUP] Active strategies: {list(adapters.keys())}")

    # -------------------------------------------------------------------------
    # 8. Signal handlers
    # -------------------------------------------------------------------------
    shutdown_requested = [0]  # count of signals received

    def shutdown_handler(sig, frame):
        shutdown_requested[0] += 1
        if shutdown_requested[0] == 1:
            log.info("[SHUTDOWN] Received interrupt signal — finishing current tick...")
        elif shutdown_requested[0] == 2:
            log.warning("[SHUTDOWN] Second interrupt — forcing shutdown NOW")
            # Run minimal cleanup
            for sid, adp in adapters.items():
                try:
                    adp.shutdown()
                except Exception:
                    pass
            try:
                ledger.save(config.ownership_path)
            except Exception:
                pass
            log.info("[SHUTDOWN] Forced shutdown complete")
            sys.exit(0)
        else:
            # Third+ interrupt — immediate exit, no cleanup
            sys.exit(1)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # -------------------------------------------------------------------------
    # 9. Main loop
    # -------------------------------------------------------------------------
    tick_count = 0
    heartbeat_interval = 60  # log heartbeat every N ticks (~5 min at 5s/tick)
    last_reconcile_at = time.time()
    last_session = None  # track session transitions

    log.info("=" * 70)
    log.info("ENGINE RUNNING")
    log.info("=" * 70)

    while shutdown_requested[0] == 0:
        tick_count += 1

        try:
            # ---- Portfolio kill switch (always check) ----
            ks_triggered, ks_reason = portfolio_ks.is_triggered()
            if ks_triggered:
                log.critical(f"[ENGINE] Portfolio kill switch: {ks_reason}")
                break

            # ---- Market session awareness ----
            session = get_market_session()
            tick_sleep = SESSION_TICK_SEC[session]

            # Log session transitions
            if session != last_session:
                now_et = datetime.now(ET).strftime("%H:%M ET")
                log.info(f"[ENGINE] Session: {session.value} ({now_et})")
                if session == MarketSession.MARKET_OPEN:
                    log.info("[ENGINE] Market open — full-speed trading")
                elif session == MarketSession.PRE_MARKET:
                    log.info("[ENGINE] Pre-market — monitoring mode (60s ticks)")
                else:
                    log.info("[ENGINE] Off-hours — idle mode (5 min ticks)")
                last_session = session

            # ---- OFF-HOURS: minimal activity ----
            if session == MarketSession.OFF_HOURS:
                time.sleep(tick_sleep)
                continue

            # ---- PRE-MARKET: refresh intelligence + equity, no strategy ticks ----
            if session == MarketSession.PRE_MARKET:
                if intelligence.should_refresh():
                    try:
                        market_ctx = intelligence.refresh()
                    except Exception as e:
                        log.error(f"[INTELLIGENCE] Refresh failed: {e}")

                try:
                    total_equity = broker.get_equity()
                    alloc_overrides = None
                    if market_ctx:
                        alloc_overrides = {
                            sid: adj.adjusted_allocation
                            for sid, adj in market_ctx.sleeve_adjustments.items()
                        }
                    sleeves.refresh(total_equity, ledger, alloc_overrides)
                except Exception as e:
                    log.error(f"[ENGINE] Failed to refresh equity: {e}")

                time.sleep(tick_sleep)
                continue

            # ================================================================
            # MARKET_OPEN: full trading behavior
            # ================================================================

            # ---- Market intelligence refresh ----
            if intelligence.should_refresh():
                try:
                    market_ctx = intelligence.refresh()
                except Exception as e:
                    log.error(f"[INTELLIGENCE] Refresh failed: {e}")
                    # Continue with stale context

            # ---- Refresh account + sleeves ----
            alloc_overrides = None
            if market_ctx:
                alloc_overrides = {
                    sid: adj.adjusted_allocation
                    for sid, adj in market_ctx.sleeve_adjustments.items()
                }
            try:
                total_equity = broker.get_equity()
                sleeves.refresh(total_equity, ledger, alloc_overrides)
            except Exception as e:
                log.error(f"[ENGINE] Failed to refresh equity: {e}")
                # Continue with stale values rather than halting

            # ---- Check for ownership conflicts ----
            conflicts = ledger.has_conflicts()
            if conflicts:
                log.critical(f"[ENGINE] Ownership conflicts detected: {conflicts}")
                portfolio_ks.trigger(f"ownership conflicts: {conflicts}")
                break

            # ---- Tick each strategy sequentially ----
            for strategy_id, adapter in adapters.items():
                if adapter.is_halted:
                    continue  # skip halted strategies

                try:
                    ctx = sleeves.get_context(strategy_id, ledger)
                    ctx.market_context = market_ctx
                    result = adapter.tick(ctx)

                    if result == TickResult.ERROR:
                        log.warning(f"[ENGINE] {strategy_id} tick returned ERROR")
                    elif result == TickResult.HALTED:
                        log.warning(
                            f"[ENGINE] {strategy_id} halted: {adapter.halt_reason}"
                        )

                except Exception as e:
                    adapter.record_error(e)
                    log.error(f"[ENGINE] {strategy_id} tick exception: {e}")
                    traceback.print_exc()

            # ---- Persist ownership ledger ----
            try:
                ledger.save(config.ownership_path)
            except Exception as e:
                log.error(f"[ENGINE] Failed to save ownership ledger: {e}")

            # ---- Periodic reconciliation ----
            now_ts = time.time()
            if now_ts - last_reconcile_at >= config.reconcile_interval_sec:
                try:
                    recon_result = reconcile(broker, ledger, config)
                    if recon_result.conflicts:
                        log.critical(
                            f"[ENGINE] Ownership conflicts on periodic reconcile: "
                            f"{recon_result.conflicts}"
                        )
                        portfolio_ks.trigger(
                            f"ownership conflicts: {recon_result.conflicts}"
                        )
                        break
                    ledger.save(config.ownership_path)
                except Exception as e:
                    log.error(f"[ENGINE] Periodic reconcile failed: {e}")
                last_reconcile_at = now_ts

            # ---- Heartbeat ----
            if tick_count % heartbeat_interval == 0:
                log_heartbeat(sleeves, ledger, adapters, tick_count)

            # ---- Sleep ----
            time.sleep(tick_sleep)

        except KeyboardInterrupt:
            log.info("[ENGINE] Keyboard interrupt")
            break

        except Exception as e:
            log.critical(f"[ENGINE] Unexpected error: {e}")
            traceback.print_exc()
            # Don't crash immediately — try to shutdown gracefully
            break

    # -------------------------------------------------------------------------
    # 10. Shutdown
    # -------------------------------------------------------------------------
    log.info("=" * 70)
    log.info("ENGINE SHUTTING DOWN")
    log.info("=" * 70)

    for strategy_id, adapter in adapters.items():
        try:
            log.info(f"[SHUTDOWN] Shutting down {strategy_id}...")
            adapter.shutdown()
        except Exception as e:
            log.error(f"[SHUTDOWN] Error shutting down {strategy_id}: {e}")

    # Final ledger save
    try:
        ledger.save(config.ownership_path)
        log.info("[SHUTDOWN] Ownership ledger saved")
    except Exception as e:
        log.error(f"[SHUTDOWN] Failed to save ownership ledger: {e}")

    log.info("ENGINE STOPPED")


# =============================================================================
# STATUS COMMAND
# =============================================================================

def show_status() -> None:
    """Show engine state and exit."""
    config = build_default_config()
    ledger = OwnershipLedger.load(config.ownership_path)

    print("=" * 60)
    print("UNIFIED ENGINE STATUS")
    print("=" * 60)
    print(f"State dir: {config.state_dir}")
    print(f"Ownership file: {config.ownership_path}")
    print(f"Last reconciled: {ledger.last_reconciled_at or 'never'}")
    print()

    active = ledger.get_active_entries()
    print(f"Active entries: {len(active)}")
    for entry in active:
        print(f"  [{entry.strategy_id}] {entry.symbol} {entry.side} "
              f"qty={entry.qty:.4f} status={entry.status} "
              f"notional=${entry.notional_at_entry:,.2f}")

    conflicts = ledger.has_conflicts()
    if conflicts:
        print(f"\nCONFLICTS: {conflicts}")
    else:
        print("\nNo ownership conflicts.")

    print("=" * 60)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Unified Multi-Strategy Trading Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m engine.main                    # Normal operation (Trend + Simple)
  python -m engine.main --trend-only       # Trend adapter only (fallback)
  python -m engine.main --status           # Show engine state and exit
        """
    )
    parser.add_argument(
        "--trend-only",
        action="store_true",
        help="Run with Trend adapter only (no Simple Bot)"
    )
    parser.add_argument(
        "--status", "-s",
        action="store_true",
        help="Show engine state and exit"
    )

    args = parser.parse_args()

    if args.status:
        show_status()
    else:
        engine_main(trend_only=args.trend_only)
