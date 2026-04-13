"""
Scheduling Runner Module

Handles scheduled execution of portfolio analysis.
Supports both interval-based and file-watch modes.
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any, Callable

from ..utils.logging import get_logger, setup_logging
from ..utils.time import is_market_hours, now_et, get_market_session_str

logger = get_logger(__name__)

# Try to import scheduling libraries
try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False
    logger.warning("apscheduler not installed - scheduled mode unavailable")

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileModifiedEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False
    logger.warning("watchdog not installed - file watch mode unavailable")


class PortfolioFileHandler(FileSystemEventHandler):
    """File system event handler for portfolio CSV changes."""

    def __init__(self, callback: Callable, csv_filename: str = "activity.csv"):
        """
        Initialize handler.

        Args:
            callback: Function to call when file changes
            csv_filename: Name of CSV file to watch
        """
        self.callback = callback
        self.csv_filename = csv_filename.lower()
        self._last_trigger = datetime.min
        self._debounce_seconds = 5  # Prevent multiple triggers

    def on_modified(self, event):
        """Handle file modification event."""
        if event.is_directory:
            return

        filename = Path(event.src_path).name.lower()
        if filename == self.csv_filename or filename.endswith('.csv'):
            now = datetime.now()
            if (now - self._last_trigger).total_seconds() > self._debounce_seconds:
                self._last_trigger = now
                logger.info(f"Portfolio file changed: {event.src_path}")
                try:
                    self.callback()
                except Exception as e:
                    logger.error(f"Error processing file change: {e}")


class ScheduledRunner:
    """
    Manages scheduled execution of portfolio analysis.

    Supports:
    - Interval mode: Run on fixed schedule (different during/outside market hours)
    - Watchdog mode: Run when portfolio CSV changes
    """

    def __init__(
        self,
        analysis_callback: Callable,
        config: Dict[str, Any]
    ):
        """
        Initialize runner.

        Args:
            analysis_callback: Function to call for analysis
            config: Configuration dictionary
        """
        self.analysis_callback = analysis_callback
        self.config = config

        # Schedule settings
        self.mode = config.get("schedule", {}).get("mode", "interval")
        self.interval_market = config.get("schedule", {}).get("interval_minutes_market_hours", 30)
        self.interval_off = config.get("schedule", {}).get("interval_minutes_off_hours", 240)
        self.watch_dir = config.get("paths", {}).get("watch_dir")
        self.csv_path = config.get("paths", {}).get("portfolio_csv_path")

        self._scheduler = None
        self._observer = None
        self._running = False

    def run_once(self) -> bool:
        """
        Run analysis once.

        Returns:
            True if analysis succeeded
        """
        logger.info("Running single analysis...")
        try:
            self.analysis_callback()
            return True
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return False

    def run_daemon(self):
        """
        Run in daemon mode (continuous scheduling).

        Blocks until interrupted.
        """
        if self.mode == "watchdog":
            self._run_watchdog_mode()
        else:
            self._run_interval_mode()

    def _run_interval_mode(self):
        """Run in interval mode using APScheduler."""
        if not APSCHEDULER_AVAILABLE:
            logger.error("APScheduler not available - cannot run interval mode")
            logger.info("Falling back to simple loop")
            self._run_simple_loop()
            return

        logger.info("Starting interval mode scheduler")
        self._scheduler = BlockingScheduler()

        # Add job that checks market hours and adjusts interval
        self._scheduler.add_job(
            self._scheduled_run,
            IntervalTrigger(minutes=1),  # Check every minute
            id='portfolio_analysis',
            name='Portfolio Analysis',
            max_instances=1
        )

        self._last_run = datetime.min
        self._running = True

        try:
            logger.info(f"Scheduler started. Market hours interval: {self.interval_market}min, "
                       f"Off-hours interval: {self.interval_off}min")
            self._scheduler.start()
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
        finally:
            self._running = False

    def _scheduled_run(self):
        """Check if we should run based on interval."""
        now = datetime.now()

        # Determine interval based on market hours
        if is_market_hours():
            interval_minutes = self.interval_market
        else:
            interval_minutes = self.interval_off

        # Check if enough time has passed
        minutes_since_last = (now - self._last_run).total_seconds() / 60

        if minutes_since_last >= interval_minutes:
            logger.info(f"Running scheduled analysis ({get_market_session_str()})")
            try:
                self.analysis_callback()
                self._last_run = now
            except Exception as e:
                logger.error(f"Scheduled analysis failed: {e}")

    def _run_watchdog_mode(self):
        """Run in watchdog mode using file system observer."""
        if not WATCHDOG_AVAILABLE:
            logger.error("Watchdog not available - cannot run watch mode")
            logger.info("Falling back to interval mode")
            self._run_interval_mode()
            return

        watch_path = self.watch_dir or (
            str(Path(self.csv_path).parent) if self.csv_path else "."
        )

        if not Path(watch_path).exists():
            logger.error(f"Watch directory does not exist: {watch_path}")
            return

        logger.info(f"Starting watchdog mode, watching: {watch_path}")

        event_handler = PortfolioFileHandler(
            callback=self.analysis_callback,
            csv_filename=Path(self.csv_path).name if self.csv_path else "activity.csv"
        )

        self._observer = Observer()
        self._observer.schedule(event_handler, watch_path, recursive=False)
        self._observer.start()
        self._running = True

        try:
            # Also run on startup
            logger.info("Running initial analysis...")
            self.analysis_callback()

            # Keep alive and do periodic health checks
            health_check_interval = 3600  # 1 hour
            last_health_check = datetime.now()

            while self._running:
                time.sleep(1)

                # Periodic health check
                if (datetime.now() - last_health_check).total_seconds() >= health_check_interval:
                    logger.info(f"Health check: Running, {get_market_session_str()}")
                    last_health_check = datetime.now()

        except KeyboardInterrupt:
            logger.info("Watchdog stopped by user")
        finally:
            self._observer.stop()
            self._observer.join()
            self._running = False

    def _run_simple_loop(self):
        """Fallback simple loop when schedulers unavailable."""
        logger.info("Running simple interval loop")
        self._running = True

        try:
            while self._running:
                # Determine interval
                if is_market_hours():
                    interval = self.interval_market * 60
                else:
                    interval = self.interval_off * 60

                logger.info(f"Running analysis ({get_market_session_str()})")
                try:
                    self.analysis_callback()
                except Exception as e:
                    logger.error(f"Analysis failed: {e}")

                # Sleep in chunks for responsiveness
                sleep_time = 0
                while sleep_time < interval and self._running:
                    time.sleep(min(60, interval - sleep_time))
                    sleep_time += 60

        except KeyboardInterrupt:
            logger.info("Loop stopped by user")
        finally:
            self._running = False

    def stop(self):
        """Stop the runner."""
        self._running = False
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
        if self._observer:
            self._observer.stop()


def run_once(analysis_callback: Callable, config: Dict[str, Any]) -> bool:
    """
    Convenience function to run analysis once.

    Args:
        analysis_callback: Analysis function
        config: Configuration dict

    Returns:
        True if successful
    """
    runner = ScheduledRunner(analysis_callback, config)
    return runner.run_once()


def run_daemon(analysis_callback: Callable, config: Dict[str, Any]):
    """
    Convenience function to run in daemon mode.

    Args:
        analysis_callback: Analysis function
        config: Configuration dict
    """
    runner = ScheduledRunner(analysis_callback, config)
    runner.run_daemon()
