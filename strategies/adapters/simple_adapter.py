"""
Simple Bot Adapter — wraps simple_bot.py (MomentumBot) for the unified engine.

This adapter:
1. Creates a MomentumBot instance and runs its startup sequence
2. Runs the main loop body as tick() calls
3. Wraps alpaca.submit_order() for sleeve validation + ownership registration
4. Overrides capital/position constants for probation mode
5. Overrides generate_client_order_id() to use ENG_SIMPLE_ prefix
6. Monitors for execution anomalies (auto-halt on probation)

Changes to simple_bot.py: ZERO — all injection via monkey-patching module globals.
"""

import logging
import time
import traceback
from typing import Optional, Set

from adapters.base import StrategyAdapter, TickResult
from engine.config import SleeveConfig
from engine.ownership import OwnershipLedger
from engine.sleeves import SleeveContext, SleeveManager

log = logging.getLogger("Engine")


class SimpleAdapter(StrategyAdapter):
    """
    Wraps the existing simple_bot.py MomentumBot without modifying it.

    Injection strategy:
    - Monkey-patch alpaca.submit_order() → validate sleeve + register ownership
    - Monkey-patch generate_client_order_id() → ENG_SIMPLE_ prefix
    - Override module constants for probation (MAX_POSITIONS, MAX_DAILY_LOSS_PCT, etc.)
    - Rate-limit tick() — simple bot's natural cadence is 5s (same as engine)
    """

    def __init__(
        self,
        config: SleeveConfig,
        ledger: OwnershipLedger,
        sleeve_manager: SleeveManager,
    ):
        super().__init__(config, ledger, sleeve_manager)

        # Simple bot internals (set during initialize)
        self._bot = None              # MomentumBot instance
        self._simple_module = None    # the simple_bot module itself

        # Current sleeve context (updated each tick)
        self._current_ctx: Optional[SleeveContext] = None

        # Original functions (saved before monkey-patching)
        self._original_submit_order = None
        self._original_generate_order_id = None
        self._original_risk_update = None
        self._original_bot_shutdown = None

        # Original constants (for cleanup on shutdown)
        self._original_constants = {}

        # Anomaly tracking for probation
        self._trades_today: int = 0
        self._daily_loss: float = 0.0
        self._last_trade_date: Optional[str] = None

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def initialize(self, ctx: SleeveContext) -> None:
        """
        Create MomentumBot and run its startup sequence.

        Replicates MomentumBot.run() startup (lines 5919-5976) without
        entering the while loop.
        """
        log.info(f"[SIMPLE] Initializing adapter (sleeve: ${ctx.sleeve_equity:,.2f}, "
                 f"probation: {ctx.config.probation})")
        self._current_ctx = ctx

        # Import simple_bot module
        import simple_bot as sb
        self._simple_module = sb

        # Apply probation overrides BEFORE creating the bot
        self._apply_constant_overrides(ctx)

        # Apply monkey-patches (order submission, order ID generation)
        self._apply_patches()

        # Create MomentumBot instance
        self._bot = sb.MomentumBot()

        # Run startup sequence (from MomentumBot.run(), lines 5926-5976)
        log.info("[SIMPLE] Running startup sequence...")

        # Verify account (line 5926)
        self._bot.verify_account_status()

        # Initialize risk manager (line 5929)
        sb.risk_manager.initialize()

        # CRITICAL: Override risk manager equity to use sleeve equity
        # Without this, risk_manager uses total account equity and sees
        # a huge "loss" when comparing against persisted start_equity
        self._override_risk_manager_equity(ctx, sb)

        # Reconcile with broker state (line 5932)
        self._bot.reconcile_broker_state()

        # Clean up old terminal intents (line 5935)
        sb.trade_manager.cleanup_terminal_intents(max_age_hours=24)

        # Log performance summary (lines 5938-5951)
        try:
            perf_summary = sb.trade_journal.get_performance_summary(lookback_hours=24)
            if "error" not in perf_summary and "message" not in perf_summary:
                log.info(f"[SIMPLE] Last 24h: {perf_summary['total_trades']} trades, "
                         f"{perf_summary['win_rate']:.0%} win rate, "
                         f"${perf_summary['total_pnl_est']:.2f} PnL")
        except Exception:
            pass

        # Start WebSocket trade updates stream (lines 5953-5961)
        sb.trade_updates_stream = sb.TradeUpdatesStream(
            trade_manager=sb.trade_manager,
            trade_journal=sb.trade_journal,
            position_manager=sb.position_manager,
            alerter=sb.alerter
        )
        sb.trade_updates_stream.start()

        # Pre-market gap scan (lines 5963-5975)
        if self._bot.market_scanner:
            import datetime as _dt
            now = _dt.datetime.now(sb.ET)
            if now.hour < 9 or (now.hour == 9 and now.minute < 30):
                try:
                    log.info("[SIMPLE] Running pre-market gap scan...")
                    gappers = self._bot.market_scanner.scan_premarket()
                    if gappers:
                        self._bot.dynamic_universe = set(self._bot.market_scanner.get_symbols()) - set(sb.CORE_SYMBOLS)
                        log.info(f"[SIMPLE] Pre-market: {len(gappers)} gappers, "
                                 f"{len(self._bot.dynamic_universe)} dynamic")
                except Exception as e:
                    log.warning(f"[SIMPLE] Pre-market scan failed: {e}")

        self._bot.running = True
        log.info("[SIMPLE] Adapter initialized successfully")

    def tick(self, ctx: SleeveContext) -> TickResult:
        """
        Execute one iteration of MomentumBot's main loop body (lines 5993-6136).

        This runs at the engine's 5s cadence, matching simple bot's natural rhythm.
        """
        if self.is_halted:
            return TickResult.HALTED

        if not self._bot or not self._bot.running:
            return TickResult.SKIPPED

        self._current_ctx = ctx
        sb = self._simple_module

        try:
            # ---- Kill switch (line 5995) ----
            kill_triggered, kill_reason = sb.kill_switch.is_triggered()
            if kill_triggered:
                log.error(f"[SIMPLE] Kill switch triggered: {kill_reason}")
                sb.kill_switch.execute_emergency_shutdown()
                self.halt(f"kill switch: {kill_reason}")
                return TickResult.HALTED

            # ---- Market session check (line 6003) ----
            session = sb.get_market_session()

            # ---- Heartbeat (line 6006) ----
            halted = sb.risk_manager.halted or sb.circuit_breaker.is_halted()
            halt_reason = (sb.risk_manager.halt_reason
                          if hasattr(sb.risk_manager, 'halt_reason')
                          else sb.circuit_breaker.get_halt_reason())
            sb.heartbeat_logger.log_heartbeat(session, halted, halt_reason)

            # ---- Market closed handling (line 6010) ----
            if session == sb.MarketSession.CLOSED:
                if sb.position_manager.positions:
                    self._handle_closed_market_positions(sb)
                return TickResult.SKIPPED

            # ---- Circuit breaker (line 6064) ----
            if sb.circuit_breaker.is_halted():
                log.warning(f"[SIMPLE] Circuit breaker: {sb.circuit_breaker.get_halt_reason()}")
                return TickResult.SKIPPED

            # ---- Alpaca clock check (line 6070) ----
            if not sb.circuit_breaker.check_alpaca_clock():
                return TickResult.SKIPPED

            # ---- Risk manager update (line 6075) ----
            # Patched update() uses sleeve equity instead of total account
            if sb.risk_manager.update():
                log.warning("[SIMPLE] Daily loss limit reached — halted")
                return TickResult.SKIPPED

            # ---- Equity snapshot (line 6081) ----
            sb.equity_snapshot_logger.log_snapshot()

            # ---- EOD close check (line 6084) ----
            if sb.AUTO_CLOSE_EOD:
                self._bot.check_eod_close()
                # Safety net: within 5 minutes of full flatten time, also
                # run the broker-authoritative flatten path so positions that
                # exist at broker but not in position_manager (e.g. after a
                # mid-session restart where reconcile_broker_state dropped
                # them) cannot slip past the EOD window and survive the
                # CLOSED branch over the weekend.
                try:
                    import datetime as _dt
                    now_et_dt = _dt.datetime.now(sb.ET)
                    eod_h, eod_m = sb.EOD_CLOSE_TIME_ET
                    minutes_to_close = (
                        (eod_h * 60 + eod_m)
                        - (now_et_dt.hour * 60 + now_et_dt.minute)
                    )
                    if 0 <= minutes_to_close <= 5:
                        self._handle_closed_market_positions(sb)
                except Exception as _e:
                    log.error(f"[SIMPLE] EOD safety net error: {_e}")

            # ---- Sync intents with broker (line 6088) ----
            sb.trade_manager.sync_all_active_intents()

            # ---- Entry timeouts (line 6091) ----
            self._bot.check_entry_timeouts()

            # ---- Manage positions (line 6094) ----
            self._bot.manage_positions()

            # ---- Cleanup stale pending (line 6097) ----
            self._bot.cleanup_pending_symbols()

            # ---- Dynamic universe discovery (line 6099) ----
            if (sb.ENABLE_DYNAMIC_UNIVERSE
                    and (time.time() - self._bot.last_dynamic_scan_time > sb.DYNAMIC_SCAN_INTERVAL_SEC)):
                if self._bot.market_scanner:
                    # Wrap the blocking Polygon call in a 10s timeout so a slow
                    # scanner cannot stall the whole engine loop (which processes
                    # all adapters sequentially).
                    from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeout
                    try:
                        with ThreadPoolExecutor(max_workers=1) as _pool:
                            _fut = _pool.submit(self._bot.market_scanner.scan)
                            watchlist = _fut.result(timeout=10)
                        new_symbols = set(self._bot.market_scanner.get_symbols())
                        dynamic_additions = new_symbols - set(sb.CORE_SYMBOLS)
                        added = dynamic_additions - self._bot.dynamic_universe
                        self._bot.dynamic_universe = dynamic_additions
                        if added:
                            top_new = [w for w in watchlist if w.symbol in added][:5]
                            for w in top_new:
                                log.info(f"[SIMPLE][SCANNER+] {w.symbol}: score={w.quality_score:.0f} | "
                                         f"chg={w.change_pct:+.1f}% RVOL={w.rvol:.1f}x")
                    except _FuturesTimeout:
                        log.warning("[SIMPLE] Scanner timed out (>10s) — skipping this cycle")
                    except Exception as e:
                        log.warning(f"[SIMPLE] Scanner failed: {e}")
                        self._bot.discover_dynamic_universe()
                else:
                    self._bot.discover_dynamic_universe()
                self._bot.last_dynamic_scan_time = time.time()

            # ---- Scan for setups (line 6128) ----
            if time.time() - self._bot.last_scan_time > self._bot.scan_interval:
                self._bot.scan_for_setups()
                self._bot.last_scan_time = time.time()

            self.record_success()
            return TickResult.OK

        except Exception as e:
            self.record_error(e)
            log.error(f"[SIMPLE] Tick error: {e}")
            traceback.print_exc()
            return TickResult.ERROR

    def shutdown(self) -> None:
        """Graceful shutdown — delegate to MomentumBot.shutdown()."""
        log.info("[SIMPLE] Shutting down...")
        sb = self._simple_module

        if self._bot:
            try:
                self._bot.running = False
                self._bot.shutdown()
            except Exception as e:
                log.error(f"[SIMPLE] Shutdown error: {e}")

        # Stop WebSocket
        if sb and sb.trade_updates_stream:
            try:
                sb.trade_updates_stream.stop()
            except Exception:
                pass

        # Restore original functions and constants
        self._remove_patches()
        self._restore_constants()

        log.info("[SIMPLE] Adapter shutdown complete")

    def get_owned_symbols(self) -> Set[str]:
        """Get symbols currently owned by simple strategy."""
        return self.ledger.get_active_symbols("SIMPLE")

    # =========================================================================
    # CLOSED MARKET POSITION HANDLING
    # =========================================================================

    def _handle_closed_market_positions(self, sb) -> None:
        """
        Ensure Simple Bot owns zero broker positions when market is closed.

        Source of truth is the BROKER + client_order_id prefix classification
        (via `MomentumBot._get_simple_owned_broker_positions`), NOT the
        ownership ledger. The ledger can drift when partial sells (TP1, TP2,
        gradual EOD reductions) mark entries `closed` while the broker
        position is still alive — previously that caused symbols like QBTS
        to slip past the safety net and survive past market close.

        flatten_symbol already cancels all open orders for the symbol
        (including bracket TP/SL children) before closing the position, so
        it's safe to call on a live bracket.

        Fixes the CRWV weekend incident AND the QBTS dynamic-universe gap.
        """
        if not self._bot:
            return

        try:
            owned_positions = self._bot._get_simple_owned_broker_positions()
        except Exception as e:
            log.error(f"[SIMPLE] Closed-market: owned-position fetch failed: {e}")
            return

        if not owned_positions:
            return

        for bp in owned_positions:
            symbol = bp["symbol"]
            log.warning(
                f"[SIMPLE] Market CLOSED with live position {symbol} — "
                f"flattening (cancel bracket + close)"
            )
            try:
                result = sb.alpaca.flatten_symbol(symbol)
                if result.get("errors"):
                    log.error(f"[SIMPLE] {symbol}: flatten errors: {result['errors']}")
                else:
                    log.info(f"[SIMPLE] {symbol}: flattened successfully")
                # Mark ledger entries closed regardless of flatten outcome —
                # the next reconcile will clean up if the flatten actually failed.
                self._close_ownership_entries(symbol)
                # Best-effort local tracker cleanup
                try:
                    sb.position_manager.remove_position(symbol)
                except Exception:
                    pass
                intent = sb.trade_manager.get_intent(symbol)
                if intent:
                    try:
                        sb.trade_manager.transition_state(symbol, sb.TradeState.CLOSED)
                        sb.trade_journal.log_exit(
                            symbol, intent,
                            reason="SAFETY_FLATTEN_MARKET_CLOSED",
                            outcome="UNKNOWN",
                        )
                    except Exception:
                        pass
            except Exception as e:
                log.error(f"[SIMPLE] {symbol}: Flatten raised: {e}")

    def _close_ownership_entries(self, symbol: str) -> None:
        """Mark all active ownership entries for a symbol as closed."""
        for entry in self.ledger.get_active_entries("SIMPLE"):
            if entry.symbol == symbol:
                self.ledger.mark_closed(entry.client_order_id)

    # =========================================================================
    # MONKEY-PATCHING
    # =========================================================================

    def _apply_patches(self) -> None:
        """
        Apply monkey-patches to simple_bot module for sleeve integration.

        Patches:
        1. alpaca.submit_order() → validate sleeve + register ownership
        2. generate_client_order_id() → ENG_SIMPLE_ prefix
        """
        sb = self._simple_module

        # --- Patch 1: Order submission ---
        self._original_submit_order = sb.alpaca.submit_order

        def patched_submit_order(
            symbol, qty, side, order_type="market",
            limit_price=None, stop_price=None,
            trail_percent=None, time_in_force="day",
            extended_hours=False, order_class=None,
            take_profit=None, stop_loss=None,
            client_order_id=None,
        ):
            """Wrap order submission with sleeve validation and ownership."""
            # Estimate notional
            price = limit_price or stop_price or 0
            if not price:
                # Try to get current price from a snapshot
                try:
                    positions = sb.alpaca.list_positions()
                    for p in positions:
                        if p["symbol"] == symbol:
                            price = float(p.get("current_price", 0))
                            break
                except Exception:
                    pass
            notional = float(qty) * float(price) if price else float(qty) * 100

            # Validate entry orders (exits always allowed)
            if side.lower() == "buy":
                allowed, reason = self.validate_order(symbol, side, float(qty), notional)
                if not allowed:
                    log.warning(
                        f"[SIMPLE] Order REJECTED by sleeve: {side} {qty} {symbol} "
                        f"| reason: {reason}"
                    )
                    raise RuntimeError(f"Sleeve rejected order: {reason}")

            # Submit to broker via original method
            result = self._original_submit_order(
                symbol=symbol, qty=qty, side=side, order_type=order_type,
                limit_price=limit_price, stop_price=stop_price,
                trail_percent=trail_percent, time_in_force=time_in_force,
                extended_hours=extended_hours, order_class=order_class,
                take_profit=take_profit, stop_loss=stop_loss,
                client_order_id=client_order_id,
            )

            # Update ownership ledger
            if side.lower() == "buy":
                # Entry: register a new active ownership entry
                broker_order_id = result.get("id") if isinstance(result, dict) else None
                self.ledger.register_order(
                    strategy_id="SIMPLE",
                    symbol=symbol,
                    side=side,
                    qty=float(qty),
                    client_order_id=client_order_id or f"ENG_SIMPLE_unknown_{time.time()}",
                    broker_order_id=broker_order_id,
                    notional=notional,
                )

                # Probation: check fill price deviation
                if self.config.auto_halt_on_anomaly and result:
                    self._check_fill_anomaly(symbol, result, limit_price)
            else:
                # Exit (manual sell, EOD flatten, gradual reduction): mark
                # matching active ledger entries as closed. Bracket TP/SL
                # legs are submitted by Alpaca internally — they do NOT pass
                # through this function, so they still rely on the periodic
                # reconciler to close ledger entries. This branch handles
                # sells the strategy issues itself.
                for entry in self.ledger.get_active_entries("SIMPLE"):
                    if entry.symbol == symbol:
                        self.ledger.mark_closed(entry.client_order_id)
                        log.info(
                            f"[SIMPLE] Ownership closed on sell: {symbol} "
                            f"(was {entry.client_order_id[:40]})"
                        )

            return result

        sb.alpaca.submit_order = patched_submit_order

        # --- Patch 2: Order ID generation ---
        self._original_generate_order_id = sb.generate_client_order_id

        def patched_generate_order_id(symbol: str, bracket_type: str, date_str: str) -> str:
            """Generate order ID with ENG_SIMPLE_ prefix."""
            import hashlib
            timestamp = sb.dt.datetime.now().strftime("%H%M%S")
            random_suffix = hashlib.sha256(
                f"{symbol}_{time.time()}_{sb.os.getpid()}".encode()
            ).hexdigest()[:4]
            return f"ENG_SIMPLE_{symbol}_{bracket_type}_{date_str}_{timestamp}_{random_suffix}"

        sb.generate_client_order_id = patched_generate_order_id

        # --- Patch 3: Risk manager update (use sleeve equity, not total account) ---
        self._original_risk_update = sb.risk_manager.update

        def patched_risk_update():
            """
            Compute Simple Bot's daily P&L from its OWN positions, not from
            sleeve equity drift. Sleeve equity moves whenever Trend Bot wins
            or loses, which would otherwise cause false halts / missed halts
            that are unrelated to Simple's own trading.

            Halt logic: sum unrealized P&L across broker positions owned by
            SIMPLE (per the ownership ledger), and compare against
            MAX_DAILY_LOSS_PCT of the sleeve's start equity.

            NOTE: realized P&L from positions that closed earlier today is
            currently NOT tracked — if a losing scalp exits before this tick
            runs, its loss is forgotten. This is conservative in one
            direction (we halt earlier on open losses, not later on closed
            losses). A follow-up should track realized P&L via the ownership
            ledger's closed entries once the ledger stores exit fill prices.
            """
            rm = sb.risk_manager

            # Without a sleeve context we cannot safely update equity. Do NOT
            # fall back to total account equity — that would silently give
            # the probation strategy access to the full account.
            if self._current_ctx is None:
                log.debug("[SIMPLE] risk_update called without sleeve context — skipping equity update")
                return rm.halted

            # Apply intelligence risk multiplier to buying power (reduces
            # effective capital available for new positions in RISK_OFF/CRISIS)
            effective_available = self._current_ctx.sleeve_available
            mctx = getattr(self._current_ctx, 'market_context', None)
            if mctx:
                _adj = mctx.sleeve_adjustments.get("SIMPLE")
                if _adj:
                    effective_available *= _adj.risk_multiplier
            rm.buying_power = effective_available

            # Compute Simple's own unrealized P&L from broker positions
            unrealized_pnl = 0.0
            try:
                simple_symbols = self.ledger.get_active_symbols("SIMPLE")
                if simple_symbols:
                    broker_positions = sb.alpaca.list_positions()
                    for bp in broker_positions:
                        if bp["symbol"] in simple_symbols:
                            try:
                                unrealized_pnl += float(bp.get("unrealized_pl", 0) or 0)
                            except (TypeError, ValueError):
                                pass
            except Exception as e:
                log.debug(f"[SIMPLE] risk_update: unrealized P&L query failed: {e}")
                # On query failure, keep prior daily_pnl rather than zero it
                return rm.halted

            rm.daily_pnl = unrealized_pnl
            rm.current_equity = rm.start_equity + rm.daily_pnl

            # Check daily loss limit (same logic as original simple_bot)
            loss_pct = abs(rm.daily_pnl / rm.start_equity) if rm.start_equity > 0 else 0

            if rm.daily_pnl < 0 and loss_pct >= sb.MAX_DAILY_LOSS_PCT:
                if not rm.halted:
                    rm.halted = True
                    log.error(
                        f"[SIMPLE] DAILY LOSS LIMIT HIT | "
                        f"unrealized=${rm.daily_pnl:.2f} ({loss_pct:.1%}) of sleeve "
                        f"(start=${rm.start_equity:,.2f}) - HALTING"
                    )
                    rm._save_risk_state()

            return rm.halted

        sb.risk_manager.update = patched_risk_update

        # --- Patch 4: MomentumBot.shutdown() ---
        # The original shutdown uses an exclusion filter: it keeps everything
        # that does NOT start with ("dir_", "TBOT_"). Under the unified engine
        # this means ENG_TREND_ and ENG_XASSET_ orders would also be treated
        # as "ours" and cancelled. Replace with an inclusion filter that only
        # touches orders belonging to SIMPLE's prefix set.
        self._original_bot_shutdown = sb.MomentumBot.shutdown

        adapter_ref = self  # capture for closure

        def patched_bot_shutdown(bot_self):
            """Adapter-aware shutdown: only cancel/flatten SIMPLE-owned orders and positions."""
            log.warning("[SIMPLE][SHUTDOWN] Initiating adapter-aware shutdown...")
            policy = getattr(sb, "SHUTDOWN_POLICY", "CANCEL_ORDERS_ONLY")
            log.warning(f"[SIMPLE][SHUTDOWN] Policy: {policy}")

            prefixes = tuple(adapter_ref.config.all_prefixes())  # ("ENG_SIMPLE_", "dir_")

            try:
                # Cancel only SIMPLE's orders (by prefix inclusion)
                try:
                    all_orders = sb.alpaca.get_orders(status="open")
                    our_orders = [
                        o for o in all_orders
                        if (o.get("client_order_id") or "").startswith(prefixes)
                    ]
                    other_count = len(all_orders) - len(our_orders)
                    for order in our_orders:
                        try:
                            sb.alpaca.cancel_order(order["id"])
                        except Exception:
                            pass
                    log.info(
                        f"[SIMPLE][SHUTDOWN] Cancelled {len(our_orders)} SIMPLE order(s)"
                        f"{f' (preserved {other_count} other-sleeve orders)' if other_count else ''}"
                    )
                except Exception as e:
                    log.error(f"[SIMPLE][SHUTDOWN] Error cancelling orders: {e}")

                # Flatten only SIMPLE-owned positions (per ownership ledger)
                if policy == "FLATTEN_ALL":
                    simple_symbols = adapter_ref.ledger.get_active_symbols("SIMPLE")
                    if not simple_symbols:
                        log.info("[SIMPLE][SHUTDOWN] No SIMPLE-owned positions to flatten")
                    else:
                        try:
                            positions = sb.alpaca.list_positions()
                        except Exception as e:
                            log.error(f"[SIMPLE][SHUTDOWN] Could not fetch positions: {e}")
                            positions = []

                        our_positions = [p for p in positions if p["symbol"] in simple_symbols]
                        for pos in our_positions:
                            symbol = pos["symbol"]
                            try:
                                sb.alpaca.flatten_symbol(symbol)
                                log.info(f"[SIMPLE][SHUTDOWN] Flattened {symbol}")
                                # Sync ledger
                                for entry in adapter_ref.ledger.get_active_entries("SIMPLE"):
                                    if entry.symbol == symbol:
                                        adapter_ref.ledger.mark_closed(entry.client_order_id)
                            except Exception as e:
                                log.error(f"[SIMPLE][SHUTDOWN] Failed to flatten {symbol}: {e}")
                        log.warning(
                            f"[SIMPLE][SHUTDOWN] Flattened {len(our_positions)} SIMPLE position(s)"
                            f" (preserved {len(positions) - len(our_positions)} other-sleeve positions)"
                        )
                else:
                    try:
                        positions = sb.alpaca.list_positions()
                        simple_symbols = adapter_ref.ledger.get_active_symbols("SIMPLE")
                        our_count = sum(1 for p in positions if p["symbol"] in simple_symbols)
                        if our_count:
                            log.warning(f"[SIMPLE][SHUTDOWN] Leaving {our_count} SIMPLE position(s) open (policy=CANCEL_ORDERS_ONLY)")
                    except Exception:
                        pass

                # Flush scan diagnostics
                try:
                    sb.scan_diagnostics_logger.flush_if_needed()
                except Exception:
                    pass

                log.info("[SIMPLE][SHUTDOWN] Shutdown complete")

            except Exception as e:
                log.error(f"[SIMPLE][SHUTDOWN] Error during shutdown: {e}")

        sb.MomentumBot.shutdown = patched_bot_shutdown

        log.info("[SIMPLE] Monkey-patches applied (submit_order, order_id, risk_update, shutdown)")

    def _remove_patches(self) -> None:
        """Restore original functions."""
        sb = self._simple_module
        if not sb:
            return

        if self._original_submit_order:
            sb.alpaca.submit_order = self._original_submit_order
        if self._original_generate_order_id:
            sb.generate_client_order_id = self._original_generate_order_id
        if self._original_risk_update:
            sb.risk_manager.update = self._original_risk_update
        if self._original_bot_shutdown:
            sb.MomentumBot.shutdown = self._original_bot_shutdown

        log.info("[SIMPLE] Monkey-patches removed")

    # =========================================================================
    # PROBATION CONSTANT OVERRIDES
    # =========================================================================

    def _apply_constant_overrides(self, ctx: SleeveContext) -> None:
        """
        Override simple_bot module constants for probation mode.

        Saves originals for restoration on shutdown.
        """
        sb = self._simple_module

        if not ctx.config.probation:
            log.info("[SIMPLE] Not in probation mode — using default constants")
            return

        overrides = {
            "MAX_POSITIONS": ctx.config.max_positions or 1,
            "MAX_DAILY_LOSS_PCT": ctx.config.max_daily_loss_pct,
            # Use most of the small sleeve for one position
            "POSITION_SIZE_PCT": 0.80,
        }

        for name, new_value in overrides.items():
            if hasattr(sb, name):
                self._original_constants[name] = getattr(sb, name)
                setattr(sb, name, new_value)
                log.info(f"[SIMPLE] Probation override: {name} = {new_value} "
                         f"(was {self._original_constants[name]})")

        # Also override risk_manager equity to use sleeve equity
        # This happens in initialize() after risk_manager is created
        log.info(f"[SIMPLE] Probation: sleeve_equity=${ctx.sleeve_equity:,.2f}, "
                 f"max_positions={ctx.config.max_positions}, "
                 f"max_daily_loss={ctx.config.max_daily_loss_pct:.0%}")

    def _restore_constants(self) -> None:
        """Restore original constants."""
        sb = self._simple_module
        if not sb:
            return

        for name, original_value in self._original_constants.items():
            if hasattr(sb, name):
                setattr(sb, name, original_value)

        if self._original_constants:
            log.info(f"[SIMPLE] Restored {len(self._original_constants)} original constants")
        self._original_constants.clear()

    # =========================================================================
    # RISK MANAGER EQUITY OVERRIDE
    # =========================================================================

    def _override_risk_manager_equity(self, ctx: SleeveContext, sb) -> None:
        """
        Override the risk manager's equity values to use sleeve equity.

        The risk manager reads total account equity from Alpaca, but under the
        engine it should only track the SIMPLE sleeve's allocation. Without this,
        the risk manager sees a massive "daily loss" because it compares total
        account equity against a stale persisted start_equity.

        IMPORTANT: This does NOT clear rm.halted. If RiskManager.initialize()
        restored a same-day halt from the state file, it's a legitimate
        loss-limit halt and must persist across restarts — otherwise any
        engine restart during trading hours would bypass the daily loss limit.
        The old "clear stale halt" comment was obsolete: stale halts from
        pre-sleeve total-equity comparison can no longer occur because the
        sleeve override runs every tick via patched_risk_update().
        """
        rm = sb.risk_manager

        # Force sleeve equity as the baseline
        old_start = rm.start_equity
        old_current = rm.current_equity
        preserved_halt = rm.halted

        rm.start_equity = ctx.sleeve_equity
        rm.current_equity = ctx.sleeve_equity
        rm.buying_power = ctx.sleeve_available
        rm.daily_pnl = 0.0  # Start fresh — no PnL within the sleeve yet
        # rm.halted intentionally NOT reset — see docstring
        # rm.daily_trade_count intentionally NOT reset — same-day restart
        #   must not bypass any per-day trade cap the strategy enforces

        # Save corrected state so it persists across restarts
        rm._save_risk_state()

        if preserved_halt:
            log.warning(
                "[SIMPLE] Risk manager halted state preserved across restart — "
                "legitimate same-day halt from prior session. "
                "Clear the halt in the state file if intentional."
            )

        log.info(
            f"[SIMPLE] Risk manager equity override: "
            f"start_equity ${old_start:,.2f} -> ${ctx.sleeve_equity:,.2f}, "
            f"current ${old_current:,.2f} -> ${ctx.sleeve_equity:,.2f}, "
            f"daily_pnl reset to $0.00, halted={rm.halted}"
        )

    def _refresh_risk_manager_equity(self, ctx: SleeveContext, sb) -> None:
        """
        Refresh risk manager equity each tick to use sleeve values.

        We monkey-patch rm.update() because the original calls
        alpaca.get_account() and overwrites current_equity with total
        account equity. Our patched version uses sleeve equity instead.
        """
        rm = sb.risk_manager
        rm.current_equity = ctx.sleeve_equity
        rm.buying_power = ctx.sleeve_available
        rm.daily_pnl = rm.current_equity - rm.start_equity

    # =========================================================================
    # ANOMALY DETECTION
    # =========================================================================

    def _check_fill_anomaly(self, symbol: str, order_result: dict, expected_price: float) -> None:
        """
        Check for fill price anomalies (probation auto-halt).

        If fill deviates more than max_fill_deviation_pct from expected,
        halt the strategy.
        """
        if not expected_price or not order_result:
            return

        fill_price = None
        if isinstance(order_result, dict):
            fill_price = order_result.get("filled_avg_price")
            if fill_price:
                fill_price = float(fill_price)

        if not fill_price:
            return  # no fill yet — will be checked later

        deviation = abs(fill_price - expected_price) / expected_price
        if deviation > self.config.max_fill_deviation_pct:
            reason = (
                f"Fill price anomaly on {symbol}: "
                f"expected ${expected_price:.2f}, got ${fill_price:.2f} "
                f"({deviation:.1%} deviation, threshold {self.config.max_fill_deviation_pct:.1%})"
            )
            log.critical(f"[SIMPLE] ANOMALY: {reason}")
            self.halt(reason)
