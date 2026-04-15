"""
Trend Bot Adapter — wraps trend_bot.py internals for the unified engine.

This adapter:
1. Replicates trend_bot.main() startup logic
2. Runs the main loop body as tick() calls
3. Monkey-patches order submission for sleeve validation + ownership registration
4. Monkey-patches equity queries to return sleeve equity
5. Overrides order ID generation to use ENG_TREND_ prefix

ZERO edits to trend_bot.py — all injection via monkey-patching.
"""

import logging
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Optional, Set

from adapters.base import StrategyAdapter, TickResult
from engine.config import SleeveConfig
from engine.ownership import OwnershipLedger
from engine.sleeves import SleeveContext, SleeveManager

try:
    from engine import atd_notify
except ImportError:
    atd_notify = None  # ATD trade notifications disabled

log = logging.getLogger("Engine")

# Minimum seconds between trend ticks (trend bot internally sleeps 60s)
TREND_TICK_INTERVAL_SEC = 55


class TrendAdapter(StrategyAdapter):
    """
    Wraps the existing trend_bot.py without modifying it.

    Injection strategy:
    - Monkey-patch trading.submit_order() → validate sleeve + register ownership
    - Monkey-patch generate_client_order_id() → ENG_TREND_ prefix
    - Monkey-patch get_portfolio_equity() → return sleeve equity
    - Monkey-patch get_positions() → filter to TREND-owned symbols via ledger
    - Rate-limit tick() to ~60s (trend bot's natural cadence)
    """

    def __init__(
        self,
        config: SleeveConfig,
        ledger: OwnershipLedger,
        sleeve_manager: SleeveManager,
    ):
        super().__init__(config, ledger, sleeve_manager)

        # Trend bot internals (set during initialize)
        self._trading = None          # TradingClient
        self._data_client = None      # StockHistoricalDataClient
        self._state = None            # BotState
        self._trend_module = None     # the trend_bot module itself

        # Rate limiting
        self._last_tick_time: float = 0.0

        # Current sleeve context (updated each tick)
        self._current_ctx: Optional[SleeveContext] = None

        # Original functions (saved before monkey-patching)
        self._original_submit_order = None
        self._original_generate_order_id = None
        self._original_get_portfolio_equity = None
        self._original_emergency_shutdown = None
        self._original_get_account = None
        self._original_get_positions = None

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def initialize(self, ctx: SleeveContext) -> None:
        """
        Replicate trend_bot.main() startup (lines 6396-6428).

        Does NOT start the main loop — that's handled by tick().
        """
        log.info(f"[TREND] Initializing adapter (sleeve: ${ctx.sleeve_equity:,.2f})")
        self._current_ctx = ctx

        # Import trend_bot module
        import trend_bot as tb
        self._trend_module = tb

        # Validate configuration (same as main() line 6397)
        tb.validate_configuration()

        # Initialize clients (lines 6400-6401)
        self._trading = tb.get_trading_client()
        self._data_client = tb.get_data_client()

        # Validate API credentials (lines 6404-6414)
        log.info("[TREND] Validating API credentials...")
        try:
            account = self._trading.get_account()
            log.info(
                f"[TREND] API valid | Account: {account.account_number} | "
                f"Equity: ${float(account.equity):,.2f} | Status: {account.status}"
            )
        except Exception as e:
            raise RuntimeError(f"[TREND] API credential validation failed: {e}")

        # Load state (line 6416)
        self._state = tb.load_state(tb.STATE_PATH)

        # Reset contaminated equity_peak from pre-sleeve era.
        # Heuristic: if equity_peak is >1.3x current sleeve equity, it's
        # almost certainly a leftover total-account value from before the engine
        # wrapped trend_bot in a 75% sleeve. Reset to current sleeve equity so
        # drawdown math (get_deployment_capital) uses the right baseline.
        if self._state.equity_peak is not None and ctx.sleeve_equity > 0:
            ratio = self._state.equity_peak / ctx.sleeve_equity
            if ratio > 1.3:
                old_peak = self._state.equity_peak
                self._state.equity_peak = ctx.sleeve_equity
                log.warning(
                    f"[TREND] equity_peak contamination detected: "
                    f"${old_peak:,.2f} -> ${ctx.sleeve_equity:,.2f} "
                    f"(ratio {ratio:.2f}x sleeve, pre-sleeve leftover). Resetting."
                )
                tb.save_state(tb.STATE_PATH, self._state)

        # Clear stale rebalance flag (line 6419)
        tb.clear_stale_rebalance_flag(self._state, tb.STATE_PATH)

        log.info(f"[TREND] State: last_rebalance={self._state.last_rebalance_date_iso}, "
                 f"regime={self._state.spy_regime}")

        # Apply monkey-patches
        self._apply_patches()

        log.info("[TREND] Adapter initialized successfully")

    def tick(self, ctx: SleeveContext) -> TickResult:
        """
        Execute one iteration of trend_bot's main loop body (lines 6502-6598).

        Rate-limited to ~60s to match trend bot's natural cadence.
        """
        # Rate limit
        elapsed = time.time() - self._last_tick_time
        if elapsed < TREND_TICK_INTERVAL_SEC:
            return TickResult.SKIPPED

        if self.is_halted:
            return TickResult.HALTED

        self._current_ctx = ctx
        self._last_tick_time = time.time()
        tb = self._trend_module

        try:
            dt = tb.now_et()

            # ---- Kill switch (line 6507) ----
            kill_triggered, kill_reason = tb.kill_switch.is_triggered()
            if kill_triggered:
                log.error(f"[TREND] Kill switch triggered: {kill_reason}")
                tb.kill_switch.execute_emergency_shutdown(self._trading)
                self.halt(f"kill switch: {kill_reason}")
                return TickResult.HALTED

            # ---- Circuit breaker (line 6520) ----
            if tb.circuit_breaker.is_halted():
                log.warning(
                    f"[TREND] Circuit breaker halted: {tb.circuit_breaker.get_halt_reason()}"
                )
                return TickResult.SKIPPED

            # ---- Daily monitoring (line 6526) ----
            if (tb.ENABLE_DAILY_MONITORING
                    and tb.is_daily_monitoring_window(dt)
                    and not self._state.rebalance_in_progress):
                try:
                    tb.daily_position_monitoring(
                        self._trading, self._data_client, self._state
                    )
                except Exception as e:
                    log.error(f"[TREND] Daily monitoring error: {e}")
                    traceback.print_exc()

            # ---- Drift mini-rebalance (lines 6534-6566) ----
            drift_ready = True
            if self._state.last_drift_mini_iso:
                try:
                    from datetime import datetime as _dt
                    last_drift = _dt.fromisoformat(self._state.last_drift_mini_iso)
                    minutes_since = (tb.now_et() - last_drift).total_seconds() / 60
                    if minutes_since < tb.DRIFT_CHECK_INTERVAL_MIN:
                        drift_ready = False
                except Exception:
                    pass

            if (tb.ENABLE_DRIFT_MINI_REBALANCE
                    and drift_ready
                    and tb.is_market_open(self._trading)
                    and not self._state.rebalance_in_progress
                    and not tb.is_rebalance_window_dynamic(self._trading)):
                try:
                    positions = tb.get_positions(self._trading)
                    total_equity = tb.get_portfolio_equity(self._trading)
                    needs_mini, max_drift, drift_reason = tb.check_drift_mini_rebalance_needed(
                        self._state, positions, total_equity, self._trading
                    )
                    if needs_mini:
                        log.info(f"[TREND] Drift mini-rebalance triggered: {drift_reason}")
                        max_sym = drift_reason.split()[0] if drift_reason else "unknown"
                        try:
                            tb.execute_drift_mini_rebalance(
                                self._trading, self._data_client, self._state, max_sym
                            )
                        finally:
                            # Always throttle — a mid-function failure in
                            # execute_drift_mini_rebalance must not cause retry
                            # storms on the ~60s trend tick cadence. On success
                            # the function already wrote this; the overwrite is
                            # harmless.
                            self._state.last_drift_mini_iso = tb.now_et().isoformat()
                            tb.save_state(tb.STATE_PATH, self._state)
                except Exception as e:
                    log.error(f"[TREND] Drift check error: {e}")

            # ---- Weekly rebalance (lines 6568-6597) ----
            already_done_today = (
                self._state.last_rebalance_date_iso == dt.date().isoformat()
            )

            if (tb.is_rebalance_window_dynamic(self._trading)
                    and not already_done_today
                    and not self._state.rebalance_in_progress):
                log.info("[TREND] Rebalance window open. Running rebalance...")
                try:
                    tb.rebalance(self._trading, self._data_client, self._state)
                except Exception as e:
                    log.error(f"[TREND] Rebalance error: {e}")
                    tb.circuit_breaker.record_api_failure("Rebalance")
                    tb.alerter.send_alert(
                        level="CRITICAL",
                        title="Rebalance Failed",
                        message=f"Weekly rebalance failed: {str(e)}",
                        context={"error": str(e), "date": dt.date().isoformat()}
                    )
                    raise  # let engine catch and record
            else:
                # Heartbeat
                cooldown = self._state.drawdown_cooldown_until_iso or "none"
                rebal_status = " [IN PROGRESS]" if self._state.rebalance_in_progress else ""
                log.debug(
                    f"[TREND] Waiting{rebal_status}... "
                    f"last_rebalance={self._state.last_rebalance_date_iso}, "
                    f"cooldown_until={cooldown}"
                )

            self.record_success()
            return TickResult.OK

        except Exception as e:
            self.record_error(e)
            log.error(f"[TREND] Tick error: {e}")
            traceback.print_exc()
            return TickResult.ERROR

    def shutdown(self) -> None:
        """Graceful shutdown — save state, cancel own orders if needed."""
        log.info("[TREND] Shutting down...")
        tb = self._trend_module
        if tb and self._state:
            try:
                tb.save_state(tb.STATE_PATH, self._state)
                log.info("[TREND] State saved")
            except Exception as e:
                log.error(f"[TREND] Failed to save state: {e}")

        # Restore original functions
        self._remove_patches()
        log.info("[TREND] Adapter shutdown complete")

    def get_owned_symbols(self) -> Set[str]:
        """Get symbols currently owned by trend strategy."""
        return self.ledger.get_active_symbols("TREND")

    # =========================================================================
    # MONKEY-PATCHING
    # =========================================================================

    def _apply_patches(self) -> None:
        """
        Apply monkey-patches to trend_bot module for sleeve integration.

        Patches:
        1. trading.submit_order() → validate sleeve + register ownership
        2. generate_client_order_id() → ENG_TREND_ prefix
        3. get_portfolio_equity() → sleeve equity
        """
        tb = self._trend_module

        # --- Patch 1: Order submission ---
        self._original_submit_order = self._trading.submit_order

        def patched_submit_order(order_request):
            """Wrap order submission with sleeve validation and ownership registration."""
            symbol = order_request.symbol
            qty = float(order_request.qty)
            side = str(order_request.side.value) if hasattr(order_request.side, 'value') else str(order_request.side)
            client_oid = order_request.client_order_id or ""

            # Generate synthetic client_order_id if missing (e.g., drift mini-rebalance)
            if not client_oid:
                ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                short_id = uuid.uuid4().hex[:6]
                client_oid = f"ENG_TREND_drift_{symbol}_{side}_{ts}_{short_id}"
                order_request.client_order_id = client_oid
                log.info(f"[TREND] Generated synthetic order ID: {client_oid}")

            # Estimate notional
            notional = 0.0
            try:
                pos = self._trading.get_open_position(symbol)
                price = float(pos.current_price)
                notional = qty * price
            except Exception:
                # No position — use a rough estimate
                notional = qty * 100  # conservative fallback

            # Validate (exits always allowed)
            if "buy" in side.lower():
                allowed, reason = self.validate_order(symbol, side, qty, notional)
                if not allowed:
                    log.warning(
                        f"[TREND] Order REJECTED by sleeve: {side} {qty:.4f} {symbol} "
                        f"| reason: {reason}"
                    )
                    raise RuntimeError(f"Sleeve rejected order: {reason}")

            # Submit to broker
            result = self._original_submit_order(order_request)

            # Best-effort price at time of fill (for ATD notifications + ledger)
            _order_price = None
            try:
                if result and result.filled_avg_price:
                    _order_price = float(result.filled_avg_price)
            except (AttributeError, TypeError, ValueError):
                pass
            if _order_price is None:
                try:
                    _pos = self._trading.get_open_position(symbol)
                    _order_price = float(_pos.current_price)
                except Exception:
                    _order_price = notional / qty if qty > 0 else 0.0

            # Update ownership ledger
            broker_order_id = str(result.id) if result and hasattr(result, 'id') else None

            if "sell" in side.lower():
                # SELL = closing a position → mark existing entries as closed
                for entry in self.ledger.get_active_entries("TREND"):
                    if entry.symbol == symbol:
                        # Capture entry data before closing for ATD notification
                        entry_qty = entry.fill_qty or entry.qty
                        entry_price = entry.fill_price or (
                            entry.notional_at_entry / entry.qty
                            if entry.qty > 0 else 0.0
                        )
                        entry_time = entry.registered_at

                        self.ledger.mark_closed(entry.client_order_id)
                        log.info(f"[TREND] Ownership closed: {symbol} (was {entry.client_order_id[:40]})")

                        # Notify ATD of exit (fire-and-forget)
                        if atd_notify and _order_price:
                            try:
                                pnl = (_order_price - entry_price) * entry_qty
                                atd_notify.notify_exit(
                                    symbol=symbol,
                                    side=entry.side,
                                    entry_price=entry_price,
                                    exit_price=_order_price,
                                    quantity=entry_qty,
                                    pnl=pnl,
                                    entry_time=entry_time,
                                )
                            except Exception as e:
                                log.debug(f"[ATD] Exit notification error: {e}")
            else:
                # BUY = opening/adding to position → register new entry
                entry_obj = self.ledger.register_order(
                    strategy_id="TREND",
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    client_order_id=client_oid,
                    broker_order_id=broker_order_id,
                    notional=notional,
                )
                # Store best-effort fill price for accurate exit P&L later
                if _order_price and entry_obj:
                    entry_obj.fill_price = _order_price
                    entry_obj.fill_qty = qty

                # Notify ATD of entry (fire-and-forget)
                if atd_notify and _order_price:
                    try:
                        atd_notify.notify_entry(
                            symbol=symbol,
                            side=side,
                            entry_price=_order_price,
                            quantity=qty,
                        )
                    except Exception as e:
                        log.debug(f"[ATD] Entry notification error: {e}")

            return result

        self._trading.submit_order = patched_submit_order

        # --- Patch 2: Order ID generation ---
        self._original_generate_order_id = tb.generate_client_order_id

        def patched_generate_order_id(reason: str, symbol: str, side: str,
                                       date_str=None) -> str:
            """Generate order ID with ENG_TREND_ prefix instead of TBOT_."""
            original_id = self._original_generate_order_id(reason, symbol, side, date_str)
            # Replace TBOT_ prefix with ENG_TREND_
            if original_id.startswith("TBOT_"):
                return "ENG_TREND_" + original_id[5:]
            return "ENG_TREND_" + original_id

        tb.generate_client_order_id = patched_generate_order_id

        # --- Patch 3: Portfolio equity ---
        self._original_get_portfolio_equity = tb.get_portfolio_equity

        def patched_get_portfolio_equity(trading_client):
            """Return sleeve equity (scaled by intelligence risk multiplier)."""
            if self._current_ctx:
                base_equity = self._current_ctx.sleeve_equity
                # Apply intelligence risk multiplier (reduces effective equity
                # in RISK_OFF/CRISIS, which naturally shrinks position sizes)
                mctx = getattr(self._current_ctx, 'market_context', None)
                if mctx:
                    adj = mctx.sleeve_adjustments.get("TREND")
                    if adj:
                        base_equity *= adj.risk_multiplier
                return base_equity
            # Fallback to real equity if no context
            return self._original_get_portfolio_equity(trading_client)

        tb.get_portfolio_equity = patched_get_portfolio_equity

        # --- Patch 4: Emergency shutdown (kill switch) ---
        # The built-in KillSwitch filters by the legacy "TBOT_" prefix, but the
        # adapter rewrites every order ID to "ENG_TREND_", so the emergency
        # shutdown finds zero orders under the unified engine. Replace it with
        # an adapter-aware version that uses this sleeve's full prefix set and
        # only touches TREND-owned positions per the ownership ledger.
        self._original_emergency_shutdown = tb.kill_switch.execute_emergency_shutdown

        def patched_emergency_shutdown(trading_client):
            """Adapter-aware emergency shutdown for unified-engine mode.

            - Cancels orders matching any of this sleeve's prefixes
              (ENG_TREND_ + legacy TBOT_).
            - Closes only TREND-owned positions per ownership ledger.
            - Updates ledger on every close so post-shutdown state is accurate.
            - Does NOT fall back to trading.cancel_orders() — that would
              cancel SIMPLE's orders on the shared account.
            """
            log.error("[KILL_SWITCH] EMERGENCY SHUTDOWN INITIATED (unified-engine)")
            try:
                if getattr(tb, "DRY_RUN", False):
                    log.warning("[KILL_SWITCH] DRY_RUN — skipping actual cancels/closes")
                    return

                prefixes = tuple(self.config.all_prefixes())

                # 1. Cancel our orders
                try:
                    all_orders = trading_client.get_orders()
                    our_orders = [
                        o for o in all_orders
                        if (o.client_order_id or "").startswith(prefixes)
                    ]
                    for order in our_orders:
                        try:
                            trading_client.cancel_order_by_id(order.id)
                        except Exception:
                            pass
                    log.warning(
                        f"[KILL_SWITCH] Cancelled {len(our_orders)} TREND orders "
                        f"(preserved {len(all_orders) - len(our_orders)} other-sleeve orders)"
                    )
                except Exception as e:
                    log.error(f"[KILL_SWITCH] Error cancelling TREND orders: {e}")
                    # Intentionally no fallback to trading_client.cancel_orders() —
                    # that would cancel SIMPLE's orders on the shared account.

                # 2. Flatten if policy demands it
                if getattr(tb, "SHUTDOWN_POLICY", "CANCEL_ORDERS_ONLY") == "FLATTEN_ALL":
                    trend_symbols = self.ledger.get_active_symbols("TREND")
                    if not trend_symbols:
                        log.info("[KILL_SWITCH] No TREND-owned positions to close")
                        return
                    try:
                        positions = trading_client.get_all_positions()
                    except Exception as e:
                        log.error(f"[KILL_SWITCH] Could not fetch positions: {e}")
                        return

                    our_positions = [p for p in positions if p.symbol in trend_symbols]
                    closed_count = 0
                    for pos in our_positions:
                        try:
                            trading_client.close_position(pos.symbol)
                            closed_count += 1
                            # Sync ledger
                            for entry in self.ledger.get_active_entries("TREND"):
                                if entry.symbol == pos.symbol:
                                    self.ledger.mark_closed(entry.client_order_id)
                        except Exception as e:
                            log.error(f"[KILL_SWITCH] Failed to close {pos.symbol}: {e}")
                    log.warning(
                        f"[KILL_SWITCH] Closed {closed_count} TREND positions "
                        f"(preserved {len(positions) - len(our_positions)} other-sleeve positions)"
                    )
            except Exception as e:
                log.error(f"[KILL_SWITCH] Unexpected error in emergency shutdown: {e}")

        tb.kill_switch.execute_emergency_shutdown = patched_emergency_shutdown

        # --- Patch 5: Sleeve-scoped trading.get_account() ---
        # trend_bot.py calls trading.get_account().buying_power directly at
        # two BP pre-flight sites (drift mini and main rebalance), bypassing
        # the sleeve abstraction. Wrap get_account so .buying_power returns
        # ctx.sleeve_available instead of the unsleeved total account BP.
        self._original_get_account = self._trading.get_account

        class _SleeveAccountProxy:
            """Read-only proxy exposing sleeve-scoped buying_power."""
            def __init__(self, real_account, sleeve_buying_power):
                object.__setattr__(self, "_real", real_account)
                object.__setattr__(self, "_sleeve_bp", sleeve_buying_power)

            def __getattr__(self, name):
                if name == "buying_power":
                    return self._sleeve_bp
                return getattr(self._real, name)

        def patched_get_account():
            acct = self._original_get_account()
            if self._current_ctx is None:
                # No sleeve context yet (pre-init or post-shutdown) — passthrough.
                return acct
            return _SleeveAccountProxy(acct, self._current_ctx.sleeve_available)

        self._trading.get_account = patched_get_account

        # --- Patch 6: Sleeve-scoped get_positions() ---
        # trend_bot.get_positions() reads trading.get_all_positions() directly,
        # picking up positions owned by other sleeves (e.g., CROSSASSET's DBC).
        # That produces phantom drift (a symbol TREND used to own but CROSSASSET
        # now holds looks massively off-target) and triggers spurious mini-
        # rebalances that cascade into correlated-buy storms (2026-04-15 SMH/
        # SOXX drift buys were triggered this way by stale DBC targets).
        # Filter to the ownership ledger so TREND sees only what TREND owns.
        self._original_get_positions = tb.get_positions

        def patched_get_positions(trading_client):
            all_positions = self._original_get_positions(trading_client)
            trend_symbols = self.ledger.get_active_symbols("TREND")
            return {sym: data for sym, data in all_positions.items() if sym in trend_symbols}

        tb.get_positions = patched_get_positions

        log.info("[TREND] Monkey-patches applied (submit_order, order_id, equity, kill_switch, get_account, get_positions)")

    def _remove_patches(self) -> None:
        """Restore original functions."""
        tb = self._trend_module
        if not tb:
            return

        if self._original_submit_order and self._trading:
            self._trading.submit_order = self._original_submit_order
        if self._original_generate_order_id:
            tb.generate_client_order_id = self._original_generate_order_id
        if self._original_get_portfolio_equity:
            tb.get_portfolio_equity = self._original_get_portfolio_equity
        if self._original_emergency_shutdown and hasattr(tb, "kill_switch"):
            tb.kill_switch.execute_emergency_shutdown = self._original_emergency_shutdown
        if self._original_get_account and self._trading:
            self._trading.get_account = self._original_get_account
        if self._original_get_positions:
            tb.get_positions = self._original_get_positions

        log.info("[TREND] Monkey-patches removed")
