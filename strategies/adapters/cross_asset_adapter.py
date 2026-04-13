"""
Cross-Asset Bot Adapter — wraps cross_asset_bot.py for the unified engine.

This adapter:
1. Replicates cross_asset_bot.main() startup logic
2. Runs the main loop body as tick() calls
3. Monkey-patches order submission for sleeve validation + ownership registration
4. Monkey-patches equity queries to return sleeve equity
5. Overrides order ID generation to use ENG_XASSET_ prefix

ZERO edits to cross_asset_bot.py — all injection via monkey-patching.
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

log = logging.getLogger("Engine")

# Cross-asset ticks every 5 minutes (weekly rebalance, most ticks are skipped)
CROSSASSET_TICK_INTERVAL_SEC = 300

# Drift threshold for inter-rebalance mini-rebalance
DRIFT_THRESHOLD_PCT = 0.10  # 10% weight drift triggers rebalance


class CrossAssetAdapter(StrategyAdapter):
    """
    Wraps cross_asset_bot.py without modifying it.

    Injection strategy:
    - Monkey-patch trading.submit_order() -> validate sleeve + register ownership
    - Monkey-patch generate_client_order_id() -> ENG_XASSET_ prefix
    - Monkey-patch get_portfolio_equity() -> return sleeve equity
    - Monkey-patch kill_switch.execute_emergency_shutdown() -> adapter-aware
    - Monkey-patch trading.get_account() -> sleeve-scoped buying_power
    - Rate-limit tick() to ~5 min (weekly rebalance doesn't need fast ticks)
    """

    def __init__(
        self,
        config: SleeveConfig,
        ledger: OwnershipLedger,
        sleeve_manager: SleeveManager,
    ):
        super().__init__(config, ledger, sleeve_manager)

        # Bot internals (set during initialize)
        self._trading = None          # TradingClient
        self._data_client = None      # StockHistoricalDataClient
        self._state = None            # CrossAssetState
        self._xasset_module = None    # the cross_asset_bot module

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

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def initialize(self, ctx: SleeveContext) -> None:
        """Initialize cross-asset bot and apply monkey-patches."""
        log.info(f"[XASSET] Initializing adapter (sleeve: ${ctx.sleeve_equity:,.2f})")
        self._current_ctx = ctx

        # Import cross_asset_bot module
        import cross_asset_bot as xab
        self._xasset_module = xab

        # Validate configuration
        xab.validate_configuration()

        # Initialize clients
        self._trading = xab.get_trading_client()
        self._data_client = xab.get_data_client()

        # Validate API credentials
        log.info("[XASSET] Validating API credentials...")
        try:
            account = self._trading.get_account()
            log.info(
                f"[XASSET] API valid | Equity: ${float(account.equity):,.2f}"
            )
        except Exception as e:
            raise RuntimeError(f"[XASSET] API credential validation failed: {e}")

        # Load state
        self._state = xab.load_state(xab.STATE_PATH)

        # Reset contaminated equity_peak from pre-sleeve era
        if self._state.equity_peak is not None and ctx.sleeve_equity > 0:
            ratio = self._state.equity_peak / ctx.sleeve_equity
            if ratio > 1.3:
                old_peak = self._state.equity_peak
                self._state.equity_peak = ctx.sleeve_equity
                log.warning(
                    f"[XASSET] equity_peak contamination: "
                    f"${old_peak:,.2f} -> ${ctx.sleeve_equity:,.2f} (ratio {ratio:.2f}x)"
                )
                xab.save_state(xab.STATE_PATH, self._state)

        # Clear stale rebalance flag
        xab.clear_stale_rebalance_flag(self._state, xab.STATE_PATH)

        log.info(f"[XASSET] State: last_rebalance={self._state.last_rebalance_date_iso}, "
                 f"halted={self._state.portfolio_halted}")

        # Apply monkey-patches
        self._apply_patches()

        log.info("[XASSET] Adapter initialized successfully")

    def tick(self, ctx: SleeveContext) -> TickResult:
        """
        Execute one iteration of cross_asset_bot's main loop.

        Rate-limited to 5 minutes (weekly rebalance doesn't need fast ticks).
        """
        # Rate limit
        elapsed = time.time() - self._last_tick_time
        if elapsed < CROSSASSET_TICK_INTERVAL_SEC:
            return TickResult.SKIPPED

        if self.is_halted:
            return TickResult.HALTED

        self._current_ctx = ctx
        self._last_tick_time = time.time()
        xab = self._xasset_module

        try:
            # ---- Kill switch ----
            ks_triggered, ks_reason = xab.kill_switch.is_triggered()
            if ks_triggered:
                log.error(f"[XASSET] Kill switch triggered: {ks_reason}")
                xab.kill_switch.execute_emergency_shutdown(self._trading)
                self.halt(f"kill switch: {ks_reason}")
                return TickResult.HALTED

            # ---- Circuit breaker ----
            if xab.circuit_breaker.is_halted():
                log.warning("[XASSET] Circuit breaker halted")
                return TickResult.SKIPPED

            # ---- Portfolio halt ----
            if self._state.portfolio_halted:
                log.warning(f"[XASSET] Portfolio halted: {self._state.portfolio_halt_reason}")
                return TickResult.HALTED

            # ---- Rebalance window check ----
            already_done = (
                self._state.last_rebalance_date_iso == xab.now_et().date().isoformat()
            )

            if (xab.is_rebalance_window_dynamic(self._trading)
                    and not already_done
                    and not self._state.rebalance_in_progress):
                log.info("[XASSET] Rebalance window open. Running rebalance...")
                try:
                    xab.rebalance(self._trading, self._data_client, self._state)
                except Exception as e:
                    log.error(f"[XASSET] Rebalance error: {e}")
                    raise  # let engine catch and record

            # ---- Drift check (inter-rebalance, when market is open) ----
            elif xab.is_market_open(self._trading) and not self._state.rebalance_in_progress:
                self._check_drift(xab)

            self.record_success()
            return TickResult.OK

        except Exception as e:
            self.record_error(e)
            log.error(f"[XASSET] Tick error: {e}")
            traceback.print_exc()
            return TickResult.ERROR

    def _check_drift(self, xab) -> None:
        """Lightweight drift check between rebalances."""
        if not self._state.last_target_weights:
            return

        try:
            positions = xab.get_positions(self._trading)
            total_equity = xab.get_portfolio_equity(self._trading)
            if total_equity <= 0:
                return

            max_drift = 0.0
            max_drift_sym = ""
            for symbol, target_w in self._state.last_target_weights.items():
                current_mv = positions.get(symbol, {}).get("market_value", 0.0)
                current_w = current_mv / total_equity
                drift = abs(current_w - target_w)
                if drift > max_drift:
                    max_drift = drift
                    max_drift_sym = symbol

            if max_drift > DRIFT_THRESHOLD_PCT:
                log.info(
                    f"[XASSET] Drift detected: {max_drift_sym} drifted {max_drift:.1%} "
                    f"(threshold {DRIFT_THRESHOLD_PCT:.0%}). Triggering mini-rebalance."
                )
                try:
                    xab.rebalance(self._trading, self._data_client, self._state)
                finally:
                    # Always throttle — same pattern as trend_adapter drift fix
                    self._state.last_rebalance_date_iso = xab.now_et().date().isoformat()
                    xab.save_state(xab.STATE_PATH, self._state)
        except Exception as e:
            log.warning(f"[XASSET] Drift check error: {e}")

    def shutdown(self) -> None:
        """Graceful shutdown — save state, restore patches."""
        log.info("[XASSET] Shutting down...")
        xab = self._xasset_module
        if xab and self._state:
            try:
                xab.save_state(xab.STATE_PATH, self._state)
                log.info("[XASSET] State saved")
            except Exception as e:
                log.error(f"[XASSET] Failed to save state: {e}")

        self._remove_patches()
        log.info("[XASSET] Adapter shutdown complete")

    def get_owned_symbols(self) -> Set[str]:
        """Get symbols currently owned by cross-asset strategy."""
        return self.ledger.get_active_symbols("CROSSASSET")

    # =========================================================================
    # MONKEY-PATCHING
    # =========================================================================

    def _apply_patches(self) -> None:
        """
        Apply monkey-patches for sleeve integration.

        Patches:
        1. trading.submit_order() -> validate sleeve + register ownership
        2. generate_client_order_id() -> ENG_XASSET_ prefix
        3. get_portfolio_equity() -> sleeve equity
        4. kill_switch.execute_emergency_shutdown() -> adapter-aware
        5. trading.get_account() -> sleeve-scoped buying_power
        """
        xab = self._xasset_module

        # --- Patch 1: Order submission ---
        self._original_submit_order = self._trading.submit_order

        def patched_submit_order(order_request):
            """Wrap order submission with sleeve validation and ownership."""
            symbol = order_request.symbol
            qty = float(order_request.qty)
            side = str(order_request.side.value) if hasattr(order_request.side, "value") else str(order_request.side)
            client_oid = order_request.client_order_id or ""

            # Generate synthetic client_order_id if missing
            if not client_oid:
                ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                short_id = uuid.uuid4().hex[:6]
                client_oid = f"ENG_XASSET_{symbol}_{side}_{ts}_{short_id}"
                order_request.client_order_id = client_oid
                log.info(f"[XASSET] Generated synthetic order ID: {client_oid}")

            # Estimate notional
            notional = 0.0
            try:
                pos = self._trading.get_open_position(symbol)
                price = float(pos.current_price)
                notional = qty * price
            except Exception:
                notional = qty * 100  # conservative fallback

            # Validate entry orders (exits always allowed)
            if "buy" in side.lower():
                allowed, reason = self.validate_order(symbol, side, qty, notional)
                if not allowed:
                    log.warning(
                        f"[XASSET] Order REJECTED by sleeve: {side} {qty:.4f} {symbol} "
                        f"| reason: {reason}"
                    )
                    raise RuntimeError(f"Sleeve rejected order: {reason}")

            # Submit to broker
            result = self._original_submit_order(order_request)

            # Update ownership ledger
            broker_order_id = str(result.id) if result and hasattr(result, "id") else None

            if "sell" in side.lower():
                # SELL = closing -> mark existing entries as closed
                for entry in self.ledger.get_active_entries("CROSSASSET"):
                    if entry.symbol == symbol:
                        self.ledger.mark_closed(entry.client_order_id)
                        log.info(f"[XASSET] Ownership closed: {symbol}")
            else:
                # BUY = opening -> register new entry
                self.ledger.register_order(
                    strategy_id="CROSSASSET",
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    client_order_id=client_oid,
                    broker_order_id=broker_order_id,
                    notional=notional,
                )

            return result

        self._trading.submit_order = patched_submit_order

        # --- Patch 2: Order ID generation ---
        self._original_generate_order_id = xab.generate_client_order_id

        def patched_generate_order_id(reason: str, symbol: str, side: str) -> str:
            """Generate order ID with ENG_XASSET_ prefix instead of XABOT_."""
            original_id = self._original_generate_order_id(reason, symbol, side)
            if original_id.startswith("XABOT_"):
                return "ENG_XASSET_" + original_id[6:]
            return "ENG_XASSET_" + original_id

        xab.generate_client_order_id = patched_generate_order_id

        # --- Patch 3: Portfolio equity ---
        self._original_get_portfolio_equity = xab.get_portfolio_equity

        def patched_get_portfolio_equity(trading_client):
            """Return sleeve equity (scaled by intelligence risk multiplier)."""
            if self._current_ctx:
                base_equity = self._current_ctx.sleeve_equity
                mctx = getattr(self._current_ctx, 'market_context', None)
                if mctx:
                    adj = mctx.sleeve_adjustments.get("CROSSASSET")
                    if adj:
                        base_equity *= adj.risk_multiplier
                return base_equity
            return self._original_get_portfolio_equity(trading_client)

        xab.get_portfolio_equity = patched_get_portfolio_equity

        # --- Patch 4: Emergency shutdown (kill switch) ---
        self._original_emergency_shutdown = xab.kill_switch.execute_emergency_shutdown

        def patched_emergency_shutdown(trading_client):
            """Adapter-aware emergency shutdown for unified-engine mode.

            - Cancels orders matching ENG_XASSET_ and legacy XABOT_ prefixes.
            - Closes only CROSSASSET-owned positions per ownership ledger.
            - Does NOT fall back to cancel_orders() (would hit other sleeves).
            """
            log.error("[KILL_SWITCH] XASSET EMERGENCY SHUTDOWN (unified-engine)")
            try:
                if getattr(xab, "DRY_RUN", False):
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
                        f"[KILL_SWITCH] Cancelled {len(our_orders)} XASSET orders "
                        f"(preserved {len(all_orders) - len(our_orders)} other-sleeve orders)"
                    )
                except Exception as e:
                    log.error(f"[KILL_SWITCH] Error cancelling XASSET orders: {e}")

                # 2. Flatten if policy demands it
                if getattr(xab, "SHUTDOWN_POLICY", "CANCEL_ORDERS_ONLY") == "FLATTEN_ALL":
                    xasset_symbols = self.ledger.get_active_symbols("CROSSASSET")
                    if not xasset_symbols:
                        log.info("[KILL_SWITCH] No XASSET-owned positions to close")
                        return
                    try:
                        positions = trading_client.get_all_positions()
                    except Exception as e:
                        log.error(f"[KILL_SWITCH] Could not fetch positions: {e}")
                        return

                    our_positions = [p for p in positions if p.symbol in xasset_symbols]
                    closed_count = 0
                    for pos in our_positions:
                        try:
                            trading_client.close_position(pos.symbol)
                            closed_count += 1
                            for entry in self.ledger.get_active_entries("CROSSASSET"):
                                if entry.symbol == pos.symbol:
                                    self.ledger.mark_closed(entry.client_order_id)
                        except Exception as e:
                            log.error(f"[KILL_SWITCH] Failed to close {pos.symbol}: {e}")
                    log.warning(
                        f"[KILL_SWITCH] Closed {closed_count} XASSET positions "
                        f"(preserved {len(positions) - len(our_positions)} other-sleeve positions)"
                    )
            except Exception as e:
                log.error(f"[KILL_SWITCH] Unexpected error: {e}")

        xab.kill_switch.execute_emergency_shutdown = patched_emergency_shutdown

        # --- Patch 5: Sleeve-scoped trading.get_account() ---
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
                return acct
            return _SleeveAccountProxy(acct, self._current_ctx.sleeve_available)

        self._trading.get_account = patched_get_account

        log.info("[XASSET] Monkey-patches applied (submit_order, order_id, equity, kill_switch, get_account)")

    def _remove_patches(self) -> None:
        """Restore original functions."""
        xab = self._xasset_module
        if not xab:
            return

        if self._original_submit_order and self._trading:
            self._trading.submit_order = self._original_submit_order
        if self._original_generate_order_id:
            xab.generate_client_order_id = self._original_generate_order_id
        if self._original_get_portfolio_equity:
            xab.get_portfolio_equity = self._original_get_portfolio_equity
        if self._original_emergency_shutdown and hasattr(xab, "kill_switch"):
            xab.kill_switch.execute_emergency_shutdown = self._original_emergency_shutdown
        if self._original_get_account and self._trading:
            self._trading.get_account = self._original_get_account

        log.info("[XASSET] Monkey-patches removed")
