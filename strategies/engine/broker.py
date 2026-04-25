"""
Broker Facade — thin wrapper over alpaca-py for account-level queries.

This is used ONLY by the engine for shared account state (equity, positions,
orders). Each strategy adapter may still use its own broker client internally
for strategy-specific operations.
"""

import logging
import os
from typing import Dict, List, Optional

log = logging.getLogger("Engine")


class BrokerFacade:
    """
    Thin Alpaca broker facade for engine-level queries.

    Provides:
    - Account equity/buying power
    - All positions (across all strategies)
    - All open orders (across all strategies)
    - Market clock

    Does NOT provide order submission — that goes through strategy adapters
    with sleeve validation.
    """

    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        from alpaca.trading.client import TradingClient
        self._client = TradingClient(api_key=api_key, secret_key=secret_key, paper=paper)

        mode = "PAPER" if paper else "LIVE"
        log.info(f"[BROKER] Initialized Alpaca TradingClient ({mode})")

    # -------------------------------------------------------------------------
    # ACCOUNT
    # -------------------------------------------------------------------------

    def get_account(self) -> dict:
        """Get account info as a dict."""
        acct = self._client.get_account()
        return {
            "id": str(acct.id),
            "equity": float(acct.equity),
            "buying_power": float(acct.buying_power),
            "cash": float(acct.cash),
            "portfolio_value": float(acct.portfolio_value),
            "status": str(acct.status),
            "pattern_day_trader": acct.pattern_day_trader,
        }

    def get_equity(self) -> float:
        """Get total account equity."""
        acct = self._client.get_account()
        return float(acct.equity)

    def get_buying_power(self) -> float:
        """Get available buying power."""
        acct = self._client.get_account()
        return float(acct.buying_power)

    # -------------------------------------------------------------------------
    # POSITIONS
    # -------------------------------------------------------------------------

    def get_all_positions(self) -> List[dict]:
        """Get all positions from broker (across all strategies)."""
        positions = self._client.get_all_positions()
        result = []
        for pos in positions:
            result.append({
                "symbol": pos.symbol,
                "qty": float(pos.qty),
                "side": str(pos.side),
                "market_value": float(pos.market_value),
                "avg_entry_price": float(pos.avg_entry_price),
                "unrealized_pl": float(pos.unrealized_pl),
                "unrealized_plpc": float(pos.unrealized_plpc),
                "current_price": float(pos.current_price),
            })
        return result

    def get_position(self, symbol: str) -> Optional[dict]:
        """Get a single position by symbol, or None if not held."""
        try:
            pos = self._client.get_open_position(symbol)
            return {
                "symbol": pos.symbol,
                "qty": float(pos.qty),
                "side": str(pos.side),
                "market_value": float(pos.market_value),
                "avg_entry_price": float(pos.avg_entry_price),
                "unrealized_pl": float(pos.unrealized_pl),
                "current_price": float(pos.current_price),
            }
        except Exception:
            return None

    # -------------------------------------------------------------------------
    # ORDERS
    # -------------------------------------------------------------------------

    def get_all_open_orders(self) -> List[dict]:
        """Get all open orders from broker (across all strategies)."""
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus

        request = GetOrdersRequest(status=QueryOrderStatus.OPEN)
        orders = self._client.get_orders(filter=request)
        result = []
        for order in orders:
            result.append({
                "id": str(order.id),
                "client_order_id": order.client_order_id or "",
                "symbol": order.symbol,
                "qty": float(order.qty) if order.qty else 0,
                "filled_qty": float(order.filled_qty) if order.filled_qty else 0,
                "side": str(order.side),
                "type": str(order.type),
                "status": str(order.status),
                "order_class": str(order.order_class) if order.order_class else "",
                "created_at": str(order.created_at) if order.created_at else "",
                "filled_avg_price": float(order.filled_avg_price) if order.filled_avg_price else None,
            })
        return result

    def get_order_by_client_id(self, client_order_id: str) -> Optional[dict]:
        """Fetch a single order by client_order_id. Returns None if not found.

        Used by the reconciler to resolve pending ledger entries against broker
        order status (including filled/cancelled orders that aren't returned by
        get_all_open_orders).
        """
        try:
            order = self._client.get_order_by_client_order_id(client_order_id)
        except Exception as e:
            log.debug(f"[BROKER] get_order_by_client_id({client_order_id!r}): {e}")
            return None
        return {
            "id": str(order.id),
            "client_order_id": order.client_order_id or "",
            "symbol": order.symbol,
            "qty": float(order.qty) if order.qty else 0,
            "filled_qty": float(order.filled_qty) if order.filled_qty else 0,
            "status": str(order.status),
            "filled_avg_price": float(order.filled_avg_price) if order.filled_avg_price else None,
        }

    # -------------------------------------------------------------------------
    # MARKET CLOCK
    # -------------------------------------------------------------------------

    def get_clock(self) -> dict:
        """Get market clock."""
        clock = self._client.get_clock()
        return {
            "is_open": clock.is_open,
            "timestamp": str(clock.timestamp),
            "next_open": str(clock.next_open),
            "next_close": str(clock.next_close),
        }

    def is_market_open(self) -> bool:
        """Check if market is currently open."""
        try:
            clock = self._client.get_clock()
            return clock.is_open
        except Exception as e:
            log.warning(f"[BROKER] Failed to get market clock: {e}")
            return False

    # -------------------------------------------------------------------------
    # RAW CLIENT ACCESS (for adapters that need SDK-level access)
    # -------------------------------------------------------------------------

    @property
    def trading_client(self):
        """Direct access to the underlying TradingClient (for adapters)."""
        return self._client


def create_broker_from_env() -> BrokerFacade:
    """Create a BrokerFacade from environment variables."""
    api_key = os.getenv("ALPACA_API_KEY", "")
    secret_key = os.getenv("ALPACA_SECRET_KEY", "")
    if not api_key or not secret_key:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY env vars.")

    live = os.getenv("LIVE_TRADING", "0") == "1"
    paper = not live
    base_url = os.getenv("ALPACA_BASE_URL", "")

    # Fail-fast: credential/mode cross-check to prevent live/paper contamination
    if live and "paper" in base_url.lower():
        raise RuntimeError(
            "FATAL: LIVE_TRADING=1 but ALPACA_BASE_URL points to paper! "
            "Check your .env file."
        )
    if paper and base_url and "paper" not in base_url.lower():
        raise RuntimeError(
            "FATAL: LIVE_TRADING is not set but ALPACA_BASE_URL points to live! "
            "Set LIVE_TRADING=1 or fix ALPACA_BASE_URL."
        )

    if not paper:
        confirm = os.getenv("I_UNDERSTAND_LIVE_TRADING", "").upper()
        if confirm != "YES":
            raise RuntimeError(
                "LIVE_TRADING=1 requires I_UNDERSTAND_LIVE_TRADING=YES"
            )
        log.warning("=" * 60)
        log.warning("***  LIVE TRADING ENABLED - REAL MONEY AT RISK  ***")
        log.warning("=" * 60)

    return BrokerFacade(api_key=api_key, secret_key=secret_key, paper=paper)
