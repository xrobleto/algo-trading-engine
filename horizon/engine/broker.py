"""Alpaca broker facade.

A thin wrapper over alpaca-py for account queries and order submission. Live
trading is refused unless explicitly confirmed via env vars — the engine
defaults to paper.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ..paths import get_secret, require_secret


class BrokerError(RuntimeError):
    pass


class BrokerFacade:
    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        from alpaca.trading.client import TradingClient
        self._client = TradingClient(api_key, secret_key, paper=paper)
        self.paper = paper

    # --- account / positions -------------------------------------------------
    def get_account(self) -> dict:
        a = self._client.get_account()
        return {
            "equity": float(a.equity),
            "cash": float(a.cash),
            "buying_power": float(a.buying_power),
            "status": str(a.status),
            "pattern_day_trader": bool(getattr(a, "pattern_day_trader", False)),
        }

    def get_equity(self) -> float:
        return self.get_account()["equity"]

    def get_positions(self) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for p in self._client.get_all_positions():
            out[p.symbol] = {
                "symbol": p.symbol,
                "qty": float(p.qty),
                "market_value": float(p.market_value),
                "avg_entry_price": float(p.avg_entry_price),
            }
        return out

    # --- orders --------------------------------------------------------------
    def get_open_orders(self) -> List[dict]:
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
        return [self._order_dict(o) for o in self._client.get_orders(req)]

    def get_order_by_client_id(self, client_order_id: str) -> Optional[dict]:
        try:
            return self._order_dict(
                self._client.get_order_by_client_id(client_order_id))
        except Exception:
            return None

    def submit_market_order(self, symbol: str, side: str, qty: float,
                            client_order_id: str) -> dict:
        from alpaca.trading.enums import OrderSide, TimeInForce
        from alpaca.trading.requests import MarketOrderRequest
        req = MarketOrderRequest(
            symbol=symbol,
            side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            qty=abs(qty),
            time_in_force=TimeInForce.DAY,
            client_order_id=client_order_id,
        )
        return self._order_dict(self._client.submit_order(req))

    def cancel_all_orders(self) -> int:
        """Cancel every open order. Returns the count attempted."""
        try:
            resp = self._client.cancel_orders()
            return len(resp) if resp else 0
        except Exception:
            count = 0
            for order in self.get_open_orders():
                try:
                    self._client.cancel_order_by_id(order["id"])
                    count += 1
                except Exception:
                    pass
            return count

    def close_all_positions(self) -> int:
        """Liquidate every position at market and cancel open orders.

        Used by the emergency-flatten command — returns the position count
        that was open at the moment of liquidation.
        """
        n_positions = len(self.get_positions())
        self._client.close_all_positions(cancel_orders=True)
        return n_positions

    def is_market_open(self) -> bool:
        return bool(self._client.get_clock().is_open)

    @staticmethod
    def _order_dict(o) -> dict:
        return {
            "id": str(o.id),
            "client_order_id": o.client_order_id,
            "symbol": o.symbol,
            "qty": float(o.qty or 0),
            "filled_qty": float(o.filled_qty or 0),
            "status": str(o.status),
            "filled_avg_price": (float(o.filled_avg_price)
                                 if o.filled_avg_price else None),
        }


def create_broker_from_env() -> BrokerFacade:
    """Build a broker from env/.env. Live trading needs explicit confirmation."""
    key = require_secret("ALPACA_API_KEY")
    secret = require_secret("ALPACA_SECRET_KEY")
    live = get_secret("LIVE_TRADING", "0") == "1"
    base_url = (get_secret("ALPACA_BASE_URL", "") or "").lower()
    if live and "paper" in base_url:
        raise BrokerError("LIVE_TRADING=1 but ALPACA_BASE_URL points to paper")
    if not live and base_url and "paper" not in base_url:
        raise BrokerError("LIVE_TRADING=0 but ALPACA_BASE_URL is not paper")
    if live and get_secret("I_UNDERSTAND_LIVE_TRADING", "") != "YES":
        raise BrokerError("Live trading requires I_UNDERSTAND_LIVE_TRADING=YES")
    return BrokerFacade(key, secret, paper=not live)
