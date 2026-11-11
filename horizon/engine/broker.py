"""Alpaca broker facade.

A thin wrapper over alpaca-py for account queries and order submission. Live
trading is refused unless explicitly confirmed via env vars — the engine
defaults to paper. --preview reads the live account through ReadOnlyBroker,
which can place, cancel and close nothing.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ..paths import get_secret, require_secret


class BrokerError(RuntimeError):
    pass


class ReadOnlyError(BrokerError):
    """A read-only broker was asked to write."""


class OrderRejected(BrokerError):
    """The broker answered an order with an error: that order was not placed."""


class BrokerFacade:
    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        from alpaca.trading.client import TradingClient
        self._client = TradingClient(api_key, secret_key, paper=paper)
        self.paper = paper
        self._rest = ("https://paper-api.alpaca.markets" if paper
                      else "https://api.alpaca.markets")
        self._headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}

    def daily_closes(self, start_iso: str, end_iso: str) -> Dict[str, float]:
        """Account equity at each session's close, keyed by session date.

        Alpaca stamps each daily point at 20:00 ET on the session date (checked
        2026-10-07 against last_equity on three dates), so the Eastern date of
        the timestamp is the session. An explicit start/end is required: with
        only a start the endpoint returns a short default window."""
        import requests
        from datetime import datetime, time as _t
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        lo = datetime.combine(datetime.fromisoformat(start_iso).date(), _t(0, 0), et)
        hi = datetime.combine(datetime.fromisoformat(end_iso).date(), _t(23, 59), et)
        resp = requests.get(f"{self._rest}/v2/account/portfolio/history",
                            headers=self._headers, timeout=20,
                            params={"timeframe": "1D", "start": lo.isoformat(),
                                    "end": hi.isoformat()})
        resp.raise_for_status()
        h = resp.json() or {}
        return {datetime.fromtimestamp(t, et).date().isoformat(): float(e)
                for t, e in zip(h.get("timestamp") or [], h.get("equity") or []) if e}

    def _rest_get(self, path: str, params: Optional[dict] = None, timeout: int = 20):
        import requests
        resp = requests.get(f"{self._rest}{path}", headers=self._headers,
                            params=params or {}, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def calendar(self, start_iso: str, end_iso: str) -> List[dict]:
        """Exchange sessions between two dates, inclusive: [{date, open, close}]
        with open/close as timezone-aware ET datetimes (half days included)."""
        from datetime import datetime
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        out = []
        for c in self._rest_get("/v2/calendar", {"start": start_iso, "end": end_iso}) or []:
            d = str(c["date"])[:10]
            out.append({"date": d,
                        "open": datetime.fromisoformat(f"{d}T{c['open']}").replace(tzinfo=et),
                        "close": datetime.fromisoformat(f"{d}T{c['close']}").replace(tzinfo=et)})
        return out

    def session_hours(self, day) -> Optional[tuple]:
        """(open, close) ET datetimes for `day` (a date), or None if the market
        is closed that day. Raises on API error."""
        rows = self.calendar(day.isoformat(), day.isoformat())
        row = next((r for r in rows if r["date"] == day.isoformat()), None)
        return (row["open"], row["close"]) if row else None

    def orders_since(self, after_utc_iso: str) -> List[dict]:
        """Every order submitted after `after_utc_iso` (any status), oldest
        first, as plain REST dicts (status strings like 'filled')."""
        return self._rest_get("/v2/orders", {"status": "all", "after": after_utc_iso,
                                             "limit": 500, "direction": "asc"}) or []

    def all_activities(self) -> List[dict]:
        """Every account activity since the account opened (fills, dividends,
        fees, transfers), oldest first."""
        out: List[dict] = []
        token = None
        while True:
            params = {"page_size": 100, "direction": "asc"}
            if token:
                params["page_token"] = token
            page = self._rest_get("/v2/account/activities", params, timeout=30) or []
            out += page
            if len(page) < 100:
                return out
            token = page[-1]["id"]

    def cash_flows_by_date(self, after_iso: str, through_iso: str) -> Dict[str, float]:
        """Net external cash in (+) / out (-) per date, after `after_iso` up to
        and including `through_iso`: deposits, withdrawals, cash journals."""
        import requests
        resp = requests.get(f"{self._rest}/v2/account/activities",
                            headers=self._headers, timeout=15,
                            params={"activity_types": "CSD,CSW,JNLC,JNLS",
                                    "after": after_iso, "page_size": 100})
        resp.raise_for_status()
        out: Dict[str, float] = {}
        for a in resp.json() or []:
            d = str(a.get("date", ""))[:10]
            if after_iso < d <= through_iso:
                out[d] = out.get(d, 0.0) + float(a.get("net_amount") or 0.0)
        return out

    # --- account / positions -------------------------------------------------
    def get_account(self) -> dict:
        a = self._client.get_account()
        return {
            "equity": float(a.equity),
            # Equity at the previous trading day's 16:00 ET close. At the 9:00
            # cycle that is the close of the session being reported (as_of),
            # which lines up exactly with close-to-close benchmark returns.
            "last_equity": float(getattr(a, "last_equity", None) or a.equity),
            "cash": float(a.cash),
            "buying_power": float(a.buying_power),
            "multiplier": float(getattr(a, "multiplier", 1) or 1),
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
                # shares not already committed to open orders: the most a sell may ask for
                "qty_available": float(getattr(p, "qty_available", None) or p.qty),
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
        from alpaca.common.exceptions import APIError
        from alpaca.trading.enums import OrderSide, TimeInForce
        from alpaca.trading.requests import MarketOrderRequest
        req = MarketOrderRequest(
            symbol=symbol,
            side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            qty=abs(qty),
            time_in_force=TimeInForce.DAY,
            client_order_id=client_order_id,
        )
        try:
            return self._order_dict(self._client.submit_order(req))
        except APIError as exc:
            raise OrderRejected(f"{side} {symbol} qty={abs(qty):g}: {exc}") from exc

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

    def seconds_until_open(self) -> float:
        """Seconds until the next regular-session open; 0 if open now."""
        from datetime import datetime, timezone
        clock = self._client.get_clock()
        if clock.is_open:
            return 0.0
        return max(0.0, (clock.next_open - datetime.now(timezone.utc)).total_seconds())

    def is_trading_day(self, day) -> bool:
        """True if the exchange calendar has a session on `day` (a date).
        Holidays return False. Raises on broker error — callers decide the
        fallback."""
        from alpaca.trading.requests import GetCalendarRequest
        cal = self._client.get_calendar(GetCalendarRequest(start=day, end=day))
        return any(str(getattr(c, "date", "")) == day.isoformat() for c in cal)

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
            "side": getattr(o.side, "value", str(o.side)).lower(),
        }


# BrokerFacade members that only read. ReadOnlyBroker passes these through and
# refuses everything else, so a method added to the facade later stays blocked
# until someone checks it and lists it here.
READ_ONLY_MEMBERS = frozenset({
    "paper", "get_account", "get_equity", "get_positions", "get_open_orders",
    "get_order_by_client_id", "is_market_open", "seconds_until_open",
    "is_trading_day", "session_hours", "calendar", "daily_closes", "orders_since",
    "all_activities", "cash_flows_by_date"})


class ReadOnlyBroker:
    """A BrokerFacade that can only read: what --preview runs on.

    Two independent locks. Any member outside READ_ONLY_MEMBERS raises
    ReadOnlyError on access (submit_market_order, cancel_all_orders,
    close_all_positions, _client, ...). And the alpaca-py client underneath
    sends nothing but GET, so a code path that reaches the client directly is
    refused before any request leaves the process.
    """

    def __init__(self, broker):
        self._broker = broker
        client = getattr(broker, "_client", None)
        if client is not None:
            _refuse_all_but_get(client)

    def __getattr__(self, name):
        if name in READ_ONLY_MEMBERS:
            return getattr(self._broker, name)
        raise ReadOnlyError(f"read-only broker: '{name}' is blocked (a preview places, "
                            f"cancels and closes nothing)")


def _refuse_all_but_get(client) -> None:
    """Make an alpaca-py REST client refuse every HTTP method except GET.
    Every client request goes through its requests.Session; if that hook is
    missing (a changed alpaca-py), refuse to build the broker at all."""
    session = getattr(client, "_session", None)
    if session is None or not callable(getattr(session, "request", None)):
        raise ReadOnlyError("cannot make this alpaca-py client read-only: no _session")
    send = session.request

    def get_only(method, url, *args, **kwargs):
        if str(method).upper() != "GET":
            raise ReadOnlyError(f"read-only broker: refused {method} {url}")
        return send(method, url, *args, **kwargs)

    session.request = get_only


def create_preview_broker_from_env() -> ReadOnlyBroker:
    """The live account, read-only, for --preview. LIVE_TRADING and
    I_UNDERSTAND_LIVE_TRADING gate order submission, which a preview never
    reaches, so they are not required here; the keys must be live keys."""
    key = require_secret("ALPACA_API_KEY")
    secret = require_secret("ALPACA_SECRET_KEY")
    base_url = (get_secret("ALPACA_BASE_URL", "") or "").lower()
    if "paper" in base_url:
        raise BrokerError("--preview reads the live account, but ALPACA_BASE_URL "
                          "points to paper")
    return ReadOnlyBroker(BrokerFacade(key, secret, paper=False))


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
