"""
Unified Engine Performance Dashboard — real-time monitoring for the trading engine.

Connects to Alpaca API for live portfolio data and reads engine state files
(ownership ledger, intelligence decisions) for operational visibility.

Usage:
    python -m streamlit run dashboard/engine_dashboard.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# PATH RESOLUTION
# ---------------------------------------------------------------------------

def get_data_dir() -> Path:
    """Resolve data directory (same logic as engine/platform.py)."""
    if algo_output := os.getenv("ALGO_OUTPUT_DIR"):
        return Path(algo_output)
    if sys.platform == "win32":
        base = os.getenv("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = str(Path.home() / "Library" / "Application Support")
    else:
        base = os.getenv("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))
    return Path(base) / "AlgoTrading"


def load_env_file(env_path: Path) -> dict:
    """Parse a .env file and return key-value dict (does NOT set os.environ)."""
    result = {}
    if not env_path.exists():
        return result
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip().strip('"').strip("'")
            if key:
                result[key] = value
    return result


def apply_env_for_mode(mode: str) -> None:
    """Load the correct .env file for paper or live mode into os.environ."""
    project_root = Path(__file__).resolve().parent.parent
    config_dir = project_root / "config"

    if mode == "paper":
        env_file = config_dir / "trend_bot.env"
    else:
        env_file = config_dir / "trend_bot_live.env"

    env_vars = load_env_file(env_file)

    # Also load cross_asset_bot.env for any supplemental keys
    for k, v in load_env_file(config_dir / "cross_asset_bot.env").items():
        if k not in env_vars:
            env_vars[k] = v

    # Apply to os.environ (override existing to ensure correct mode)
    for k, v in env_vars.items():
        os.environ[k] = v


# ---------------------------------------------------------------------------
# SLEEVE SYMBOL MAPPING (mirrors engine/config.py known_symbols)
# ---------------------------------------------------------------------------

TREND_SYMBOLS: Set[str] = {
    "SPY", "QQQ", "IWM",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLC",
    "SMH", "IBB", "XHB",
    "MTUM", "QUAL",
    "TQQQ", "UPRO", "SOXL", "TECL", "FAS",
    "ARKK", "XBI", "KWEB", "SOXX", "IGV", "CIBR", "SKYY",
    "SGOV", "BIL",
}

CROSSASSET_SYMBOLS: Set[str] = {
    "TLT", "IEF", "SHY", "TBT",
    "GLD", "SLV", "DBC", "USO", "UNG", "DBA", "GLL",
    "UUP", "FXE", "FXY",
    "PDBC",
}

def classify_symbol(symbol: str) -> str:
    """Classify a symbol to a sleeve using config-based known_symbols."""
    if symbol in TREND_SYMBOLS:
        return "TREND"
    if symbol in CROSSASSET_SYMBOLS:
        return "CROSSASSET"
    return "UNKNOWN"


# Mirrors engine/config.py order_prefix + legacy_prefixes per sleeve.
# Used to classify dynamically-discovered symbols (e.g., SIMPLE's QBTS) that
# aren't in any known_symbols set but were placed by the engine.
ORDER_PREFIX_MAP: Dict[str, str] = {
    "ENG_TREND_": "TREND",
    "ENG_SIMPLE_": "SIMPLE",
    "ENG_XASSET_": "CROSSASSET",
    # Legacy prefixes (pre-unified-engine)
    "TBOT_": "TREND",
    "XABOT_": "CROSSASSET",
    "dir_": "SIMPLE",
}


def classify_by_order_prefix(client_order_id: str) -> str:
    """Classify a client_order_id to a sleeve by prefix match."""
    if not client_order_id:
        return "UNKNOWN"
    for prefix, sleeve in ORDER_PREFIX_MAP.items():
        if client_order_id.startswith(prefix):
            return sleeve
    return "UNKNOWN"


# ---------------------------------------------------------------------------
# ALPACA CLIENT
# ---------------------------------------------------------------------------

class AlpacaClient:
    """Lightweight Alpaca REST client for dashboard reads."""

    def __init__(self):
        self.api_key = os.getenv("ALPACA_API_KEY", "")
        self.secret_key = os.getenv("ALPACA_SECRET_KEY", "")
        self.base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
        self.headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
        }

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key and self.secret_key)

    @property
    def is_live(self) -> bool:
        return "paper" not in self.base_url

    def _get(self, endpoint: str) -> dict:
        resp = requests.get(f"{self.base_url}/v2/{endpoint}", headers=self.headers, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_account(self) -> dict:
        return self._get("account")

    def get_positions(self) -> list:
        return self._get("positions")

    def get_orders(self, status: str = "open", limit: int = 50) -> list:
        return self._get(f"orders?status={status}&limit={limit}")

    def get_portfolio_history(self, period: str = "1M", timeframe: str = "1D") -> dict:
        return self._get(f"account/portfolio/history?period={period}&timeframe={timeframe}")


# ---------------------------------------------------------------------------
# STATE FILE READERS
# ---------------------------------------------------------------------------

def load_ownership_ledger(data_dir: Path, mode: str = "paper") -> dict:
    """Load the engine ownership ledger JSON."""
    ledger_path = data_dir / "data" / "state" / f"engine_ownership_{mode}.json"
    if not ledger_path.exists():
        return {"entries": {}, "last_reconciled_at": ""}
    with open(ledger_path, "r") as f:
        return json.load(f)


def load_intelligence_log(data_dir: Path, max_lines: int = 200) -> List[dict]:
    """Load recent intelligence decisions from JSONL."""
    jsonl_path = data_dir / "logs" / "intelligence_decisions.jsonl"
    if not jsonl_path.exists():
        return []
    records = []
    with open(jsonl_path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records[-max_lines:]


def load_engine_log_tail(data_dir: Path, lines: int = 100) -> List[str]:
    """Load last N lines of engine.log."""
    log_path = data_dir / "logs" / "engine.log"
    if not log_path.exists():
        return []
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        all_lines = f.readlines()
    return all_lines[-lines:]


def check_kill_switch(data_dir: Path) -> bool:
    """Check if the kill switch file exists."""
    ks_path = data_dir / "data" / "state" / "HALT_ALL_TRADING"
    return ks_path.exists()


# ---------------------------------------------------------------------------
# DASHBOARD
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="Algo Engine Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Resolve data directory
    data_dir = get_data_dir()

    # =========================================================================
    # SIDEBAR
    # =========================================================================

    with st.sidebar:
        st.title("Engine Controls")

        # Mode selector
        mode = st.radio("Account", ["Paper", "Live"], index=0, horizontal=True)
        mode_key = mode.lower()

        # Load correct env file based on mode
        apply_env_for_mode(mode_key)

        # Initialize Alpaca client AFTER env is set
        alpaca = AlpacaClient()

        if alpaca.is_live:
            st.error(f"Mode: LIVE", icon="🔴")
        else:
            st.success(f"Mode: PAPER", icon="🟢")

        st.caption(f"Data dir: `{data_dir}`")

        # Auto-refresh
        refresh_enabled = st.toggle("Auto-refresh", value=True)
        refresh_interval = st.slider("Refresh interval (sec)", 10, 120, 30)

        if refresh_enabled:
            st.caption(f"Refreshing every {refresh_interval}s")

        st.divider()

        # Kill switch status
        ks_active = check_kill_switch(data_dir)
        if ks_active:
            st.error("KILL SWITCH ACTIVE", icon="🛑")
            st.caption("All new entries are blocked.")
        else:
            st.success("Kill switch: OFF", icon="✅")

        st.divider()
        st.caption("Engine Performance Dashboard v1.0")

    # =========================================================================
    # HEADER
    # =========================================================================

    st.title("Unified Trading Engine")

    if not alpaca.is_configured:
        st.error(
            "Alpaca API keys not found. Set `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` "
            "in your environment or config/*.env files."
        )
        st.stop()

    # =========================================================================
    # ACCOUNT OVERVIEW
    # =========================================================================

    try:
        account = alpaca.get_account()
        positions = alpaca.get_positions()
        open_orders = alpaca.get_orders("open")
    except Exception as e:
        st.error(f"Failed to connect to Alpaca: {e}")
        st.stop()

    equity = float(account.get("equity", 0))
    cash = float(account.get("cash", 0))
    last_equity = float(account.get("last_equity", 0))
    daily_pnl = equity - last_equity
    daily_pnl_pct = (daily_pnl / last_equity * 100) if last_equity else 0

    total_market_value = sum(float(p.get("market_value", 0)) for p in positions)
    total_unrealized_pl = sum(float(p.get("unrealized_pl", 0)) for p in positions)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Equity", f"${equity:,.2f}", f"{daily_pnl:+,.2f} ({daily_pnl_pct:+.2f}%)")
    col2.metric("Cash", f"${cash:,.2f}")
    col3.metric("Invested", f"${total_market_value:,.2f}")
    col4.metric("Unrealized P&L", f"${total_unrealized_pl:,.2f}")
    col5.metric("Open Orders", f"{len(open_orders)}")

    st.divider()

    # =========================================================================
    # BUILD POSITION OWNERS MAP
    # =========================================================================

    ledger = load_ownership_ledger(data_dir, mode_key)
    entries = ledger.get("entries", {})
    position_owners: Dict[str, str] = {}

    # Tier 1: Config-based classification (hardcoded known_symbols)
    for p in positions:
        sym = p.get("symbol", "")
        c = classify_symbol(sym)
        if c != "UNKNOWN":
            position_owners[sym] = c

    # Tier 2: Order-prefix classification (catches SIMPLE's dynamic universe
    # and any other engine-placed symbols not in known_symbols sets).
    unassigned_syms = {
        p.get("symbol", "") for p in positions
        if p.get("symbol", "") and p.get("symbol", "") not in position_owners
    }
    if unassigned_syms:
        try:
            closed_orders = alpaca.get_orders(status="closed", limit=200)
            for order in closed_orders:
                sym = order.get("symbol", "")
                if sym in unassigned_syms and sym not in position_owners:
                    owner = classify_by_order_prefix(order.get("client_order_id", ""))
                    if owner != "UNKNOWN":
                        position_owners[sym] = owner
        except Exception:
            pass  # Silent fail — fall through to Tier 3

    # Tier 3: Local ledger fallback (only useful when dashboard runs on engine host)
    for coid, entry in entries.items():
        sym = entry.get("symbol", "")
        if sym not in position_owners and entry.get("status") in ("filled", "pending", "partially_filled"):
            position_owners[sym] = entry.get("strategy_id", "UNKNOWN")

    # =========================================================================
    # INTELLIGENCE REGIME
    # =========================================================================

    intel_records = load_intelligence_log(data_dir)

    col_left, col_right = st.columns([2, 3])

    with col_left:
        st.subheader("Market Intelligence")

        if intel_records:
            latest = intel_records[-1]
            regime = latest.get("regime", "UNKNOWN")
            score = latest.get("regime_score", 50)
            regime_changed = latest.get("regime_changed", False)
            ts = latest.get("_ts", "")

            regime_colors = {
                "RISK_ON": "🟢", "CAUTIOUS": "🟡",
                "RISK_OFF": "🟠", "CRISIS": "🔴",
            }
            badge = regime_colors.get(regime, "⚪")

            st.metric(
                "Current Regime",
                f"{badge} {regime}",
                f"Score: {score:.1f}/100" + (" (CHANGED)" if regime_changed else ""),
            )

            # Source health
            sources = latest.get("sources", {})
            if sources:
                source_data = []
                for name, info in sources.items():
                    source_data.append({
                        "Source": name.title(),
                        "Score": f"{info.get('score', 50):.0f}",
                        "Status": "✅" if info.get("available") else "❌",
                        "Fetch (ms)": f"{info.get('fetch_ms', 0):.0f}",
                    })
                st.dataframe(
                    pd.DataFrame(source_data),
                    hide_index=True,
                    use_container_width=True,
                )

            # Allocation adjustments
            allocs = latest.get("allocations", {})
            risk_mults = latest.get("risk_multipliers", {})
            gates = latest.get("entry_gates", {})
            if allocs:
                adj_data = []
                for sid in allocs:
                    adj_data.append({
                        "Sleeve": sid,
                        "Allocation": f"{allocs[sid]:.1%}",
                        "Risk Mult": f"{risk_mults.get(sid, 1.0):.2f}x",
                        "Entries": "✅" if gates.get(sid, True) else "🚫 BLOCKED",
                    })
                st.dataframe(
                    pd.DataFrame(adj_data),
                    hide_index=True,
                    use_container_width=True,
                )

            st.caption(f"Last refresh: {ts[:19] if ts else 'N/A'}")
        else:
            st.info("No intelligence data yet — logs are on Railway, not local.")

    with col_right:
        st.subheader("Regime History")

        if len(intel_records) >= 2:
            hist_df = pd.DataFrame([
                {
                    "time": r.get("_ts", "")[:16],
                    "score": r.get("regime_score", 50),
                    "regime": r.get("regime", "CAUTIOUS"),
                }
                for r in intel_records
            ])
            hist_df["time"] = pd.to_datetime(hist_df["time"], errors="coerce")
            hist_df = hist_df.dropna(subset=["time"])

            if not hist_df.empty:
                st.line_chart(hist_df.set_index("time")["score"], height=250)

                regime_counts = hist_df["regime"].value_counts()
                st.bar_chart(regime_counts, height=150)
        else:
            st.info("Regime history will appear after multiple intelligence refreshes (logs on Railway).")

    st.divider()

    # =========================================================================
    # SLEEVE PERFORMANCE
    # =========================================================================

    st.subheader("Sleeve Performance")

    sleeve_config = {
        "TREND": {"alloc": 0.65},
        "SIMPLE": {"alloc": 0.20},
        "CROSSASSET": {"alloc": 0.12},
    }

    sleeve_stats: Dict[str, dict] = {}
    for sid in sleeve_config:
        sleeve_stats[sid] = {
            "market_value": 0.0,
            "unrealized_pl": 0.0,
            "position_count": 0,
        }

    unassigned_positions = []

    for pos in positions:
        symbol = pos.get("symbol", "")
        owner = position_owners.get(symbol)
        mv = float(pos.get("market_value", 0))
        upl = float(pos.get("unrealized_pl", 0))

        if owner and owner in sleeve_stats:
            sleeve_stats[owner]["market_value"] += mv
            sleeve_stats[owner]["unrealized_pl"] += upl
            sleeve_stats[owner]["position_count"] += 1
        else:
            unassigned_positions.append(pos)

    cols = st.columns(len(sleeve_config))
    for i, (sid, cfg) in enumerate(sleeve_config.items()):
        stats = sleeve_stats[sid]
        target_equity = equity * cfg["alloc"]
        utilization = (stats["market_value"] / target_equity * 100) if target_equity else 0

        with cols[i]:
            st.markdown(f"**{sid}** ({cfg['alloc']:.0%} sleeve)")
            st.metric("Market Value", f"${stats['market_value']:,.2f}")
            st.metric("Unrealized P&L", f"${stats['unrealized_pl']:,.2f}")
            st.metric("Positions", f"{stats['position_count']}")
            st.progress(min(utilization / 100, 1.0), text=f"Utilization: {utilization:.0f}%")

    if unassigned_positions:
        st.warning(
            f"{len(unassigned_positions)} position(s) not assigned to any sleeve: "
            f"{', '.join(p['symbol'] for p in unassigned_positions)}"
        )

    st.divider()

    # =========================================================================
    # ACTIVE POSITIONS
    # =========================================================================

    st.subheader("Active Positions")

    if positions:
        pos_data = []
        for pos in positions:
            symbol = pos.get("symbol", "")
            qty = float(pos.get("qty", 0))
            avg_entry = float(pos.get("avg_entry_price", 0))
            current = float(pos.get("current_price", 0))
            mv = float(pos.get("market_value", 0))
            upl = float(pos.get("unrealized_pl", 0))
            upl_pct = float(pos.get("unrealized_plpc", 0)) * 100
            side = pos.get("side", "long")
            owner = position_owners.get(symbol, "Unassigned")

            pos_data.append({
                "Sleeve": owner,
                "Symbol": symbol,
                "Side": side.upper(),
                "Qty": f"{qty:.2f}",
                "Avg Entry": f"${avg_entry:.2f}",
                "Current": f"${current:.2f}",
                "Mkt Value": f"${mv:,.2f}",
                "P&L": f"${upl:,.2f}",
                "P&L %": f"{upl_pct:+.2f}%",
            })

        df = pd.DataFrame(pos_data)
        st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "P&L": st.column_config.TextColumn(width="small"),
                "P&L %": st.column_config.TextColumn(width="small"),
            },
        )
    else:
        st.info("No active positions.")

    st.divider()

    # =========================================================================
    # EQUITY CURVE
    # =========================================================================

    st.subheader("Equity Curve")

    period_options = {"1 Week": "1W", "1 Month": "1M", "3 Months": "3M", "1 Year": "1A"}
    selected_period = st.selectbox("Period", list(period_options.keys()), index=1)

    try:
        history = alpaca.get_portfolio_history(
            period=period_options[selected_period],
            timeframe="1D",
        )
        timestamps = history.get("timestamp", [])
        equities = history.get("equity", [])

        if timestamps and equities:
            curve_df = pd.DataFrame({
                "Date": pd.to_datetime(timestamps, unit="s"),
                "Equity": equities,
            })
            curve_df = curve_df.set_index("Date")

            start_eq = equities[0] if equities else 0
            end_eq = equities[-1] if equities else 0
            period_return = ((end_eq - start_eq) / start_eq * 100) if start_eq else 0

            peak = max(equities)
            drawdown = ((end_eq - peak) / peak * 100) if peak else 0

            c1, c2, c3 = st.columns(3)
            c1.metric("Period Return", f"{period_return:+.2f}%")
            c2.metric("Peak", f"${peak:,.2f}")
            c3.metric("Drawdown from Peak", f"{drawdown:.2f}%")

            st.line_chart(curve_df, height=300)
    except Exception as e:
        st.warning(f"Could not load portfolio history: {e}")

    st.divider()

    # =========================================================================
    # TRADE JOURNAL
    # =========================================================================

    st.subheader("Trade Journal")

    if entries:
        journal_data = []
        for coid, entry in entries.items():
            journal_data.append({
                "Sleeve": entry.get("strategy_id", ""),
                "Symbol": entry.get("symbol", ""),
                "Side": entry.get("side", "").upper(),
                "Qty": entry.get("qty", 0),
                "Status": entry.get("status", ""),
                "Entry Notional": f"${entry.get('notional_at_entry', 0):,.2f}",
                "Fill Price": f"${entry.get('fill_price', 0):.2f}" if entry.get("fill_price") else "-",
                "Opened": entry.get("registered_at", "")[:19],
                "Closed": entry.get("closed_at", "")[:19] if entry.get("closed_at") else "-",
                "Order ID": coid[:30],
            })

        journal_df = pd.DataFrame(journal_data)

        col_f1, col_f2 = st.columns(2)
        with col_f1:
            sleeve_filter = st.multiselect(
                "Filter by sleeve",
                options=sorted(journal_df["Sleeve"].unique()),
                default=sorted(journal_df["Sleeve"].unique()),
            )
        with col_f2:
            status_filter = st.multiselect(
                "Filter by status",
                options=sorted(journal_df["Status"].unique()),
                default=sorted(journal_df["Status"].unique()),
            )

        filtered = journal_df[
            journal_df["Sleeve"].isin(sleeve_filter) & journal_df["Status"].isin(status_filter)
        ]
        st.dataframe(filtered, hide_index=True, use_container_width=True)
        st.caption(f"Showing {len(filtered)} of {len(journal_df)} entries")
    else:
        st.info("No trade journal entries yet — ledger is on Railway, not local.")

    st.divider()

    # =========================================================================
    # ENGINE HEALTH
    # =========================================================================

    st.subheader("Engine Health")

    log_lines = load_engine_log_tail(data_dir, lines=200)

    if log_lines:
        errors = [l.strip() for l in log_lines if "ERROR" in l or "CRITICAL" in l]
        warnings = [l.strip() for l in log_lines if "WARNING" in l]

        c1, c2, c3 = st.columns(3)
        c1.metric("Recent Errors", len(errors))
        c2.metric("Recent Warnings", len(warnings))

        heartbeats = [l for l in log_lines if "[ENGINE] tick=" in l]
        if heartbeats:
            last_hb = heartbeats[-1].strip()
            c3.metric("Last Heartbeat", last_hb[1:20] if last_hb.startswith("[") else "See logs")
        else:
            c3.metric("Last Heartbeat", "N/A")

        reconcile_lines = [l for l in log_lines if "[RECONCILE]" in l]
        if reconcile_lines:
            st.caption(f"Last reconcile activity: {reconcile_lines[-1].strip()[:120]}")

        if errors:
            with st.expander(f"Recent Errors ({len(errors)})", expanded=len(errors) <= 5):
                for err in errors[-10:]:
                    st.code(err, language=None)

        if warnings:
            with st.expander(f"Recent Warnings ({len(warnings)})"):
                for w in warnings[-10:]:
                    st.code(w, language=None)

        with st.expander("Engine Log (last 50 lines)"):
            st.code("".join(log_lines[-50:]), language=None)
    else:
        st.info("No engine logs found locally. Engine logs are on Railway.")

    # =========================================================================
    # AUTO-REFRESH
    # =========================================================================

    if refresh_enabled:
        import time as _time
        _time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
