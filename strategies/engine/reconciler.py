"""
Startup Reconciler — rebuilds ownership state from broker + local state.

On engine startup, the reconciler:
1. Fetches all broker positions and open orders
2. Classifies each by client_order_id prefix
3. Cross-references with the local ownership ledger
4. Detects orphans, conflicts, and unclassified positions
5. Produces a clean ownership snapshot for the engine to use
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from engine.config import EngineConfig, SleeveConfig
from engine.ownership import OwnershipEntry, OwnershipLedger

log = logging.getLogger("Engine")


# =============================================================================
# RECONCILIATION RESULT
# =============================================================================

@dataclass
class ReconciliationResult:
    """Output of the reconciliation process."""
    timestamp: str = ""

    # Raw broker data
    broker_positions: List[dict] = field(default_factory=list)
    broker_orders: List[dict] = field(default_factory=list)

    # Classification results
    classified_positions: Dict[str, str] = field(default_factory=dict)   # symbol -> strategy_id
    classified_orders: Dict[str, str] = field(default_factory=dict)      # client_order_id -> strategy_id
    unclassified_positions: List[str] = field(default_factory=list)      # symbols with no match
    unclassified_orders: List[str] = field(default_factory=list)         # client_order_ids with no match
    conflicts: List[str] = field(default_factory=list)                   # symbols with multiple owners

    # Rebuilt ledger
    ownership_snapshot: Optional[OwnershipLedger] = None

    # Summary
    errors: List[str] = field(default_factory=list)

    @property
    def is_safe(self) -> bool:
        """True if no conflicts and no unclassified active positions."""
        return len(self.conflicts) == 0 and len(self.unclassified_positions) == 0


# =============================================================================
# PREFIX CLASSIFIER
# =============================================================================

def classify_by_prefix(
    client_order_id: str,
    config: EngineConfig,
) -> Optional[str]:
    """
    Classify a client_order_id to a strategy_id by prefix matching.

    Checks both current and legacy prefixes for migration support.

    Returns:
        strategy_id if matched, None if unclassified
    """
    if not client_order_id:
        return None

    for strategy_id, sleeve_config in config.sleeves.items():
        for prefix in sleeve_config.all_prefixes():
            if client_order_id.startswith(prefix):
                return strategy_id

    return None


def classify_position_by_orders(
    symbol: str,
    orders: List[dict],
    config: EngineConfig,
) -> Optional[str]:
    """
    Classify a position by looking at its associated orders.

    If a position has no direct order match (e.g., filled bracket legs),
    check ALL orders for the same symbol.
    """
    for order in orders:
        if order.get("symbol") == symbol:
            strategy_id = classify_by_prefix(order.get("client_order_id", ""), config)
            if strategy_id:
                return strategy_id

    return None


# =============================================================================
# MAIN RECONCILIATION
# =============================================================================

def reconcile(
    broker,  # BrokerFacade
    existing_ledger: OwnershipLedger,
    config: EngineConfig,
) -> ReconciliationResult:
    """
    Perform full startup reconciliation.

    Steps:
    1. Fetch broker state (positions + open orders)
    2. Classify everything by prefix
    3. Cross-reference with existing ledger
    4. Detect conflicts and orphans
    5. Build merged ownership snapshot

    Args:
        broker: BrokerFacade instance
        existing_ledger: Previously persisted ownership ledger
        config: Engine configuration

    Returns:
        ReconciliationResult with classified ownership and any issues
    """
    result = ReconciliationResult(
        timestamp=datetime.now(timezone.utc).isoformat()
    )

    log.info("[RECONCILE] Starting broker state reconciliation...")

    # -------------------------------------------------------------------------
    # Step 1: Fetch broker state
    # -------------------------------------------------------------------------
    try:
        result.broker_positions = broker.get_all_positions()
        result.broker_orders = broker.get_all_open_orders()
        log.info(
            f"[RECONCILE] Broker state: {len(result.broker_positions)} positions, "
            f"{len(result.broker_orders)} open orders"
        )
    except Exception as e:
        error_msg = f"Failed to fetch broker state: {e}"
        log.error(f"[RECONCILE] {error_msg}")
        result.errors.append(error_msg)
        result.ownership_snapshot = existing_ledger
        return result

    # -------------------------------------------------------------------------
    # Step 2: Classify open orders by prefix
    # -------------------------------------------------------------------------
    for order in result.broker_orders:
        coid = order.get("client_order_id", "")
        strategy_id = classify_by_prefix(coid, config)
        if strategy_id:
            result.classified_orders[coid] = strategy_id
        elif coid:  # Has a client_order_id but no matching prefix
            result.unclassified_orders.append(coid)

    log.info(
        f"[RECONCILE] Orders classified: {len(result.classified_orders)} matched, "
        f"{len(result.unclassified_orders)} unclassified"
    )

    # -------------------------------------------------------------------------
    # Step 3: Classify positions
    # -------------------------------------------------------------------------
    # For each position, try to classify via:
    # a) Associated open orders (most reliable)
    # b) Existing ledger (secondary evidence)
    # c) Symbol-based heuristic (e.g., is it a TREND_BOT_SYMBOL?)

    for pos in result.broker_positions:
        symbol = pos["symbol"]
        qty = pos["qty"]

        # Method A: Classify by associated orders
        strategy_id = classify_position_by_orders(
            symbol, result.broker_orders, config
        )

        # Method B: Check existing ledger
        if not strategy_id:
            strategy_id = existing_ledger.get_owner(symbol)
            if strategy_id:
                # Validate: if config has known_symbols for BOTH the claimed
                # owner AND another strategy, prefer the one whose current
                # known_symbols actually contains this symbol.  This prevents
                # stale ledger entries from a prior config (e.g. TLT was TREND,
                # now is CROSSASSET) from mis-classifying positions after a
                # sleeve migration.
                claimed_sleeve = config.sleeves.get(strategy_id)
                if claimed_sleeve and claimed_sleeve.known_symbols and symbol not in claimed_sleeve.known_symbols:
                    # The claimed owner no longer lists this symbol — check if
                    # another sleeve now claims it via known_symbols.
                    new_owner = None
                    for sid, sc in config.sleeves.items():
                        if sid != strategy_id and symbol in sc.known_symbols:
                            new_owner = sid
                            break
                    if new_owner:
                        log.warning(
                            f"[RECONCILE] {symbol}: ledger says {strategy_id} but "
                            f"config migrated to {new_owner} — reclassifying"
                        )
                        strategy_id = new_owner
                    else:
                        log.debug(f"[RECONCILE] {symbol}: classified via existing ledger -> {strategy_id}")
                else:
                    log.debug(f"[RECONCILE] {symbol}: classified via existing ledger -> {strategy_id}")

        # Method C: Check known_symbols sets (heuristic fallback)
        if not strategy_id:
            for sid, sleeve_config in config.sleeves.items():
                if symbol in sleeve_config.known_symbols:
                    strategy_id = sid
                    log.debug(f"[RECONCILE] {symbol}: classified via known_symbols -> {strategy_id}")
                    break

        if strategy_id:
            # Check for conflicts (same symbol classified to different strategies)
            if symbol in result.classified_positions:
                existing_owner = result.classified_positions[symbol]
                if existing_owner != strategy_id:
                    log.error(
                        f"[RECONCILE] CONFLICT: {symbol} classified as both "
                        f"{existing_owner} and {strategy_id}"
                    )
                    result.conflicts.append(symbol)
            result.classified_positions[symbol] = strategy_id
        else:
            log.warning(f"[RECONCILE] UNCLASSIFIED position: {symbol} qty={qty}")
            result.unclassified_positions.append(symbol)

    log.info(
        f"[RECONCILE] Positions classified: {len(result.classified_positions)} matched, "
        f"{len(result.unclassified_positions)} unclassified, "
        f"{len(result.conflicts)} conflicts"
    )

    # -------------------------------------------------------------------------
    # Step 3.5: Resolve pending ledger entries against broker order status
    # -------------------------------------------------------------------------
    # For any ledger entry still marked "pending", query the broker to see if
    # the order has since filled/cancelled. This keeps the ledger in sync with
    # fills that happen between reconciles (e.g. a buy placed at tick N fills
    # seconds later — without this step, the entry stays stuck at pending
    # forever because get_all_open_orders only returns currently-open orders).
    #
    # Two-path resolution:
    #   Primary   — query Alpaca order history by client_order_id
    #   Fallback  — if the order API returns None but the symbol is in live
    #               broker positions, infer the fill from the position record.
    #               This guards against transient order-history API failures
    #               and handles notional fractional orders whose order record
    #               may not surface cleanly via the by-client-id endpoint.
    broker_position_map = {p["symbol"]: p for p in result.broker_positions}

    resolved = 0
    for coid, entry in list(existing_ledger.entries.items()):
        if entry.status != "pending":
            continue
        if coid.startswith("RECONCILE_"):
            continue  # synthetic entries have no broker order

        order = broker.get_order_by_client_id(coid)

        if order is None:
            # Primary path failed — try position-based fallback for buy orders.
            # A filled buy will always show up as a broker position; if we can
            # see it there, the order must have gone through.
            if entry.side == "buy":
                pos = broker_position_map.get(entry.symbol)
                if pos:
                    fill_price = float(pos.get("avg_entry_price", 0)) or entry.fill_price or 0.0
                    fill_qty = abs(float(pos.get("qty", entry.qty)))
                    existing_ledger.update_status(
                        coid, "filled",
                        fill_price=fill_price,
                        fill_qty=fill_qty,
                    )
                    log.info(
                        f"[RECONCILE] {entry.symbol}: inferred fill from broker position "
                        f"(order API returned None) — qty={fill_qty:.4f}, "
                        f"avg_price={fill_price:.4f}"
                    )
                    resolved += 1
            continue

        alpaca_status = order["status"].lower()
        if "filled" in alpaca_status and "partial" not in alpaca_status:
            existing_ledger.update_status(
                coid, "filled",
                fill_price=order.get("filled_avg_price"),
                fill_qty=order.get("filled_qty"),
            )
            resolved += 1
        elif "partially_filled" in alpaca_status:
            existing_ledger.update_status(
                coid, "partially_filled",
                fill_price=order.get("filled_avg_price"),
                fill_qty=order.get("filled_qty"),
            )
        elif alpaca_status in ("canceled", "cancelled", "expired", "rejected"):
            existing_ledger.update_status(coid, "cancelled")
            resolved += 1
    if resolved:
        log.info(f"[RECONCILE] Resolved {resolved} pending ledger entries from broker")

    # -------------------------------------------------------------------------
    # Step 4: Merge broker state into the existing ledger (in place)
    # -------------------------------------------------------------------------
    # We mutate existing_ledger rather than building a fresh one so that any
    # adapter holding a reference to this object stays valid across periodic
    # reconciles.
    existing_ledger.last_reconciled_at = result.timestamp

    # Close active entries whose broker position has disappeared
    broker_symbols = {p["symbol"] for p in result.broker_positions}
    for coid, entry in list(existing_ledger.entries.items()):
        if entry.is_active and entry.symbol not in broker_symbols:
            entry.status = "closed"
            entry.closed_at = result.timestamp
            log.info(f"[RECONCILE] {entry.symbol}: position gone at broker — marking closed")

    # Add classified positions that aren't in the existing ledger yet
    for pos in result.broker_positions:
        symbol = pos["symbol"]
        strategy_id = result.classified_positions.get(symbol)
        if not strategy_id:
            continue  # unclassified — don't add to ledger

        existing_symbols = existing_ledger.get_active_symbols(strategy_id)
        if symbol in existing_symbols:
            continue  # already tracked

        synthetic_coid = f"RECONCILE_{strategy_id}_{symbol}_{result.timestamp}"
        existing_ledger.register_order(
            strategy_id=strategy_id,
            symbol=symbol,
            side="buy" if float(pos.get("qty", 0)) > 0 else "sell",
            qty=abs(float(pos.get("qty", 0))),
            client_order_id=synthetic_coid,
            notional=abs(float(pos.get("market_value", 0))),
        )
        existing_ledger.update_status(synthetic_coid, "filled",
                                      fill_price=float(pos.get("avg_entry_price", 0)))
        log.info(
            f"[RECONCILE] {symbol}: added synthetic ownership entry -> {strategy_id} "
            f"(qty={pos['qty']}, mv=${pos.get('market_value', 0):,.2f})"
        )

    result.ownership_snapshot = existing_ledger

    # -------------------------------------------------------------------------
    # Step 5: Summary
    # -------------------------------------------------------------------------
    if result.conflicts:
        log.error(f"[RECONCILE] OWNERSHIP CONFLICTS detected: {result.conflicts}")
    if result.unclassified_positions:
        log.warning(f"[RECONCILE] UNCLASSIFIED positions: {result.unclassified_positions}")
    if result.is_safe:
        log.info("[RECONCILE] Reconciliation complete — no conflicts, safe to trade")
    else:
        log.warning("[RECONCILE] Reconciliation complete — ISSUES DETECTED (see above)")

    return result
