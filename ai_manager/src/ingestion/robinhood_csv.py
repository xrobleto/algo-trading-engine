"""
Robinhood CSV Activity Ledger Parser

Parses Robinhood activity exports and reconstructs portfolio holdings.

Expected CSV columns:
    Activity Date, Process Date, Settle Date, Instrument, Description,
    Trans Code, Quantity, Price, Amount

Features:
    - Handles embedded newlines in Description (quoted CSV fields)
    - Parses money with "$", "()", and commas
    - Supports fractional shares (Decimal precision)
    - Reconstructs holdings with average cost basis
    - Tracks dividends and fees separately
"""

import csv
import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from ..utils.logging import get_logger
from ..utils.time import parse_date
from ..utils.typing import Transaction, TransactionType, Holding

logger = get_logger(__name__)


# ============================================================
# MONEY PARSING
# ============================================================

def parse_money(value: str) -> Optional[Decimal]:
    """
    Parse a money string into a Decimal.

    Handles:
        - "$9.90" -> Decimal("9.90")
        - "($52.69)" -> Decimal("-52.69")
        - "$1,234.56" -> Decimal("1234.56")
        - "" or None -> None
        - "$0.10 " (trailing space) -> Decimal("0.10")

    Args:
        value: Money string to parse

    Returns:
        Decimal value or None if empty/invalid
    """
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    try:
        # Check for negative format: (value)
        is_negative = value.startswith("(") and value.endswith(")")
        if is_negative:
            value = value[1:-1]

        # Remove dollar sign and commas
        value = value.replace("$", "").replace(",", "").strip()

        if not value:
            return None

        result = Decimal(value)
        if is_negative:
            result = -result

        return result

    except (InvalidOperation, ValueError) as e:
        logger.warning(f"Failed to parse money value '{value}': {e}")
        return None


def parse_quantity(value: str) -> Optional[Decimal]:
    """
    Parse a quantity string into a Decimal.

    Handles fractional shares (e.g., "0.123456").

    Args:
        value: Quantity string to parse

    Returns:
        Decimal value or None if empty/invalid
    """
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    try:
        # Remove commas if present
        value = value.replace(",", "")
        return Decimal(value)
    except (InvalidOperation, ValueError) as e:
        logger.warning(f"Failed to parse quantity '{value}': {e}")
        return None


# ============================================================
# TRANSACTION CLASSIFICATION
# ============================================================

# Default transaction code mappings
DEFAULT_BUY_CODES = {"Buy"}
DEFAULT_SELL_CODES = {"Sell"}
DEFAULT_DIVIDEND_CODES = {"CDIV", "MDIV"}  # Cash dividend, Manufactured dividend
DEFAULT_FEE_CODES = {"GOLD"}  # Robinhood Gold subscription


def classify_transaction(
    trans_code: str,
    buy_codes: set = None,
    sell_codes: set = None,
    dividend_codes: set = None,
    fee_codes: set = None
) -> TransactionType:
    """
    Classify a transaction code into a TransactionType.

    Args:
        trans_code: The Trans Code from the CSV
        buy_codes: Set of codes that indicate BUY
        sell_codes: Set of codes that indicate SELL
        dividend_codes: Set of codes that indicate DIVIDEND
        fee_codes: Set of codes that indicate FEE

    Returns:
        TransactionType enum value
    """
    if not trans_code:
        return TransactionType.UNKNOWN

    trans_code = trans_code.strip()

    buy_codes = buy_codes or DEFAULT_BUY_CODES
    sell_codes = sell_codes or DEFAULT_SELL_CODES
    dividend_codes = dividend_codes or DEFAULT_DIVIDEND_CODES
    fee_codes = fee_codes or DEFAULT_FEE_CODES

    if trans_code in buy_codes:
        return TransactionType.BUY
    elif trans_code in sell_codes:
        return TransactionType.SELL
    elif trans_code in dividend_codes:
        return TransactionType.DIVIDEND
    elif trans_code in fee_codes:
        return TransactionType.FEE
    else:
        return TransactionType.UNKNOWN


# ============================================================
# CSV PARSER
# ============================================================

@dataclass
class ParseResult:
    """Result of parsing a CSV file."""
    transactions: List[Transaction]
    holdings: Dict[str, Holding]
    total_dividends: Decimal
    total_fees: Decimal
    cash_balance: Decimal
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class RobinhoodCSVParser:
    """
    Parser for Robinhood activity CSV exports.

    Handles:
        - Multi-line quoted descriptions
        - Money parsing with $ and ()
        - Fractional shares
        - Holdings reconstruction with average cost
    """

    # Expected column names (case-insensitive matching)
    EXPECTED_COLUMNS = {
        "activity date", "process date", "settle date",
        "instrument", "description", "trans code",
        "quantity", "price", "amount"
    }

    def __init__(
        self,
        buy_codes: Optional[List[str]] = None,
        sell_codes: Optional[List[str]] = None,
        dividend_codes: Optional[List[str]] = None,
        fee_codes: Optional[List[str]] = None,
        cost_basis_method: str = "average",
        starting_balances: Optional[Dict[str, Tuple[float, float]]] = None
    ):
        """
        Initialize parser with configuration.

        Args:
            buy_codes: Transaction codes for BUY
            sell_codes: Transaction codes for SELL
            dividend_codes: Transaction codes for DIVIDEND
            fee_codes: Transaction codes for FEE
            cost_basis_method: "average" or "fifo"
            starting_balances: Pre-CSV holdings {symbol: (shares, avg_cost)}
        """
        self.buy_codes = set(buy_codes) if buy_codes else DEFAULT_BUY_CODES
        self.sell_codes = set(sell_codes) if sell_codes else DEFAULT_SELL_CODES
        self.dividend_codes = set(dividend_codes) if dividend_codes else DEFAULT_DIVIDEND_CODES
        self.fee_codes = set(fee_codes) if fee_codes else DEFAULT_FEE_CODES
        self.cost_basis_method = cost_basis_method
        self.starting_balances = starting_balances or {}

    def parse_file(self, file_path: str) -> ParseResult:
        """
        Parse a Robinhood CSV file.

        Args:
            file_path: Path to the CSV file

        Returns:
            ParseResult with transactions, holdings, and metadata
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        logger.info(f"Parsing Robinhood CSV: {file_path}")

        transactions = []
        warnings = []
        errors = []

        # Read CSV with proper quoting for embedded newlines
        with open(path, 'r', encoding='utf-8-sig') as f:
            # Detect delimiter (comma vs semicolon)
            sample = f.read(2048)
            f.seek(0)

            dialect = csv.Sniffer().sniff(sample, delimiters=',;\t')

            reader = csv.DictReader(f, dialect=dialect)

            # Validate columns
            if reader.fieldnames:
                actual_columns = {c.lower().strip() for c in reader.fieldnames if c}
                missing = self.EXPECTED_COLUMNS - actual_columns
                if missing:
                    warnings.append(f"Missing expected columns: {missing}")

            # Parse rows
            for row_num, row in enumerate(reader, start=2):
                try:
                    txn = self._parse_row(row, row_num)
                    if txn:
                        transactions.append(txn)
                except Exception as e:
                    errors.append(f"Row {row_num}: {e}")
                    logger.warning(f"Failed to parse row {row_num}: {e}")

        logger.info(f"Parsed {len(transactions)} transactions")

        # Sort by date (oldest first)
        transactions.sort(key=lambda t: (
            t.activity_date,
            t.process_date or t.activity_date,
            t.settle_date or t.activity_date
        ))

        # Reconstruct holdings
        holdings, total_dividends, total_fees, cash = reconstruct_holdings(
            transactions,
            cost_basis_method=self.cost_basis_method,
            starting_balances=self.starting_balances
        )

        return ParseResult(
            transactions=transactions,
            holdings=holdings,
            total_dividends=total_dividends,
            total_fees=total_fees,
            cash_balance=cash,
            warnings=warnings,
            errors=errors
        )

    def _parse_row(self, row: Dict[str, str], row_num: int) -> Optional[Transaction]:
        """
        Parse a single CSV row into a Transaction.

        Args:
            row: Dictionary from csv.DictReader
            row_num: Row number for error reporting

        Returns:
            Transaction object or None if row should be skipped
        """
        # Normalize column names (handle case variations)
        normalized = {}
        for key, value in row.items():
            if key:
                normalized[key.lower().strip()] = value

        # Extract fields
        activity_date_str = normalized.get("activity date", "").strip()
        process_date_str = normalized.get("process date", "").strip()
        settle_date_str = normalized.get("settle date", "").strip()
        instrument = normalized.get("instrument", "").strip() or None
        description = normalized.get("description", "").strip()
        trans_code = normalized.get("trans code", "").strip()
        quantity_str = normalized.get("quantity", "").strip()
        price_str = normalized.get("price", "").strip()
        amount_str = normalized.get("amount", "").strip()

        # Parse activity date (required)
        activity_date = parse_date(activity_date_str)
        if not activity_date:
            logger.debug(f"Row {row_num}: Skipping row with no activity date")
            return None

        # Parse other dates
        process_date = parse_date(process_date_str)
        settle_date = parse_date(settle_date_str)

        # Classify transaction
        trans_type = classify_transaction(
            trans_code,
            self.buy_codes,
            self.sell_codes,
            self.dividend_codes,
            self.fee_codes
        )

        # Parse numeric fields
        quantity = parse_quantity(quantity_str)
        price = parse_money(price_str)
        amount = parse_money(amount_str)

        # Log unknown transaction types
        if trans_type == TransactionType.UNKNOWN and trans_code:
            logger.debug(f"Row {row_num}: Unknown trans code '{trans_code}'")

        return Transaction(
            activity_date=activity_date,
            process_date=process_date,
            settle_date=settle_date,
            symbol=instrument,
            description=description,
            trans_code=trans_code,
            trans_type=trans_type,
            quantity=quantity,
            price=price,
            amount=amount,
            raw_row=dict(row)
        )


# ============================================================
# HOLDINGS RECONSTRUCTION
# ============================================================

@dataclass
class _HoldingState:
    """Internal state for reconstructing a holding."""
    shares: Decimal = Decimal("0")
    total_cost: Decimal = Decimal("0")
    dividends: Decimal = Decimal("0")
    fees: Decimal = Decimal("0")
    last_activity: Optional[date] = None
    # For FIFO
    lots: List[Tuple[Decimal, Decimal]] = field(default_factory=list)  # [(qty, price), ...]


def reconstruct_holdings(
    transactions: List[Transaction],
    cost_basis_method: str = "average",
    starting_balances: Optional[Dict[str, Tuple[float, float]]] = None
) -> Tuple[Dict[str, Holding], Decimal, Decimal, Decimal]:
    """
    Reconstruct portfolio holdings from transaction history.

    Args:
        transactions: List of transactions sorted by date
        cost_basis_method: "average" or "fifo"
        starting_balances: Optional dict of pre-existing holdings before CSV begins.
                          Format: {symbol: (shares, avg_cost_per_share)}
                          Use this when CSV doesn't include full history.

    Returns:
        Tuple of:
            - Dict mapping symbol to Holding
            - Total dividends received
            - Total fees paid
            - Cash balance
    """
    holdings_state: Dict[str, _HoldingState] = {}
    total_dividends = Decimal("0")
    total_fees = Decimal("0")
    cash = Decimal("0")

    # Initialize with starting balances (pre-CSV holdings)
    if starting_balances:
        for symbol, (shares, avg_cost) in starting_balances.items():
            if shares > 0:
                shares_dec = Decimal(str(shares))
                cost_dec = Decimal(str(avg_cost))
                holdings_state[symbol] = _HoldingState(
                    shares=shares_dec,
                    total_cost=shares_dec * cost_dec,
                )
                logger.info(f"Starting balance: {symbol} = {shares} shares @ ${avg_cost:.2f}")

    for txn in transactions:
        symbol = txn.symbol

        # Handle transactions without a symbol (fees, etc.)
        if txn.trans_type == TransactionType.FEE:
            if txn.amount:
                fee_amount = abs(txn.amount)
                total_fees += fee_amount
                cash -= fee_amount
            continue

        if txn.trans_type == TransactionType.DIVIDEND:
            if txn.amount:
                total_dividends += txn.amount
                cash += txn.amount
                if symbol:
                    if symbol not in holdings_state:
                        holdings_state[symbol] = _HoldingState()
                    holdings_state[symbol].dividends += txn.amount
            continue

        # BUY or SELL requires a symbol
        if not symbol:
            continue

        if symbol not in holdings_state:
            holdings_state[symbol] = _HoldingState()

        state = holdings_state[symbol]
        state.last_activity = txn.activity_date

        if txn.trans_type == TransactionType.BUY:
            if txn.quantity and txn.quantity > 0:
                qty = txn.quantity

                # Determine cost for this purchase
                if txn.amount:
                    # Use the actual amount (negative for buys)
                    purchase_cost = abs(txn.amount)
                elif txn.price:
                    purchase_cost = qty * txn.price
                else:
                    logger.warning(f"BUY without price or amount for {symbol}")
                    purchase_cost = Decimal("0")

                # Update holdings
                state.shares += qty
                state.total_cost += purchase_cost
                cash -= purchase_cost

                # For FIFO, track lots
                if cost_basis_method == "fifo" and txn.price:
                    state.lots.append((qty, txn.price))

        elif txn.trans_type == TransactionType.SELL:
            if txn.quantity and txn.quantity > 0:
                qty = txn.quantity

                if qty > state.shares:
                    logger.warning(
                        f"SELL {qty} shares of {symbol} but only {state.shares} held. "
                        "This may indicate missing BUY transactions."
                    )

                # Always subtract shares (allow negative to track missing BUY history)
                # This ensures correct final balance when CSV is incomplete
                if state.shares > 0:
                    # Only calculate cost basis if we have shares to sell
                    sell_qty = min(qty, state.shares)
                    if cost_basis_method == "fifo":
                        # FIFO: sell from oldest lots first
                        sold_cost = _fifo_sell(state, sell_qty)
                    else:
                        # Average cost method
                        avg_cost = state.total_cost / state.shares if state.shares > 0 else Decimal("0")
                        sold_cost = avg_cost * sell_qty
                        state.total_cost -= sold_cost

                state.shares -= qty  # Allow negative shares

                # Update cash with sale proceeds
                if txn.amount:
                    cash += txn.amount
                elif txn.price:
                    cash += qty * txn.price

    # Convert internal state to Holding objects
    holdings = {}
    for symbol, state in holdings_state.items():
        if state.shares > 0:
            avg_cost = (
                state.total_cost / state.shares
                if state.shares > 0
                else Decimal("0")
            )

            holdings[symbol] = Holding(
                symbol=symbol,
                shares=state.shares,
                avg_cost=avg_cost,
                total_cost=state.total_cost,
                dividends_received=state.dividends,
                fees_paid=state.fees,
                last_activity_date=state.last_activity
            )

    logger.info(f"Reconstructed {len(holdings)} holdings from {len(transactions)} transactions")

    return holdings, total_dividends, total_fees, cash


def _fifo_sell(state: _HoldingState, qty: Decimal) -> Decimal:
    """
    Process a FIFO sell, removing shares from oldest lots first.

    Args:
        state: Holding state with lots
        qty: Quantity to sell

    Returns:
        Cost basis of sold shares
    """
    remaining = qty
    sold_cost = Decimal("0")

    while remaining > 0 and state.lots:
        lot_qty, lot_price = state.lots[0]

        if lot_qty <= remaining:
            # Sell entire lot
            sold_cost += lot_qty * lot_price
            remaining -= lot_qty
            state.lots.pop(0)
        else:
            # Sell partial lot
            sold_cost += remaining * lot_price
            state.lots[0] = (lot_qty - remaining, lot_price)
            remaining = Decimal("0")

    state.total_cost -= sold_cost
    return sold_cost


# ============================================================
# PORTFOLIO SNAPSHOT BUILDER
# ============================================================

def build_portfolio_snapshot(
    holdings: Dict[str, Holding],
    prices: Dict[str, float],
    sector_map: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Build a portfolio snapshot with current prices.

    Args:
        holdings: Dict of Holding objects
        prices: Dict mapping symbol to current price
        sector_map: Optional dict mapping symbol to sector

    Returns:
        Dict with portfolio metrics
    """
    total_value = 0.0
    total_cost = Decimal("0")

    enriched_holdings = []

    for symbol, holding in holdings.items():
        price = prices.get(symbol)

        # Update holding with current data
        if price:
            holding.current_price = price
            holding.current_value = float(holding.shares) * price
            total_value += holding.current_value

            if float(holding.total_cost) > 0:
                holding.unrealized_pnl = holding.current_value - float(holding.total_cost)
                holding.unrealized_pnl_pct = (holding.unrealized_pnl / float(holding.total_cost)) * 100

        total_cost += holding.total_cost

        # Add sector if available
        if sector_map:
            holding.sector = sector_map.get(symbol)

        enriched_holdings.append(holding)

    # Sort by current value (descending)
    enriched_holdings.sort(
        key=lambda h: h.current_value if h.current_value else 0,
        reverse=True
    )

    # Calculate concentration metrics
    if total_value > 0:
        top_1_pct = enriched_holdings[0].current_value / total_value * 100 if enriched_holdings else 0
        top_3_pct = sum(h.current_value or 0 for h in enriched_holdings[:3]) / total_value * 100
        top_5_pct = sum(h.current_value or 0 for h in enriched_holdings[:5]) / total_value * 100
    else:
        top_1_pct = top_3_pct = top_5_pct = 0

    # Calculate sector allocations
    sector_allocations = {}
    if total_value > 0:
        for h in enriched_holdings:
            sector = h.sector or "Unknown"
            sector_allocations[sector] = sector_allocations.get(sector, 0) + (h.current_value or 0)

        # Convert to percentages
        for sector in sector_allocations:
            sector_allocations[sector] = (sector_allocations[sector] / total_value) * 100

    return {
        "holdings": enriched_holdings,
        "total_value": total_value,
        "total_cost": float(total_cost),
        "total_unrealized_pnl": total_value - float(total_cost) if total_value > 0 else 0,
        "top_1_holding_pct": top_1_pct,
        "top_3_holdings_pct": top_3_pct,
        "top_5_holdings_pct": top_5_pct,
        "sector_allocations": sector_allocations,
        "num_holdings": len(enriched_holdings),
    }
