"""Tests for Robinhood CSV ingestion module."""

import pytest
from decimal import Decimal
from datetime import date
from pathlib import Path
import tempfile
import os

from src.ingestion.robinhood_csv import (
    parse_money,
    parse_quantity,
    classify_transaction,
    RobinhoodCSVParser,
    reconstruct_holdings,
)
from src.utils.typing import TransactionType


class TestParseMoney:
    """Tests for parse_money function."""

    def test_simple_positive(self):
        assert parse_money("$9.90") == Decimal("9.90")

    def test_positive_with_commas(self):
        assert parse_money("$1,234.56") == Decimal("1234.56")

    def test_negative_parentheses(self):
        assert parse_money("($52.69)") == Decimal("-52.69")

    def test_negative_minus_sign(self):
        assert parse_money("-$100.00") == Decimal("-100.00")

    def test_no_dollar_sign(self):
        assert parse_money("500.25") == Decimal("500.25")

    def test_large_number(self):
        assert parse_money("$1,234,567.89") == Decimal("1234567.89")

    def test_empty_string(self):
        assert parse_money("") is None

    def test_none_input(self):
        assert parse_money(None) is None

    def test_invalid_string(self):
        assert parse_money("N/A") is None

    def test_whitespace(self):
        assert parse_money("  $10.00  ") == Decimal("10.00")

    def test_zero(self):
        assert parse_money("$0.00") == Decimal("0.00")

    def test_negative_zero(self):
        assert parse_money("($0.00)") == Decimal("0.00")


class TestParseQuantity:
    """Tests for parse_quantity function."""

    def test_whole_number(self):
        assert parse_quantity("100") == Decimal("100")

    def test_fractional_shares(self):
        assert parse_quantity("0.123456") == Decimal("0.123456")

    def test_with_commas(self):
        assert parse_quantity("1,000") == Decimal("1000")

    def test_empty_string(self):
        assert parse_quantity("") is None

    def test_none_input(self):
        assert parse_quantity(None) is None

    def test_negative_quantity(self):
        assert parse_quantity("-50") == Decimal("-50")

    def test_very_small_fraction(self):
        assert parse_quantity("0.00000001") == Decimal("0.00000001")


class TestClassifyTransaction:
    """Tests for classify_transaction function."""

    def test_buy(self):
        assert classify_transaction("Buy") == TransactionType.BUY

    def test_buy_lowercase(self):
        assert classify_transaction("buy") == TransactionType.BUY

    def test_sell(self):
        assert classify_transaction("Sell") == TransactionType.SELL

    def test_dividend_cdiv(self):
        assert classify_transaction("CDIV") == TransactionType.DIVIDEND

    def test_dividend_mdiv(self):
        assert classify_transaction("MDIV") == TransactionType.DIVIDEND

    def test_dividend_word(self):
        assert classify_transaction("Dividend") == TransactionType.DIVIDEND

    def test_gold_transfer(self):
        assert classify_transaction("GOLD") == TransactionType.GOLD_TRANSFER

    def test_stock_split(self):
        assert classify_transaction("STSP") == TransactionType.STOCK_SPLIT

    def test_acats(self):
        assert classify_transaction("ACATS") == TransactionType.ACATS_TRANSFER

    def test_unknown(self):
        assert classify_transaction("UNKNOWN") == TransactionType.OTHER


class TestRobinhoodCSVParser:
    """Tests for RobinhoodCSVParser class."""

    @pytest.fixture
    def sample_csv_content(self):
        return '''Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
01/15/2024,01/15/2024,01/17/2024,AAPL,APPLE INC,Buy,10,$150.00,"($1,500.00)"
01/20/2024,01/20/2024,01/22/2024,AAPL,APPLE INC,Sell,5,$155.00,$775.00
02/01/2024,02/01/2024,02/01/2024,AAPL,APPLE INC,CDIV,,$0.24,$2.40
01/10/2024,01/10/2024,01/12/2024,MSFT,MICROSOFT CORP,Buy,5,$350.00,"($1,750.00)"
'''

    @pytest.fixture
    def parser(self):
        return RobinhoodCSVParser()

    def test_parse_file(self, parser, sample_csv_content):
        """Test parsing a sample CSV file."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False
        ) as f:
            f.write(sample_csv_content)
            temp_path = f.name

        try:
            transactions = parser.parse_file(temp_path)
            assert len(transactions) == 4

            # Check first transaction (buy)
            aapl_buy = transactions[0]
            assert aapl_buy.symbol == "AAPL"
            assert aapl_buy.trans_type == TransactionType.BUY
            assert aapl_buy.quantity == Decimal("10")
            assert aapl_buy.price == Decimal("150.00")
            assert aapl_buy.amount == Decimal("-1500.00")

            # Check sell transaction
            aapl_sell = transactions[1]
            assert aapl_sell.trans_type == TransactionType.SELL
            assert aapl_sell.quantity == Decimal("5")

            # Check dividend
            aapl_div = transactions[2]
            assert aapl_div.trans_type == TransactionType.DIVIDEND

        finally:
            os.unlink(temp_path)

    def test_parse_csv_with_quotes(self, parser):
        """Test parsing CSV with embedded quotes in description."""
        csv_content = '''Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
01/15/2024,01/15/2024,01/17/2024,TEST,"Test ""Quoted"" Company",Buy,10,$100.00,"($1,000.00)"
'''
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False
        ) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            transactions = parser.parse_file(temp_path)
            assert len(transactions) == 1
            assert transactions[0].symbol == "TEST"
        finally:
            os.unlink(temp_path)


class TestReconstructHoldings:
    """Tests for reconstruct_holdings function."""

    @pytest.fixture
    def sample_transactions(self):
        from src.utils.typing import Transaction
        return [
            Transaction(
                activity_date=date(2024, 1, 10),
                settle_date=date(2024, 1, 12),
                symbol="AAPL",
                description="APPLE INC",
                trans_type=TransactionType.BUY,
                quantity=Decimal("10"),
                price=Decimal("150.00"),
                amount=Decimal("-1500.00"),
            ),
            Transaction(
                activity_date=date(2024, 1, 20),
                settle_date=date(2024, 1, 22),
                symbol="AAPL",
                description="APPLE INC",
                trans_type=TransactionType.BUY,
                quantity=Decimal("5"),
                price=Decimal("160.00"),
                amount=Decimal("-800.00"),
            ),
            Transaction(
                activity_date=date(2024, 2, 1),
                settle_date=date(2024, 2, 3),
                symbol="AAPL",
                description="APPLE INC",
                trans_type=TransactionType.SELL,
                quantity=Decimal("3"),
                price=Decimal("170.00"),
                amount=Decimal("510.00"),
            ),
        ]

    def test_basic_reconstruction(self, sample_transactions):
        """Test basic holding reconstruction with buys and sells."""
        holdings = reconstruct_holdings(sample_transactions)

        assert "AAPL" in holdings
        aapl = holdings["AAPL"]

        # Should have 10 + 5 - 3 = 12 shares
        assert aapl.quantity == Decimal("12")

        # Average cost basis: (1500 + 800) / (10 + 5) = 153.33
        # After selling 3: remaining cost basis proportionally
        expected_avg_cost = (Decimal("1500") + Decimal("800")) / Decimal("15")
        assert abs(aapl.avg_cost_basis - expected_avg_cost) < Decimal("0.01")

    def test_empty_transactions(self):
        """Test with no transactions."""
        holdings = reconstruct_holdings([])
        assert holdings == {}

    def test_complete_sell(self):
        """Test when all shares are sold."""
        from src.utils.typing import Transaction
        transactions = [
            Transaction(
                activity_date=date(2024, 1, 10),
                settle_date=date(2024, 1, 12),
                symbol="AAPL",
                description="APPLE INC",
                trans_type=TransactionType.BUY,
                quantity=Decimal("10"),
                price=Decimal("150.00"),
                amount=Decimal("-1500.00"),
            ),
            Transaction(
                activity_date=date(2024, 2, 1),
                settle_date=date(2024, 2, 3),
                symbol="AAPL",
                description="APPLE INC",
                trans_type=TransactionType.SELL,
                quantity=Decimal("10"),
                price=Decimal("170.00"),
                amount=Decimal("1700.00"),
            ),
        ]

        holdings = reconstruct_holdings(transactions)
        # Position closed, should not appear in holdings
        assert "AAPL" not in holdings or holdings["AAPL"].quantity == Decimal("0")


class TestCSVEdgeCases:
    """Tests for edge cases in CSV parsing."""

    @pytest.fixture
    def parser(self):
        return RobinhoodCSVParser()

    def test_fractional_shares(self, parser):
        """Test parsing fractional share quantities."""
        csv_content = '''Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
01/15/2024,01/15/2024,01/17/2024,AMZN,AMAZON.COM INC,Buy,0.123456,$180.00,($22.22)
'''
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False
        ) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            transactions = parser.parse_file(temp_path)
            assert len(transactions) == 1
            assert transactions[0].quantity == Decimal("0.123456")
        finally:
            os.unlink(temp_path)

    def test_missing_price(self, parser):
        """Test parsing transaction with missing price (like dividends)."""
        csv_content = '''Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
02/01/2024,02/01/2024,02/01/2024,AAPL,APPLE INC,CDIV,,,($5.00)
'''
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False
        ) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            transactions = parser.parse_file(temp_path)
            assert len(transactions) == 1
            assert transactions[0].trans_type == TransactionType.DIVIDEND
            assert transactions[0].price is None or transactions[0].price == Decimal("0")
        finally:
            os.unlink(temp_path)
