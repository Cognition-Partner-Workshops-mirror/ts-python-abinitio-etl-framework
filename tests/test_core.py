"""
Unit tests for the Core ETL module.
Tests cover: JournalEntry model validation, TransactionProcessor logic for
purchases and refunds, batch processing, and CoreProcessor end-to-end flow.
"""
import pytest
import pandas as pd
from pathlib import Path

from etl.core.journal_entry import JournalEntry
from etl.core.transaction_processor import TransactionProcessor
from etl.core.core_processor import CoreProcessor


class TestJournalEntry:
    """Tests for the JournalEntry Pydantic model and its validation rules."""

    def test_valid_debit_entry(self):
        """A valid DEBIT entry should be created without errors."""
        entry = JournalEntry(
            journal_id="JRN-0001",
            txn_id="TXN001",
            entry_type="DEBIT",
            account="CUST_1001",
            amount=49.99,
            currency="USD",
            txn_timestamp="2024-01-15 09:23:45",
            description="Purchase debit from customer 1001",
        )
        assert entry.entry_type == "DEBIT"
        assert entry.amount == 49.99

    def test_valid_credit_entry(self):
        """A valid CREDIT entry should be created without errors."""
        entry = JournalEntry(
            journal_id="JRN-0002",
            txn_id="TXN001",
            entry_type="CREDIT",
            account="MERCH_MERCHANT_A",
            amount=49.99,
            currency="USD",
            txn_timestamp="2024-01-15 09:23:45",
            description="Purchase credit to merchant MERCHANT_A",
        )
        assert entry.entry_type == "CREDIT"
        assert entry.account == "MERCH_MERCHANT_A"

    def test_invalid_entry_type_rejected(self):
        """An entry_type other than CREDIT/DEBIT should raise a ValidationError."""
        with pytest.raises(Exception):
            JournalEntry(
                journal_id="JRN-0003",
                txn_id="TXN001",
                entry_type="INVALID",
                account="CUST_1001",
                amount=10.00,
                currency="USD",
                txn_timestamp="2024-01-15 09:23:45",
                description="Bad entry",
            )

    def test_negative_amount_rejected(self):
        """A negative amount should raise a ValidationError."""
        with pytest.raises(Exception):
            JournalEntry(
                journal_id="JRN-0004",
                txn_id="TXN001",
                entry_type="DEBIT",
                account="CUST_1001",
                amount=-10.00,
                currency="USD",
                txn_timestamp="2024-01-15 09:23:45",
                description="Negative amount",
            )

    def test_zero_amount_rejected(self):
        """A zero amount should raise a ValidationError."""
        with pytest.raises(Exception):
            JournalEntry(
                journal_id="JRN-0005",
                txn_id="TXN001",
                entry_type="CREDIT",
                account="CUST_1001",
                amount=0.0,
                currency="USD",
                txn_timestamp="2024-01-15 09:23:45",
                description="Zero amount",
            )


class TestTransactionProcessorPurchase:
    """Tests for TransactionProcessor handling PURCHASE transactions (txn_type=1)."""

    @pytest.fixture
    def processor(self):
        return TransactionProcessor(currency="USD")

    @pytest.fixture
    def purchase_row(self):
        """A sample purchase transaction row."""
        return pd.Series({
            "txn_id": "TXN001",
            "txn_timestamp": "2024-01-15 09:23:45",
            "customer_id": "1001",
            "txn_type": 1,
            "merchant_name": "MERCHANT_A",
            "merchant_category": "Retail",
            "amount": 49.99,
            "item_count": 1,
            "sku": "SKU-100",
            "quantity": 1,
            "line_total": 49.99,
            "channel": "WEB",
        })

    def test_purchase_generates_two_entries(self, processor, purchase_row):
        """A purchase transaction should produce exactly 2 journal entries."""
        entries = processor.process_transaction(purchase_row)
        assert len(entries) == 2

    def test_purchase_debit_entry(self, processor, purchase_row):
        """First entry for a purchase should be a DEBIT to the customer account."""
        entries = processor.process_transaction(purchase_row)
        debit = entries[0]
        assert debit.entry_type == "DEBIT"
        assert debit.account == "CUST_1001"
        assert debit.amount == 49.99

    def test_purchase_credit_entry(self, processor, purchase_row):
        """Second entry for a purchase should be a CREDIT to the merchant account."""
        entries = processor.process_transaction(purchase_row)
        credit = entries[1]
        assert credit.entry_type == "CREDIT"
        assert credit.account == "MERCH_MERCHANT_A"
        assert credit.amount == 49.99

    def test_purchase_entries_have_sequential_ids(self, processor, purchase_row):
        """Journal IDs should be sequential across entries."""
        entries = processor.process_transaction(purchase_row)
        assert entries[0].journal_id == "JRN-0001"
        assert entries[1].journal_id == "JRN-0002"


class TestTransactionProcessorRefund:
    """Tests for TransactionProcessor handling REFUND transactions (txn_type=2)."""

    @pytest.fixture
    def processor(self):
        return TransactionProcessor(currency="USD")

    @pytest.fixture
    def refund_row(self):
        """A sample refund transaction row."""
        return pd.Series({
            "txn_id": "TXN003",
            "txn_timestamp": "2024-01-15 14:45:00",
            "customer_id": "1004",
            "txn_type": 2,
            "merchant_name": "MERCHANT_A",
            "merchant_category": "Retail",
            "amount": 75.00,
            "item_count": 2,
            "sku": "SKU-100",
            "quantity": 2,
            "line_total": 49.99,
            "channel": "WEB",
        })

    def test_refund_generates_two_entries(self, processor, refund_row):
        """A refund transaction should produce exactly 2 journal entries."""
        entries = processor.process_transaction(refund_row)
        assert len(entries) == 2

    def test_refund_credit_entry(self, processor, refund_row):
        """First entry for a refund should be a CREDIT to the customer account."""
        entries = processor.process_transaction(refund_row)
        credit = entries[0]
        assert credit.entry_type == "CREDIT"
        assert credit.account == "CUST_1004"
        assert credit.amount == 75.00

    def test_refund_debit_entry(self, processor, refund_row):
        """Second entry for a refund should be a DEBIT to the merchant account."""
        entries = processor.process_transaction(refund_row)
        debit = entries[1]
        assert debit.entry_type == "DEBIT"
        assert debit.account == "MERCH_MERCHANT_A"
        assert debit.amount == 75.00


class TestTransactionProcessorBatch:
    """Tests for TransactionProcessor.process_batch() with multiple transactions."""

    @pytest.fixture
    def processor(self):
        return TransactionProcessor(currency="USD")

    @pytest.fixture
    def batch_df(self):
        """A DataFrame with a mix of purchase and refund transactions."""
        return pd.DataFrame([
            {"txn_id": "TXN001", "txn_timestamp": "2024-01-15 09:23:45", "customer_id": "1001",
             "txn_type": 1, "merchant_name": "MERCHANT_A", "merchant_category": "Retail",
             "amount": 49.99, "item_count": 1, "sku": "SKU-100", "quantity": 1,
             "line_total": 49.99, "channel": "WEB"},
            {"txn_id": "TXN002", "txn_timestamp": "2024-01-15 10:15:22", "customer_id": "1002",
             "txn_type": 1, "merchant_name": "MERCHANT_B", "merchant_category": "Electronics",
             "amount": 29.99, "item_count": 1, "sku": "SKU-200", "quantity": 1,
             "line_total": 29.99, "channel": "STORE"},
            {"txn_id": "TXN003", "txn_timestamp": "2024-01-15 14:45:00", "customer_id": "1004",
             "txn_type": 2, "merchant_name": "MERCHANT_A", "merchant_category": "Retail",
             "amount": 75.00, "item_count": 2, "sku": "SKU-100", "quantity": 2,
             "line_total": 49.99, "channel": "WEB"},
        ])

    def test_batch_generates_correct_entry_count(self, processor, batch_df):
        """3 transactions should produce 6 journal entries (2 per transaction)."""
        result_df = processor.process_batch(batch_df)
        assert len(result_df) == 6

    def test_batch_has_correct_columns(self, processor, batch_df):
        """Output DataFrame should contain all journal entry columns."""
        result_df = processor.process_batch(batch_df)
        expected_cols = {"journal_id", "txn_id", "entry_type", "account",
                         "amount", "currency", "txn_timestamp", "description"}
        assert set(result_df.columns) == expected_cols

    def test_batch_debits_equal_credits(self, processor, batch_df):
        """Total debit amounts must equal total credit amounts (accounting balance)."""
        result_df = processor.process_batch(batch_df)
        total_debits = result_df.loc[result_df["entry_type"] == "DEBIT", "amount"].sum()
        total_credits = result_df.loc[result_df["entry_type"] == "CREDIT", "amount"].sum()
        assert abs(total_debits - total_credits) < 0.001


class TestCoreProcessor:
    """End-to-end tests for CoreProcessor pipeline."""

    @pytest.fixture
    def sample_input(self, tmp_path):
        """Create a temporary validated transactions file for testing."""
        content = (
            "TXN001|2024-01-15 09:23:45|1001|1|MERCHANT_A|Retail|49.99|1|SKU-100|1|49.99|WEB\n"
            "TXN002|2024-01-15 10:15:22|1002|1|MERCHANT_B|Electronics|29.99|1|SKU-200|1|29.99|STORE\n"
            "TXN003|2024-01-15 14:45:00|1004|2|MERCHANT_A|Retail|75.00|2|SKU-100|2|49.99|WEB\n"
        )
        input_file = tmp_path / "validated_transactions.dat"
        input_file.write_text(content)
        return str(input_file)

    @pytest.fixture
    def output_dir(self, tmp_path):
        """Create a temporary output directory."""
        out = tmp_path / "output"
        out.mkdir()
        return str(out)

    def test_process_returns_summary(self, sample_input, output_dir):
        """process() should return a dict with all expected summary keys."""
        processor = CoreProcessor(input_file=sample_input, output_dir=output_dir)
        summary = processor.process()
        assert "total_transactions" in summary
        assert "journal_entries_generated" in summary
        assert "total_debits" in summary
        assert "total_credits" in summary
        assert "total_debit_amount" in summary
        assert "total_credit_amount" in summary

    def test_process_correct_transaction_count(self, sample_input, output_dir):
        """Should process all 3 transactions from the input file."""
        processor = CoreProcessor(input_file=sample_input, output_dir=output_dir)
        summary = processor.process()
        assert summary["total_transactions"] == 3

    def test_process_correct_entry_count(self, sample_input, output_dir):
        """3 transactions should generate 6 journal entries."""
        processor = CoreProcessor(input_file=sample_input, output_dir=output_dir)
        summary = processor.process()
        assert summary["journal_entries_generated"] == 6

    def test_process_writes_output_file(self, sample_input, output_dir):
        """process() should create journal_entries.dat in the output directory."""
        processor = CoreProcessor(input_file=sample_input, output_dir=output_dir)
        processor.process()
        output_file = Path(output_dir) / "journal_entries.dat"
        assert output_file.exists()

    def test_process_output_file_has_correct_rows(self, sample_input, output_dir):
        """Output file should have 6 pipe-separated rows (one per journal entry)."""
        processor = CoreProcessor(input_file=sample_input, output_dir=output_dir)
        processor.process()
        output_file = Path(output_dir) / "journal_entries.dat"
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 6

    def test_process_debits_equal_credits(self, sample_input, output_dir):
        """Accounting balance: total debit amount must equal total credit amount."""
        processor = CoreProcessor(input_file=sample_input, output_dir=output_dir)
        summary = processor.process()
        assert abs(summary["total_debit_amount"] - summary["total_credit_amount"]) < 0.001

    def test_process_debit_credit_counts(self, sample_input, output_dir):
        """Each transaction produces one debit and one credit, so counts should be equal."""
        processor = CoreProcessor(input_file=sample_input, output_dir=output_dir)
        summary = processor.process()
        # 3 transactions -> 3 debits and 3 credits
        assert summary["total_debits"] == 3
        assert summary["total_credits"] == 3
