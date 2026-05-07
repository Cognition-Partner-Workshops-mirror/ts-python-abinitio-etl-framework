"""Tests for the inbound ETL module — file reader, validator, and processor."""
import pytest
import os
from pathlib import Path

import pandas as pd

from etl.inbound.file_reader import PipeDelimitedReader
from etl.inbound.validator import NullCheckValidator
from etl.inbound.inbound_processor import InboundProcessor

# Column names matching the transactions.dat layout
TRANSACTION_COLUMNS = [
    "txn_id", "txn_timestamp", "customer_id", "txn_type",
    "merchant_name", "merchant_category", "amount", "item_count",
    "sku", "quantity", "line_total", "channel",
]


# ---------------------------------------------------------------------------
# PipeDelimitedReader tests
# ---------------------------------------------------------------------------

class TestPipeDelimitedReader:
    """Tests for PipeDelimitedReader reading pipe-separated source files."""

    def test_read_returns_dataframe_with_correct_shape(self):
        """Verify that the reader returns a DataFrame with expected rows and columns."""
        reader = PipeDelimitedReader(
            file_path="data/sample/transactions.dat",
            column_names=TRANSACTION_COLUMNS,
        )
        df = reader.read()
        # transactions.dat has 5 data rows and 12 columns
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert list(df.columns) == TRANSACTION_COLUMNS

    def test_read_parses_values_correctly(self):
        """Ensure individual cell values are parsed from the pipe-delimited layout."""
        reader = PipeDelimitedReader(
            file_path="data/sample/transactions.dat",
            column_names=TRANSACTION_COLUMNS,
        )
        df = reader.read()
        # First row checks
        assert df.iloc[0]["txn_id"] == "TXN001"
        assert df.iloc[0]["merchant_name"] == "MERCHANT_A"
        assert df.iloc[0]["channel"] == "WEB"

    def test_read_with_metadata_returns_expected_keys(self):
        """Check that read_with_metadata returns data, row_count, file_path, and columns."""
        reader = PipeDelimitedReader(
            file_path="data/sample/transactions.dat",
            column_names=TRANSACTION_COLUMNS,
        )
        result = reader.read_with_metadata()
        assert "data" in result
        assert "row_count" in result
        assert "file_path" in result
        assert "columns" in result
        assert result["row_count"] == 5
        assert result["file_path"] == "data/sample/transactions.dat"
        assert result["columns"] == TRANSACTION_COLUMNS


# ---------------------------------------------------------------------------
# NullCheckValidator tests
# ---------------------------------------------------------------------------

class TestNullCheckValidator:
    """Tests for NullCheckValidator detecting null/empty fields."""

    def test_detects_null_and_empty_fields(self):
        """Validate that null/empty values in required fields are flagged."""
        reader = PipeDelimitedReader(
            file_path="data/sample/transactions_with_nulls.dat",
            column_names=TRANSACTION_COLUMNS,
        )
        df = reader.read()
        validator = NullCheckValidator(
            required_fields=["txn_timestamp", "customer_id", "merchant_name", "amount", "channel"],
        )
        result = validator.validate(df)

        # Rows with blanks: TXN002 (txn_timestamp), TXN003 (customer_id),
        # TXN004 (merchant_name), TXN005 (amount), TXN006 (channel)
        assert result["stats"]["rejected"] == 5
        assert result["stats"]["valid"] == 2  # TXN001 and TXN007
        assert len(result["rejection_reasons"]) == 5

    def test_passes_all_valid_records(self):
        """When all required fields are populated, every record should pass."""
        reader = PipeDelimitedReader(
            file_path="data/sample/transactions.dat",
            column_names=TRANSACTION_COLUMNS,
        )
        df = reader.read()
        validator = NullCheckValidator(
            required_fields=["txn_id", "txn_timestamp", "customer_id", "amount"],
        )
        result = validator.validate(df)

        assert result["stats"]["total"] == 5
        assert result["stats"]["valid"] == 5
        assert result["stats"]["rejected"] == 0
        assert len(result["rejection_reasons"]) == 0

    def test_rejection_reason_structure(self):
        """Verify each rejection reason dict contains row_index, field, and reason."""
        df = pd.DataFrame({
            "id": ["1", "2"],
            "name": ["Alice", ""],
        })
        validator = NullCheckValidator(required_fields=["name"])
        result = validator.validate(df)

        assert len(result["rejection_reasons"]) == 1
        reason = result["rejection_reasons"][0]
        assert reason["row_index"] == 1
        assert reason["field"] == "name"
        assert reason["reason"] == "Null or empty value"


# ---------------------------------------------------------------------------
# InboundProcessor end-to-end tests
# ---------------------------------------------------------------------------

class TestInboundProcessor:
    """End-to-end tests for the InboundProcessor orchestration class."""

    def test_end_to_end_processing(self, tmp_path):
        """Run the full inbound pipeline and verify output files and stats."""
        output_dir = str(tmp_path / "processed")

        processor = InboundProcessor(
            input_file="data/sample/transactions_with_nulls.dat",
            column_names=TRANSACTION_COLUMNS,
            required_fields=["txn_timestamp", "customer_id", "merchant_name", "amount", "channel"],
            output_dir=output_dir,
        )
        summary = processor.process()

        # Verify summary stats
        assert summary["stats"]["total"] == 7
        assert summary["stats"]["valid"] == 2
        assert summary["stats"]["rejected"] == 5

        # Verify output files were created
        assert os.path.exists(summary["output_file"])
        assert os.path.exists(summary["reject_file"])

        # Verify valid output file has the right number of rows
        valid_df = pd.read_csv(summary["output_file"], sep="|", header=None)
        assert len(valid_df) == 2

        # Verify reject output file has the right number of rows
        rejected_df = pd.read_csv(summary["reject_file"], sep="|", header=None)
        assert len(rejected_df) == 5

    def test_all_valid_records_no_rejects(self, tmp_path):
        """When all records pass validation, the reject file should be empty."""
        output_dir = str(tmp_path / "processed")

        processor = InboundProcessor(
            input_file="data/sample/transactions.dat",
            column_names=TRANSACTION_COLUMNS,
            required_fields=["txn_id", "customer_id"],
            output_dir=output_dir,
        )
        summary = processor.process()

        assert summary["stats"]["valid"] == 5
        assert summary["stats"]["rejected"] == 0

        # Reject file should exist but be empty (0 bytes or just empty)
        reject_path = summary["reject_file"]
        assert os.path.exists(reject_path)
        assert os.path.getsize(reject_path) == 0
