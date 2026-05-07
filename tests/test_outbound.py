"""Tests for the outbound ETL module — tilde-delimited export pipeline."""
import pytest
import os
import pandas as pd
from pathlib import Path

from etl.outbound.file_writer import TildeDelimitedWriter
from etl.outbound.format_converter import FormatConverter
from etl.outbound.outbound_processor import OutboundProcessor


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df():
    """DataFrame matching the journal entry schema used across tests."""
    return pd.DataFrame(
        {
            "journal_id": ["JRN-0001", "JRN-0002", "JRN-0003", "JRN-0004"],
            "txn_id": ["TXN001", "TXN001", "TXN002", "TXN002"],
            "entry_type": ["DEBIT", "CREDIT", "DEBIT", "CREDIT"],
            "account": ["CUST_1001", "MERCH_MERCHANT_A", "CUST_1002", "MERCH_MERCHANT_B"],
            "amount": [49.99, 49.99, 29.99, 29.99],
            "currency": ["USD", "USD", "USD", "USD"],
            "txn_timestamp": [
                "2024-01-15 09:23:45",
                "2024-01-15 09:23:45",
                "2024-01-15 10:15:22",
                "2024-01-15 10:15:22",
            ],
            "description": [
                "Purchase debit from customer 1001",
                "Purchase credit to MERCHANT_A",
                "Purchase debit from customer 1002",
                "Purchase credit to MERCHANT_B",
            ],
        }
    )


@pytest.fixture
def pipe_file(tmp_path):
    """Create a temporary pipe-separated journal entry file."""
    content = (
        "JRN-0001|TXN001|DEBIT|CUST_1001|49.99|USD|2024-01-15 09:23:45|Purchase debit from customer 1001\n"
        "JRN-0002|TXN001|CREDIT|MERCH_MERCHANT_A|49.99|USD|2024-01-15 09:23:45|Purchase credit to MERCHANT_A\n"
        "JRN-0003|TXN002|DEBIT|CUST_1002|29.99|USD|2024-01-15 10:15:22|Purchase debit from customer 1002\n"
        "JRN-0004|TXN002|CREDIT|MERCH_MERCHANT_B|29.99|USD|2024-01-15 10:15:22|Purchase credit to MERCHANT_B\n"
    )
    file_path = tmp_path / "journal_entries.dat"
    file_path.write_text(content)
    return str(file_path)


# ---------------------------------------------------------------------------
# TildeDelimitedWriter tests
# ---------------------------------------------------------------------------

class TestTildeDelimitedWriter:
    """Verify tilde-separated file writing with header and trailer rows."""

    def test_write_produces_tilde_separated_output(self, tmp_path, sample_df):
        """Data rows should use tilde (~) as the field delimiter."""
        output = str(tmp_path / "output.dat")
        writer = TildeDelimitedWriter(output_path=output)
        stats = writer.write(sample_df, include_header=False, include_trailer=False)

        lines = Path(output).read_text().strip().split("\n")
        # Each data row must contain tildes
        for line in lines:
            assert "~" in line, f"Expected tilde delimiter in: {line}"
        assert stats["record_count"] == 4

    def test_write_includes_header_and_trailer(self, tmp_path, sample_df):
        """Output should start with HDR and end with TRL when flags are set."""
        output = str(tmp_path / "output.dat")
        writer = TildeDelimitedWriter(output_path=output)
        writer.write(sample_df)

        lines = Path(output).read_text().strip().split("\n")
        assert lines[0].startswith("HDR~JOURNAL_EXPORT~")
        assert lines[-1].startswith("TRL~")
        # 4 data rows + 1 header + 1 trailer = 6 lines
        assert len(lines) == 6

    def test_write_without_header_or_trailer(self, tmp_path, sample_df):
        """With both flags off, only data rows should be written."""
        output = str(tmp_path / "output.dat")
        writer = TildeDelimitedWriter(output_path=output)
        writer.write(sample_df, include_header=False, include_trailer=False)

        lines = Path(output).read_text().strip().split("\n")
        assert len(lines) == 4
        assert not lines[0].startswith("HDR")
        assert not lines[-1].startswith("TRL")

    def test_write_returns_file_size(self, tmp_path, sample_df):
        """Stats dict should include a positive file_size_bytes value."""
        output = str(tmp_path / "output.dat")
        writer = TildeDelimitedWriter(output_path=output)
        stats = writer.write(sample_df)
        assert stats["file_size_bytes"] > 0
        assert stats["output_path"] == output

    def test_build_header_format(self):
        """Header must follow: HDR~JOURNAL_EXPORT~{date}~{count}."""
        writer = TildeDelimitedWriter(output_path="/dev/null")
        header = writer._build_header(record_count=10, export_date="2024-01-16")
        assert header == "HDR~JOURNAL_EXPORT~2024-01-16~10"

    def test_build_trailer_format(self):
        """Trailer must follow: TRL~{count}~{debit}~{credit}~{date}."""
        writer = TildeDelimitedWriter(output_path="/dev/null")
        trailer = writer._build_trailer(
            record_count=6,
            total_debit=154.98,
            total_credit=154.98,
            export_date="2024-01-16",
        )
        assert trailer == "TRL~6~154.98~154.98~2024-01-16"


# ---------------------------------------------------------------------------
# FormatConverter tests
# ---------------------------------------------------------------------------

class TestFormatConverter:
    """Verify pipe-to-tilde format conversion and metadata augmentation."""

    COLUMN_NAMES = [
        "journal_id", "txn_id", "entry_type", "account",
        "amount", "currency", "txn_timestamp", "description",
    ]

    def test_pipe_to_tilde_reads_pipe_file(self, pipe_file, tmp_path):
        """Converter should load a pipe-separated file into a DataFrame."""
        converter = FormatConverter()
        df = converter.pipe_to_tilde(
            input_path=pipe_file,
            output_path=str(tmp_path / "out.dat"),
            column_names=self.COLUMN_NAMES,
        )
        assert len(df) == 4
        assert list(df.columns) == self.COLUMN_NAMES

    def test_pipe_to_tilde_parses_amount_as_float(self, pipe_file, tmp_path):
        """The amount column should be cast to float for numeric ops."""
        converter = FormatConverter()
        df = converter.pipe_to_tilde(
            input_path=pipe_file,
            output_path=str(tmp_path / "out.dat"),
            column_names=self.COLUMN_NAMES,
        )
        assert df["amount"].dtype == float
        assert df["amount"].iloc[0] == 49.99

    def test_add_export_metadata_adds_timestamp(self, sample_df):
        """add_export_metadata should append an export_timestamp column."""
        converter = FormatConverter()
        result = converter.add_export_metadata(sample_df)
        assert "export_timestamp" in result.columns
        assert len(result) == len(sample_df)


# ---------------------------------------------------------------------------
# OutboundProcessor end-to-end tests
# ---------------------------------------------------------------------------

class TestOutboundProcessor:
    """End-to-end tests for the outbound processing pipeline."""

    def test_process_creates_output_file(self, pipe_file, tmp_path):
        """process() should produce a tilde-separated output file."""
        output_dir = str(tmp_path / "output")
        processor = OutboundProcessor(
            input_file=pipe_file,
            output_dir=output_dir,
            output_filename="journal_export.dat",
        )
        result = processor.process()

        output_path = os.path.join(output_dir, "journal_export.dat")
        assert os.path.exists(output_path)
        assert result["output_file"] == output_path

    def test_process_returns_correct_record_count(self, pipe_file, tmp_path):
        """Summary should reflect the number of journal entries processed."""
        processor = OutboundProcessor(
            input_file=pipe_file,
            output_dir=str(tmp_path / "output"),
        )
        result = processor.process()
        assert result["record_count"] == 4

    def test_process_balanced_check(self, pipe_file, tmp_path):
        """When debits equal credits the balanced flag should be True."""
        processor = OutboundProcessor(
            input_file=pipe_file,
            output_dir=str(tmp_path / "output"),
        )
        result = processor.process()
        assert result["balanced"] is True
        assert abs(result["total_debit_amount"] - result["total_credit_amount"]) < 1e-9

    def test_process_debit_credit_totals(self, pipe_file, tmp_path):
        """Debit and credit totals should match expected sums."""
        processor = OutboundProcessor(
            input_file=pipe_file,
            output_dir=str(tmp_path / "output"),
        )
        result = processor.process()
        # 49.99 + 29.99 = 79.98 for both debits and credits
        assert abs(result["total_debit_amount"] - 79.98) < 1e-9
        assert abs(result["total_credit_amount"] - 79.98) < 1e-9

    def test_output_file_has_header_data_trailer(self, pipe_file, tmp_path):
        """Output file should contain HDR, data rows, and TRL lines."""
        output_dir = str(tmp_path / "output")
        processor = OutboundProcessor(
            input_file=pipe_file,
            output_dir=output_dir,
        )
        processor.process()

        output_path = os.path.join(output_dir, "journal_export.dat")
        lines = Path(output_path).read_text().strip().split("\n")

        # Header validation
        assert lines[0].startswith("HDR~JOURNAL_EXPORT~")
        header_parts = lines[0].split("~")
        assert header_parts[3] == "4"  # record count

        # Data rows should be tilde-separated journal entries
        assert lines[1].startswith("JRN-0001~TXN001~DEBIT~")
        assert lines[4].startswith("JRN-0004~TXN002~CREDIT~")

        # Trailer validation
        assert lines[-1].startswith("TRL~4~")

        # Total lines: 1 header + 4 data + 1 trailer = 6
        assert len(lines) == 6

    def test_process_unbalanced_entries(self, tmp_path):
        """When debits != credits the balanced flag should be False."""
        # Create an unbalanced input file (more debits than credits)
        content = (
            "JRN-0001|TXN001|DEBIT|CUST_1001|100.00|USD|2024-01-15 09:00:00|Debit entry\n"
            "JRN-0002|TXN001|CREDIT|MERCH_A|50.00|USD|2024-01-15 09:00:00|Credit entry\n"
        )
        input_file = tmp_path / "unbalanced.dat"
        input_file.write_text(content)

        processor = OutboundProcessor(
            input_file=str(input_file),
            output_dir=str(tmp_path / "output"),
        )
        result = processor.process()
        assert result["balanced"] is False
        assert result["total_debit_amount"] == 100.00
        assert result["total_credit_amount"] == 50.00
