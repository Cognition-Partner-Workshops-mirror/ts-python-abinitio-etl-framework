"""
Outbound processor for the Ab Initio ETL framework.

Orchestrates the full outbound pipeline:
  1. Read pipe-separated journal entry files (produced by the Core module).
  2. Convert data into a tilde-separated (~) export format.
  3. Write the output file with header and trailer rows for reconciliation.

This module ties together FormatConverter (reading) and TildeDelimitedWriter
(writing) into a single process() call suitable for batch scheduling.
"""
import logging
import os
from typing import Dict, List

from etl.outbound.format_converter import FormatConverter
from etl.outbound.file_writer import TildeDelimitedWriter

logger = logging.getLogger(__name__)

# Column layout for the upstream pipe-separated journal entry files
JOURNAL_COLUMNS: List[str] = [
    "journal_id",
    "txn_id",
    "entry_type",
    "account",
    "amount",
    "currency",
    "txn_timestamp",
    "description",
]


class OutboundProcessor:
    """
    End-to-end orchestrator for converting pipe-separated journal entries
    into tilde-separated export files with header/trailer rows.
    """

    def __init__(
        self,
        input_file: str,
        output_dir: str = "data/output",
        output_filename: str = "journal_export.dat",
    ):
        """
        Initialise the outbound processor.

        Args:
            input_file: Path to the pipe-separated journal entry source file.
            output_dir: Directory where the tilde-separated output will be
                        written (created automatically if it does not exist).
            output_filename: Name of the output file within output_dir.
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.output_filename = output_filename

    def process(self) -> Dict[str, object]:
        """
        Execute the full outbound ETL pipeline.

        Steps:
            1. Read the pipe-separated journal entry file via FormatConverter.
            2. Compute debit/credit totals for the trailer row.
            3. Write the tilde-separated output via TildeDelimitedWriter.
            4. Return a summary dict with counts and balance check.

        Returns:
            Summary dict with keys:
                input_file, output_file, record_count,
                total_debit_amount, total_credit_amount, balanced.
        """
        logger.info("=== Outbound processing started ===")
        logger.info("Input file : %s", self.input_file)

        # Step 1 — Read the pipe-separated source file
        converter = FormatConverter()
        output_path = os.path.join(self.output_dir, self.output_filename)

        df = converter.pipe_to_tilde(
            input_path=self.input_file,
            output_path=output_path,
            column_names=JOURNAL_COLUMNS,
        )

        logger.info("Records loaded: %d", len(df))

        # Step 2 — Calculate debit and credit totals for reconciliation
        total_debit = float(df.loc[df["entry_type"] == "DEBIT", "amount"].sum())
        total_credit = float(df.loc[df["entry_type"] == "CREDIT", "amount"].sum())
        balanced = abs(total_debit - total_credit) < 1e-9

        logger.info(
            "Totals — debit: %.2f, credit: %.2f, balanced: %s",
            total_debit,
            total_credit,
            balanced,
        )

        # Step 3 — Write the tilde-separated output with header and trailer
        writer = TildeDelimitedWriter(output_path=output_path)
        write_stats = writer.write(df, include_header=True, include_trailer=True)

        logger.info("Output file: %s (%d bytes)", output_path, write_stats["file_size_bytes"])
        logger.info("=== Outbound processing complete ===")

        # Step 4 — Return processing summary
        return {
            "input_file": self.input_file,
            "output_file": output_path,
            "record_count": write_stats["record_count"],
            "total_debit_amount": total_debit,
            "total_credit_amount": total_credit,
            "balanced": balanced,
        }
