"""
Core ETL processor that orchestrates the full pipeline:
  1. Read validated transaction data from pipe-separated input file
  2. Convert transactions into double-entry journal entries
  3. Write journal entries to pipe-separated output file
  4. Return summary statistics for monitoring and reconciliation
"""
import logging
import os
from pathlib import Path

import pandas as pd

from etl.core.transaction_processor import TransactionProcessor

logger = logging.getLogger(__name__)

# Column names for the validated transaction input file (pipe-separated)
INPUT_COLUMNS = [
    "txn_id", "txn_timestamp", "customer_id", "txn_type",
    "merchant_name", "merchant_category", "amount", "item_count",
    "sku", "quantity", "line_total", "channel",
]


class CoreProcessor:
    """
    Orchestrates the core ETL processing pipeline.

    Reads validated transaction files (pipe-separated), applies double-entry
    journal entry transformation via TransactionProcessor, and writes the
    resulting journal entries to a pipe-separated output file.
    """

    def __init__(self, input_file: str, output_dir: str = "data/processed", currency: str = "USD"):
        """
        Initialize the core processor.

        Args:
            input_file: Path to the validated transactions input file (pipe-separated).
            output_dir: Directory for writing output files (default "data/processed").
            currency: Currency code for journal entries (default "USD").
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.currency = currency
        # Initialize the transaction processor for journal entry generation
        self.transaction_processor = TransactionProcessor(currency=currency)

    def process(self) -> dict:
        """
        Execute the full core processing pipeline.

        Steps:
          1. Read the pipe-separated validated transaction file into a DataFrame
          2. Process all transactions into journal entries using TransactionProcessor
          3. Write the journal entries to {output_dir}/journal_entries.dat
          4. Compute and return summary statistics

        Returns:
            Dictionary with processing summary:
              - total_transactions: Number of input transactions processed
              - journal_entries_generated: Total journal entries created
              - total_debits: Count of DEBIT entries
              - total_credits: Count of CREDIT entries
              - total_debit_amount: Sum of all DEBIT amounts
              - total_credit_amount: Sum of all CREDIT amounts
        """
        # Step 1: Read validated transaction input file
        logger.info(f"Reading validated transactions from: {self.input_file}")
        df = pd.read_csv(
            self.input_file,
            sep="|",
            header=None,
            names=INPUT_COLUMNS,
        )
        logger.info(f"Loaded {len(df)} transactions from input file")

        # Step 2: Convert transactions into journal entries
        logger.info("Processing transactions into double-entry journal entries")
        journal_df = self.transaction_processor.process_batch(df)

        # Step 3: Write journal entries to output file
        os.makedirs(self.output_dir, exist_ok=True)
        output_path = Path(self.output_dir) / "journal_entries.dat"
        journal_df.to_csv(str(output_path), sep="|", index=False, header=False)
        logger.info(f"Wrote {len(journal_df)} journal entries to: {output_path}")

        # Step 4: Compute summary statistics for reconciliation
        total_transactions = len(df)
        journal_entries_generated = len(journal_df)
        total_debits = int((journal_df["entry_type"] == "DEBIT").sum())
        total_credits = int((journal_df["entry_type"] == "CREDIT").sum())
        total_debit_amount = float(journal_df.loc[journal_df["entry_type"] == "DEBIT", "amount"].sum())
        total_credit_amount = float(journal_df.loc[journal_df["entry_type"] == "CREDIT", "amount"].sum())

        summary = {
            "total_transactions": total_transactions,
            "journal_entries_generated": journal_entries_generated,
            "total_debits": total_debits,
            "total_credits": total_credits,
            "total_debit_amount": total_debit_amount,
            "total_credit_amount": total_credit_amount,
        }

        logger.info(f"Processing complete. Summary: {summary}")
        return summary
