"""Inbound ETL processor that orchestrates file reading and validation.

This module ties together the PipeDelimitedReader and NullCheckValidator
to implement the full inbound processing flow: read → validate → write.
Valid records are written to a processed output file while rejected records
are routed to a separate reject file, mirroring the Ab Initio reject-port
pattern used in production graphs.
"""
import logging
import os
from typing import Dict, List, Optional

import pandas as pd

from etl.inbound.file_reader import PipeDelimitedReader
from etl.inbound.validator import NullCheckValidator

logger = logging.getLogger(__name__)


class InboundProcessor:
    """
    Orchestrates the end-to-end inbound ETL flow for pipe-delimited files.

    Steps:
        1. Read the source file using PipeDelimitedReader.
        2. Validate records with NullCheckValidator.
        3. Write valid records to the processed output directory.
        4. Write rejected records to a separate reject file.
        5. Return summary statistics for logging and monitoring.
    """

    def __init__(
        self,
        input_file: str,
        column_names: List[str],
        required_fields: List[str],
        output_dir: str = "data/processed",
    ) -> None:
        """
        Initialise the processor with input/output paths and schema metadata.

        Args:
            input_file: Path to the pipe-separated source file.
            column_names: Ordered column names matching the source file layout.
            required_fields: Columns that must not be null/empty.
            output_dir: Directory to write validated and rejected output files
                        (default "data/processed").
        """
        self.input_file = input_file
        self.column_names = column_names
        self.required_fields = required_fields
        self.output_dir = output_dir

    def process(self) -> Dict:
        """
        Execute the full inbound processing pipeline.

        Reads the source file, validates records for null/empty required
        fields, writes valid and rejected records to separate output files,
        and returns a summary dict.

        Returns:
            dict with keys:
                - input_file: source file path
                - output_file: path to validated output file
                - reject_file: path to rejected records file
                - stats: validation summary counts
                - rejection_reasons: list of per-row rejection details
        """
        logger.info(f"Starting inbound processing for: {self.input_file}")

        # Step 1 — Read source file
        reader = PipeDelimitedReader(
            file_path=self.input_file,
            column_names=self.column_names,
        )
        read_result = reader.read_with_metadata()
        df = read_result["data"]
        logger.info(f"Read {read_result['row_count']} records from source file")

        # Step 2 — Validate records for null/empty required fields
        validator = NullCheckValidator(required_fields=self.required_fields)
        validation_result = validator.validate(df)

        valid_df = validation_result["valid_records"]
        rejected_df = validation_result["rejected_records"]

        # Step 3 — Ensure the output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Step 4 — Write valid records to processed output file (pipe-separated)
        output_file = os.path.join(self.output_dir, "validated_transactions.dat")
        valid_df.to_csv(output_file, sep="|", index=False, header=False)
        logger.info(f"Wrote {len(valid_df)} valid records to {output_file}")

        # Step 5 — Write rejected records to reject file (pipe-separated)
        reject_file = os.path.join(self.output_dir, "rejected_transactions.dat")
        rejected_df.to_csv(reject_file, sep="|", index=False, header=False)
        logger.info(f"Wrote {len(rejected_df)} rejected records to {reject_file}")

        # Build summary for caller / monitoring
        summary = {
            "input_file": self.input_file,
            "output_file": output_file,
            "reject_file": reject_file,
            "stats": validation_result["stats"],
            "rejection_reasons": validation_result["rejection_reasons"],
        }
        logger.info(f"Inbound processing complete — stats: {summary['stats']}")
        return summary
