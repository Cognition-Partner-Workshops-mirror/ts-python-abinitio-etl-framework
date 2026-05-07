"""
Tilde-delimited file writer for the Ab Initio outbound ETL pipeline.

Produces output files in tilde-separated (~) format with a header row,
data rows, and a trailer/summary row. This format is the standard export
layout consumed by downstream settlement and reconciliation systems.

Output layout:
    HDR~JOURNAL_EXPORT~{export_date}~{record_count}
    {journal_id}~{txn_id}~{entry_type}~{account}~{amount}~{currency}~{txn_timestamp}~{description}
    ...
    TRL~{record_count}~{total_debit_amount}~{total_credit_amount}~{export_date}
"""
import logging
import os
from datetime import datetime
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)

# Tilde character used as the field delimiter in output files
TILDE_DELIMITER = "~"


class TildeDelimitedWriter:
    """
    Writes a pandas DataFrame to a tilde-separated (~) flat file with
    optional header and trailer rows for downstream consumption.
    """

    def __init__(self, output_path: str, encoding: str = "utf-8"):
        """
        Initialise the writer.

        Args:
            output_path: Filesystem path where the output file will be written.
            encoding: Character encoding for the output file (default utf-8).
        """
        self.output_path = output_path
        self.encoding = encoding

    def write(
        self,
        df: pd.DataFrame,
        include_header: bool = True,
        include_trailer: bool = True,
    ) -> Dict[str, object]:
        """
        Write the DataFrame to a tilde-separated output file.

        Optionally prepends a header row and appends a trailer row that
        contains record counts and debit/credit totals for reconciliation.

        Args:
            df: DataFrame containing journal entry rows to export.
            include_header: Whether to write the HDR line at the top.
            include_trailer: Whether to write the TRL line at the bottom.

        Returns:
            Stats dict with keys: output_path, record_count, file_size_bytes.
        """
        record_count = len(df)
        export_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            "Writing %d records to %s (header=%s, trailer=%s)",
            record_count,
            self.output_path,
            include_header,
            include_trailer,
        )

        # Ensure the output directory exists before writing
        output_dir = os.path.dirname(self.output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        lines: list[str] = []

        # Build header row if requested
        if include_header:
            lines.append(self._build_header(record_count, export_date))

        # Build tilde-separated data rows from the DataFrame
        for _, row in df.iterrows():
            line = TILDE_DELIMITER.join(str(val) for val in row)
            lines.append(line)

        # Build trailer row if requested
        if include_trailer:
            # Calculate debit and credit totals from the entry_type column
            total_debit = float(
                df.loc[df["entry_type"] == "DEBIT", "amount"].sum()
            )
            total_credit = float(
                df.loc[df["entry_type"] == "CREDIT", "amount"].sum()
            )
            lines.append(
                self._build_trailer(record_count, total_debit, total_credit, export_date)
            )

        # Write all lines to the output file
        with open(self.output_path, "w", encoding=self.encoding) as fh:
            fh.write("\n".join(lines) + "\n")

        file_size = os.path.getsize(self.output_path)
        logger.info("Output written: %d bytes", file_size)

        return {
            "output_path": self.output_path,
            "record_count": record_count,
            "file_size_bytes": file_size,
        }

    def _build_header(self, record_count: int, export_date: str) -> str:
        """
        Build the header line for the output file.

        Format: HDR~JOURNAL_EXPORT~{export_date}~{record_count}

        Args:
            record_count: Total number of data rows in the export.
            export_date: Date string (YYYY-MM-DD) for the export run.

        Returns:
            The formatted header string.
        """
        return TILDE_DELIMITER.join(
            ["HDR", "JOURNAL_EXPORT", export_date, str(record_count)]
        )

    def _build_trailer(
        self,
        record_count: int,
        total_debit: float,
        total_credit: float,
        export_date: str,
    ) -> str:
        """
        Build the trailer/summary line for the output file.

        Format: TRL~{record_count}~{total_debit_amount}~{total_credit_amount}~{export_date}

        Args:
            record_count: Total number of data rows in the export.
            total_debit: Sum of amounts for DEBIT entries.
            total_credit: Sum of amounts for CREDIT entries.
            export_date: Date string (YYYY-MM-DD) for the export run.

        Returns:
            The formatted trailer string.
        """
        return TILDE_DELIMITER.join(
            [
                "TRL",
                str(record_count),
                f"{total_debit:.2f}",
                f"{total_credit:.2f}",
                export_date,
            ]
        )
