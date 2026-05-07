"""Pipe-delimited file reader for Ab Initio inbound ETL processing.

This module provides the PipeDelimitedReader class to read pipe-separated (|)
source files commonly used in Ab Initio ETL pipelines. It converts raw flat
files into pandas DataFrames for downstream validation and transformation.
"""
import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


class PipeDelimitedReader:
    """
    Reads pipe-separated (|) flat files into pandas DataFrames.

    Ab Initio source feeds are typically delivered as pipe-delimited files
    without headers. This reader assigns column names from the DML schema
    and returns a structured DataFrame for downstream processing.
    """

    def __init__(
        self,
        file_path: str,
        column_names: List[str],
        encoding: str = "utf-8",
    ) -> None:
        """
        Initialise the reader with file location and schema metadata.

        Args:
            file_path: Path to the pipe-separated source file.
            column_names: Ordered list of column names matching the file layout.
            encoding: Character encoding of the source file (default utf-8).
        """
        self.file_path = file_path
        self.column_names = column_names
        self.encoding = encoding

    def read(self) -> pd.DataFrame:
        """
        Read the pipe-delimited file and return a DataFrame.

        Splits each line on the pipe character and assigns the configured
        column names.  Rows where the column count does not match the
        expected schema are logged and skipped.

        Returns:
            pd.DataFrame with one column per entry in column_names.
        """
        logger.info(f"Reading pipe-delimited file: {self.file_path}")

        # Read raw file, splitting on pipe delimiter; no header row expected
        df = pd.read_csv(
            self.file_path,
            sep="|",
            header=None,
            names=self.column_names,
            encoding=self.encoding,
            dtype=str,  # keep all values as strings to preserve raw data
            keep_default_na=False,  # prevent pandas from auto-converting blanks to NaN
        )

        logger.info(f"Read {len(df)} records from {self.file_path}")
        return df

    def read_with_metadata(self) -> dict:
        """
        Read the file and return a dict containing the DataFrame plus metadata.

        This is useful for audit logging and downstream orchestration where
        the caller needs row counts and schema information alongside the data.

        Returns:
            dict with keys:
                - data: pd.DataFrame of file contents
                - row_count: number of records read
                - file_path: source file path
                - columns: list of column names
        """
        df = self.read()
        metadata = {
            "data": df,
            "row_count": len(df),
            "file_path": self.file_path,
            "columns": list(self.column_names),
        }
        logger.info(
            f"Metadata — file: {self.file_path}, rows: {metadata['row_count']}, "
            f"columns: {len(metadata['columns'])}"
        )
        return metadata
