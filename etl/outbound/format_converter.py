"""
Format converter for the Ab Initio outbound ETL pipeline.

Handles reading pipe-separated (|) journal entry files produced by the Core
module and returning a pandas DataFrame suitable for tilde-delimited output.
Also provides hooks for attaching export-specific metadata columns.
"""
import logging
from datetime import datetime
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)

# Delimiter used in the upstream pipe-separated journal files
PIPE_DELIMITER = "|"


class FormatConverter:
    """
    Reads pipe-separated source files and prepares DataFrames for
    tilde-delimited output.
    """

    def pipe_to_tilde(
        self,
        input_path: str,
        output_path: str,
        column_names: List[str],
    ) -> pd.DataFrame:
        """
        Read a pipe-separated journal entry file and return a DataFrame
        ready for tilde-delimited writing.

        The output_path is accepted here so callers can thread it through
        the pipeline, but the actual file writing is handled by
        TildeDelimitedWriter.

        Args:
            input_path: Path to the pipe-separated source file.
            output_path: Intended destination path (stored for reference).
            column_names: Ordered list of column names matching the source
                          file's field layout.

        Returns:
            A pandas DataFrame with the source data loaded and typed.
        """
        logger.info("Reading pipe-separated file: %s", input_path)

        # Read the pipe-separated file without a header row
        df = pd.read_csv(
            input_path,
            sep=PIPE_DELIMITER,
            header=None,
            names=column_names,
            dtype=str,  # Read all fields as strings initially
        )

        # Cast the amount column to float for numeric operations
        df["amount"] = df["amount"].astype(float)

        logger.info(
            "Loaded %d records with columns: %s",
            len(df),
            list(df.columns),
        )
        return df

    def add_export_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Attach export-specific metadata columns to the DataFrame.

        Currently adds an export_timestamp column recording when the
        export was generated. Additional metadata fields can be added
        here as downstream requirements evolve.

        Args:
            df: DataFrame to augment with metadata.

        Returns:
            The same DataFrame with the metadata column(s) appended.
        """
        # Record the exact timestamp of this export run
        df = df.copy()
        df["export_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info("Added export_timestamp metadata column")
        return df
