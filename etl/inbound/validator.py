"""Null / empty value validator for inbound ETL records.

This module provides the NullCheckValidator class that scans a DataFrame for
null, NaN, empty-string, or whitespace-only values in designated required
fields.  Records that fail validation are separated into a rejection set with
detailed reason metadata, following the Ab Initio reject-port pattern.
"""
import logging
from typing import Dict, List

import pandas as pd

logger = logging.getLogger(__name__)


class NullCheckValidator:
    """
    Validates DataFrame records to ensure required fields are not null or empty.

    In Ab Initio graphs the equivalent logic is typically handled by a
    'Filter by Expression' or 'Validate' component that routes bad records
    to a reject port.  This class reproduces that behaviour in Python.
    """

    def __init__(self, required_fields: List[str]) -> None:
        """
        Initialise the validator with the list of fields that must be populated.

        Args:
            required_fields: Column names that must not contain null, NaN,
                             empty strings, or whitespace-only values.
        """
        self.required_fields = required_fields

    def validate(self, df: pd.DataFrame) -> Dict:
        """
        Validate the DataFrame and split it into valid / rejected sets.

        Checks each required field for:
            - None / NaN values
            - Empty strings ("")
            - Whitespace-only strings ("   ")

        Args:
            df: Input DataFrame to validate.

        Returns:
            dict with keys:
                - valid_records: DataFrame of rows that passed all checks
                - rejected_records: DataFrame of rows that failed at least one check
                - rejection_reasons: list of dicts describing each failure
                - stats: summary counts (total, valid, rejected)
        """
        logger.info(
            f"Validating {len(df)} records against required fields: {self.required_fields}"
        )

        rejection_reasons: List[Dict] = []
        rejected_indices: set = set()

        # Iterate over each required field and flag rows with bad values
        for field in self.required_fields:
            if field not in df.columns:
                logger.warning(f"Required field '{field}' not found in DataFrame columns — skipping")
                continue

            for idx, value in df[field].items():
                # Check for None, NaN, empty string, or whitespace-only
                if self._is_null_or_empty(value):
                    rejection_reasons.append({
                        "row_index": int(idx),
                        "field": field,
                        "reason": "Null or empty value",
                    })
                    rejected_indices.add(idx)

        # Split DataFrame into valid and rejected subsets
        rejected_records = df.loc[list(rejected_indices)] if rejected_indices else pd.DataFrame(columns=df.columns)
        valid_records = df.drop(index=list(rejected_indices))

        stats = {
            "total": len(df),
            "valid": len(valid_records),
            "rejected": len(rejected_records),
        }
        logger.info(f"Validation complete — {stats}")

        return {
            "valid_records": valid_records,
            "rejected_records": rejected_records,
            "rejection_reasons": rejection_reasons,
            "stats": stats,
        }

    @staticmethod
    def _is_null_or_empty(value) -> bool:
        """
        Return True if the value is considered null or empty.

        Handles None, float NaN, empty strings, and whitespace-only strings.
        """
        if value is None:
            return True
        if isinstance(value, float) and pd.isna(value):
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        return False
