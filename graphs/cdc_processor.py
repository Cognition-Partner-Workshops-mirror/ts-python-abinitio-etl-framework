"""CDC (Change Data Capture) processing pattern for Ab Initio graphs."""
import logging
import hashlib
from typing import Dict, List, Any, Tuple
import pandas as pd

logger = logging.getLogger(__name__)


class CDCProcessor:
    """
    Implements CDC pattern:
    Compare source snapshot vs target to produce INSERT/UPDATE/DELETE records.
    Equivalent to the Ab Initio 'Compare Records by Key' component.
    """

    def __init__(self, key_columns: List[str], compare_columns: List[str] = None):
        self.key_columns = key_columns
        self.compare_columns = compare_columns  # None = compare all non-key columns

    def process(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """
        Compute CDC delta between source and target DataFrames.

        Returns:
            Dict with 'inserts', 'updates', 'deletes' DataFrames.
        """
        source_df = source_df.copy()
        target_df = target_df.copy()

        source_df["_hash"] = self._row_hash(source_df)
        target_df["_hash"] = self._row_hash(target_df)

        source_df = source_df.set_index(self.key_columns)
        target_df = target_df.set_index(self.key_columns)

        source_keys = set(source_df.index)
        target_keys = set(target_df.index)

        # Inserts: in source, not in target
        insert_keys = source_keys - target_keys
        inserts = source_df.loc[list(insert_keys)].reset_index().drop(columns=["_hash"])

        # Deletes: in target, not in source
        delete_keys = target_keys - source_keys
        deletes = target_df.loc[list(delete_keys)].reset_index().drop(columns=["_hash"])

        # Updates: in both, but hash changed
        common_keys = source_keys & target_keys
        updates = []
        for key in common_keys:
            src_hash = source_df.loc[key, "_hash"] if not isinstance(source_df.loc[key], pd.DataFrame) else source_df.loc[key, "_hash"].iloc[0]
            tgt_hash = target_df.loc[key, "_hash"] if not isinstance(target_df.loc[key], pd.DataFrame) else target_df.loc[key, "_hash"].iloc[0]
            if src_hash != tgt_hash:
                row = source_df.loc[key]
                if isinstance(row, pd.Series):
                    updates.append(row.name if isinstance(row.name, tuple) else {self.key_columns[0]: row.name})
        update_keys = [list(k) if isinstance(k, tuple) else [k] for k in updates]
        updates_df = source_df.loc[[k[0] if len(k) == 1 else tuple(k) for k in update_keys]].reset_index().drop(columns=["_hash"]) if update_keys else pd.DataFrame()

        stats = {
            "inserts": len(inserts),
            "updates": len(updates_df),
            "deletes": len(deletes),
        }
        logger.info(f"CDC result: {stats}")

        return {"inserts": inserts, "updates": updates_df, "deletes": deletes, "stats": stats}

    def _row_hash(self, df: pd.DataFrame) -> pd.Series:
        """Compute a row-level MD5 hash for change detection."""
        cols = self.compare_columns or [c for c in df.columns if c not in self.key_columns]
        return df[cols].astype(str).apply(
            lambda row: hashlib.md5("||".join(row.values).encode()).hexdigest(),
            axis=1,
        )
