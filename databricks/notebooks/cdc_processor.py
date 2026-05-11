# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor — Delta Lake MERGE with Change Data Feed
# MAGIC
# MAGIC **Migrated from:** `graphs/cdc_processor.py` (Ab Initio CDC via row hashing)
# MAGIC
# MAGIC ## Ab Initio → Databricks Migration Summary
# MAGIC
# MAGIC | Ab Initio Concept | Databricks Equivalent | Notes |
# MAGIC |---|---|---|
# MAGIC | `CDCProcessor.process()` | Delta Lake `MERGE INTO` | Single SQL statement handles INSERT/UPDATE/DELETE |
# MAGIC | MD5 row hashing (`_row_hash()`) | Column-level `WHEN MATCHED AND` conditions | Delta MERGE compares columns directly — no hashing needed |
# MAGIC | pandas DataFrame comparison | PySpark DataFrame joins | Distributed comparison scales to billions of rows |
# MAGIC | Manual INSERT/UPDATE/DELETE sets | MERGE `WHEN MATCHED` / `WHEN NOT MATCHED` | Declarative CDC in one atomic transaction |
# MAGIC | Return `{"inserts": df, ...}` | Delta Change Data Feed (CDF) | Downstream consumers read changes via `table_changes()` |
# MAGIC | Key column index (`set_index`) | MERGE `ON` condition | Join keys specified declaratively |
# MAGIC
# MAGIC ## Usage
# MAGIC Configure via widgets:
# MAGIC - `source_path`: Path to source (incoming) data
# MAGIC - `target_table`: Fully qualified Delta table name
# MAGIC - `key_columns`: Comma-separated merge key columns
# MAGIC - `compare_columns`: Comma-separated columns for change detection (empty = all non-key)

# COMMAND ----------

# Widget parameters — equivalent to Ab Initio PSET configuration for the CDC pipeline.
# In the legacy system, customer_cdc.pset defined CDC_SOURCE_PATH, TARGET_TABLE, KEY_COLS.
dbutils.widgets.text("source_path", "", "Source Data Path")
dbutils.widgets.text("target_table", "", "Target Delta Table")
dbutils.widgets.text("key_columns", "", "Merge Key Columns (comma-separated)")
dbutils.widgets.text("compare_columns", "", "Compare Columns (comma-separated, empty=all)")
dbutils.widgets.dropdown("file_format", "csv", ["csv", "parquet", "json", "delta"], "Source Format")
dbutils.widgets.text("delimiter", ",", "CSV Delimiter")
dbutils.widgets.dropdown("enable_cdf", "true", ["true", "false"], "Enable Change Data Feed")
dbutils.widgets.dropdown("handle_deletes", "true", ["true", "false"], "Detect Deletes")

# COMMAND ----------

import logging
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from typing import Optional, List, Dict, Any

# Configure logging — replaces Ab Initio's job log output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_processor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

class CDCConfig:
    """
    CDC pipeline configuration from widget parameters.

    Migrated from Ab Initio PSET (customer_cdc.pset) which defined:
      CDC_SOURCE_PATH, TARGET_TABLE, KEY_COLUMNS, BATCH_SIZE, etc.
    """

    def __init__(self):
        self.source_path: str = dbutils.widgets.get("source_path")
        self.target_table: str = dbutils.widgets.get("target_table")
        self.key_columns: List[str] = [
            c.strip() for c in dbutils.widgets.get("key_columns").split(",") if c.strip()
        ]
        # Compare columns: empty means all non-key columns (same as Ab Initio default)
        compare_raw = dbutils.widgets.get("compare_columns").strip()
        self.compare_columns: Optional[List[str]] = (
            [c.strip() for c in compare_raw.split(",") if c.strip()] if compare_raw else None
        )
        self.file_format: str = dbutils.widgets.get("file_format")
        self.delimiter: str = dbutils.widgets.get("delimiter")
        self.enable_cdf: bool = dbutils.widgets.get("enable_cdf") == "true"
        self.handle_deletes: bool = dbutils.widgets.get("handle_deletes") == "true"

    def validate(self) -> None:
        """Validate required configuration — fail fast before processing."""
        if not self.source_path:
            raise ValueError("source_path is required")
        if not self.target_table:
            raise ValueError("target_table is required")
        if not self.key_columns:
            raise ValueError("key_columns is required (comma-separated list of merge keys)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Processor Implementation
# MAGIC
# MAGIC ### Why Delta MERGE replaces row hashing
# MAGIC
# MAGIC The Ab Initio `CDCProcessor` used MD5 row hashing to detect changes:
# MAGIC 1. Hash all non-key columns per row in both source and target
# MAGIC 2. Compare hashes to find INSERTs (new keys), UPDATEs (changed hashes), DELETEs (missing keys)
# MAGIC 3. Return separate DataFrames for each change type
# MAGIC
# MAGIC Delta Lake MERGE replaces this with a single declarative SQL statement:
# MAGIC - **No hashing needed** — MERGE compares column values directly
# MAGIC - **Atomic** — INSERT/UPDATE/DELETE happen in one ACID transaction
# MAGIC - **Scalable** — distributed Spark execution vs single-node pandas
# MAGIC - **Change Data Feed** — downstream consumers get change records automatically

# COMMAND ----------

class CDCProcessor:
    """
    CDC processor using Delta Lake MERGE and Change Data Feed.

    Replaces Ab Initio's CDCProcessor which used pandas + MD5 hashing.
    Delta MERGE provides the same INSERT/UPDATE/DELETE detection in a single
    atomic operation, with native support for downstream change propagation
    via Change Data Feed (CDF).
    """

    def __init__(self, spark: SparkSession, config: CDCConfig):
        self.spark = spark
        self.config = config

    def process(
        self,
        schema: Optional[StructType] = None,
    ) -> Dict[str, Any]:
        """
        Execute CDC processing: read source, MERGE into target Delta table.

        This replaces the full Ab Initio CDC flow:
          1. CDCProcessor.__init__(key_columns) → config.key_columns
          2. source_df / target_df loading → spark.read + DeltaTable.forName
          3. _row_hash() for change detection → MERGE column-level comparison
          4. set operations (insert_keys, delete_keys, common_keys) → MERGE ON clause
          5. Return {"inserts": df, "updates": df, "deletes": df} → CDF table_changes()

        Args:
            schema: Optional PySpark StructType for source data enforcement.

        Returns:
            Dict with merge status, change counts, and timing.
        """
        self.config.validate()
        start_time = time.time()
        logger.info(
            f"Starting CDC: {self.config.source_path} → {self.config.target_table} "
            f"(keys: {self.config.key_columns})"
        )

        try:
            # Step 1: Read incoming source data (the "new snapshot")
            source_df = self._read_source(schema)
            source_count = source_df.count()
            logger.info(f"Source data read: {source_count} records")

            # Step 2: Ensure target table exists with CDF enabled
            target_exists = self._ensure_target_table(source_df)
            pre_merge_count = (
                self.spark.table(self.config.target_table).count() if target_exists else 0
            )

            # Step 3: Capture the current table version for CDF queries
            pre_merge_version = self._get_table_version() if target_exists else 0

            # Step 4: Execute Delta MERGE
            # This single operation replaces the Ab Initio CDCProcessor's:
            #   - insert_keys = source_keys - target_keys (WHEN NOT MATCHED)
            #   - update_keys where hash changed (WHEN MATCHED AND columns differ)
            #   - delete_keys = target_keys - source_keys (WHEN NOT MATCHED BY SOURCE)
            merge_metrics = self._execute_merge(source_df)

            # Step 5: Compute change statistics
            # In Ab Initio, these came from the returned DataFrames.
            # With Delta, we read them from the MERGE output metrics or CDF.
            post_merge_count = self.spark.table(self.config.target_table).count()
            post_merge_version = self._get_table_version()

            elapsed = round(time.time() - start_time, 2)
            result = {
                "status": "success",
                "source_path": self.config.source_path,
                "target_table": self.config.target_table,
                "source_records": source_count,
                "pre_merge_target_records": pre_merge_count,
                "post_merge_target_records": post_merge_count,
                "merge_metrics": merge_metrics,
                "cdf_enabled": self.config.enable_cdf,
                "pre_merge_version": pre_merge_version,
                "post_merge_version": post_merge_version,
                "duration_seconds": elapsed,
            }
            logger.info(f"CDC completed: {result}")
            return result

        except Exception as e:
            elapsed = round(time.time() - start_time, 2)
            error_result = {
                "status": "failed",
                "source_path": self.config.source_path,
                "target_table": self.config.target_table,
                "error": str(e),
                "error_type": type(e).__name__,
                "duration_seconds": elapsed,
            }
            logger.error(f"CDC failed: {error_result}")
            raise

    def _read_source(self, schema: Optional[StructType] = None) -> DataFrame:
        """Read incoming source data — equivalent to Ab Initio's source input port."""
        reader = self.spark.read.format(self.config.file_format)
        if schema is not None:
            reader = reader.schema(schema)
        if self.config.file_format == "csv":
            reader = reader.option("header", "true") \
                           .option("sep", self.config.delimiter) \
                           .option("inferSchema", "false" if schema else "true")
        return reader.load(self.config.source_path)

    def _ensure_target_table(self, source_df: DataFrame) -> bool:
        """
        Create target Delta table if it doesn't exist, with CDF enabled.

        In Ab Initio, the target table was assumed to exist (created by a separate
        deployment graph). In Databricks, we create it on first run with proper
        table properties including Change Data Feed.
        """
        try:
            self.spark.table(self.config.target_table)
            # Table exists — ensure CDF is enabled
            if self.config.enable_cdf:
                self.spark.sql(
                    f"ALTER TABLE {self.config.target_table} "
                    f"SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
                )
            return True
        except AnalysisException:
            # Table doesn't exist — create from source schema
            logger.info(f"Target table {self.config.target_table} not found, creating...")
            writer = source_df.limit(0).write.format("delta")
            if self.config.enable_cdf:
                writer = writer.option("delta.enableChangeDataFeed", "true")
            writer.saveAsTable(self.config.target_table)
            logger.info(f"Created target table {self.config.target_table}")
            return False

    def _get_table_version(self) -> int:
        """Get current Delta table version for CDF range queries."""
        history = self.spark.sql(f"DESCRIBE HISTORY {self.config.target_table} LIMIT 1")
        return history.collect()[0]["version"]

    def _build_merge_condition(self) -> str:
        """
        Build the MERGE ON condition from key columns.

        Replaces Ab Initio's set_index(key_columns) which established the join
        keys for the source-target comparison.
        """
        return " AND ".join(
            [f"target.{col} = source.{col}" for col in self.config.key_columns]
        )

    def _build_update_condition(self, source_df: DataFrame) -> str:
        """
        Build the update detection condition (column-level change check).

        Replaces Ab Initio's MD5 row hashing (_row_hash). Instead of hashing all
        non-key columns into an MD5 digest and comparing digests, Delta MERGE
        compares column values directly. This is:
          - More transparent (no hash collisions possible)
          - Self-documenting (you see which columns are compared)
          - Efficient (Spark optimizes column comparisons in the join)
        """
        if self.config.compare_columns:
            compare_cols = self.config.compare_columns
        else:
            # Default: compare all non-key columns (same as Ab Initio's default behavior)
            compare_cols = [
                c for c in source_df.columns if c not in self.config.key_columns
            ]

        if not compare_cols:
            # No columns to compare — treat all matched rows as updates
            return "1 = 1"

        # OR-chain: any column change triggers an update
        conditions = []
        for col in compare_cols:
            # Handle NULLs: NULL != value should be a change, NULL == NULL should not
            conditions.append(
                f"(NOT (target.{col} <=> source.{col}))"
            )
        return " OR ".join(conditions)

    def _build_update_set(self, source_df: DataFrame) -> Dict[str, str]:
        """Build the SET clause for WHEN MATCHED — update all non-key columns."""
        update_cols = [c for c in source_df.columns if c not in self.config.key_columns]
        return {col: f"source.{col}" for col in update_cols}

    def _execute_merge(self, source_df: DataFrame) -> Dict[str, Any]:
        """
        Execute Delta MERGE — the core CDC operation.

        This single MERGE statement replaces the entire Ab Initio CDCProcessor.process():
          - WHEN NOT MATCHED → INSERT (was: insert_keys = source_keys - target_keys)
          - WHEN MATCHED AND changed → UPDATE (was: hash comparison on common_keys)
          - WHEN NOT MATCHED BY SOURCE → DELETE (was: delete_keys = target_keys - source_keys)

        All three operations execute atomically in one Delta transaction.
        """
        target_delta = DeltaTable.forName(self.spark, self.config.target_table)
        merge_condition = self._build_merge_condition()
        update_condition = self._build_update_condition(source_df)
        update_set = self._build_update_set(source_df)

        logger.info(f"MERGE condition: {merge_condition}")
        logger.info(f"Update detection: {update_condition}")

        # Build the MERGE operation
        merge_builder = (
            target_delta.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            # WHEN MATCHED and columns changed → UPDATE
            # Replaces: hash comparison on common_keys in Ab Initio CDCProcessor
            .whenMatchedUpdate(
                condition=update_condition,
                set=update_set,
            )
            # WHEN NOT MATCHED → INSERT new records
            # Replaces: insert_keys = source_keys - target_keys
            .whenNotMatchedInsertAll()
        )

        # Optional: WHEN NOT MATCHED BY SOURCE → DELETE
        # Replaces: delete_keys = target_keys - source_keys
        # In Ab Initio, deletes were always computed. In production Delta pipelines,
        # you may want to soft-delete (set a flag) rather than hard-delete.
        if self.config.handle_deletes:
            merge_builder = merge_builder.whenNotMatchedBySourceDelete()

        # Execute the MERGE
        merge_builder.execute()

        # Collect merge metrics from the Delta transaction log
        # These replace the Ab Initio CDCProcessor's stats dict
        metrics = self._get_merge_metrics()
        logger.info(f"MERGE metrics: {metrics}")
        return metrics

    def _get_merge_metrics(self) -> Dict[str, Any]:
        """
        Extract merge operation metrics from Delta history.

        Replaces the Ab Initio CDCProcessor's stats dict:
          {"inserts": N, "updates": N, "deletes": N}
        Delta provides richer metrics via the transaction log.
        """
        history = self.spark.sql(
            f"DESCRIBE HISTORY {self.config.target_table} LIMIT 1"
        )
        row = history.collect()[0]
        metrics = row["operationMetrics"] if row["operationMetrics"] else {}
        return {
            "inserts": int(metrics.get("numTargetRowsInserted", 0)),
            "updates": int(metrics.get("numTargetRowsUpdated", 0)),
            "deletes": int(metrics.get("numTargetRowsDeleted", 0)),
            "rows_matched": int(metrics.get("numTargetRowsMatchedUpdated", 0)),
            "rows_not_matched": int(metrics.get("numTargetRowsNotMatchedBySourceDeleted", 0)),
            "source_rows": int(metrics.get("numSourceRows", 0)),
        }

    @staticmethod
    def read_changes(
        spark: SparkSession,
        table_name: str,
        start_version: int,
        end_version: Optional[int] = None,
    ) -> DataFrame:
        """
        Read Change Data Feed (CDF) for downstream consumers.

        This replaces the Ab Initio pattern of returning separate DataFrames
        for inserts/updates/deletes. With CDF, downstream consumers can read
        all changes as a single stream with _change_type metadata:
          - "insert" → new rows
          - "update_preimage" → row before update
          - "update_postimage" → row after update
          - "delete" → removed rows

        Usage:
            changes_df = CDCProcessor.read_changes(spark, "catalog.schema.table", 5, 6)
            inserts = changes_df.filter("_change_type = 'insert'")
            updates = changes_df.filter("_change_type = 'update_postimage'")
            deletes = changes_df.filter("_change_type = 'delete'")
        """
        reader = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", start_version)
        )
        if end_version is not None:
            reader = reader.option("endingVersion", end_version)
        return reader.table(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution
# MAGIC
# MAGIC Run the CDC merge. In a Databricks Job, widgets are populated from job parameters.

# COMMAND ----------

# Main execution — runs when notebook is executed as a job or interactively.
try:
    config = CDCConfig()
    cdc = CDCProcessor(spark, config)

    # Execute the CDC merge
    # To use a schema from databricks.schemas, import and pass it:
    #   from databricks.schemas.customer import CUSTOMER_SCHEMA
    #   result = cdc.process(schema=CUSTOMER_SCHEMA)
    result = cdc.process()

    # Log CDF availability for downstream consumers
    if result.get("cdf_enabled") and result.get("pre_merge_version") is not None:
        logger.info(
            f"Change Data Feed available: "
            f"spark.read.option('readChangeFeed', 'true')"
            f".option('startingVersion', {result['pre_merge_version']})"
            f".table('{config.target_table}')"
        )

    # Return result for Databricks Workflows chaining
    dbutils.notebook.exit(str(result))

except ValueError as e:
    logger.error(f"Configuration error: {e}")
    dbutils.notebook.exit(f'{{"status": "failed", "error": "{e}"}}')

except AnalysisException as e:
    logger.error(f"Spark analysis error: {e}")
    dbutils.notebook.exit(f'{{"status": "failed", "error": "{e}"}}')
