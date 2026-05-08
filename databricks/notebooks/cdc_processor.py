# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor — Delta Lake MERGE Notebook
# MAGIC
# MAGIC **Migrated from:** `graphs/cdc_processor.py` (Ab Initio hash-based CDC processor)
# MAGIC
# MAGIC ## Ab Initio → Databricks Mapping
# MAGIC | Ab Initio Concept | Databricks Equivalent |
# MAGIC |---|---|
# MAGIC | `CDCProcessor` with row-level MD5 hashing | Delta Lake `MERGE INTO` with Change Data Feed (CDF) |
# MAGIC | Pandas DataFrame compare (source vs target) | Spark SQL MERGE statement with match conditions |
# MAGIC | Manual INSERT/UPDATE/DELETE classification | Delta MERGE `WHEN MATCHED` / `WHEN NOT MATCHED` clauses |
# MAGIC | `hashlib.md5` row hash for change detection | `sha2(concat_ws(...))` in Spark SQL |
# MAGIC | PSET `KEY_COLUMNS` / `HASH_COLUMNS` | Widget parameters for merge key and compare columns |
# MAGIC
# MAGIC ## Key Changes
# MAGIC - Ab Initio CDC used in-memory Pandas comparison with explicit hash computation.
# MAGIC   Delta Lake MERGE performs this atomically at the storage layer.
# MAGIC - Change Data Feed (CDF) provides built-in audit trail of all changes,
# MAGIC   replacing the manual audit_customer_changes.mp graph.
# MAGIC - Delete detection uses a source-target anti-join (soft delete pattern).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — Notebook Widgets
# MAGIC Maps Ab Initio PSET parameters (from customer_cdc.pset) to Databricks widgets.

# COMMAND ----------

# Widget definitions mapped from Ab Initio customer_cdc.pset parameters
# SOURCE_PATH -> source data location
dbutils.widgets.text("source_path", "", "Source Data Path")
# TARGET_TABLE -> Delta table to merge into
dbutils.widgets.text("target_table", "", "Target Delta Table")
# KEY_COLUMNS -> merge key columns (comma-separated)
dbutils.widgets.text("key_columns", "", "Key Columns (comma-separated)")
# HASH_COLUMNS -> columns to compare for change detection (comma-separated, empty=all)
dbutils.widgets.text("hash_columns", "", "Hash/Compare Columns (comma-separated, empty=all non-key)")
# ENABLE_DELETES -> whether to soft-delete records missing from source
dbutils.widgets.dropdown("enable_deletes", "true", ["true", "false"], "Enable Soft Deletes")
# FILE_FORMAT -> source file format
dbutils.widgets.dropdown("file_format", "csv", ["csv", "parquet", "json", "delta"], "Source File Format")

# COMMAND ----------

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, when, coalesce,
)
from delta.tables import DeltaTable
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_processor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Resolution
# MAGIC Resolves widget values, equivalent to PSETManager.load_pset() in Ab Initio.

# COMMAND ----------

# Resolve parameters from widgets (equivalent to PSET resolution)
source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
key_columns_str = dbutils.widgets.get("key_columns")
hash_columns_str = dbutils.widgets.get("hash_columns")
enable_deletes = dbutils.widgets.get("enable_deletes").lower() == "true"
file_format = dbutils.widgets.get("file_format")

# Parse comma-separated column lists
key_columns = [c.strip() for c in key_columns_str.split(",") if c.strip()]
hash_columns = [c.strip() for c in hash_columns_str.split(",") if c.strip()] if hash_columns_str else []

# Validate required parameters
if not source_path or not target_table or not key_columns:
    raise ValueError(
        "source_path, target_table, and key_columns are required. "
        "Set them via job parameters or notebook widgets."
    )

logger.info(
    f"CDC Processor starting: source={source_path}, target={target_table}, "
    f"keys={key_columns}, hash_cols={hash_columns or 'all non-key'}, "
    f"deletes={'enabled' if enable_deletes else 'disabled'}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Snapshot
# MAGIC Reads the current source data snapshot.
# MAGIC In Ab Initio, this was the snapshot_customer.mp graph step.

# COMMAND ----------

def read_source_snapshot(
    spark: SparkSession,
    path: str,
    file_format: str,
) -> DataFrame:
    """
    Read current source snapshot for CDC comparison.

    Ab Initio equivalent: snapshot_customer.mp graph execution.

    Args:
        spark: Active SparkSession
        path: Source data path
        file_format: File format (csv, parquet, json, delta)

    Returns:
        Source DataFrame
    """
    logger.info(f"Reading source snapshot from {path}")
    reader = spark.read.format(file_format)
    if file_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    df = reader.load(path)
    count = df.count()
    logger.info(f"Source snapshot loaded: {count} records")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Row Hash for Change Detection
# MAGIC Replaces `CDCProcessor._row_hash()` MD5 hashing with Spark's `sha2()`.

# COMMAND ----------

def add_row_hash(
    df: DataFrame,
    key_columns: list,
    hash_columns: list,
) -> DataFrame:
    """
    Add a row-level hash column for change detection.

    Ab Initio equivalent: CDCProcessor._row_hash() using hashlib.md5.
    Spark equivalent: sha2(concat_ws("||", ...)) computed in parallel.

    Args:
        df: Input DataFrame
        key_columns: Columns that form the merge key (excluded from hash if hash_columns empty)
        hash_columns: Specific columns to hash. If empty, uses all non-key columns.

    Returns:
        DataFrame with _row_hash column added
    """
    # Determine columns to hash (mirrors CDCProcessor logic)
    if hash_columns:
        cols_to_hash = hash_columns
    else:
        cols_to_hash = [c for c in df.columns if c not in key_columns]

    # Compute SHA-256 hash of concatenated column values
    # (Ab Initio used MD5; SHA-256 provides stronger collision resistance)
    hash_expr = sha2(
        concat_ws("||", *[coalesce(col(c).cast("string"), lit("__NULL__")) for c in cols_to_hash]),
        256
    )

    return df.withColumn("_row_hash", hash_expr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake MERGE (CDC Apply)
# MAGIC Replaces the manual INSERT/UPDATE/DELETE classification in CDCProcessor.process()
# MAGIC with an atomic Delta Lake MERGE INTO statement.

# COMMAND ----------

def execute_cdc_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    key_columns: list,
    hash_columns: list,
    enable_deletes: bool,
) -> dict:
    """
    Execute CDC merge into Delta Lake table.

    Ab Initio equivalent: CDCProcessor.process() producing inserts/updates/deletes
    DataFrames, then applying them in separate steps.
    Spark equivalent: Single atomic MERGE INTO statement.

    Args:
        spark: Active SparkSession
        source_df: Current source snapshot DataFrame
        target_table: Target Delta table name
        key_columns: Columns forming the merge key
        hash_columns: Columns to hash for change detection
        enable_deletes: Whether to soft-delete records not in source

    Returns:
        Summary dict with CDC statistics
    """
    start_time = datetime.utcnow()

    # Add row hash to source for change detection
    source_with_hash = add_row_hash(source_df, key_columns, hash_columns)

    # Add CDC metadata columns to source
    source_with_hash = (
        source_with_hash
        .withColumn("_cdc_timestamp", current_timestamp())
        .withColumn("_cdc_operation", lit("UPSERT"))
        .withColumn("_is_deleted", lit(False))
    )

    # Check if target table exists
    target_exists = spark.catalog.tableExists(target_table)

    if not target_exists:
        # First run: create table from source (all records are inserts)
        logger.info(f"Target table {target_table} does not exist. Creating with initial load.")
        source_with_hash.write.format("delta").saveAsTable(target_table)

        # Enable Change Data Feed on the new table
        spark.sql(f"""
            ALTER TABLE {target_table}
            SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)

        record_count = source_df.count()
        duration = (datetime.utcnow() - start_time).total_seconds()
        return {
            "target_table": target_table,
            "initial_load": True,
            "inserts": record_count,
            "updates": 0,
            "deletes": 0,
            "duration_seconds": round(duration, 2),
            "status": "success",
        }

    # Build merge condition from key columns
    # (equivalent to CDCProcessor using key_columns for set_index/join)
    delta_table = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in key_columns]
    )

    # Build update columns (all non-key, non-internal columns)
    update_columns = {
        c: f"source.{c}"
        for c in source_with_hash.columns
        if c not in key_columns
    }

    # Execute MERGE INTO
    # This replaces the manual insert/update/delete classification in CDCProcessor.process()
    merge_builder = (
        delta_table.alias("target")
        .merge(source_with_hash.alias("source"), merge_condition)
        # UPDATE when key matches but row hash differs (change detected)
        .whenMatchedUpdate(
            condition="target._row_hash != source._row_hash",
            set=update_columns,
        )
        # INSERT when key not found in target (new record)
        .whenNotMatchedInsertAll()
    )

    # Handle deletes: soft-delete records in target not present in source
    # (equivalent to CDCProcessor's delete_keys = target_keys - source_keys)
    if enable_deletes:
        merge_builder = merge_builder.whenNotMatchedBySourceUpdate(
            set={
                "_is_deleted": "true",
                "_cdc_operation": "'DELETE'",
                "_cdc_timestamp": "current_timestamp()",
            }
        )

    merge_builder.execute()

    # Collect CDC statistics from Delta table history
    # (replaces CDCProcessor's stats dict)
    history = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()
    operation_metrics = history[0]["operationMetrics"] if history else {}

    duration = (datetime.utcnow() - start_time).total_seconds()
    result = {
        "target_table": target_table,
        "initial_load": False,
        "inserts": int(operation_metrics.get("numTargetRowsInserted", 0)),
        "updates": int(operation_metrics.get("numTargetRowsUpdated", 0)),
        "deletes": int(operation_metrics.get("numTargetRowsDeleted", 0)),
        "duration_seconds": round(duration, 2),
        "operation_metrics": dict(operation_metrics) if operation_metrics else {},
        "status": "success",
    }
    logger.info(f"CDC merge completed: {result}")
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute CDC Pipeline
# MAGIC Orchestrates the full CDC flow.
# MAGIC Replaces the sequential graph execution in `run_customer_cdc.ksh`.

# COMMAND ----------

# Main execution block
spark = SparkSession.builder.getOrCreate()

logger.info("=" * 60)
logger.info("CDC PROCESSOR — Delta Lake MERGE")
logger.info(f"  Source:       {source_path}")
logger.info(f"  Target:       {target_table}")
logger.info(f"  Key Columns:  {key_columns}")
logger.info(f"  Deletes:      {'enabled' if enable_deletes else 'disabled'}")
logger.info("=" * 60)

# Step 1: Read current source snapshot
# (equivalent to run_customer_cdc.ksh Step 1: snapshot_customer.mp)
source_df = read_source_snapshot(spark, source_path, file_format)

# Step 2: Execute CDC merge
# (equivalent to Steps 2+3: cdc_detect_customer.mp + apply_customer_changes.mp)
result = execute_cdc_merge(
    spark, source_df, target_table, key_columns, hash_columns, enable_deletes
)

# Step 3: Log results for audit trail
# (equivalent to Step 4: audit_customer_changes.mp — now handled by Delta CDF)
logger.info(f"CDC pipeline complete. Results: {result}")
logger.info(
    "Audit trail available via Delta Change Data Feed: "
    f"SELECT * FROM table_changes('{target_table}', 1)"
)

# Return result for Databricks workflow task value
dbutils.notebook.exit(str(result))
