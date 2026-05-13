# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor — Delta Lake MERGE Notebook
# MAGIC **Converted from:** `graphs/cdc_processor.py` (Ab Initio hash-based CDC pattern)
# MAGIC
# MAGIC ## Ab Initio → Databricks Mapping
# MAGIC | Ab Initio Concept | Databricks Equivalent |
# MAGIC |---|---|
# MAGIC | `CDCProcessor.process()` with hash comparison | Delta Lake `MERGE INTO` with Change Data Feed |
# MAGIC | MD5 `_row_hash` for change detection | `sha2(concat_ws(...))` hash column |
# MAGIC | `source_df.set_index(key_columns)` | `MERGE ON` key join condition |
# MAGIC | Manual INSERT/UPDATE/DELETE detection | `WHEN MATCHED / WHEN NOT MATCHED` clauses |
# MAGIC | `pd.DataFrame` operations | PySpark DataFrame + Delta DML |
# MAGIC | Partition-based parallelism (`-partition 4`) | Spark's native shuffle partitions |
# MAGIC
# MAGIC ## Key Improvements over Ab Initio Pattern
# MAGIC - Delta Lake MERGE is ACID-compliant — no partial writes on failure.
# MAGIC - Change Data Feed (CDF) provides built-in CDC tracking without custom hash logic.
# MAGIC - No need for separate "previous snapshot" files — Delta time travel handles history.

# COMMAND ----------

# Widget parameters — mapped from Ab Initio PSET customer_cdc.pset
dbutils.widgets.text("source_path", "/mnt/raw/cdc_input", "Source CDC Data Path")
dbutils.widgets.text("target_table", "lakehouse.bronze.customer", "Target Delta Table")
dbutils.widgets.text("key_columns", "customer_id", "Key Columns (comma-separated)")
dbutils.widgets.text("compare_columns", "", "Compare Columns (empty = all non-key)")
dbutils.widgets.text("batch_date", "", "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("partition_count", "8", "Partition Count")
dbutils.widgets.dropdown("enable_deletes", "true", ["true", "false"], "Enable Delete Detection")

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, when, coalesce,
)
from delta.tables import DeltaTable

# Configure logging — replaces Ab Initio per-partition log files
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_processor")

# COMMAND ----------

# Retrieve parameters — equivalent to Ab Initio PSET loading
source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
key_columns = [c.strip() for c in dbutils.widgets.get("key_columns").split(",")]
compare_columns_raw = dbutils.widgets.get("compare_columns")
compare_columns = [c.strip() for c in compare_columns_raw.split(",") if c.strip()] or None
batch_date = dbutils.widgets.get("batch_date") or datetime.now().strftime("%Y-%m-%d")
partition_count = int(dbutils.widgets.get("partition_count"))
enable_deletes = dbutils.widgets.get("enable_deletes") == "true"

logger.info(
    f"CDC processor starting — source={source_path}, target={target_table}, "
    f"keys={key_columns}, batch_date={batch_date}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Read Source Data
# MAGIC Replaces the Ab Initio CDCProcessor's source_df input — the current snapshot from the source system.

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# Read the incoming CDC data (current source snapshot)
source_df = (
    spark.read.format("delta").load(source_path)
    .repartition(partition_count)
)

# Determine which columns to use for change detection
if compare_columns:
    hash_cols = compare_columns
else:
    hash_cols = [c for c in source_df.columns if c not in key_columns]

# Add row-level hash for change detection — equivalent to CDCProcessor._row_hash()
source_df = source_df.withColumn(
    "_row_hash",
    sha2(concat_ws("||", *[col(c).cast("string") for c in hash_cols]), 256),
)

source_count = source_df.count()
logger.info(f"Source snapshot: {source_count} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Delta Lake MERGE
# MAGIC Replaces the Ab Initio CDCProcessor.process() method that manually computed
# MAGIC inserts, updates, and deletes using pandas set operations.
# MAGIC
# MAGIC Delta Lake MERGE atomically handles all three operations in a single statement.

# COMMAND ----------

def build_merge_condition(keys: list) -> str:
    """
    Build the MERGE ON condition from key columns.
    Equivalent to Ab Initio's `self.key_columns` used for set_index().
    """
    return " AND ".join([f"target.{k} = source.{k}" for k in keys])


def run_cdc_merge(
    spark_session: SparkSession,
    source: DataFrame,
    target_table_name: str,
    keys: list,
    hash_cols: list,
    do_deletes: bool,
) -> dict:
    """
    Execute CDC merge against a Delta Lake table.
    Replaces the Ab Initio CDCProcessor.process() method.

    The Ab Initio pattern:
      1. Hash all rows (source + target)
      2. Compute insert_keys = source_keys - target_keys
      3. Compute delete_keys = target_keys - source_keys
      4. Compute updates where hash changed

    Delta Lake MERGE handles this atomically:
      - WHEN MATCHED AND hash changed → UPDATE
      - WHEN NOT MATCHED BY TARGET → INSERT
      - WHEN NOT MATCHED BY SOURCE → DELETE (optional)

    Args:
        spark_session: Active SparkSession
        source: Source DataFrame with _row_hash column
        target_table_name: Fully-qualified Delta table name
        keys: Primary key columns for matching
        hash_cols: Columns used for change detection hash
        do_deletes: Whether to apply DELETE operations

    Returns:
        CDC statistics dict matching Ab Initio CDCProcessor output format
    """
    start_time = datetime.now()
    merge_condition = build_merge_condition(keys)

    # Check if target table exists; if not, create it from source
    try:
        target_delta = DeltaTable.forName(spark_session, target_table_name)
    except Exception:
        logger.info(f"Target table {target_table_name} does not exist — creating from source (initial load)")
        source.drop("_row_hash").write.format("delta").saveAsTable(target_table_name)
        return {
            "operation": "initial_load",
            "inserts": source.count(),
            "updates": 0,
            "deletes": 0,
            "batch_date": batch_date,
            "status": "success",
        }

    # Add hash to target for comparison — same concept as Ab Initio's _row_hash on target_df
    target_with_hash = (
        spark_session.table(target_table_name)
        .withColumn(
            "_row_hash",
            sha2(concat_ws("||", *[col(c).cast("string") for c in hash_cols]), 256),
        )
    )

    # Build the Delta MERGE operation — replaces manual insert/update/delete detection
    # Column list for UPDATE SET and INSERT (excluding key columns and hash)
    update_cols = {
        c: f"source.{c}"
        for c in source.columns
        if c not in keys and c != "_row_hash"
    }

    # Build MERGE using Delta Lake API
    merge_builder = (
        target_delta.alias("target")
        .merge(source.alias("source"), merge_condition)
        .whenMatchedUpdate(
            # Only update when the row hash has changed — matches Ab Initio hash comparison
            condition="source._row_hash != target._row_hash",
            set={
                **{c: f"source.{c}" for c in source.columns if c not in keys and c != "_row_hash"},
                "_cdc_operation": lit("UPDATE"),
                "_cdc_timestamp": current_timestamp(),
            },
        )
        .whenNotMatchedInsert(
            values={
                **{c: f"source.{c}" for c in source.columns if c != "_row_hash"},
                "_cdc_operation": lit("INSERT"),
                "_cdc_timestamp": current_timestamp(),
            },
        )
    )

    # Optional DELETE detection — equivalent to Ab Initio delete_keys = target_keys - source_keys
    if do_deletes:
        merge_builder = merge_builder.whenNotMatchedBySourceDelete()

    # Execute the MERGE
    merge_builder.execute()

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"CDC merge completed in {elapsed:.1f}s")

    # Collect CDC metrics using Delta Lake history — replaces Ab Initio stats dict
    history = spark_session.sql(f"DESCRIBE HISTORY {target_table_name} LIMIT 1").collect()
    metrics = history[0]["operationMetrics"] if history else {}

    result = {
        "operation": "merge",
        "inserts": int(metrics.get("numTargetRowsInserted", 0)),
        "updates": int(metrics.get("numTargetRowsUpdated", 0)),
        "deletes": int(metrics.get("numTargetRowsDeleted", 0)),
        "duration_seconds": round(elapsed, 2),
        "batch_date": batch_date,
        "status": "success",
    }
    logger.info(f"CDC result: {result}")
    return result

# COMMAND ----------

# Execute CDC merge — replaces Ab Initio CDCProcessor.process() call
try:
    cdc_result = run_cdc_merge(
        spark_session=spark,
        source=source_df,
        target_table_name=target_table,
        keys=key_columns,
        hash_cols=hash_cols,
        do_deletes=enable_deletes,
    )
    logger.info(f"CDC processor completed: {cdc_result}")
    dbutils.notebook.exit(str(cdc_result))
except Exception as e:
    logger.error(f"CDC merge failed: {e}")
    dbutils.notebook.exit(f"FAILED: CDC merge error — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Merge: Query Change Data Feed (Optional)
# MAGIC Delta Lake's Change Data Feed replaces the Ab Initio audit trail graph.
# MAGIC Enable with: `ALTER TABLE <table> SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`

# COMMAND ----------

# Uncomment to query recent changes via Change Data Feed:
# changes_df = (
#     spark.read.format("delta")
#     .option("readChangeFeed", "true")
#     .option("startingVersion", 0)
#     .table(target_table)
# )
# display(changes_df)
