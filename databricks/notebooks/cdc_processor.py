# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor — Delta Lake MERGE with Change Data Feed
# MAGIC **Migrated from:** `graphs/cdc_processor.py`
# MAGIC
# MAGIC ## Concept Mapping
# MAGIC | Ab Initio Concept                    | Databricks Equivalent                                |
# MAGIC |--------------------------------------|------------------------------------------------------|
# MAGIC | `CDCProcessor` (hash-based compare)  | Delta Lake `MERGE INTO` with match conditions        |
# MAGIC | MD5 row hash for change detection    | Column-level comparison in MERGE WHEN MATCHED clause |
# MAGIC | Separate INSERT/UPDATE/DELETE outputs | Single MERGE statement handles all three operations  |
# MAGIC | pandas DataFrame comparison          | Spark DataFrame operations at scale                  |
# MAGIC | Source snapshot vs target compare     | Delta Change Data Feed (CDF) for downstream capture  |
# MAGIC | `_hash` column for change detection  | Hash column optional; MERGE handles comparison       |
# MAGIC
# MAGIC ## Key Differences
# MAGIC - Ab Initio CDC compares two full snapshots using MD5 hashing and set operations.
# MAGIC - Delta Lake MERGE performs the comparison and apply in a single atomic operation.
# MAGIC - Delta Change Data Feed (CDF) provides a built-in CDC log for downstream consumers,
# MAGIC   eliminating the need for a separate audit trail graph.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters (replaces PSET: customer_cdc.pset)

# COMMAND ----------

dbutils.widgets.text("source_path", "", "Source Data Path")
dbutils.widgets.text("target_table", "", "Target Delta Table")
dbutils.widgets.text("key_columns", "customer_id", "Key Columns (comma-separated)")
dbutils.widgets.text("hash_columns", "", "Hash Columns for Change Detection (comma-separated, empty=all)")
dbutils.widgets.text("partition_count", "8", "Number of Partitions")
dbutils.widgets.text("batch_size", "100000", "Batch Size")
dbutils.widgets.text("max_errors", "50", "Max Allowed Errors")
dbutils.widgets.text("audit_table", "", "Audit Log Table")
dbutils.widgets.text("run_timestamp", "", "Run Timestamp")

source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
key_columns = [c.strip() for c in dbutils.widgets.get("key_columns").split(",")]
hash_columns_raw = dbutils.widgets.get("hash_columns")
hash_columns = [c.strip() for c in hash_columns_raw.split(",") if c.strip()] if hash_columns_raw else []
partition_count = int(dbutils.widgets.get("partition_count"))
batch_size = int(dbutils.widgets.get("batch_size"))
max_errors = int(dbutils.widgets.get("max_errors"))
audit_table = dbutils.widgets.get("audit_table")
run_timestamp = dbutils.widgets.get("run_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import logging
from datetime import datetime
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_processor")

run_id = f"cdc_{run_timestamp or datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}"
logger.info(f"CDC run: {run_id} | target={target_table} | keys={key_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Read Source Snapshot
# MAGIC
# MAGIC Replaces the Ab Initio `CDCProcessor.process()` source DataFrame input.
# MAGIC Reads the latest source extract and repartitions for parallel processing.

# COMMAND ----------

try:
    source_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(source_path)
        .repartition(partition_count)
    )
    source_count = source_df.count()
    logger.info(f"Source snapshot: {source_count} records from {source_path}")

except Exception as e:
    logger.error(f"Failed to read source: {e}")
    dbutils.notebook.exit(f'{{"status": "FAILED", "phase": "source_read", "error": "{e}"}}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Compute Row Hash for Change Detection
# MAGIC
# MAGIC Replaces the Ab Initio `CDCProcessor._row_hash()` MD5-based change detection.
# MAGIC Uses `md5(concat_ws())` in Spark SQL for distributed hash computation.

# COMMAND ----------

# Determine columns to hash (all non-key columns if hash_columns not specified)
if not hash_columns:
    hash_columns = [c for c in source_df.columns if c not in key_columns]

logger.info(f"Hash columns for change detection: {hash_columns}")

# Add hash column to source — equivalent to CDCProcessor._row_hash()
source_with_hash = source_df.withColumn(
    "_row_hash",
    F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in hash_columns]))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3: Delta Lake MERGE (Replaces Snapshot Compare + Separate Apply)
# MAGIC
# MAGIC The Ab Initio CDC pattern:
# MAGIC 1. Computes set differences (source_keys - target_keys → inserts)
# MAGIC 2. Computes hash differences on common keys → updates
# MAGIC 3. Computes (target_keys - source_keys) → deletes
# MAGIC 4. Applies each as separate operations
# MAGIC
# MAGIC Delta MERGE combines all four steps into a single atomic transaction.

# COMMAND ----------

# Build the MERGE join condition on key columns
merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])

# Build the update condition: hash mismatch means the record changed
update_set = {c: f"source.{c}" for c in source_df.columns if c not in key_columns}
update_set["_row_hash"] = "source._row_hash"
update_set["_cdc_operation"] = "'UPDATE'"
update_set["_cdc_timestamp"] = "current_timestamp()"

# Build insert values
insert_values = {c: f"source.{c}" for c in source_df.columns}
insert_values["_row_hash"] = "source._row_hash"
insert_values["_cdc_operation"] = "'INSERT'"
insert_values["_cdc_timestamp"] = "current_timestamp()"

try:
    # Check if target table exists; if not, create it from source
    table_exists = spark.catalog.tableExists(target_table)

    if not table_exists:
        logger.info(f"Target table {target_table} does not exist — performing initial full load")
        (
            source_with_hash
            .withColumn("_cdc_operation", F.lit("INSERT"))
            .withColumn("_cdc_timestamp", F.current_timestamp())
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        merge_stats = {
            "inserts": source_count,
            "updates": 0,
            "deletes": 0,
            "initial_load": True,
        }
    else:
        # Register source as temp view for SQL MERGE
        source_with_hash.createOrReplaceTempView("source_cdc")

        # Build and execute MERGE statement
        update_clause = ", ".join([f"target.{k} = {v}" for k, v in update_set.items()])
        insert_cols = ", ".join(insert_values.keys())
        insert_vals = ", ".join(insert_values.values())

        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING source_cdc AS source
        ON {merge_condition}
        WHEN MATCHED AND target._row_hash != source._row_hash THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE
        """

        logger.info(f"Executing MERGE into {target_table}")
        spark.sql(merge_sql)

        # Capture merge statistics via Delta history
        history_df = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1")
        latest_op = history_df.collect()[0]
        op_metrics = latest_op["operationMetrics"] or {}

        merge_stats = {
            "inserts": int(op_metrics.get("numTargetRowsInserted", 0)),
            "updates": int(op_metrics.get("numTargetRowsUpdated", 0)),
            "deletes": int(op_metrics.get("numTargetRowsDeleted", 0)),
            "initial_load": False,
        }

    logger.info(f"MERGE complete: {merge_stats}")

except Exception as e:
    logger.error(f"MERGE failed: {e}")
    dbutils.notebook.exit(f'{{"status": "FAILED", "phase": "merge", "error": "{e}", "run_id": "{run_id}"}}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4: Write Audit Trail
# MAGIC
# MAGIC Replaces the Ab Initio `audit_customer_changes.mp` graph step.
# MAGIC Uses Delta Change Data Feed (CDF) for built-in audit; also writes an explicit
# MAGIC audit record for backward compatibility with the existing audit table pattern.

# COMMAND ----------

if audit_table:
    try:
        audit_record = spark.createDataFrame([{
            "run_id": run_id,
            "target_table": target_table,
            "run_timestamp": datetime.utcnow().isoformat(),
            "source_record_count": source_count,
            "inserts": merge_stats["inserts"],
            "updates": merge_stats["updates"],
            "deletes": merge_stats["deletes"],
            "initial_load": merge_stats["initial_load"],
            "status": "SUCCESS",
        }])

        audit_record.write.format("delta").mode("append").saveAsTable(audit_table)
        logger.info(f"Audit record written to {audit_table}")

    except Exception as e:
        logger.warning(f"Audit write failed (non-fatal): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

import json

summary = {
    "status": "SUCCESS",
    "run_id": run_id,
    "target_table": target_table,
    "source_records": source_count,
    "inserts": merge_stats["inserts"],
    "updates": merge_stats["updates"],
    "deletes": merge_stats["deletes"],
    "key_columns": key_columns,
    "hash_columns": hash_columns,
    "partitions_used": partition_count,
}

logger.info(f"CDC processor summary: {summary}")
dbutils.notebook.exit(json.dumps(summary))
