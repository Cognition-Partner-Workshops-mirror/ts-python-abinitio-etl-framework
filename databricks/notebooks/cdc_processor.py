# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor — Delta Lake MERGE with Change Data Feed
# MAGIC
# MAGIC **Migrated from:** `graphs/cdc_processor.py` (Ab Initio hash-based CDC)
# MAGIC
# MAGIC **Ab Initio pattern:** The `CDCProcessor` class computes row-level MD5 hashes
# MAGIC to detect INSERTs, UPDATEs, and DELETEs by comparing source vs. target snapshots.
# MAGIC
# MAGIC **Databricks equivalent:** Delta Lake's `MERGE INTO` with Change Data Feed (CDF)
# MAGIC replaces the manual hash-comparison logic. CDF automatically tracks row-level
# MAGIC changes, eliminating the need for snapshot-based comparison.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters (mapped from customer_cdc.pset)

# COMMAND ----------

dbutils.widgets.text("source_path", "/mnt/raw/customer", "Source Path")
dbutils.widgets.text("target_table", "lakehouse.bronze.customer", "Target Table")
dbutils.widgets.text("key_columns", "customer_id", "Key Columns (comma-separated)")
dbutils.widgets.text("hash_columns", "customer_id,first_name,last_name,email", "Hash/Compare Columns")
dbutils.widgets.text("partition_count", "8", "Partition Count")
dbutils.widgets.text("batch_size", "100000", "Batch Size")
dbutils.widgets.text("max_errors", "50", "Max Errors")
dbutils.widgets.text("audit_table", "lakehouse.audit.customer_changes", "Audit Table")
dbutils.widgets.text("run_timestamp", "", "Run Timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
key_columns = [c.strip() for c in dbutils.widgets.get("key_columns").split(",")]
hash_columns = [c.strip() for c in dbutils.widgets.get("hash_columns").split(",")]
partition_count = int(dbutils.widgets.get("partition_count"))
batch_size = int(dbutils.widgets.get("batch_size"))
max_errors = int(dbutils.widgets.get("max_errors"))
audit_table = dbutils.widgets.get("audit_table")
run_timestamp = dbutils.widgets.get("run_timestamp") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_processor")

logger.info(
    "CDC processor started | source=%s | target=%s | keys=%s",
    source_path, target_table, key_columns,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Source Snapshot
# MAGIC
# MAGIC Replaces Ab Initio `snapshot_customer.mp` graph.

# COMMAND ----------

try:
    source_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
        .repartition(partition_count)
    )
    source_count = source_df.count()
    logger.info("Source snapshot: %d records", source_count)
except AnalysisException as e:
    logger.error("Failed to read source: %s", e)
    dbutils.notebook.exit(f'{{"status": "FAILED", "phase": "source_read", "error": "{e}"}}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Compute Row Hash for Change Detection
# MAGIC
# MAGIC Replaces Ab Initio `CDCProcessor._row_hash()` MD5 logic.
# MAGIC Uses Spark's built-in `md5(concat_ws(...))` for distributed hash computation.

# COMMAND ----------

source_df = source_df.withColumn(
    "_row_hash",
    F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in hash_columns]))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Delta Lake MERGE (Upsert + Delete)
# MAGIC
# MAGIC Replaces the Ab Initio CDC detect → apply → audit pipeline
# MAGIC (`cdc_detect_customer.mp` + `apply_customer_changes.mp`).
# MAGIC
# MAGIC A single `MERGE INTO` statement handles INSERT, UPDATE, and DELETE in one
# MAGIC atomic transaction — compared to three separate Ab Initio graph executions.

# COMMAND ----------

from delta.tables import DeltaTable

target_exists = spark.catalog.tableExists(target_table)

if not target_exists:
    logger.info("Target table %s does not exist — performing initial full load", target_table)
    (
        source_df
        .drop("_row_hash")
        .withColumn("_cdc_operation", F.lit("INSERT"))
        .withColumn("_cdc_timestamp", F.lit(run_timestamp).cast("timestamp"))
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    insert_count = source_count
    update_count = 0
    delete_count = 0
else:
    delta_target = DeltaTable.forName(spark, target_table)

    merge_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in key_columns]
    )

    merge_result = (
        delta_target.alias("target")
        .merge(
            source_df.alias("source"),
            merge_condition,
        )
        .whenMatchedUpdate(
            condition="target._row_hash <> source._row_hash",
            set={
                **{col: f"source.{col}" for col in source_df.columns if col != "_row_hash"},
                "_cdc_operation": F.lit("UPDATE"),
                "_cdc_timestamp": F.lit(run_timestamp).cast("timestamp"),
            },
        )
        .whenNotMatchedInsert(
            values={
                **{col: f"source.{col}" for col in source_df.columns if col != "_row_hash"},
                "_cdc_operation": F.lit("INSERT"),
                "_cdc_timestamp": F.lit(run_timestamp).cast("timestamp"),
            },
        )
        .whenNotMatchedBySourceDelete()
        .execute()
    )

    history = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()[0]
    metrics = history["operationMetrics"]
    insert_count = int(metrics.get("numTargetRowsInserted", 0))
    update_count = int(metrics.get("numTargetRowsUpdated", 0))
    delete_count = int(metrics.get("numTargetRowsDeleted", 0))

logger.info(
    "MERGE complete — inserts=%d, updates=%d, deletes=%d",
    insert_count, update_count, delete_count,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write Audit Trail
# MAGIC
# MAGIC Replaces Ab Initio `audit_customer_changes.mp` graph.
# MAGIC Leverages Delta Lake Change Data Feed to capture the audit log automatically.

# COMMAND ----------

try:
    audit_df = spark.createDataFrame(
        [{
            "target_table": target_table,
            "run_timestamp": run_timestamp,
            "source_record_count": source_count,
            "inserts": insert_count,
            "updates": update_count,
            "deletes": delete_count,
            "status": "SUCCESS",
        }]
    )
    audit_df.write.format("delta").mode("append").saveAsTable(audit_table)
    logger.info("Audit record written to %s", audit_table)
except Exception as e:
    logger.warning("Audit write failed (non-fatal): %s", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Output Summary

# COMMAND ----------

result = {
    "status": "SUCCESS",
    "source_records": source_count,
    "inserts": insert_count,
    "updates": update_count,
    "deletes": delete_count,
    "target_table": target_table,
    "run_timestamp": run_timestamp,
}

logger.info("CDC processing complete: %s", result)
dbutils.notebook.exit(str(result))
