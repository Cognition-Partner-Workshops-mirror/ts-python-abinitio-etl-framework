# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor — Delta Lake MERGE with Change Data Feed
# MAGIC
# MAGIC **Migrated from:** `graphs/cdc_processor.py` (Ab Initio hash-based CDC / "Compare Records by Key")
# MAGIC
# MAGIC **Ab Initio approach:** Pandas-based row hashing (MD5) to detect INSERTs,
# MAGIC UPDATEs, DELETEs by comparing source snapshot against target.
# MAGIC
# MAGIC **Databricks approach:** Delta Lake `MERGE INTO` with Change Data Feed (CDF)
# MAGIC enabled. CDF automatically tracks row-level changes, replacing the manual
# MAGIC hash-comparison pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters (mapped from PSET: customer_cdc.pset)

# COMMAND ----------

dbutils.widgets.text("source_path", "/mnt/raw/customer", "Source Path")
dbutils.widgets.text("target_table", "lakehouse.silver.customer_master", "Target Table")
dbutils.widgets.text("key_columns", "customer_id", "Key Columns (comma-separated)")
dbutils.widgets.text("hash_columns", "customer_id,name,address,phone,email,status", "Hash Columns")
dbutils.widgets.text("partition_count", "8", "Partition Count")
dbutils.widgets.text("batch_size", "100000", "Batch Size")
dbutils.widgets.text("max_errors", "50", "Max Tolerated Errors")
dbutils.widgets.text("audit_table", "lakehouse.ops.customer_change_audit", "Audit Table")
dbutils.widgets.text("retention_days", "90", "Retention Days")
dbutils.widgets.text("run_timestamp", "", "Run Timestamp")

# COMMAND ----------

import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SOURCE_PATH = dbutils.widgets.get("source_path")
TARGET_TABLE = dbutils.widgets.get("target_table")
KEY_COLUMNS = [c.strip() for c in dbutils.widgets.get("key_columns").split(",")]
HASH_COLUMNS = [c.strip() for c in dbutils.widgets.get("hash_columns").split(",")]
PARTITION_COUNT = int(dbutils.widgets.get("partition_count"))
MAX_ERRORS = int(dbutils.widgets.get("max_errors"))
AUDIT_TABLE = dbutils.widgets.get("audit_table")
RETENTION_DAYS = int(dbutils.widgets.get("retention_days"))
RUN_TIMESTAMP = dbutils.widgets.get("run_timestamp") or datetime.utcnow().isoformat()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_processor")

spark = SparkSession.builder.getOrCreate()

logger.info(
    f"CDC Processor started | source={SOURCE_PATH} | target={TARGET_TABLE} "
    f"| keys={KEY_COLUMNS} | run_ts={RUN_TIMESTAMP}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read Source Snapshot
# MAGIC
# MAGIC Replaces Ab Initio `snapshot_customer.mp` graph. Reads the current source
# MAGIC state and computes a row-level hash for change detection.

# COMMAND ----------

def read_source_snapshot(path: str) -> DataFrame:
    """Read current source data and add a change-detection hash column."""
    logger.info(f"Reading source snapshot from {path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .repartition(PARTITION_COUNT)
    )

    # Compute row hash for change detection (mirrors CDCProcessor._row_hash)
    hash_expr = F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in HASH_COLUMNS]))
    df = df.withColumn("_row_hash", hash_expr)

    record_count = df.count()
    logger.info(f"Source snapshot: {record_count} records")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Delta Lake MERGE (Replaces Hash-Based CDC)
# MAGIC
# MAGIC The Ab Initio CDC pattern (CDCProcessor.process) manually compares DataFrames
# MAGIC to produce insert/update/delete sets. Delta Lake MERGE does this atomically:
# MAGIC
# MAGIC | Ab Initio CDC Step | Delta Lake Equivalent |
# MAGIC |---|---|
# MAGIC | `source_keys - target_keys` → inserts | `WHEN NOT MATCHED THEN INSERT` |
# MAGIC | `target_keys - source_keys` → deletes | `WHEN NOT MATCHED BY SOURCE THEN DELETE` |
# MAGIC | hash mismatch on common keys → updates | `WHEN MATCHED AND hash changed THEN UPDATE` |

# COMMAND ----------

def run_cdc_merge(source_df: DataFrame, target_table: str) -> dict:
    """
    Execute Delta Lake MERGE to apply CDC changes.

    Replaces: cdc_detect_customer.mp + apply_customer_changes.mp
    """
    logger.info(f"Running CDC MERGE into {target_table}")

    # Build the merge key condition
    key_condition = " AND ".join([f"target.{k} = source.{k}" for k in KEY_COLUMNS])

    # Columns to update (excluding keys and internal columns)
    source_columns = [c for c in source_df.columns if c not in KEY_COLUMNS and not c.startswith("_")]
    update_set = {c: f"source.{c}" for c in source_columns}
    update_set["_row_hash"] = "source._row_hash"
    update_set["_updated_at"] = "current_timestamp()"

    # All columns for insert
    insert_columns = {c: f"source.{c}" for c in source_df.columns}
    insert_columns["_created_at"] = "current_timestamp()"
    insert_columns["_updated_at"] = "current_timestamp()"

    try:
        target_dt = DeltaTable.forName(spark, target_table)

        (
            target_dt.alias("target")
            .merge(source_df.alias("source"), key_condition)
            # UPDATE: key matches but hash changed (content modified)
            .whenMatchedUpdate(
                condition="target._row_hash != source._row_hash",
                set=update_set,
            )
            # INSERT: new records not in target
            .whenNotMatchedInsert(values=insert_columns)
            # DELETE: records in target but not in source (soft-delete via flag)
            .whenNotMatchedBySourceUpdate(set={
                "_is_deleted": "true",
                "_deleted_at": "current_timestamp()",
            })
            .execute()
        )

        logger.info("MERGE executed successfully")

    except AnalysisException:
        # Target table does not exist — first run: create from source
        logger.info(f"Target table {target_table} not found — creating from source snapshot")
        (
            source_df
            .withColumn("_created_at", F.current_timestamp())
            .withColumn("_updated_at", F.current_timestamp())
            .withColumn("_is_deleted", F.lit(False))
            .withColumn("_deleted_at", F.lit(None).cast("timestamp"))
            .write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(target_table)
        )
        logger.info(f"Created target table with {source_df.count()} initial records")

    # Collect CDC metrics from Change Data Feed
    metrics = _collect_cdf_metrics(target_table)
    return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Collect Change Data Feed Metrics
# MAGIC
# MAGIC Replaces the `stats` dict from `CDCProcessor.process()`. Delta Lake Change
# MAGIC Data Feed provides a built-in change log with `_change_type` column.

# COMMAND ----------

def _collect_cdf_metrics(target_table: str) -> dict:
    """Read the latest Change Data Feed entries to produce CDC statistics."""
    try:
        changes_df = (
            spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "latest")
            .table(target_table)
        )
        insert_count = changes_df.filter(F.col("_change_type") == "insert").count()
        update_count = changes_df.filter(F.col("_change_type").isin("update_preimage", "update_postimage")).count() // 2
        delete_count = changes_df.filter(F.col("_change_type") == "delete").count()
    except Exception as e:
        logger.warning(f"Could not read CDF metrics: {e}")
        insert_count = update_count = delete_count = -1

    metrics = {
        "inserts": insert_count,
        "updates": update_count,
        "deletes": delete_count,
        "run_timestamp": RUN_TIMESTAMP,
    }
    logger.info(f"CDC metrics: {metrics}")
    return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Write Audit Trail
# MAGIC
# MAGIC Replaces Ab Initio `audit_customer_changes.mp` graph.

# COMMAND ----------

def write_audit_trail(metrics: dict, audit_table: str) -> None:
    """Write CDC run metrics to the audit table for compliance tracking."""
    logger.info(f"Writing audit record to {audit_table}")

    audit_record = {
        "run_timestamp": RUN_TIMESTAMP,
        "target_table": TARGET_TABLE,
        "inserts": metrics.get("inserts", 0),
        "updates": metrics.get("updates", 0),
        "deletes": metrics.get("deletes", 0),
        "status": "success",
        "recorded_at": datetime.utcnow().isoformat(),
    }

    spark.createDataFrame([audit_record]).write.mode("append").saveAsTable(audit_table)
    logger.info(f"Audit record written: {audit_record}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Apply Retention Policy
# MAGIC
# MAGIC Mirrors RETENTION_DAYS=90 from customer_cdc.pset. Uses Delta Lake VACUUM
# MAGIC to enforce data retention.

# COMMAND ----------

def apply_retention(target_table: str, retention_days: int) -> None:
    """Vacuum old Delta Lake versions beyond retention window."""
    logger.info(f"Applying {retention_days}-day retention on {target_table}")
    spark.sql(f"VACUUM {target_table} RETAIN {retention_days * 24} HOURS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Execute Pipeline

# COMMAND ----------

start_time = datetime.now()
logger.info(f"CDC pipeline execution started at {start_time}")

try:
    source_df = read_source_snapshot(SOURCE_PATH)
    cdc_metrics = run_cdc_merge(source_df, TARGET_TABLE)
    write_audit_trail(cdc_metrics, AUDIT_TABLE)
    apply_retention(TARGET_TABLE, RETENTION_DAYS)

    elapsed = (datetime.now() - start_time).total_seconds()
    cdc_metrics["duration_seconds"] = round(elapsed, 2)
    cdc_metrics["overall_status"] = "success"

    logger.info(f"CDC pipeline completed in {elapsed:.1f}s")

except Exception as e:
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.error(f"CDC pipeline failed after {elapsed:.1f}s: {e}")
    cdc_metrics = {
        "overall_status": "failed",
        "error": str(e),
        "duration_seconds": round(elapsed, 2),
        "run_timestamp": RUN_TIMESTAMP,
    }
    raise

finally:
    spark.createDataFrame([cdc_metrics]).write.mode("append").saveAsTable(
        "lakehouse.ops.pipeline_run_log"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Summary

# COMMAND ----------

dbutils.notebook.exit(str(cdc_metrics))
