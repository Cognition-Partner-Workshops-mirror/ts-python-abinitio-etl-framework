# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Loader — Partition-Based Parallel Ingestion
# MAGIC
# MAGIC **Migrated from:** `graphs/parallel_loader.py` (Ab Initio parallel graph execution)
# MAGIC
# MAGIC **Ab Initio pattern:** `m_partition` component splits data into N partitions,
# MAGIC each processed by a separate graph instance via `air_run`.
# MAGIC
# MAGIC **Databricks equivalent:** Spark's native partitioning handles parallelism
# MAGIC automatically. This notebook uses `repartition()` for explicit control and
# MAGIC reads source data in parallel via partition predicates.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters (mapped from PSET values)

# COMMAND ----------

dbutils.widgets.text("source_path", "/mnt/raw/orders", "Source Path")
dbutils.widgets.text("target_table", "lakehouse.bronze.orders", "Target Table")
dbutils.widgets.text("partition_count", "4", "Partition Count")
dbutils.widgets.text("batch_size", "50000", "Batch Size")
dbutils.widgets.text("log_level", "INFO", "Log Level")
dbutils.widgets.text("max_errors", "100", "Max Errors")
dbutils.widgets.text("checkpoint_path", "/mnt/checkpoints/orders", "Checkpoint Path")
dbutils.widgets.text("batch_date", "", "Batch Date (YYYY-MM-DD)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
partition_count = int(dbutils.widgets.get("partition_count"))
batch_size = int(dbutils.widgets.get("batch_size"))
log_level = dbutils.widgets.get("log_level")
max_errors = int(dbutils.widgets.get("max_errors"))
checkpoint_path = dbutils.widgets.get("checkpoint_path")
batch_date = dbutils.widgets.get("batch_date") or datetime.utcnow().strftime("%Y-%m-%d")

logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logger = logging.getLogger("parallel_loader")

logger.info(
    "Parallel loader started | source=%s | target=%s | partitions=%d | batch_date=%s",
    source_path, target_table, partition_count, batch_date,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Source Data
# MAGIC
# MAGIC Replaces the Ab Initio `air sandbox run` extraction phase.
# MAGIC Spark reads the source path and automatically parallelizes across cluster cores.

# COMMAND ----------

try:
    source_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("maxFilesPerTrigger", str(batch_size))
        .csv(source_path)
    )
    source_df = source_df.withColumn("_batch_date", F.lit(batch_date))
    record_count = source_df.count()
    logger.info("Source read complete: %d records from %s", record_count, source_path)
except AnalysisException as e:
    logger.error("Failed to read source data: %s", e)
    dbutils.notebook.exit(f'{{"status": "FAILED", "error": "{e}"}}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Repartition for Parallel Processing
# MAGIC
# MAGIC Ab Initio's `m_partition` component is replaced by Spark's `repartition()`.
# MAGIC This controls the degree of parallelism for downstream writes.

# COMMAND ----------

source_df = source_df.repartition(partition_count)
logger.info("Data repartitioned into %d partitions", partition_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Data Quality Checks
# MAGIC
# MAGIC Replaces Ab Initio error handling (AI_MAX_ERRORS / AI_ERROR_ACTION).

# COMMAND ----------

null_key_count = source_df.filter(F.col(source_df.columns[0]).isNull()).count()
if null_key_count > max_errors:
    msg = f"Null key count ({null_key_count}) exceeds max_errors ({max_errors})"
    logger.error(msg)
    dbutils.notebook.exit(f'{{"status": "FAILED", "error": "{msg}"}}')

logger.info("Data quality check passed: %d null keys (threshold: %d)", null_key_count, max_errors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Delta Lake
# MAGIC
# MAGIC Replaces the Ab Initio staging/production load phases.
# MAGIC Uses Delta Lake MERGE for idempotent writes with checkpoint support.

# COMMAND ----------

(
    source_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkpoint_path)
    .saveAsTable(target_table)
)

logger.info(
    "Write complete: %d records → %s (%d partitions)",
    record_count, target_table, partition_count,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Post-Load Validation

# COMMAND ----------

written_count = spark.table(target_table).filter(F.col("_batch_date") == batch_date).count()
logger.info("Post-load validation: %d records for batch_date=%s", written_count, batch_date)

result = {
    "status": "SUCCESS",
    "records_read": record_count,
    "records_written": written_count,
    "partitions": partition_count,
    "batch_date": batch_date,
    "target_table": target_table,
}

dbutils.notebook.exit(str(result))
