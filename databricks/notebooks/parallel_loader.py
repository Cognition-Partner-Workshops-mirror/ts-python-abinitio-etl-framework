# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Loader — PySpark Notebook
# MAGIC **Migrated from:** `graphs/parallel_loader.py`
# MAGIC
# MAGIC ## Concept Mapping
# MAGIC | Ab Initio Concept               | Databricks Equivalent                              |
# MAGIC |---------------------------------|----------------------------------------------------|
# MAGIC | `air_run` subprocess per partition | Spark native parallelism via `repartition()`       |
# MAGIC | `PartitionManager.generate_ranges` | Spark partitioning (hash/range) on ingestion      |
# MAGIC | `ThreadPoolExecutor` orchestration | Spark DAG scheduler handles parallelism natively  |
# MAGIC | `-pset` parameter file           | Databricks widgets / job parameters                |
# MAGIC | `m_partition` component          | `repartition()` / `partitionBy()` on write         |
# MAGIC | Partition-level error tracking    | Spark accumulator + structured streaming metrics  |
# MAGIC
# MAGIC ## Key Differences
# MAGIC - Ab Initio spawns OS-level processes per partition; Spark distributes work across
# MAGIC   executors automatically — no manual subprocess management needed.
# MAGIC - Partition ranges are handled by Spark's Catalyst optimizer; explicit range
# MAGIC   calculation is replaced by `repartition()` or reading with partition predicates.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters (replaces PSET)

# COMMAND ----------

dbutils.widgets.text("source_path", "", "Source Data Path")
dbutils.widgets.text("target_table", "", "Target Delta Table")
dbutils.widgets.text("partition_count", "4", "Number of Partitions")
dbutils.widgets.text("batch_date", "", "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("max_errors", "100", "Max Allowed Errors")
dbutils.widgets.text("checkpoint_dir", "", "Checkpoint Directory")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"], "Log Level")

source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
partition_count = int(dbutils.widgets.get("partition_count"))
batch_date = dbutils.widgets.get("batch_date")
max_errors = int(dbutils.widgets.get("max_errors"))
checkpoint_dir = dbutils.widgets.get("checkpoint_dir")
log_level = dbutils.widgets.get("log_level")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Spark Session & Logging

# COMMAND ----------

import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel(log_level)

logging.basicConfig(level=getattr(logging, log_level))
logger = logging.getLogger("parallel_loader")

run_id = f"parallel_load_{batch_date}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
logger.info(f"Starting parallel loader run: {run_id}")
logger.info(
    f"Parameters: source_path={source_path}, target_table={target_table}, "
    f"partitions={partition_count}, batch_date={batch_date}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Ingest Source Data with Partition-Based Parallelism
# MAGIC
# MAGIC Replaces the Ab Initio `PartitionManager.generate_ranges()` + `air_run` per-partition
# MAGIC subprocess pattern. Spark handles parallelism natively through `repartition()`.

# COMMAND ----------

error_count = spark.sparkContext.accumulator(0)
records_loaded = spark.sparkContext.accumulator(0)

try:
    # Read source data — Spark auto-parallelizes across available cores
    raw_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(source_path)
    )

    total_records = raw_df.count()
    logger.info(f"Source record count: {total_records}")

    # Repartition for parallel processing (replaces PartitionManager.generate_ranges)
    partitioned_df = raw_df.repartition(partition_count)
    logger.info(f"Repartitioned into {partition_count} partitions")

except AnalysisException as e:
    logger.error(f"Failed to read source data: {e}")
    dbutils.notebook.exit(f'{{"status": "FAILED", "error": "Source read failed: {e}", "run_id": "{run_id}"}}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Data Quality Checks
# MAGIC
# MAGIC Replaces Ab Initio error handling (`AI_MAX_ERRORS` / `AI_ERROR_ACTION=ABORT`).

# COMMAND ----------

# Separate clean records from corrupt/malformed rows
if "_corrupt_record" in partitioned_df.columns:
    corrupt_df = partitioned_df.filter(F.col("_corrupt_record").isNotNull())
    corrupt_count = corrupt_df.count()
    clean_df = partitioned_df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    if corrupt_count > max_errors:
        error_msg = (
            f"Error threshold exceeded: {corrupt_count} corrupt records > max_errors={max_errors}"
        )
        logger.error(error_msg)
        dbutils.notebook.exit(
            f'{{"status": "FAILED", "error": "{error_msg}", "run_id": "{run_id}", '
            f'"corrupt_count": {corrupt_count}}}'
        )
    elif corrupt_count > 0:
        logger.warning(f"Found {corrupt_count} corrupt records (within threshold)")
else:
    clean_df = partitioned_df
    corrupt_count = 0

logger.info(f"Clean records: {clean_df.count()}, Corrupt records: {corrupt_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3: Add Audit Columns & Write to Delta
# MAGIC
# MAGIC Replaces the Ab Initio graph's output component that writes to the target table.
# MAGIC Adds audit metadata columns not present in the Ab Initio pattern.

# COMMAND ----------

# Add ingestion metadata (audit trail)
output_df = (
    clean_df
    .withColumn("_batch_date", F.lit(batch_date))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_run_id", F.lit(run_id))
    .withColumn("_source_file", F.input_file_name())
)

# Write to Delta table with optimized partitioning
try:
    (
        output_df
        .repartition(partition_count)
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    final_count = output_df.count()
    logger.info(f"Successfully wrote {final_count} records to {target_table}")

except Exception as e:
    logger.error(f"Delta write failed: {e}")
    dbutils.notebook.exit(
        f'{{"status": "FAILED", "error": "Delta write failed: {e}", "run_id": "{run_id}"}}'
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4: Post-Load Validation & Summary
# MAGIC
# MAGIC Replaces the Ab Initio `ParallelLoader.run_graph()` summary dict that reports
# MAGIC per-partition status, timing, and success/failure counts.

# COMMAND ----------

# Validate written data
try:
    target_count = spark.table(target_table).filter(F.col("_run_id") == run_id).count()
    source_count = clean_df.count()
    count_match = target_count == source_count

    summary = {
        "status": "SUCCESS" if count_match else "WARNING",
        "run_id": run_id,
        "batch_date": batch_date,
        "source_records": total_records,
        "clean_records": source_count,
        "corrupt_records": corrupt_count,
        "records_written": target_count,
        "count_match": count_match,
        "target_table": target_table,
        "partitions_used": partition_count,
    }

    if not count_match:
        logger.warning(
            f"Count mismatch: source={source_count}, target={target_count}"
        )

    logger.info(f"Parallel loader summary: {summary}")

except Exception as e:
    summary = {"status": "FAILED", "error": str(e), "run_id": run_id}
    logger.error(f"Post-load validation failed: {e}")

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps(summary))
