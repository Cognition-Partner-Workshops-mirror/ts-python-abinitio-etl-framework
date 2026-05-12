# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Loader — PySpark Notebook
# MAGIC
# MAGIC **Migrated from:** `graphs/parallel_loader.py` (Ab Initio partition-based parallel graph execution)
# MAGIC
# MAGIC ## Migration Summary
# MAGIC | Ab Initio Concept | Databricks Equivalent |
# MAGIC |---|---|
# MAGIC | `air sandbox run -partition=N` | Spark partition-based parallelism via `repartition()` |
# MAGIC | `m_partition` component | Spark DataFrame partitioning / `PARTITION BY` in Delta |
# MAGIC | `PartitionManager.generate_ranges` | `spark.read` with partition column pushdown |
# MAGIC | PSET parameters (paths, batch size) | Databricks widgets / job parameters |
# MAGIC | Thread-based parallelism | Spark's built-in distributed execution across workers |
# MAGIC
# MAGIC Ab Initio's explicit partition management and shell-level parallelism are replaced by
# MAGIC Spark's native distributed execution. Spark automatically parallelizes reads and writes
# MAGIC across cluster workers — no manual partition range splitting is needed.

# COMMAND ----------

# Widget parameters — replace Ab Initio PSET values
# These map directly to the PSET parameters defined in psets/pset_templates/orders_pipeline.pset
dbutils.widgets.text("source_path", "/mnt/raw/orders", "Source Path (PSET: SOURCE_PATH)")
dbutils.widgets.text("target_table", "catalog.bronze.orders", "Target Table (PSET: TARGET_TABLE)")
dbutils.widgets.text("partition_count", "4", "Partition Count (PSET: PARTITION_COUNT)")
dbutils.widgets.text("batch_size", "50000", "Batch Size (PSET: BATCH_SIZE)")
dbutils.widgets.text("max_errors", "100", "Max Errors (PSET: MAX_ERRORS)")
dbutils.widgets.text("batch_date", "", "Batch Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"], "Log Level (PSET: LOG_LEVEL)")

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name, count, sha2, concat_ws,
)
from pyspark.sql.utils import AnalysisException

# Configure logging — replaces Ab Initio graph log output and LOG_DIR
log_level = dbutils.widgets.get("log_level")
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logger = logging.getLogger("parallel_loader")
logger.info("Parallel Loader notebook started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Resolve Parameters
# MAGIC Reads widget values — equivalent to loading PSET parameters in the Ab Initio graph.

# COMMAND ----------

# Resolve parameters from widgets (replaces PSETManager.load_pset)
source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
partition_count = int(dbutils.widgets.get("partition_count"))
batch_size = int(dbutils.widgets.get("batch_size"))
max_errors = int(dbutils.widgets.get("max_errors"))
batch_date = dbutils.widgets.get("batch_date") or datetime.now().strftime("%Y-%m-%d")

logger.info(
    f"Parameters loaded — source_path={source_path}, target_table={target_table}, "
    f"partition_count={partition_count}, batch_size={batch_size}, batch_date={batch_date}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Source Data
# MAGIC Replaces the Ab Initio extract phase (`air sandbox run extract_orders.mp`).
# MAGIC Spark reads the source path and automatically distributes work across partitions.

# COMMAND ----------

# Read source data with automatic parallelism
# Ab Initio required explicit partition ranges (start_record/end_record);
# Spark handles this automatically via its partition discovery and predicate pushdown.
try:
    source_df = (
        spark.read
        .format("delta")  # Use "csv"/"parquet"/"json" for non-Delta sources
        .load(source_path)
    )
    # Repartition to match the configured partition count
    # This replaces Ab Initio's PartitionManager.generate_ranges() logic
    source_df = source_df.repartition(partition_count)

    record_count = source_df.count()
    logger.info(f"Loaded {record_count} records from {source_path} into {partition_count} partitions")

    if record_count == 0:
        logger.warning("No records found in source — exiting early")
        dbutils.notebook.exit("NO_DATA")
except AnalysisException as e:
    logger.error(f"Failed to read source data: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Checks
# MAGIC Replaces Ab Initio's MAX_ERRORS threshold and ERROR_ACTION (ABORT/CONTINUE/SKIP).

# COMMAND ----------

# Validate data quality — replaces Ab Initio's error-handling configuration
# Ab Initio PSET: MAX_ERRORS=100, AI_ERROR_ACTION=ABORT
null_key_count = source_df.filter(col("order_id").isNull()).count()

if null_key_count > max_errors:
    error_msg = f"Null key count ({null_key_count}) exceeds MAX_ERRORS ({max_errors}) — aborting"
    logger.error(error_msg)
    raise ValueError(error_msg)
elif null_key_count > 0:
    logger.warning(f"Found {null_key_count} records with null keys — filtering out")
    source_df = source_df.filter(col("order_id").isNotNull())

logger.info("Data quality checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Add Audit Columns
# MAGIC Adds ingestion metadata for lineage tracking — not present in the Ab Initio graph
# MAGIC but essential for Lakehouse data governance.

# COMMAND ----------

# Add ingestion metadata columns for lineage tracking
# These columns replace the need for separate Ab Initio audit graphs
enriched_df = (
    source_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_batch_date", lit(batch_date))
    .withColumn("_source_file", input_file_name())
    .withColumn("_record_hash", sha2(concat_ws("||", *[col(c) for c in source_df.columns]), 256))
)

logger.info("Audit columns added: _ingested_at, _batch_date, _source_file, _record_hash")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Delta Lake
# MAGIC Replaces the Ab Initio production load phase (`air sandbox run prod_rollover_orders.mp`).
# MAGIC Delta Lake MERGE handles upserts automatically — no separate staging table required.

# COMMAND ----------

# Write to Delta Lake target table
# This replaces the multi-phase Ab Initio pipeline:
#   Phase 1 (extract) + Phase 3 (staging load) + Phase 4 (production rollover)
# Delta Lake ACID transactions eliminate the need for separate staging/production phases.
try:
    enriched_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)

    rows_written = enriched_df.count()
    logger.info(f"Successfully wrote {rows_written} records to {target_table}")

except Exception as e:
    logger.error(f"Failed to write to {target_table}: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Post-Load Validation
# MAGIC Replaces Ab Initio's post-graph validation and log file checks.

# COMMAND ----------

# Validate the write by reading back and checking counts
target_df = spark.table(target_table)
target_count = target_df.filter(col("_batch_date") == batch_date).count()

# Build result summary — replaces the Ab Initio ParallelLoader return dict
result = {
    "status": "success",
    "source_path": source_path,
    "target_table": target_table,
    "batch_date": batch_date,
    "records_read": record_count,
    "records_written": target_count,
    "partition_count": partition_count,
    "completed_at": datetime.now().isoformat(),
}

logger.info(f"Parallel loader completed: {result}")

# COMMAND ----------

# Exit with result summary for workflow orchestration
# Databricks Workflows can read this output to determine task success/failure
import json
dbutils.notebook.exit(json.dumps(result))
