# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Loader — Partition-Based Ingestion
# MAGIC
# MAGIC **Migrated from:** `graphs/parallel_loader.py` (Ab Initio `m_partition` / `air_run` pattern)
# MAGIC
# MAGIC **Ab Initio approach:** Python wrapper invokes `air_run` CLI per partition via
# MAGIC ThreadPoolExecutor, each partition processing a range of records.
# MAGIC
# MAGIC **Databricks approach:** Spark-native partitioned reads — data is split by a
# MAGIC partition column (or range) at the source, processed in parallel across the
# MAGIC cluster, and written to Delta Lake in a single atomic operation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters (mapped from PSET)

# COMMAND ----------

# Widget defaults mirror the PSET template values from psets/pset_templates/orders_pipeline.pset
dbutils.widgets.text("source_path", "/mnt/raw/orders", "Source Path")
dbutils.widgets.text("target_table", "lakehouse.bronze.orders", "Target Table")
dbutils.widgets.text("partition_count", "4", "Partition Count")
dbutils.widgets.text("batch_size", "50000", "Batch Size")
dbutils.widgets.text("batch_date", "", "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("max_errors", "100", "Max Tolerated Errors")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"], "Log Level")

# COMMAND ----------

import logging
from datetime import datetime, date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SOURCE_PATH = dbutils.widgets.get("source_path")
TARGET_TABLE = dbutils.widgets.get("target_table")
PARTITION_COUNT = int(dbutils.widgets.get("partition_count"))
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
MAX_ERRORS = int(dbutils.widgets.get("max_errors"))
LOG_LEVEL = dbutils.widgets.get("log_level")
BATCH_DATE = dbutils.widgets.get("batch_date") or date.today().isoformat()

logging.basicConfig(level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("parallel_loader")

spark = SparkSession.builder.getOrCreate()

logger.info(
    f"Parallel Loader started | source={SOURCE_PATH} | target={TARGET_TABLE} "
    f"| partitions={PARTITION_COUNT} | batch_date={BATCH_DATE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read Source Data
# MAGIC
# MAGIC Replaces the Ab Initio `air_run` per-partition invocation. Spark handles
# MAGIC parallelism natively via its partition-based execution model.

# COMMAND ----------

def read_source(path: str, partition_count: int) -> DataFrame:
    """
    Read source data and repartition for parallel processing.

    Ab Initio equivalent: PartitionManager.generate_ranges() + air_run per partition.
    Spark equivalent: repartition() distributes work across executors.
    """
    logger.info(f"Reading source data from {path}")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("badRecordsPath", f"/mnt/rejects/{TARGET_TABLE}/{BATCH_DATE}")
            .csv(path)
        )
    except AnalysisException as e:
        logger.error(f"Failed to read source: {e}")
        raise

    record_count = df.count()
    logger.info(f"Source records: {record_count}")

    # Repartition for parallel processing (mirrors Ab Initio partition_count)
    df = df.repartition(partition_count)
    logger.info(f"Repartitioned to {partition_count} partitions")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Validate and Transform

# COMMAND ----------

def validate_and_transform(df: DataFrame) -> DataFrame:
    """
    Apply data quality checks and transformations.

    Replaces Ab Initio graph validation components (Reformat, Filter, etc.).
    Records exceeding MAX_ERRORS cause the pipeline to abort — mirroring
    AI_ERROR_ACTION=ABORT from setenv.ksh.
    """
    # Add ingestion metadata (replaces PSET RECORD_SOURCE parameter)
    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_date", F.lit(BATCH_DATE))
        .withColumn("_source_file", F.input_file_name())
    )

    # Count nulls in key columns as "errors"
    error_df = df.filter(F.col(df.columns[0]).isNull())
    error_count = error_df.count()

    if error_count > MAX_ERRORS:
        msg = (
            f"Error threshold exceeded: {error_count} errors > {MAX_ERRORS} max. "
            f"Pipeline aborted (mirrors AI_ERROR_ACTION=ABORT)."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    if error_count > 0:
        logger.warning(f"{error_count} records with null keys — writing to reject path")
        (
            error_df.write
            .mode("append")
            .json(f"/mnt/rejects/{TARGET_TABLE}/{BATCH_DATE}")
        )
        df = df.filter(F.col(df.columns[0]).isNotNull())

    logger.info(f"Validation complete: {df.count()} valid records, {error_count} rejected")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Write to Delta Lake
# MAGIC
# MAGIC Replaces the Ab Initio staging/production load graphs. Delta Lake provides
# MAGIC ACID transactions, eliminating the need for separate staging-then-rollover
# MAGIC steps used in the KornShell pipeline.

# COMMAND ----------

def write_to_delta(df: DataFrame, target_table: str) -> dict:
    """
    Write DataFrame to a Delta Lake table.

    Ab Initio equivalent: load_staging_orders.mp + prod_rollover_orders.mp
    Delta Lake equivalent: single atomic write with schema enforcement.
    """
    logger.info(f"Writing to Delta table: {target_table}")

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    # Collect write metrics
    detail = spark.sql(f"DESCRIBE DETAIL {target_table}").collect()[0]
    metrics = {
        "target_table": target_table,
        "records_written": df.count(),
        "partition_count": df.rdd.getNumPartitions(),
        "num_files": detail["numFiles"],
        "size_bytes": detail["sizeInBytes"],
        "batch_date": BATCH_DATE,
    }
    logger.info(f"Write complete: {metrics}")
    return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Execute Pipeline

# COMMAND ----------

start_time = datetime.now()
logger.info(f"Pipeline execution started at {start_time}")

try:
    raw_df = read_source(SOURCE_PATH, PARTITION_COUNT)
    clean_df = validate_and_transform(raw_df)
    metrics = write_to_delta(clean_df, TARGET_TABLE)

    elapsed = (datetime.now() - start_time).total_seconds()
    metrics["duration_seconds"] = round(elapsed, 2)
    metrics["overall_status"] = "success"

    logger.info(f"Pipeline completed successfully in {elapsed:.1f}s")

except Exception as e:
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.error(f"Pipeline failed after {elapsed:.1f}s: {e}")
    metrics = {
        "overall_status": "failed",
        "error": str(e),
        "duration_seconds": round(elapsed, 2),
        "batch_date": BATCH_DATE,
    }
    raise

finally:
    # Emit metrics for Databricks SQL alerting (replaces AutoSys status reporting)
    spark.createDataFrame([metrics]).write.mode("append").saveAsTable(
        "lakehouse.ops.pipeline_run_log"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Summary

# COMMAND ----------

dbutils.notebook.exit(str(metrics))
