# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Loader — PySpark Notebook
# MAGIC
# MAGIC **Migrated from:** `graphs/parallel_loader.py` (Ab Initio parallel graph execution orchestrator)
# MAGIC
# MAGIC ## Ab Initio → Databricks Mapping
# MAGIC | Ab Initio Concept | Databricks Equivalent |
# MAGIC |---|---|
# MAGIC | `air_run` with `-partition=N` | Spark's built-in partition parallelism via `repartition()` |
# MAGIC | `PartitionManager.generate_ranges` | DataFrame partitioning by column or hash |
# MAGIC | `ThreadPoolExecutor` for parallel graph runs | Spark's native distributed execution across executors |
# MAGIC | PSET parameters (`PARTITION_COUNT`, `BATCH_SIZE`) | Databricks job parameters / notebook widgets |
# MAGIC | `m_partition` component | `repartition()` / `coalesce()` |
# MAGIC
# MAGIC ## Key Changes
# MAGIC - Ab Initio required explicit partition range management and subprocess-based execution.
# MAGIC   Spark handles data partitioning natively across the cluster.
# MAGIC - Error handling per partition is replaced by Spark's built-in task retry and stage-level recovery.
# MAGIC - Logging integrated with Spark's log4j and Databricks driver logs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — Notebook Widgets
# MAGIC Maps Ab Initio PSET parameters to Databricks notebook widgets.

# COMMAND ----------

# Widget definitions (equivalent to Ab Initio PSET parameters)
# SOURCE_PATH: Ab Initio PSET SOURCE_PATH -> widget for source data location
dbutils.widgets.text("source_path", "", "Source Path (cloud storage)")
# TARGET_TABLE: Ab Initio PSET TARGET_TABLE -> Delta table name
dbutils.widgets.text("target_table", "", "Target Delta Table")
# PARTITION_COUNT: Ab Initio PSET PARTITION_COUNT -> Spark repartition count
dbutils.widgets.text("partition_count", "8", "Partition Count")
# BATCH_SIZE: Ab Initio PSET BATCH_SIZE -> rows per micro-batch (Auto Loader)
dbutils.widgets.text("batch_size", "50000", "Batch Size")
# FILE_FORMAT: source file format (csv, parquet, json)
dbutils.widgets.dropdown("file_format", "csv", ["csv", "parquet", "json", "delta"], "Source File Format")
# LOAD_MODE: overwrite or append
dbutils.widgets.dropdown("load_mode", "append", ["append", "overwrite", "merge"], "Load Mode")

# COMMAND ----------

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name, sha2, concat_ws,
)
from pyspark.sql.utils import AnalysisException
from datetime import datetime

# Configure logging (replaces Ab Initio's per-partition log files)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("parallel_loader")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Session and Parameter Resolution
# MAGIC In Ab Initio, the Co>Operating System managed the runtime environment.
# MAGIC In Databricks, SparkSession is the entry point to all Spark functionality.

# COMMAND ----------

# Resolve widget parameters (equivalent to PSET parameter resolution in PSETManager)
source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
partition_count = int(dbutils.widgets.get("partition_count"))
batch_size = int(dbutils.widgets.get("batch_size"))
file_format = dbutils.widgets.get("file_format")
load_mode = dbutils.widgets.get("load_mode")

# Validate required parameters (equivalent to PSET validation in Ab Initio)
if not source_path or not target_table:
    raise ValueError(
        "source_path and target_table are required. "
        "Set them via job parameters or notebook widgets."
    )

logger.info(
    f"Parallel Loader starting: source={source_path}, target={target_table}, "
    f"partitions={partition_count}, batch_size={batch_size}, format={file_format}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingestion
# MAGIC Replaces Ab Initio's `air_run` subprocess execution with native Spark reads.
# MAGIC Partition-based parallelism is handled by Spark's distributed read.

# COMMAND ----------

def read_source_data(
    spark: SparkSession,
    path: str,
    file_format: str,
    partition_count: int,
) -> DataFrame:
    """
    Read source data with partition-based parallelism.

    Ab Initio equivalent: PartitionManager.generate_ranges() + air_run per partition.
    Spark equivalent: Distributed read with repartition().

    Args:
        spark: Active SparkSession
        path: Source data path (cloud storage URI)
        file_format: Source file format (csv, parquet, json, delta)
        partition_count: Number of partitions for parallel processing

    Returns:
        DataFrame repartitioned for parallel processing
    """
    logger.info(f"Reading source data from {path} (format={file_format})")

    # Read source data based on format
    reader = spark.read.format(file_format)

    # CSV-specific options (Ab Initio DML delimiter handling)
    if file_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    try:
        df = reader.load(path)
    except AnalysisException as e:
        logger.error(f"Failed to read source data from {path}: {e}")
        raise

    # Repartition for parallel processing
    # (replaces Ab Initio's explicit partition range assignment via PartitionManager)
    original_partitions = df.rdd.getNumPartitions()
    df = df.repartition(partition_count)

    # Add ingestion metadata columns (for lineage tracking)
    df = (
        df.withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", input_file_name())
          .withColumn("_batch_id", lit(datetime.utcnow().strftime("%Y%m%d_%H%M%S")))
    )

    record_count = df.count()
    logger.info(
        f"Source data loaded: {record_count} records, "
        f"repartitioned from {original_partitions} -> {partition_count} partitions"
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Lake
# MAGIC Replaces Ab Initio's target database write components.
# MAGIC Uses Delta Lake for ACID transactions and schema evolution.

# COMMAND ----------

def write_to_delta(
    df: DataFrame,
    target_table: str,
    load_mode: str,
    batch_size: int,
) -> dict:
    """
    Write DataFrame to Delta Lake table.

    Ab Initio equivalent: Output component writing to Teradata/Oracle target.
    Spark equivalent: Delta Lake write with ACID guarantees.

    Args:
        df: Source DataFrame to write
        target_table: Fully-qualified Delta table name (catalog.schema.table)
        load_mode: Write mode - append, overwrite, or merge
        batch_size: Rows per write batch (controls memory pressure)

    Returns:
        Summary dict with write statistics
    """
    start_time = datetime.utcnow()
    record_count = df.count()
    logger.info(f"Writing {record_count} records to {target_table} (mode={load_mode})")

    try:
        if load_mode in ("append", "overwrite"):
            # Standard append/overwrite write
            (
                df.write
                  .format("delta")
                  .mode(load_mode)
                  .option("maxRecordsPerFile", batch_size)
                  .saveAsTable(target_table)
            )
        elif load_mode == "merge":
            # Merge/upsert handled by cdc_processor notebook
            logger.warning(
                "Merge mode requested — delegate to cdc_processor notebook "
                "for proper MERGE INTO semantics."
            )
            (
                df.write
                  .format("delta")
                  .mode("append")
                  .option("maxRecordsPerFile", batch_size)
                  .saveAsTable(target_table)
            )

        duration = (datetime.utcnow() - start_time).total_seconds()
        result = {
            "target_table": target_table,
            "records_written": record_count,
            "load_mode": load_mode,
            "partition_count": df.rdd.getNumPartitions(),
            "duration_seconds": round(duration, 2),
            "status": "success",
        }
        logger.info(f"Write completed: {result}")
        return result

    except Exception as e:
        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.error(f"Write failed after {duration:.1f}s: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Parallel Load Pipeline
# MAGIC Orchestrates the full ingestion pipeline.
# MAGIC Replaces `ParallelLoader.run_graph()` from the Ab Initio framework.

# COMMAND ----------

# Main execution block
spark = SparkSession.builder.getOrCreate()

logger.info("=" * 60)
logger.info("PARALLEL LOADER — Databricks Migration")
logger.info(f"  Source:      {source_path}")
logger.info(f"  Target:      {target_table}")
logger.info(f"  Partitions:  {partition_count}")
logger.info(f"  Load Mode:   {load_mode}")
logger.info("=" * 60)

# Step 1: Read source data with partition parallelism
source_df = read_source_data(spark, source_path, file_format, partition_count)

# Step 2: Write to Delta Lake target
result = write_to_delta(source_df, target_table, load_mode, batch_size)

# Step 3: Report results (replaces Ab Initio's run_graph return dict)
logger.info(f"Pipeline complete: {result}")

# Return result for Databricks workflow task value
dbutils.notebook.exit(str(result))
