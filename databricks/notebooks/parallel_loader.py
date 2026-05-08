# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Loader — Partition-Based Parallel Ingestion
# MAGIC
# MAGIC **Migrated from:** `graphs/parallel_loader.py` (Ab Initio parallel graph execution)
# MAGIC
# MAGIC **Ab Initio equivalent:** `m_partition` component + `air sandbox run` with `-partition` flag
# MAGIC
# MAGIC **What changed:**
# MAGIC - Ab Initio PartitionManager (manual range splitting) → Spark native partitioning via `repartition()`
# MAGIC - Ab Initio `air_run` subprocess calls → SparkSession read/write with partition pruning
# MAGIC - Thread-based parallelism → Spark's distributed execution engine
# MAGIC - PSET file parameters → Databricks widgets and job parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — Widget Parameters
# MAGIC Maps Ab Initio PSET parameters to Databricks notebook widgets.
# MAGIC
# MAGIC | Ab Initio PSET Key  | Databricks Widget       | Description                          |
# MAGIC |---------------------|-------------------------|--------------------------------------|
# MAGIC | SOURCE_PATH         | source_path             | Source data location (cloud storage)  |
# MAGIC | TARGET_TABLE        | target_table            | Delta Lake target table name          |
# MAGIC | PARTITION_COUNT     | partition_count          | Number of Spark output partitions     |
# MAGIC | BATCH_SIZE          | batch_size              | Rows per micro-batch (for streaming)  |
# MAGIC | MAX_ERRORS          | max_errors              | Error threshold before abort          |
# MAGIC | BATCH_DATE          | batch_date              | Processing date (YYYY-MM-DD)          |

# COMMAND ----------

# Widget definitions — equivalent to Ab Initio PSET parameter loading
dbutils.widgets.text("source_path", "", "Source Path")
dbutils.widgets.text("target_table", "", "Target Delta Table")
dbutils.widgets.text("partition_count", "4", "Partition Count")
dbutils.widgets.text("batch_size", "50000", "Batch Size")
dbutils.widgets.text("max_errors", "100", "Max Errors")
dbutils.widgets.text("batch_date", "", "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("log_level", "INFO", "Log Level")

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Configure logging — replaces Ab Initio graph-level log files
log_level = dbutils.widgets.get("log_level")
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logger = logging.getLogger("parallel_loader")

# Resolve widget parameters — equivalent to PSET resolution in Ab Initio
source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
partition_count = int(dbutils.widgets.get("partition_count"))
batch_size = int(dbutils.widgets.get("batch_size"))
max_errors = int(dbutils.widgets.get("max_errors"))
batch_date = dbutils.widgets.get("batch_date") or datetime.now().strftime("%Y-%m-%d")

logger.info(
    f"Parallel Loader started | source={source_path} | target={target_table} | "
    f"partitions={partition_count} | batch_date={batch_date}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Session — Replaces Ab Initio Co>Operating System Runtime
# MAGIC The Ab Initio Co>Op runtime manages parallel execution, memory, and I/O.
# MAGIC In Databricks, SparkSession handles all of this natively.

# COMMAND ----------

# SparkSession is pre-configured in Databricks — equivalent to Ab Initio Co>Op runtime
spark = SparkSession.builder.getOrCreate()

# Set Spark configurations for parallel load performance
# Equivalent to Ab Initio AI_DEFAULT_PARTITIONS and AI_MAX_PARTITIONS
spark.conf.set("spark.sql.shuffle.partitions", str(partition_count))
spark.conf.set("spark.sql.files.maxRecordsPerFile", str(batch_size))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Ingestion — Replaces Ab Initio Extract Graph
# MAGIC Ab Initio reads via graph components (Read File, Read DB) with partition ranges.
# MAGIC Spark reads the entire dataset and distributes across executors automatically.

# COMMAND ----------

def ingest_source(path: str, num_partitions: int, date_filter: str) -> DataFrame:
    """
    Read source data and repartition for parallel processing.

    Replaces the Ab Initio pattern of:
      1. PartitionManager.generate_ranges() — manual range splitting
      2. air_run -partition=N -start_record=X -end_record=Y — per-partition execution

    Spark handles partitioning natively via repartition() and predicate pushdown.
    """
    logger.info(f"Ingesting source data from: {path}")

    try:
        # Auto-detect format from path extension — Ab Initio used DML-defined layouts
        if path.endswith(".parquet") or "parquet" in path.lower():
            df = spark.read.parquet(path)
        elif path.endswith(".csv") or path.endswith(".dat"):
            # CSV/DAT ingestion — mirrors Ab Initio delimited file reading
            df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("nullValue", "")
                .load(path)
            )
        else:
            # Default: Delta Lake (preferred format in Databricks)
            df = spark.read.format("delta").load(path)

        # Apply date filter if the source has a date column — equivalent to PSET BATCH_DATE
        if date_filter and "batch_date" in [c.lower() for c in df.columns]:
            df = df.filter(F.col("batch_date") == date_filter)
            logger.info(f"Applied batch_date filter: {date_filter}")

        # Repartition for parallel write — replaces Ab Initio PartitionManager
        df = df.repartition(num_partitions)

        record_count = df.count()
        logger.info(f"Ingested {record_count} records across {num_partitions} partitions")
        return df

    except AnalysisException as e:
        logger.error(f"Source ingestion failed: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks — Replaces Ab Initio Reject Processing
# MAGIC Ab Initio graphs route bad records to reject files via reject ports.
# MAGIC In Spark, we separate good/bad records and track error counts against the threshold.

# COMMAND ----------

def validate_and_split(df: DataFrame, error_threshold: int) -> tuple:
    """
    Validate records and split into good/rejected DataFrames.

    Replaces Ab Initio reject port pattern where bad records are routed
    to a separate output file defined in the graph component.
    """
    # Tag records with basic null-check validation
    validated = df.withColumn(
        "_is_valid",
        # At minimum, no row should be entirely null
        F.least(*[F.col(c).isNotNull() for c in df.columns])
    )

    good_records = validated.filter(F.col("_is_valid")).drop("_is_valid")
    rejected = validated.filter(~F.col("_is_valid")).drop("_is_valid")

    reject_count = rejected.count()
    logger.info(f"Validation complete | good={good_records.count()} | rejected={reject_count}")

    if reject_count > error_threshold:
        error_msg = (
            f"Error threshold exceeded: {reject_count} rejected records > "
            f"max_errors={error_threshold}. Aborting load."
        )
        logger.error(error_msg)
        # Equivalent to Ab Initio AI_ERROR_ACTION=ABORT
        raise RuntimeError(error_msg)

    return good_records, rejected

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Lake — Replaces Ab Initio Load Graph
# MAGIC Ab Initio writes via graph output components (Write DB, Write File) per partition.
# MAGIC Spark writes all partitions concurrently to Delta Lake in a single atomic operation.

# COMMAND ----------

def write_to_delta(df: DataFrame, table_name: str, date_val: str) -> dict:
    """
    Write validated records to a Delta Lake table with batch metadata.

    Replaces Ab Initio production load graph (prod_rollover_orders.mp):
      - Atomic partition-level write
      - Batch date tracking for auditability
      - Automatic schema evolution
    """
    logger.info(f"Writing to Delta table: {table_name}")

    # Add audit metadata — equivalent to Ab Initio RECORD_SOURCE parameter
    df_with_meta = (
        df.withColumn("_batch_date", F.lit(date_val))
          .withColumn("_loaded_at", F.current_timestamp())
          .withColumn("_source_system", F.lit("abinitio_migration"))
    )

    # Write to Delta — atomic, ACID-compliant (replaces Ab Initio checkpoint/restart)
    (
        df_with_meta.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")  # Handle schema evolution
        .saveAsTable(table_name)
    )

    row_count = df_with_meta.count()
    logger.info(f"Successfully wrote {row_count} records to {table_name}")

    return {
        "table": table_name,
        "records_written": row_count,
        "batch_date": date_val,
        "partitions": partition_count,
        "status": "success",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration — Main Execution Flow
# MAGIC Replaces the Ab Initio ParallelLoader.run_graph() method which coordinated
# MAGIC ThreadPoolExecutor-based partition execution via subprocess calls.

# COMMAND ----------

# Main execution — equivalent to ParallelLoader.run_graph()
try:
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"PARALLEL LOADER STARTED | {batch_date}")
    logger.info("=" * 60)

    # Step 1: Ingest source data (replaces Ab Initio extract phase)
    source_df = ingest_source(source_path, partition_count, batch_date)

    # Step 2: Validate and split (replaces Ab Initio reject processing)
    good_df, reject_df = validate_and_split(source_df, max_errors)

    # Step 3: Persist rejects for investigation (replaces Ab Initio reject file output)
    if reject_df.count() > 0:
        reject_path = f"{source_path.rstrip('/')}_rejects/{batch_date}"
        reject_df.write.format("delta").mode("append").save(reject_path)
        logger.warning(f"Rejected records written to: {reject_path}")

    # Step 4: Write to Delta Lake (replaces Ab Initio production load graph)
    result = write_to_delta(good_df, target_table, batch_date)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"PARALLEL LOADER COMPLETED | duration={elapsed:.2f}s | {result}")

    # Return result for Databricks Workflow task value — used by downstream tasks
    dbutils.notebook.exit(
        f"SUCCESS|records={result['records_written']}|duration={elapsed:.2f}s"
    )

except Exception as e:
    logger.error(f"PARALLEL LOADER FAILED: {e}")
    dbutils.notebook.exit(f"FAILED|error={str(e)[:200]}")
    raise
