# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Loader — Partition-Based Parallel Ingestion
# MAGIC
# MAGIC **Migrated from:** `graphs/parallel_loader.py` (Ab Initio parallel graph execution)
# MAGIC
# MAGIC ## Ab Initio → Databricks Migration Summary
# MAGIC
# MAGIC | Ab Initio Concept | Databricks Equivalent | Notes |
# MAGIC |---|---|---|
# MAGIC | `air_run -partition=N` | Spark's native partitioning (`repartition()`) | Spark distributes work across executors automatically |
# MAGIC | `PartitionManager.generate_ranges()` | `spark.read` with partition pushdown | Spark calculates partition boundaries from data source |
# MAGIC | `ThreadPoolExecutor` parallel execution | Spark's parallel task scheduling | Spark DAG scheduler handles parallelism natively |
# MAGIC | PSET config (`-pset path`) | Databricks widgets / job parameters | Environment-aware configuration via `dbutils.widgets` |
# MAGIC | `air_run` subprocess calls | `spark.read().write()` pipeline | No external process calls needed — Spark IS the engine |
# MAGIC | Callback on partition completion | Spark listener / `StreamingQueryListener` | Event-driven monitoring via SparkListener API |
# MAGIC
# MAGIC ## Usage
# MAGIC Run this notebook as a Databricks Job or interactively. Configure via widgets:
# MAGIC - `source_path`: Path to source data (cloud storage, DBFS, or volume)
# MAGIC - `target_table`: Fully qualified Delta table name (catalog.schema.table)
# MAGIC - `partition_count`: Number of Spark partitions for parallel processing
# MAGIC - `file_format`: Source file format (csv, parquet, json, etc.)
# MAGIC - `delimiter`: Field delimiter for CSV files

# COMMAND ----------

# Widget parameters — equivalent to Ab Initio PSET configuration.
# In Ab Initio, PSET files provided environment-aware parameters per pipeline.
# In Databricks, widgets serve the same purpose and can be set by job parameters.
dbutils.widgets.text("source_path", "", "Source Data Path")
dbutils.widgets.text("target_table", "", "Target Delta Table")
dbutils.widgets.text("partition_count", "8", "Partition Count")
dbutils.widgets.dropdown("file_format", "csv", ["csv", "parquet", "json", "delta", "orc"], "Source Format")
dbutils.widgets.text("delimiter", ",", "CSV Delimiter")

# COMMAND ----------

import logging
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException
from typing import Optional, Dict, Any

# Configure logging — replaces Ab Initio's job log output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("parallel_loader")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration & Spark Session
# MAGIC
# MAGIC In Ab Initio, the Co>Operating System managed the runtime engine.
# MAGIC In Databricks, SparkSession is the entry point — already available as `spark`.

# COMMAND ----------

class ParallelLoaderConfig:
    """
    Encapsulates loader configuration from widget parameters.

    Migrated from Ab Initio PSET parameters — the PSET file defined source paths,
    batch sizes, and partition counts per environment (dev/uat/prod). Databricks
    widgets + job parameters provide the same environment-aware configuration.
    """

    def __init__(self):
        # Read widget values — equivalent to PSET parameter resolution
        self.source_path: str = dbutils.widgets.get("source_path")
        self.target_table: str = dbutils.widgets.get("target_table")
        self.partition_count: int = int(dbutils.widgets.get("partition_count"))
        self.file_format: str = dbutils.widgets.get("file_format")
        self.delimiter: str = dbutils.widgets.get("delimiter")

    def validate(self) -> None:
        """Validate required configuration — fail fast like Ab Initio graph pre-checks."""
        if not self.source_path:
            raise ValueError("source_path is required — set via widget or job parameter")
        if not self.target_table:
            raise ValueError("target_table is required — set via widget or job parameter")
        if self.partition_count < 1:
            raise ValueError(f"partition_count must be >= 1, got {self.partition_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallel Loader Implementation
# MAGIC
# MAGIC ### Ab Initio vs Spark Parallelism
# MAGIC
# MAGIC In Ab Initio, `PartitionManager.generate_ranges()` manually split records into
# MAGIC ranges and `ParallelLoader` dispatched each range to a separate `air_run` process
# MAGIC via `ThreadPoolExecutor`. Each partition ran as an independent OS subprocess.
# MAGIC
# MAGIC In Spark, parallelism is **native to the engine**:
# MAGIC - `spark.read` automatically partitions data based on the source (file splits, JDBC partitions)
# MAGIC - `repartition(N)` explicitly controls the degree of parallelism
# MAGIC - Spark's DAG scheduler distributes partition tasks across cluster executors
# MAGIC - No manual range calculation or thread management is needed

# COMMAND ----------

class ParallelLoader:
    """
    Partition-based parallel data loader for Delta Lake.

    Migrated from Ab Initio's ParallelLoader which orchestrated parallel graph
    execution via air_run subprocesses. In Databricks, Spark handles partition-level
    parallelism natively — this class provides the orchestration layer for reading
    source data, repartitioning, applying optional transforms, and writing to Delta.
    """

    def __init__(self, spark: SparkSession, config: ParallelLoaderConfig):
        self.spark = spark
        self.config = config

    def load(
        self,
        schema: Optional[StructType] = None,
        transform_fn: Optional[callable] = None,
        write_mode: str = "append",
        partition_columns: Optional[list] = None,
    ) -> Dict[str, Any]:
        """
        Execute partition-based parallel ingestion from source to Delta Lake.

        This replaces the full Ab Initio parallel_loader flow:
          1. PartitionManager.generate_ranges() → spark.read with auto-partitioning
          2. ParallelLoader._run_partition() → Spark task execution per partition
          3. ThreadPoolExecutor orchestration → Spark DAG scheduler
          4. Result collection + status → returned metrics dict

        Args:
            schema: Optional PySpark StructType to enforce on read (from databricks.schemas).
            transform_fn: Optional function(DataFrame) -> DataFrame for in-flight transforms.
            write_mode: Delta write mode — "append", "overwrite", or "merge".
            partition_columns: Optional list of columns to partition the Delta table by.

        Returns:
            Dict with load status, record counts, partition info, and timing.
        """
        self.config.validate()
        start_time = time.time()
        logger.info(
            f"Starting parallel load: {self.config.source_path} → {self.config.target_table} "
            f"({self.config.partition_count} partitions)"
        )

        try:
            # Step 1: Read source data
            # In Ab Initio, the graph component read from source via a configured input port.
            # In Spark, we read via the DataFrameReader with format auto-detection.
            source_df = self._read_source(schema)
            source_count = source_df.count()
            logger.info(f"Source data read: {source_count} records")

            # Step 2: Repartition for parallel processing
            # Replaces PartitionManager.generate_ranges() — Spark distributes records
            # across partitions automatically (hash or round-robin based).
            source_df = source_df.repartition(self.config.partition_count)
            logger.info(f"Data repartitioned into {self.config.partition_count} partitions")

            # Step 3: Apply optional transforms (equivalent to Ab Initio transform components)
            if transform_fn is not None:
                source_df = transform_fn(source_df)
                logger.info("Transform function applied")

            # Step 4: Write to Delta Lake
            # In Ab Initio, each partition wrote to a separate output file via air_run.
            # In Delta Lake, the writer handles concurrent partition writes atomically.
            self._write_delta(source_df, write_mode, partition_columns)
            target_count = self.spark.table(self.config.target_table).count()

            elapsed = round(time.time() - start_time, 2)
            result = {
                "status": "success",
                "source_path": self.config.source_path,
                "target_table": self.config.target_table,
                "source_records": source_count,
                "target_records": target_count,
                "partition_count": self.config.partition_count,
                "write_mode": write_mode,
                "duration_seconds": elapsed,
            }
            logger.info(f"Parallel load completed: {result}")
            return result

        except Exception as e:
            elapsed = round(time.time() - start_time, 2)
            error_result = {
                "status": "failed",
                "source_path": self.config.source_path,
                "target_table": self.config.target_table,
                "error": str(e),
                "error_type": type(e).__name__,
                "duration_seconds": elapsed,
            }
            logger.error(f"Parallel load failed: {error_result}")
            raise

    def _read_source(self, schema: Optional[StructType] = None) -> DataFrame:
        """
        Read source data with format-specific options.

        In Ab Initio, the input component parsed data according to the DML record layout.
        In Spark, the DataFrameReader handles parsing; the StructType schema (from
        databricks.schemas) enforces the same field definitions.
        """
        reader = self.spark.read.format(self.config.file_format)

        # Apply schema enforcement if provided (equivalent to DML-driven parsing)
        if schema is not None:
            reader = reader.schema(schema)

        # CSV-specific options — Ab Initio DML delimiter metadata maps here
        if self.config.file_format == "csv":
            reader = reader.option("header", "true") \
                           .option("sep", self.config.delimiter) \
                           .option("inferSchema", "false" if schema else "true")

        return reader.load(self.config.source_path)

    def _write_delta(
        self,
        df: DataFrame,
        write_mode: str,
        partition_columns: Optional[list],
    ) -> None:
        """
        Write DataFrame to Delta Lake table.

        In Ab Initio, each partition wrote output independently via the output port.
        In Delta Lake, writes are ACID-transactional — all partitions commit atomically
        via the Delta transaction log, preventing partial writes.
        """
        writer = df.write.format("delta").mode(write_mode)

        # Table partitioning — analogous to Ab Initio's output file partitioning
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        # Enable optimized writes for better file sizing
        writer = writer.option("optimizeWrite", "true")

        writer.saveAsTable(self.config.target_table)
        logger.info(f"Data written to {self.config.target_table} (mode={write_mode})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution
# MAGIC
# MAGIC The cell below runs the parallel loader. In a Databricks Job, this executes
# MAGIC automatically with job parameters populating the widgets.

# COMMAND ----------

# Main execution — runs when notebook is executed as a job or interactively.
# Wrapped in try/except for graceful error handling (replaces Ab Initio's
# error port routing pattern).
try:
    config = ParallelLoaderConfig()
    loader = ParallelLoader(spark, config)

    # Execute the parallel load
    # To use a schema from databricks.schemas, import and pass it:
    #   from databricks.schemas.customer import CUSTOMER_SCHEMA
    #   result = loader.load(schema=CUSTOMER_SCHEMA)
    result = loader.load()

    # Return result as notebook output (for Databricks Workflows chaining)
    dbutils.notebook.exit(str(result))

except ValueError as e:
    # Configuration validation errors — log and exit with error status
    logger.error(f"Configuration error: {e}")
    dbutils.notebook.exit(f'{{"status": "failed", "error": "{e}"}}')

except AnalysisException as e:
    # Spark analysis errors (bad table name, schema mismatch, etc.)
    logger.error(f"Spark analysis error: {e}")
    dbutils.notebook.exit(f'{{"status": "failed", "error": "{e}"}}')
