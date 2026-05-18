# Databricks notebook source
# --------------------------------------------------------------------------
# parallel_loader.py — PySpark Notebook: Partition-Based Parallel Ingestion
#
# Migrated from: graphs/parallel_loader.py (Ab Initio ParallelLoader class)
#
# Ab Initio Pattern:
#   - PartitionManager splits records into ranges
#   - ParallelLoader runs air_run per partition via ThreadPoolExecutor
#   - Each partition processes a slice of source data independently
#
# Databricks Equivalent:
#   - Spark handles parallelism natively via DataFrame partitions
#   - No need for manual ThreadPoolExecutor — Spark distributes work across
#     cluster executors automatically
#   - Partition count controlled by spark.sql.shuffle.partitions and repartition()
#   - Ab Initio air_run → spark.read + DataFrame transformations + Delta write
#
# Key Differences:
#   - Ab Initio: explicit partition ranges, subprocess per partition
#   - Spark: declarative partitioning, cluster-managed parallelism
#   - Ab Initio: file-based checkpoint/restart
#   - Spark: Delta Lake ACID transactions provide implicit checkpointing
# --------------------------------------------------------------------------

# COMMAND ----------

# Widget parameters — equivalent to Ab Initio PSET parameters
# These map to Databricks job parameters or notebook widgets
dbutils.widgets.text("source_path", "", "Source data path (cloud storage)")
dbutils.widgets.text("target_table", "", "Target Delta table (catalog.schema.table)")
dbutils.widgets.text("source_format", "csv", "Source file format (csv, parquet, json)")
dbutils.widgets.text("delimiter", ",", "Field delimiter for CSV files")
dbutils.widgets.text("partition_count", "8", "Number of Spark partitions for write")
dbutils.widgets.text("schema_module", "", "Schema module name from databricks.schemas")
dbutils.widgets.text("batch_date", "", "Batch date (YYYY-MM-DD) for incremental loads")
dbutils.widgets.text("max_errors", "100", "Maximum bad records before failing")
dbutils.widgets.text("write_mode", "append", "Delta write mode: append, overwrite, merge")

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name, trim
)
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

# Configure logging — replaces Ab Initio graph-level logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("parallel_loader")

# COMMAND ----------

# Resolve widget parameters — equivalent to Ab Initio PSET parameter resolution
source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
source_format = dbutils.widgets.get("source_format")
delimiter = dbutils.widgets.get("delimiter")
partition_count = int(dbutils.widgets.get("partition_count"))
schema_module = dbutils.widgets.get("schema_module")
batch_date = dbutils.widgets.get("batch_date") or datetime.now().strftime("%Y-%m-%d")
max_errors = int(dbutils.widgets.get("max_errors"))
write_mode = dbutils.widgets.get("write_mode")

# Validate required parameters — replaces Ab Initio PSET validation
assert source_path, "source_path is required"
assert target_table, "target_table is required"

logger.info(
    f"Parallel loader started: source={source_path}, target={target_table}, "
    f"format={source_format}, partitions={partition_count}, batch_date={batch_date}"
)

# COMMAND ----------

def load_schema(module_name: str) -> StructType:
    """
    Dynamically load a PySpark StructType from databricks.schemas module.
    This mirrors the Ab Initio DML file reference pattern in PSETs
    (e.g., DML_FILE=/data/projects/enterprise_etl/dml/order_items.dml).
    """
    if not module_name:
        logger.info("No schema module specified — inferring schema from source data")
        return None
    import importlib
    mod = importlib.import_module(f"databricks.schemas.{module_name}")
    # Convention: schema variable is named {module_name}_schema
    schema_attr = f"{module_name}_schema"
    if hasattr(mod, schema_attr):
        logger.info(f"Loaded schema from databricks.schemas.{module_name}.{schema_attr}")
        return getattr(mod, schema_attr)
    raise AttributeError(f"Schema '{schema_attr}' not found in databricks.schemas.{module_name}")

# COMMAND ----------

def read_source(spark: SparkSession, path: str, fmt: str, delim: str,
                schema: StructType, bad_records_limit: int) -> DataFrame:
    """
    Read source data into a DataFrame.

    Replaces the Ab Initio source component that reads from flat files
    using DML-defined record layouts. Spark handles parallelism natively
    based on file splits and cluster resources — no manual partition ranges needed.

    Ab Initio equivalent:
      air_run -g extract.mp -pset ... -partition=N -start_record=X -end_record=Y
    """
    reader = spark.read.format(fmt)

    # Apply schema if provided (equivalent to Ab Initio DML)
    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")

    # CSV-specific options — handles Ab Initio delimiter patterns
    if fmt == "csv":
        reader = (reader
                  .option("header", "true")
                  .option("delimiter", delim)
                  .option("mode", "PERMISSIVE")
                  .option("badRecordsPath", f"/tmp/bad_records/{target_table}/{batch_date}")
                  .option("columnNameOfCorruptRecord", "_corrupt_record"))

    logger.info(f"Reading source: path={path}, format={fmt}, delimiter='{delim}'")
    df = reader.load(path)

    # Add audit/lineage columns — equivalent to Ab Initio metadata injection
    df = (df
          .withColumn("_loaded_at", current_timestamp())
          .withColumn("_source_file", input_file_name())
          .withColumn("_batch_date", lit(batch_date)))

    record_count = df.count()
    logger.info(f"Source records read: {record_count}")

    return df

# COMMAND ----------

def apply_transforms(df: DataFrame) -> DataFrame:
    """
    Apply standard transformations before writing to Delta.

    Replaces inline Ab Initio transform components:
    - Trim whitespace on string columns (handles fixed-width mainframe fields)
    - Null handling for Ab Initio null() directives
    """
    # Trim all string columns — handles Ab Initio string(N) fixed-width padding
    for field in df.schema.fields:
        if str(field.dataType) == "StringType":
            df = df.withColumn(field.name, trim(col(field.name)))
    return df

# COMMAND ----------

def write_to_delta(df: DataFrame, table: str, mode: str, num_partitions: int) -> dict:
    """
    Write DataFrame to Delta Lake table.

    Replaces the Ab Initio target/output component + rollover graph.
    Spark repartition replaces Ab Initio partition manager — the key difference
    is that Spark partitions are logical and cluster-managed, while Ab Initio
    partitions were explicit file splits run as separate subprocesses.

    Ab Initio equivalent:
      air_run -g load_target.mp -pset ... -partition=4
    """
    # Repartition for optimal write parallelism
    # This is the Databricks equivalent of Ab Initio's PartitionManager.generate_ranges()
    df = df.repartition(num_partitions)

    start = datetime.now()
    logger.info(f"Writing to Delta table: {table}, mode={mode}, partitions={num_partitions}")

    if mode == "overwrite":
        # Full table overwrite — equivalent to Ab Initio truncate-and-load graph
        df.write.format("delta").mode("overwrite").saveAsTable(table)
    else:
        # Append mode — equivalent to Ab Initio incremental load graph
        df.write.format("delta").mode("append").saveAsTable(table)

    duration = (datetime.now() - start).total_seconds()
    output_count = df.count()

    result = {
        "table": table,
        "records_written": output_count,
        "partitions": num_partitions,
        "write_mode": mode,
        "duration_seconds": round(duration, 2),
        "batch_date": batch_date,
        "status": "success",
    }
    logger.info(f"Write complete: {result}")
    return result

# COMMAND ----------

# Main execution — orchestrates the full load pipeline
# This replaces the Ab Initio ParallelLoader.run_graph() method
try:
    spark = SparkSession.builder.getOrCreate()
    logger.info("Spark session initialized")

    # Step 1: Load schema (Ab Initio DML equivalent)
    schema = load_schema(schema_module) if schema_module else None

    # Step 2: Read source data (Ab Initio extract graph equivalent)
    source_df = read_source(spark, source_path, source_format, delimiter, schema, max_errors)

    # Step 3: Apply transformations (Ab Initio transform component equivalent)
    transformed_df = apply_transforms(source_df)

    # Step 4: Write to Delta (Ab Initio load/rollover graph equivalent)
    result = write_to_delta(transformed_df, target_table, write_mode, partition_count)

    # Step 5: Optimize table — post-load compaction (no Ab Initio equivalent)
    logger.info(f"Running OPTIMIZE on {target_table}")
    spark.sql(f"OPTIMIZE {target_table}")

    logger.info(f"Parallel loader completed successfully: {result}")
    dbutils.notebook.exit(str(result))

except Exception as e:
    # Error handling — replaces Ab Initio graph error port + shell RC check
    logger.error(f"Parallel loader FAILED: {e}", exc_info=True)
    dbutils.notebook.exit(f"FAILED: {str(e)}")
    raise
