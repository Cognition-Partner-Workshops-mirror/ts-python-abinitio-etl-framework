# Databricks notebook source
# --------------------------------------------------------------------------
# cdc_processor.py — PySpark Notebook: Delta Lake MERGE-based CDC Processing
#
# Migrated from: graphs/cdc_processor.py (Ab Initio CDCProcessor class)
#
# Ab Initio Pattern:
#   - CDCProcessor compares source vs target DataFrames using row-level MD5 hashing
#   - Produces three output sets: inserts, updates, deletes
#   - Equivalent to Ab Initio "Compare Records by Key" component
#   - Uses pandas DataFrames with manual set operations
#
# Databricks Equivalent:
#   - Delta Lake MERGE (UPSERT) handles insert/update/delete in a single atomic op
#   - Delta Change Data Feed (CDF) tracks row-level changes automatically
#   - No need for manual hash computation — Delta handles change detection
#   - MERGE is ACID-compliant and handles concurrent writes safely
#
# Key Differences:
#   - Ab Initio: explicit MD5 hashing, separate insert/update/delete DataFrames
#   - Delta MERGE: declarative WHEN MATCHED/NOT MATCHED clauses
#   - Ab Initio: snapshot-based comparison (current vs previous file)
#   - Delta: built-in versioning via transaction log, CDF for downstream consumers
# --------------------------------------------------------------------------

# COMMAND ----------

# Widget parameters — mapped from Ab Initio PSET (pset_templates/customer_cdc.pset)
# PSET mapping:
#   SOURCE_PATH → source_path
#   TARGET_TABLE → target_table
#   KEY_COLUMNS → key_columns
#   HASH_COLUMNS → compare_columns (used for change detection in Ab Initio, optional here)
#   PARTITION_COUNT → partition_count
#   BATCH_SIZE → (handled by Spark automatically)
#   MAX_ERRORS → max_errors
#   AUDIT_TABLE → audit_table
dbutils.widgets.text("source_path", "", "Path to incoming CDC source data")
dbutils.widgets.text("source_format", "csv", "Source file format (csv, parquet, delta)")
dbutils.widgets.text("target_table", "", "Target Delta table for MERGE")
dbutils.widgets.text("key_columns", "", "Comma-separated primary key columns for MERGE")
dbutils.widgets.text("compare_columns", "", "Comma-separated columns for change detection (optional)")
dbutils.widgets.text("partition_count", "8", "Number of Spark partitions")
dbutils.widgets.text("run_timestamp", "", "Pipeline run timestamp")
dbutils.widgets.text("max_errors", "50", "Maximum bad records before failing")
dbutils.widgets.text("audit_table", "", "Audit table for change tracking (optional)")
dbutils.widgets.text("enable_deletes", "true", "Process deletes — set false for append-only CDC")
dbutils.widgets.text("delimiter", ",", "Source file delimiter")

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, md5, concat_ws, coalesce, input_file_name
)
from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_processor")

# COMMAND ----------

# Resolve parameters — equivalent to Ab Initio PSET resolution
source_path = dbutils.widgets.get("source_path")
source_format = dbutils.widgets.get("source_format")
target_table = dbutils.widgets.get("target_table")
key_columns = [c.strip() for c in dbutils.widgets.get("key_columns").split(",") if c.strip()]
compare_columns_str = dbutils.widgets.get("compare_columns")
compare_columns = [c.strip() for c in compare_columns_str.split(",") if c.strip()] if compare_columns_str else []
partition_count = int(dbutils.widgets.get("partition_count"))
run_timestamp = dbutils.widgets.get("run_timestamp") or datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
max_errors = int(dbutils.widgets.get("max_errors"))
audit_table = dbutils.widgets.get("audit_table")
enable_deletes = dbutils.widgets.get("enable_deletes").lower() == "true"
delimiter = dbutils.widgets.get("delimiter")

# Validate required parameters
assert source_path, "source_path is required"
assert target_table, "target_table is required"
assert key_columns, "key_columns is required (comma-separated PKs)"

logger.info(
    f"CDC processor started: source={source_path}, target={target_table}, "
    f"keys={key_columns}, run_ts={run_timestamp}"
)

# COMMAND ----------

def read_cdc_source(spark: SparkSession, path: str, fmt: str, delim: str) -> DataFrame:
    """
    Read incoming CDC source data.

    Replaces Ab Initio Step 1: snapshot_customer.mp graph that captures
    current state from the source system. In Databricks, this reads from
    cloud storage (S3/ADLS/GCS) where the source system lands files.
    """
    reader = spark.read.format(fmt)
    if fmt == "csv":
        reader = (reader
                  .option("header", "true")
                  .option("delimiter", delim)
                  .option("inferSchema", "true"))
    df = reader.load(path)

    # Add metadata columns for lineage tracking
    df = (df
          .withColumn("_cdc_loaded_at", current_timestamp())
          .withColumn("_source_file", input_file_name()))

    record_count = df.count()
    logger.info(f"CDC source records read: {record_count}")
    return df

# COMMAND ----------

def compute_row_hash(df: DataFrame, columns: list) -> DataFrame:
    """
    Compute MD5 row hash for change detection — mirrors the Ab Initio
    CDCProcessor._row_hash() method.

    In Ab Initio, hash-based comparison is the primary CDC mechanism.
    In Delta Lake, this is optional since MERGE handles updates directly,
    but we retain it for audit/lineage purposes and to support the same
    change-detection semantics as the original Ab Initio pipeline.
    """
    if not columns:
        # If no compare columns specified, use all non-key, non-metadata columns
        columns = [c for c in df.columns
                   if c not in key_columns and not c.startswith("_")]

    hash_expr = md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in columns]))
    return df.withColumn("_row_hash", hash_expr)

# COMMAND ----------

def execute_merge(source_df: DataFrame, target_table_name: str,
                  keys: list, process_deletes: bool) -> dict:
    """
    Execute Delta Lake MERGE — replaces the entire Ab Initio CDC pipeline:
      - CDCProcessor.process() (hash comparison → inserts/updates/deletes)
      - cdc_detect_customer.mp graph (compare records by key)
      - apply_customer_changes.mp graph (apply changes to target)

    A single MERGE statement replaces three separate Ab Initio graphs and
    the manual hash-based comparison logic.

    The MERGE join condition uses the key_columns (maps to Ab Initio KEY_COLUMNS PSET).
    """
    spark = SparkSession.builder.getOrCreate()
    target_dt = DeltaTable.forName(spark, target_table_name)

    # Build merge condition from key columns
    # Ab Initio equivalent: KEY_COLUMNS=customer_id in customer_cdc.pset
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])

    # Build update set — all non-key, non-metadata columns
    update_columns = {c: f"source.{c}" for c in source_df.columns
                      if c not in keys and not c.startswith("_")}
    # Add audit columns on update
    update_columns["_cdc_updated_at"] = "current_timestamp()"
    update_columns["_cdc_operation"] = "'UPDATE'"

    # Build insert values — all source columns plus audit fields
    insert_values = {c: f"source.{c}" for c in source_df.columns if not c.startswith("_")}
    insert_values["_cdc_loaded_at"] = "current_timestamp()"
    insert_values["_cdc_operation"] = "'INSERT'"

    logger.info(f"Executing MERGE into {target_table_name} on keys: {keys}")
    start = datetime.now()

    # Build and execute MERGE statement
    # This single operation replaces three Ab Initio graphs:
    #   1. cdc_detect (compare records by key) — handled by MERGE join condition
    #   2. apply inserts/updates — handled by WHEN NOT MATCHED / WHEN MATCHED
    #   3. apply deletes — handled by WHEN NOT MATCHED BY SOURCE (if enabled)
    merge_builder = (target_dt.alias("target")
                     .merge(source_df.alias("source"), merge_condition)
                     .whenMatchedUpdate(set=update_columns)
                     .whenNotMatchedInsert(values=insert_values))

    if process_deletes:
        # Delete records missing from source — Ab Initio delete_keys = target_keys - source_keys
        merge_builder = merge_builder.whenNotMatchedBySourceDelete()

    merge_builder.execute()

    duration = (datetime.now() - start).total_seconds()

    # Collect merge metrics from Delta history
    history = spark.sql(f"DESCRIBE HISTORY {target_table_name} LIMIT 1").collect()
    metrics = history[0]["operationMetrics"] if history else {}

    result = {
        "target_table": target_table_name,
        "merge_keys": keys,
        "deletes_enabled": process_deletes,
        "duration_seconds": round(duration, 2),
        "run_timestamp": run_timestamp,
        "delta_metrics": str(metrics),
        "status": "success",
    }
    logger.info(f"MERGE complete: {result}")
    return result

# COMMAND ----------

def write_audit_record(spark: SparkSession, audit_tbl: str, merge_result: dict) -> None:
    """
    Write audit record for this CDC run.

    Replaces Ab Initio Step 4: audit_customer_changes.mp graph.
    In Ab Initio, a separate graph writes to AUDIT.CUSTOMER_CHANGES.
    In Databricks, we write directly to the audit Delta table.
    """
    if not audit_tbl:
        logger.info("No audit table configured — skipping audit record")
        return

    audit_df = spark.createDataFrame([{
        "run_timestamp": run_timestamp,
        "target_table": merge_result["target_table"],
        "merge_keys": ",".join(merge_result["merge_keys"]),
        "deletes_enabled": merge_result["deletes_enabled"],
        "duration_seconds": merge_result["duration_seconds"],
        "delta_metrics": merge_result.get("delta_metrics", ""),
        "status": merge_result["status"],
        "audit_created_at": datetime.now().isoformat(),
    }])

    audit_df.write.format("delta").mode("append").saveAsTable(audit_tbl)
    logger.info(f"Audit record written to {audit_tbl}")

# COMMAND ----------

# Main execution — orchestrates the full CDC pipeline
# Replaces the 4-step KornShell orchestration in run_customer_cdc.ksh:
#   Step 1: Snapshot current state → read_cdc_source()
#   Step 2: Compare with previous → handled by MERGE join condition
#   Step 3: Apply changes → execute_merge()
#   Step 4: Generate audit trail → write_audit_record()
try:
    spark = SparkSession.builder.getOrCreate()
    logger.info("Spark session initialized for CDC processing")

    # Step 1: Read source data (Ab Initio: snapshot_customer.mp)
    source_df = read_cdc_source(spark, source_path, source_format, delimiter)

    # Repartition for optimal MERGE performance
    # Ab Initio equivalent: -partition 8 in run_customer_cdc.ksh
    source_df = source_df.repartition(partition_count)

    # Optional: compute row hash for audit/lineage (Ab Initio: _row_hash)
    if compare_columns:
        source_df = compute_row_hash(source_df, compare_columns)

    # Steps 2+3: Compare and apply changes via Delta MERGE
    # (Ab Initio: cdc_detect_customer.mp + apply_customer_changes.mp)
    merge_result = execute_merge(source_df, target_table, key_columns, enable_deletes)

    # Step 4: Write audit record (Ab Initio: audit_customer_changes.mp)
    write_audit_record(spark, audit_table, merge_result)

    logger.info(f"CDC processor completed successfully: {merge_result}")
    dbutils.notebook.exit(str(merge_result))

except Exception as e:
    # Error handling — replaces Ab Initio error port + shell set -e
    logger.error(f"CDC processor FAILED: {e}", exc_info=True)
    dbutils.notebook.exit(f"FAILED: {str(e)}")
    raise
