# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor — Delta Lake MERGE with Change Data Feed
# MAGIC
# MAGIC **Migrated from:** `graphs/cdc_processor.py` (Ab Initio hash-based CDC pattern)
# MAGIC
# MAGIC ## Migration Summary
# MAGIC | Ab Initio Concept | Databricks Equivalent |
# MAGIC |---|---|
# MAGIC | CDCProcessor.process() (pandas hash comparison) | Delta Lake MERGE statement |
# MAGIC | MD5 row hashing for change detection | Delta Change Data Feed (CDF) |
# MAGIC | Separate INSERT/UPDATE/DELETE DataFrames | Single MERGE handles all three |
# MAGIC | Manual key-set comparison (source_keys − target_keys) | MERGE ON condition (join) |
# MAGIC | `compare_columns` hash → update detection | `WHEN MATCHED AND hash != hash` or column comparison |
# MAGIC | `key_columns` for record identity | MERGE ON key column equality |
# MAGIC | PSET: HASH_COLUMNS, KEY_COLUMNS | Widget parameters |
# MAGIC
# MAGIC Delta Lake's MERGE operation replaces the entire Ab Initio CDC pattern:
# MAGIC - **Inserts**: `WHEN NOT MATCHED THEN INSERT` (source_keys − target_keys)
# MAGIC - **Updates**: `WHEN MATCHED AND <change detected> THEN UPDATE` (hash comparison)
# MAGIC - **Deletes**: `WHEN NOT MATCHED BY SOURCE THEN DELETE` (target_keys − source_keys)
# MAGIC
# MAGIC Change Data Feed (CDF) provides built-in change tracking downstream, eliminating
# MAGIC the need for separate audit graphs.

# COMMAND ----------

# Widget parameters — mapped from customer_cdc.pset
# See psets/pset_templates/customer_cdc.pset for original PSET definitions
dbutils.widgets.text("source_path", "/mnt/raw/customer", "Source Path (PSET: SOURCE_PATH)")
dbutils.widgets.text("target_table", "catalog.bronze.customer_master", "Target Table (PSET: TARGET_TABLE)")
dbutils.widgets.text("key_columns", "customer_id", "Key Columns (PSET: KEY_COLUMNS)")
dbutils.widgets.text("hash_columns", "customer_id,name,address,phone,email,status", "Hash Columns (PSET: HASH_COLUMNS)")
dbutils.widgets.text("partition_count", "8", "Partition Count (PSET: PARTITION_COUNT)")
dbutils.widgets.text("max_errors", "50", "Max Errors (PSET: MAX_ERRORS)")
dbutils.widgets.text("run_timestamp", "", "Run Timestamp")

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, when, count as spark_count,
)
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_processor")
logger.info("CDC Processor notebook started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Resolve Parameters
# MAGIC Reads widget values — equivalent to loading customer_cdc.pset parameters.

# COMMAND ----------

# Resolve parameters from widgets (replaces PSETManager.load_pset for customer_cdc)
source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
key_columns = [c.strip() for c in dbutils.widgets.get("key_columns").split(",")]
hash_columns = [c.strip() for c in dbutils.widgets.get("hash_columns").split(",")]
partition_count = int(dbutils.widgets.get("partition_count"))
max_errors = int(dbutils.widgets.get("max_errors"))
run_timestamp = dbutils.widgets.get("run_timestamp") or datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

logger.info(
    f"CDC parameters — source_path={source_path}, target_table={target_table}, "
    f"keys={key_columns}, hash_cols={hash_columns}, partitions={partition_count}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Source Snapshot
# MAGIC Replaces Ab Initio Step 1: `snapshot_customer.mp` — reads current state from source system.

# COMMAND ----------

# Read the current source snapshot
# In Ab Initio, this was a separate graph (snapshot_customer.mp) that wrote to
# CURRENT_SNAPSHOT_PATH. In Spark, we read directly from the source.
try:
    source_df = (
        spark.read
        .format("delta")
        .load(source_path)
        .repartition(partition_count)
    )
    source_count = source_df.count()
    logger.info(f"Source snapshot: {source_count} records from {source_path}")

    if source_count == 0:
        logger.warning("Empty source snapshot — skipping CDC processing")
        dbutils.notebook.exit('{"status": "NO_DATA", "inserts": 0, "updates": 0, "deletes": 0}')
except AnalysisException as e:
    logger.error(f"Failed to read source: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compute Row Hash
# MAGIC Replaces the Ab Initio CDCProcessor._row_hash() method.
# MAGIC The hash is computed over the configured HASH_COLUMNS to detect changes.

# COMMAND ----------

# Compute row-level hash for change detection
# This replaces the Ab Initio CDCProcessor._row_hash() method which used MD5.
# SHA-256 is used here for stronger collision resistance.
hash_col_refs = [col(c).cast("string") for c in hash_columns if c in source_df.columns]
source_df = source_df.withColumn(
    "_row_hash",
    sha2(concat_ws("||", *hash_col_refs), 256)
)

logger.info(f"Row hash computed over columns: {hash_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta Lake MERGE (Upsert + Delete)
# MAGIC Replaces the entire Ab Initio CDC pattern:
# MAGIC - Step 2: `cdc_detect_customer.mp` (hash-based comparison)
# MAGIC - Step 3: `apply_customer_changes.mp` (apply INSERT/UPDATE/DELETE)
# MAGIC
# MAGIC A single MERGE statement handles all three CDC operations atomically.

# COMMAND ----------

# Build the MERGE condition on key columns
# Replaces Ab Initio's key-set intersection/difference logic
merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])

# Add CDC metadata columns to the source
source_with_meta = (
    source_df
    .withColumn("_cdc_timestamp", lit(run_timestamp))
    .withColumn("_updated_at", current_timestamp())
)

# Check if target table exists; create it if this is the first run
try:
    delta_target = DeltaTable.forName(spark, target_table)
    target_exists = True
    logger.info(f"Target table {target_table} exists — performing MERGE")
except AnalysisException:
    target_exists = False
    logger.info(f"Target table {target_table} does not exist — creating with initial load")

# COMMAND ----------

if not target_exists:
    # Initial load — no MERGE needed; write the full source as the baseline
    # This replaces the Ab Initio "first snapshot" scenario
    source_with_meta.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)

    # Enable Change Data Feed on the newly created table
    spark.sql(f"""
        ALTER TABLE {target_table}
        SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    result = {
        "status": "initial_load",
        "records_loaded": source_count,
        "inserts": source_count,
        "updates": 0,
        "deletes": 0,
        "run_timestamp": run_timestamp,
    }
    logger.info(f"Initial load completed: {result}")

else:
    # MERGE — replaces Ab Initio CDCProcessor.process() which computed separate
    # insert/update/delete DataFrames using pandas set operations and hash comparison
    merge_result = (
        delta_target.alias("target")
        .merge(
            source_with_meta.alias("source"),
            merge_condition
        )
        # UPDATE when key matches but hash differs (row changed)
        # Replaces: Ab Initio common_keys loop where src_hash != tgt_hash
        .whenMatchedUpdateAll(
            condition="target._row_hash != source._row_hash"
        )
        # INSERT when key exists in source but not in target (new record)
        # Replaces: Ab Initio insert_keys = source_keys - target_keys
        .whenNotMatchedInsertAll()
        # DELETE when key exists in target but not in source (removed record)
        # Replaces: Ab Initio delete_keys = target_keys - source_keys
        .whenNotMatchedBySourceDelete()
        .execute()
    )

    logger.info("MERGE operation completed")

    # Collect CDC statistics from the MERGE operation history
    # This replaces the Ab Initio CDCProcessor return dict {"inserts":..., "updates":..., "deletes":...}
    history_df = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1")
    latest_metrics = history_df.select("operationMetrics").first()

    result = {
        "status": "success",
        "merge_metrics": str(latest_metrics),
        "run_timestamp": run_timestamp,
        "target_table": target_table,
    }
    logger.info(f"CDC MERGE completed: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Audit Trail via Change Data Feed
# MAGIC Replaces Ab Initio Step 4: `audit_customer_changes.mp`.
# MAGIC Delta Lake Change Data Feed (CDF) automatically tracks all changes, eliminating
# MAGIC the need for a separate audit graph.

# COMMAND ----------

# Query Change Data Feed to produce audit trail
# This replaces the separate Ab Initio audit_customer_changes.mp graph and the
# AUDIT.CUSTOMER_CHANGES table from customer_cdc.pset
try:
    # Read changes since the current run timestamp
    cdf_df = (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "latest")
        .table(target_table)
    )

    # Log CDC statistics matching Ab Initio's output format
    cdc_stats = (
        cdf_df.groupBy("_change_type")
        .agg(spark_count("*").alias("count"))
        .collect()
    )
    stats_dict = {row["_change_type"]: row["count"] for row in cdc_stats}
    logger.info(
        f"CDC Audit — inserts: {stats_dict.get('insert', 0)}, "
        f"updates: {stats_dict.get('update_postimage', 0)}, "
        f"deletes: {stats_dict.get('delete', 0)}"
    )
except Exception as e:
    # CDF may not be available for the initial load version
    logger.warning(f"Could not read Change Data Feed (expected on first run): {e}")

# COMMAND ----------

# Exit with result summary for Databricks Workflow orchestration
import json
dbutils.notebook.exit(json.dumps(result))
