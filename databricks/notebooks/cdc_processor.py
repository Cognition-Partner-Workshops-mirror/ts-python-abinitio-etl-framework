# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor — Delta Lake MERGE with Change Data Feed
# MAGIC
# MAGIC **Migrated from:** `graphs/cdc_processor.py` (Ab Initio hash-based CDC)
# MAGIC
# MAGIC **Ab Initio equivalent:** `Compare Records by Key` component in CDC detection graphs
# MAGIC
# MAGIC **What changed:**
# MAGIC - Pandas-based row hashing (MD5) → Delta Lake MERGE statement (native CDC)
# MAGIC - Manual INSERT/UPDATE/DELETE detection → Delta Lake Change Data Feed (CDF)
# MAGIC - In-memory DataFrame comparison → Distributed Spark SQL MERGE
# MAGIC - No audit trail → Automatic CDF versioning with `table_changes()` function

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — Widget Parameters
# MAGIC Maps Ab Initio PSET parameters (from customer_cdc.pset) to Databricks widgets.
# MAGIC
# MAGIC | Ab Initio PSET Key         | Databricks Widget       | Description                          |
# MAGIC |----------------------------|-------------------------|--------------------------------------|
# MAGIC | SOURCE_PATH                | source_path             | Incoming source data path            |
# MAGIC | TARGET_TABLE               | target_table            | Delta Lake target table              |
# MAGIC | KEY_COLUMNS                | key_columns             | Comma-separated merge key columns    |
# MAGIC | HASH_COLUMNS               | compare_columns         | Columns to compare for changes       |
# MAGIC | PARTITION_COUNT            | partition_count          | Spark shuffle partitions             |
# MAGIC | AUDIT_TABLE                | audit_table             | Table to log CDC statistics          |
# MAGIC | BATCH_SIZE                 | batch_size              | Processing batch size                |
# MAGIC | MAX_ERRORS                 | max_errors              | Error threshold before abort         |

# COMMAND ----------

# Widget definitions — equivalent to Ab Initio PSET parameter loading
dbutils.widgets.text("source_path", "", "Source Data Path")
dbutils.widgets.text("target_table", "", "Target Delta Table")
dbutils.widgets.text("key_columns", "customer_id", "Key Columns (comma-separated)")
dbutils.widgets.text("compare_columns", "", "Compare Columns (comma-separated, empty=all)")
dbutils.widgets.text("partition_count", "8", "Partition Count")
dbutils.widgets.text("audit_table", "", "Audit Table")
dbutils.widgets.text("batch_size", "100000", "Batch Size")
dbutils.widgets.text("max_errors", "50", "Max Errors")
dbutils.widgets.text("log_level", "INFO", "Log Level")

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

# Configure logging — replaces Ab Initio graph-level log files
log_level = dbutils.widgets.get("log_level")
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logger = logging.getLogger("cdc_processor")

# Resolve widget parameters — equivalent to PSET resolution
source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
key_columns = [c.strip() for c in dbutils.widgets.get("key_columns").split(",")]
compare_columns_raw = dbutils.widgets.get("compare_columns")
compare_columns = [c.strip() for c in compare_columns_raw.split(",") if c.strip()] or None
partition_count = int(dbutils.widgets.get("partition_count"))
audit_table = dbutils.widgets.get("audit_table")
max_errors = int(dbutils.widgets.get("max_errors"))

logger.info(
    f"CDC Processor started | source={source_path} | target={target_table} | "
    f"keys={key_columns} | partitions={partition_count}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Session Setup

# COMMAND ----------

# SparkSession is pre-configured in Databricks
spark = SparkSession.builder.getOrCreate()

# Enable Change Data Feed on Delta tables — core CDC capability
# Replaces Ab Initio's hash-based comparison pattern entirely
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
spark.conf.set("spark.sql.shuffle.partitions", str(partition_count))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Data Ingestion
# MAGIC Reads the incoming source snapshot for CDC comparison.
# MAGIC In Ab Initio, this was the `snapshot_customer.mp` graph step.

# COMMAND ----------

def read_source_data(path: str) -> DataFrame:
    """
    Read source snapshot data for CDC processing.

    Replaces Ab Initio graph step: snapshot_customer.mp
    The source snapshot captures the current state of the upstream system.
    """
    logger.info(f"Reading source snapshot from: {path}")

    try:
        if path.endswith(".parquet") or "parquet" in path.lower():
            df = spark.read.parquet(path)
        elif path.endswith(".csv") or path.endswith(".dat"):
            df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("nullValue", "")
                .load(path)
            )
        else:
            df = spark.read.format("delta").load(path)

        record_count = df.count()
        logger.info(f"Source snapshot loaded: {record_count} records")
        return df

    except AnalysisException as e:
        logger.error(f"Failed to read source data: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake MERGE — Replaces Ab Initio CDC Hash Comparison
# MAGIC
# MAGIC The Ab Initio CDCProcessor used MD5 row hashing to detect changes:
# MAGIC   1. Hash each row in source and target
# MAGIC   2. Compare keys: source-only=INSERT, target-only=DELETE, hash-changed=UPDATE
# MAGIC
# MAGIC Delta Lake MERGE replaces this entirely:
# MAGIC   - Single atomic MERGE INTO statement handles INSERT/UPDATE/DELETE
# MAGIC   - Change Data Feed (CDF) automatically tracks all changes
# MAGIC   - No manual hash computation needed

# COMMAND ----------

def build_merge_condition(keys: list) -> str:
    """
    Build the MERGE ON condition from key columns.

    Replaces Ab Initio CDCProcessor.key_columns used for set-based
    key comparison (source_keys - target_keys, etc.)
    """
    conditions = [f"target.{k} = source.{k}" for k in keys]
    return " AND ".join(conditions)


def build_update_set(source_df: DataFrame, keys: list, cols_to_compare: list = None) -> dict:
    """
    Build the UPDATE SET clause from non-key columns.

    Replaces the Ab Initio hash-based update detection:
      - Ab Initio hashed all compare_columns and compared hashes
      - Delta MERGE compares actual column values natively
    """
    if cols_to_compare:
        update_cols = cols_to_compare
    else:
        # Update all non-key columns (same as Ab Initio compare_columns=None behavior)
        update_cols = [c for c in source_df.columns if c not in keys]

    return {col: f"source.{col}" for col in update_cols}


def execute_merge(
    source_df: DataFrame,
    target_table_name: str,
    keys: list,
    cols_to_compare: list = None,
) -> dict:
    """
    Execute Delta Lake MERGE — atomic INSERT/UPDATE/DELETE in one operation.

    This single function replaces the entire Ab Initio CDCProcessor.process() method:
      - source_keys - target_keys (INSERTS) → WHEN NOT MATCHED THEN INSERT
      - target_keys - source_keys (DELETES) → WHEN NOT MATCHED BY SOURCE THEN DELETE
      - hash comparison (UPDATES)            → WHEN MATCHED AND columns changed THEN UPDATE

    Returns CDC statistics matching the Ab Initio return format:
      {"inserts": N, "updates": N, "deletes": N}
    """
    logger.info(f"Executing MERGE into {target_table_name}")

    merge_condition = build_merge_condition(keys)
    update_columns = build_update_set(source_df, keys, cols_to_compare)

    # Build change detection condition for MATCHED clause
    # Replaces Ab Initio _row_hash MD5 comparison
    if cols_to_compare:
        change_cols = cols_to_compare
    else:
        change_cols = [c for c in source_df.columns if c not in keys]

    change_condition = " OR ".join(
        [f"target.{c} <> source.{c} OR (target.{c} IS NULL AND source.{c} IS NOT NULL) "
         f"OR (target.{c} IS NOT NULL AND source.{c} IS NULL)"
         for c in change_cols]
    )

    # Check if target table exists — create if first run
    try:
        target_delta = DeltaTable.forName(spark, target_table_name)
    except AnalysisException:
        logger.info(f"Target table {target_table_name} does not exist — creating from source")
        source_df.write.format("delta").saveAsTable(target_table_name)
        # Enable CDF on the newly created table
        spark.sql(
            f"ALTER TABLE {target_table_name} SET TBLPROPERTIES "
            f"('delta.enableChangeDataFeed' = 'true')"
        )
        record_count = source_df.count()
        return {"inserts": record_count, "updates": 0, "deletes": 0, "initial_load": True}

    # Capture pre-merge counts for statistics
    pre_merge_version = spark.sql(
        f"DESCRIBE HISTORY {target_table_name} LIMIT 1"
    ).collect()[0]["version"]

    # Execute the MERGE — replaces Ab Initio CDCProcessor.process() entirely
    merge_builder = (
        target_delta.alias("target")
        .merge(source_df.alias("source"), merge_condition)
    )

    # WHEN MATCHED AND data changed → UPDATE (replaces hash-based update detection)
    update_set = {col: F.col(f"source.{col}") for col in update_columns}
    merge_builder = merge_builder.whenMatchedUpdate(
        condition=change_condition,
        set=update_set,
    )

    # WHEN NOT MATCHED → INSERT (replaces source_keys - target_keys set operation)
    merge_builder = merge_builder.whenNotMatchedInsertAll()

    # WHEN NOT MATCHED BY SOURCE → DELETE (replaces target_keys - source_keys set operation)
    merge_builder = merge_builder.whenNotMatchedBySourceDelete()

    # Execute the merge
    merge_builder.execute()

    # Collect CDC statistics from Delta change log — replaces manual counting
    post_merge_version = spark.sql(
        f"DESCRIBE HISTORY {target_table_name} LIMIT 1"
    ).collect()[0]["version"]

    stats = get_cdc_stats(target_table_name, pre_merge_version + 1, post_merge_version)
    logger.info(f"MERGE completed | {stats}")

    return stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Statistics via Change Data Feed
# MAGIC Delta Lake CDF automatically tracks every row-level change.
# MAGIC This replaces the Ab Initio manual counting of inserts/updates/deletes.

# COMMAND ----------

def get_cdc_stats(table_name: str, start_version: int, end_version: int) -> dict:
    """
    Query Change Data Feed to get INSERT/UPDATE/DELETE counts.

    Replaces Ab Initio CDCProcessor stats dict:
      {"inserts": len(inserts), "updates": len(updates_df), "deletes": len(deletes)}

    Delta CDF provides this automatically via the _change_type column:
      - "insert"          → new rows
      - "update_preimage" → old values of updated rows
      - "update_postimage"→ new values of updated rows
      - "delete"          → removed rows
    """
    try:
        changes_df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", start_version)
            .option("endingVersion", end_version)
            .table(table_name)
        )

        # Count each change type — replaces Ab Initio manual set operations
        stats_df = changes_df.groupBy("_change_type").count().collect()
        stats = {row["_change_type"]: row["count"] for row in stats_df}

        return {
            "inserts": stats.get("insert", 0),
            "updates": stats.get("update_postimage", 0),
            "deletes": stats.get("delete", 0),
            "start_version": start_version,
            "end_version": end_version,
        }

    except Exception as e:
        logger.warning(f"Could not read CDF stats: {e}")
        return {"inserts": 0, "updates": 0, "deletes": 0, "error": str(e)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Trail — Replaces Ab Initio audit_customer_changes.mp
# MAGIC Records CDC run metadata for compliance and debugging.

# COMMAND ----------

def write_audit_record(
    audit_tbl: str,
    target_tbl: str,
    stats: dict,
    run_timestamp: str,
) -> None:
    """
    Write a CDC audit record — replaces Ab Initio audit graph.

    Ab Initio used a separate graph (audit_customer_changes.mp) to write
    change counts and timestamps to the AUDIT.CUSTOMER_CHANGES table.
    In Databricks, we write directly from the CDC notebook.
    """
    if not audit_tbl:
        logger.info("No audit table configured — skipping audit record")
        return

    audit_data = [{
        "target_table": target_tbl,
        "run_timestamp": run_timestamp,
        "inserts": stats.get("inserts", 0),
        "updates": stats.get("updates", 0),
        "deletes": stats.get("deletes", 0),
        "start_version": stats.get("start_version"),
        "end_version": stats.get("end_version"),
        "recorded_at": datetime.now().isoformat(),
    }]

    audit_df = spark.createDataFrame(audit_data)
    audit_df.write.format("delta").mode("append").saveAsTable(audit_tbl)
    logger.info(f"Audit record written to {audit_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution — CDC Pipeline
# MAGIC
# MAGIC Replaces the full Ab Initio CDC pipeline sequence:
# MAGIC   1. `snapshot_customer.mp`        → read_source_data()
# MAGIC   2. `cdc_detect_customer.mp`      → execute_merge() (Delta MERGE)
# MAGIC   3. `apply_customer_changes.mp`   → (included in MERGE — atomic)
# MAGIC   4. `audit_customer_changes.mp`   → write_audit_record()

# COMMAND ----------

try:
    start_time = datetime.now()
    run_ts = start_time.strftime("%Y-%m-%d_%H:%M:%S")
    logger.info("=" * 60)
    logger.info(f"CDC PROCESSOR STARTED | {run_ts}")
    logger.info("=" * 60)

    # Step 1: Read source snapshot (replaces snapshot_customer.mp)
    source_df = read_source_data(source_path)

    # Step 2+3: MERGE into target (replaces cdc_detect + apply graphs)
    # Delta MERGE atomically detects AND applies changes in one operation
    cdc_stats = execute_merge(source_df, target_table, key_columns, compare_columns)

    # Step 4: Write audit trail (replaces audit_customer_changes.mp)
    write_audit_record(audit_table, target_table, cdc_stats, run_ts)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"CDC PROCESSOR COMPLETED | duration={elapsed:.2f}s | "
        f"inserts={cdc_stats.get('inserts', 0)} | "
        f"updates={cdc_stats.get('updates', 0)} | "
        f"deletes={cdc_stats.get('deletes', 0)}"
    )

    # Return result for downstream workflow tasks
    dbutils.notebook.exit(
        f"SUCCESS|inserts={cdc_stats.get('inserts', 0)}|"
        f"updates={cdc_stats.get('updates', 0)}|"
        f"deletes={cdc_stats.get('deletes', 0)}|"
        f"duration={elapsed:.2f}s"
    )

except Exception as e:
    logger.error(f"CDC PROCESSOR FAILED: {e}")
    dbutils.notebook.exit(f"FAILED|error={str(e)[:200]}")
    raise
