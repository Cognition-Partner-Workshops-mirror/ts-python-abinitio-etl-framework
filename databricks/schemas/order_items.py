# --------------------------------------------------------------------------
# order_items.py — Databricks Delta Lake schema for Ab Initio order_items.dml
#
# Source DML (dml/order_items.dml):
#   record
#     decimal(",") order_id;
#     decimal(",") item_count;
#     string(",")[item_count] item_names;
#     decimal(",")[item_count] item_quantities;
#     string("\n") order_status;
#   end;
#
# Type Mapping Decisions:
#   - decimal → LongType: order_id and item_count are integer identifiers
#   - string[item_count] → ArrayType(StringType): Ab Initio variable-length array
#     mapped to Spark native array; item_count becomes implicit via array length
#   - decimal[item_count] → ArrayType(LongType): variable-length integer array
#   - The Ab Initio [item_count] pattern (count-prefixed arrays) is flattened into
#     Spark arrays — the count field is retained for backward compatibility but
#     array.size() is the canonical way to get the count in Spark
# --------------------------------------------------------------------------
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, ArrayType
)

# PySpark StructType definition matching the Ab Initio order_items.dml layout
order_items_schema = StructType([
    StructField("order_id", LongType(), nullable=False),            # Ab Initio: decimal(",")
    StructField("item_count", LongType(), nullable=True),           # Ab Initio: decimal(",") — array length prefix
    StructField("item_names", ArrayType(StringType()), nullable=True),      # Ab Initio: string(",")[item_count]
    StructField("item_quantities", ArrayType(LongType()), nullable=True),   # Ab Initio: decimal(",")[item_count]
    StructField("order_status", StringType(), nullable=True),       # Ab Initio: string("\n")
])

# Delta Lake DDL — use in Databricks SQL or spark.sql()
ORDER_ITEMS_DDL = """
-- Delta Lake table for order item records
-- Migrated from: dml/order_items.dml
-- Note: Ab Initio variable-length arrays [item_count] mapped to Spark ARRAY<> columns
CREATE TABLE IF NOT EXISTS catalog.bronze.order_items (
    order_id         BIGINT          NOT NULL  COMMENT 'Unique order identifier (Ab Initio: decimal)',
    item_count       BIGINT                    COMMENT 'Number of line items — redundant with array size, kept for compat (Ab Initio: decimal)',
    item_names       ARRAY<STRING>             COMMENT 'Item name array (Ab Initio: string[item_count])',
    item_quantities  ARRAY<BIGINT>             COMMENT 'Item quantity array (Ab Initio: decimal[item_count])',
    order_status     STRING                    COMMENT 'Order status code (Ab Initio: string)',
    _loaded_at       TIMESTAMP       NOT NULL  DEFAULT current_timestamp()  COMMENT 'Databricks ingestion timestamp',
    _source_file     STRING                    COMMENT 'Source file path for lineage tracking'
)
USING DELTA
COMMENT 'Order items table — migrated from Ab Initio order_items.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'bronze'
);
"""
