# order_items_schema.py — Converted from dml/order_items.dml
# Ab Initio DML → PySpark StructType + Delta Lake DDL
#
# Type mapping decisions:
#   decimal(",") order_id            → LongType (integer identifier)
#   decimal(",") item_count          → IntegerType (count of items in the order)
#   string(",")[item_count] item_names      → ArrayType(StringType) — Ab Initio variable-length array
#   decimal(",")[item_count] item_quantities → ArrayType(IntegerType) — Ab Initio variable-length array
#   string("\n") order_status        → StringType
#
# Note: Ab Initio supports dynamic-length arrays via `[item_count]` syntax. In Delta Lake,
# these are represented as native ARRAY types. The item_count field is retained for
# backward compatibility but is redundant with SIZE(item_names).

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    ArrayType,
)

# PySpark StructType — Ab Initio variable-length arrays → Spark ArrayType
ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("item_count", IntegerType(), nullable=True),
    StructField("item_names", ArrayType(StringType()), nullable=True),
    StructField("item_quantities", ArrayType(IntegerType()), nullable=True),
    StructField("order_status", StringType(), nullable=True),
])

# Delta Lake DDL
ORDER_ITEMS_DDL = """
-- Converted from: dml/order_items.dml
-- Ab Initio variable-length arrays [item_count] → Delta Lake ARRAY<> types
CREATE TABLE IF NOT EXISTS lakehouse.bronze.order_items (
    order_id         BIGINT         NOT NULL  COMMENT 'Ab Initio decimal — order identifier',
    item_count       INT                      COMMENT 'Ab Initio decimal — number of items (redundant with SIZE(item_names))',
    item_names       ARRAY<STRING>            COMMENT 'Ab Initio string[item_count] — variable-length array of item names',
    item_quantities  ARRAY<INT>               COMMENT 'Ab Initio decimal[item_count] — variable-length array of quantities',
    order_status     STRING                   COMMENT 'Ab Initio string — current order status'
)
USING DELTA
COMMENT 'Order items table — migrated from Ab Initio DML with variable-length arrays'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze',
    'source' = 'abinitio.dml.order_items'
);
"""
