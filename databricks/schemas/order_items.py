"""
Delta Lake schema for Order Items record.

Source: dml/order_items.dml
Migrated from Ab Initio DML to PySpark StructType + Delta Lake DDL.

Type Mapping:
  - decimal(",")              -> LongType (integer fields: order_id, item_count)
  - string(",")[item_count]   -> ArrayType(StringType) (variable-length array of item names)
  - decimal(",")[item_count]  -> ArrayType(LongType) (variable-length array of quantities)
  - string("\n")              -> StringType (order status, last field)

Notes:
  - Ab Initio supports variable-length arrays via [item_count] notation.
    In Delta Lake, these are mapped to native ARRAY types.
  - item_count is retained as a denormalized field for compatibility, though
    array length can be derived via size(item_names) in Spark SQL.
"""

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, ArrayType,
)


# PySpark StructType definition equivalent to order_items.dml record layout
order_items_schema = StructType([
    # order_id: Ab Initio decimal(",") -> Spark LongType (integer key)
    StructField("order_id", LongType(), nullable=False),
    # item_count: Ab Initio decimal(",") -> Spark LongType (array length indicator)
    StructField("item_count", LongType(), nullable=True),
    # item_names: Ab Initio string(",")[item_count] -> Spark ArrayType(StringType)
    StructField("item_names", ArrayType(StringType()), nullable=True),
    # item_quantities: Ab Initio decimal(",")[item_count] -> Spark ArrayType(LongType)
    StructField("item_quantities", ArrayType(LongType()), nullable=True),
    # order_status: Ab Initio string("\n") -> Spark StringType
    StructField("order_status", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
ORDER_ITEMS_DDL = """
-- Delta Lake table definition for Order Items record
-- Source: dml/order_items.dml
-- Variable-length arrays mapped to Spark ARRAY types
CREATE TABLE IF NOT EXISTS catalog.bronze.order_items (
    order_id          BIGINT            NOT NULL  COMMENT 'Unique order identifier (Ab Initio decimal)',
    item_count        BIGINT                      COMMENT 'Number of line items in the order',
    item_names        ARRAY<STRING>               COMMENT 'Variable-length array of item names (Ab Initio string[item_count])',
    item_quantities   ARRAY<BIGINT>               COMMENT 'Variable-length array of item quantities (Ab Initio decimal[item_count])',
    order_status      STRING                      COMMENT 'Current order status'
)
USING DELTA
COMMENT 'Order items with variable-length arrays - migrated from Ab Initio DML (order_items.dml)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality'                    = 'bronze',
    'source.system'              = 'ab_initio',
    'source.dml'                 = 'order_items.dml'
);
"""
