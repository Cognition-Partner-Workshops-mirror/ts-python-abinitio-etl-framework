"""
Delta Lake schema definition for Order Items record.

Migrated from: dml/order_items.dml
Ab Initio DML → PySpark StructType + Delta Lake DDL

Type Mapping Decisions:
  - decimal(",") order_id         → LongType (integer identifier)
  - decimal(",") item_count       → IntegerType (count field, small range)
  - string(",")[item_count]       → ArrayType(StringType) (variable-length array)
  - decimal(",")[item_count]      → ArrayType(IntegerType) (variable-length array)
  - string("\n") order_status     → StringType (enumerated status value)

Note: Ab Initio variable-length arrays like string(",")[item_count] are length-prefixed
      by the item_count field. In Delta Lake, these are stored as native ARRAY columns.
      The item_count field is retained for backward compatibility but can be derived
      from SIZE(item_names) in queries.
"""

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
    StructField("item_count", IntegerType(), nullable=False),
    StructField("item_names", ArrayType(StringType()), nullable=True),
    StructField("item_quantities", ArrayType(IntegerType()), nullable=True),
    StructField("order_status", StringType(), nullable=False),
])

# Delta Lake DDL — uses native ARRAY<> syntax for variable-length fields
ORDER_ITEMS_DDL = """
-- Delta Lake table definition for Order Items record
-- Migrated from: dml/order_items.dml
-- Ab Initio variable-length arrays → Delta Lake ARRAY columns
CREATE TABLE IF NOT EXISTS catalog.schema.order_items (
    order_id         BIGINT           NOT NULL  COMMENT 'Ab Initio decimal → BIGINT (order key)',
    item_count       INT              NOT NULL  COMMENT 'Ab Initio decimal → INT (array length prefix)',
    item_names       ARRAY<STRING>              COMMENT 'Ab Initio string[item_count] → ARRAY<STRING>',
    item_quantities  ARRAY<INT>                 COMMENT 'Ab Initio decimal[item_count] → ARRAY<INT>',
    order_status     STRING           NOT NULL  COMMENT 'Ab Initio string → STRING (e.g. SHIPPED, DELIVERED)'
)
USING DELTA
COMMENT 'Order line items — migrated from Ab Initio DML (variable-length arrays preserved)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'silver',
    'source.system' = 'abinitio',
    'source.dml' = 'dml/order_items.dml'
);
"""
