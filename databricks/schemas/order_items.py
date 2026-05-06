"""
Delta Lake schema definition for order_items record.

Source: dml/order_items.dml
Type Mapping:
  - decimal(",")              → LongType (integer identifiers)
  - string(",")[item_count]   → ArrayType(StringType) — variable-length array
  - decimal(",")[item_count]  → ArrayType(LongType)   — variable-length array

Note: Ab Initio variable-length arrays (field[count]) are mapped to Spark
ArrayType. The count field is retained for backward compatibility but the
array length is self-describing in Delta Lake / Parquet.
"""
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, ArrayType,
)

order_items_schema = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("item_count", LongType(), nullable=True),
    StructField("item_names", ArrayType(StringType()), nullable=True),
    StructField("item_quantities", ArrayType(LongType()), nullable=True),
    StructField("order_status", StringType(), nullable=True),
])

ORDER_ITEMS_DDL = """\
CREATE TABLE IF NOT EXISTS lakehouse.bronze.order_items (
    order_id         BIGINT       NOT NULL,
    item_count       BIGINT,
    item_names       ARRAY<STRING>,
    item_quantities  ARRAY<BIGINT>,
    order_status     STRING
)
USING DELTA
COMMENT 'Order line items — migrated from Ab Initio dml/order_items.dml (variable-length arrays)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
