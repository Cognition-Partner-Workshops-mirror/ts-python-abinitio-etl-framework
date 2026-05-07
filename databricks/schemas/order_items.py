# -----------------------------------------------------------------------------
# Delta Lake Schema: order_items
# Source: dml/order_items.dml
#
# Type Mapping:
#   decimal(",")                -> LongType (integer key)
#   decimal(",")                -> IntegerType (count field)
#   string(",")[item_count]     -> ArrayType(StringType) — variable-length array
#   decimal(",")[item_count]    -> ArrayType(LongType) — variable-length array
#   string("\n")                -> StringType
#
# Note: Ab Initio variable-length arrays (type[count_field]) are flattened to
# Spark ArrayType. The count field is preserved for backward compatibility but
# is redundant with size(array) in Spark.
# -----------------------------------------------------------------------------
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType, ArrayType,
)

order_items_schema = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("item_count", IntegerType(), nullable=True),
    StructField("item_names", ArrayType(StringType()), nullable=True),
    StructField("item_quantities", ArrayType(LongType()), nullable=True),
    StructField("order_status", StringType(), nullable=True),
])

# Delta Lake DDL
ORDER_ITEMS_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.bronze.order_items (
    order_id         BIGINT              NOT NULL  COMMENT 'Primary key — mapped from Ab Initio decimal',
    item_count       INT                           COMMENT 'Number of line items — mapped from Ab Initio decimal (array length)',
    item_names       ARRAY<STRING>                 COMMENT 'Variable-length item name array — mapped from Ab Initio string[item_count]',
    item_quantities  ARRAY<BIGINT>                 COMMENT 'Variable-length quantity array — mapped from Ab Initio decimal[item_count]',
    order_status     STRING                        COMMENT 'Mapped from Ab Initio string (record-terminating field)'
)
USING DELTA
COMMENT 'Order line items with variable-length arrays — migrated from dml/order_items.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
