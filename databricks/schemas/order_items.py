"""
Delta Lake schema for order item records.

Migrated from: dml/order_items.dml
Ab Initio record layout → PySpark StructType + Delta Lake DDL.

Type Mapping Decisions:
  - decimal(",") order_id          → LongType: Integer key.
  - decimal(",") item_count        → IntegerType: Count of items in the order; IntegerType
    suffices since item counts are small.
  - string(",")[item_count]        → ArrayType(StringType): Ab Initio variable-length array
    indexed by item_count maps to a Spark ARRAY<STRING>. The item_count field is retained
    for backward compatibility but is redundant (use size(item_names) in Spark).
  - decimal(",")[item_count]       → ArrayType(IntegerType): Parallel array of quantities.
  - string("\n") order_status      → StringType: Order status code.

  Note: Ab Initio's variable-length arrays (type[count]) are modeled as Spark ARRAY types.
  The explicit count field is preserved for auditability but is logically redundant in
  the Delta Lake representation since array length is intrinsic.
"""

from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType, ArrayType,
)


# PySpark StructType definition for the order_items record
order_items_schema = StructType([
    # order_id: Ab Initio decimal(",") → LongType
    StructField("order_id", LongType(), nullable=False),
    # item_count: Ab Initio decimal(",") → IntegerType (small count value)
    StructField("item_count", IntegerType(), nullable=True),
    # item_names: Ab Initio string(",")[item_count] → ArrayType(StringType)
    StructField("item_names", ArrayType(StringType()), nullable=True),
    # item_quantities: Ab Initio decimal(",")[item_count] → ArrayType(IntegerType)
    StructField("item_quantities", ArrayType(IntegerType()), nullable=True),
    # order_status: Ab Initio string("\n") → StringType
    StructField("order_status", StringType(), nullable=True),
])


# Delta Lake DDL using ARRAY types for the variable-length fields
ORDER_ITEMS_DDL = """
-- Delta Lake table for order line items with variable-length arrays.
-- Migrated from Ab Initio DML: dml/order_items.dml
-- Ab Initio variable-length arrays (type[item_count]) are mapped to Spark ARRAY<> types.
-- The item_count field is kept for backward compatibility but is redundant
-- in Delta Lake — use SIZE(item_names) instead.
CREATE TABLE IF NOT EXISTS catalog.bronze.order_items (
    order_id         BIGINT       NOT NULL  COMMENT 'Order identifier (Ab Initio decimal → BIGINT)',
    item_count       INT                    COMMENT 'Number of line items (redundant with array length)',
    item_names       ARRAY<STRING>          COMMENT 'Item names (Ab Initio string array[item_count])',
    item_quantities  ARRAY<INT>             COMMENT 'Item quantities (Ab Initio decimal array[item_count])',
    order_status     STRING                 COMMENT 'Order status code'
)
USING DELTA
COMMENT 'Order line items table — migrated from Ab Initio dml/order_items.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
