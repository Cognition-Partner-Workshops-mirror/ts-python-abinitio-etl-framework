"""
Delta Lake schema for order_items.dml — Variable-length order line items.

Source DML (Ab Initio):
    record
      decimal(",") order_id;
      decimal(",") item_count;
      string(",")[item_count] item_names;
      decimal(",")[item_count] item_quantities;
      string("\\n") order_status;
    end;

Type Mapping Applied:
    Ab Initio decimal (bare) → Spark LongType (order_id) / IntegerType (item_count)
        Rationale: order_id is a record identifier → LongType for range safety.
        item_count controls variable-length array sizing → IntegerType since array
        lengths are bounded by practical limits (not billions of items).
    Ab Initio string[item_count] → Spark ArrayType(StringType())
        Rationale: Ab Initio's "[item_count]" notation defines a variable-length
        array whose size is determined at runtime by the item_count field. In Delta
        Lake, this maps to an ARRAY<STRING> column. The item_count field is retained
        for backward compatibility but becomes redundant (use size(item_names) in Spark).
    Ab Initio decimal[item_count] → Spark ArrayType(LongType())
        Rationale: Same variable-length array pattern, with bare decimal elements
        mapping to LongType array entries.
    Ab Initio string → Spark StringType
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    ArrayType,
)


# PySpark StructType definition for the order_items record
ORDER_ITEMS_SCHEMA = StructType([
    # Ab Initio: decimal(",") order_id → LongType (record identifier)
    StructField("order_id", LongType(), nullable=False),
    # Ab Initio: decimal(",") item_count → IntegerType (array length control field)
    # Retained for compatibility; in Spark use size(item_names) instead
    StructField("item_count", IntegerType(), nullable=True),
    # Ab Initio: string(",")[item_count] item_names → ArrayType(StringType())
    # Variable-length array governed by item_count in Ab Initio
    StructField("item_names", ArrayType(StringType()), nullable=True),
    # Ab Initio: decimal(",")[item_count] item_quantities → ArrayType(LongType())
    # Variable-length array governed by item_count in Ab Initio
    StructField("item_quantities", ArrayType(LongType()), nullable=True),
    # Ab Initio: string("\n") order_status → StringType
    StructField("order_status", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
ORDER_ITEMS_DDL = """\
-- Delta Lake table migrated from Ab Initio DML: order_items.dml
-- Variable-length order line items
-- NOTE: item_count is retained for backward compatibility. In Spark SQL, use
-- size(item_names) or size(item_quantities) to get the array length dynamically.
CREATE TABLE IF NOT EXISTS order_items (
    order_id          BIGINT          NOT NULL  COMMENT 'Ab Initio decimal (bare) → BIGINT identifier',
    item_count        INT                       COMMENT 'Ab Initio decimal (bare) → INT array-length control field',
    item_names        ARRAY<STRING>             COMMENT 'Ab Initio string[item_count] → ARRAY<STRING> variable-length',
    item_quantities   ARRAY<BIGINT>             COMMENT 'Ab Initio decimal[item_count] → ARRAY<BIGINT> variable-length',
    order_status      STRING                    COMMENT 'Ab Initio string → STRING'
)
USING DELTA
COMMENT 'Order line items with variable-length arrays — migrated from Ab Initio DML (order_items.dml)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'migration.source'                 = 'abinitio',
    'migration.source_dml'             = 'dml/order_items.dml'
);
"""
