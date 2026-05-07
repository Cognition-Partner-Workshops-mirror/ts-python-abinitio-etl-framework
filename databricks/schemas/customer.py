# -----------------------------------------------------------------------------
# Delta Lake Schema: customer
# Source: dml/customer.dml
#
# Type Mapping:
#   decimal(",") -> LongType (integer key, no precision/scale specified)
#   string(",")  -> StringType
#   string("\n") -> StringType (newline is the Ab Initio record delimiter)
# -----------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, LongType, StringType

customer_schema = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
])

# Delta Lake DDL
CUSTOMER_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.bronze.customer (
    customer_id   BIGINT       NOT NULL  COMMENT 'Primary key — mapped from Ab Initio decimal (no precision)',
    first_name    STRING                 COMMENT 'Mapped from Ab Initio string',
    last_name     STRING                 COMMENT 'Mapped from Ab Initio string',
    email         STRING                 COMMENT 'Mapped from Ab Initio string (record-terminating field)'
)
USING DELTA
COMMENT 'Customer master record — migrated from dml/customer.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
