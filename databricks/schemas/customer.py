"""
Delta Lake schema definition for customer record.

Source: dml/customer.dml
Type Mapping:
  - decimal(",") → LongType (integer key, no precision/scale specified)
  - string(",")  → StringType
"""
from pyspark.sql.types import StructType, StructField, LongType, StringType

customer_schema = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
])

CUSTOMER_DDL = """\
CREATE TABLE IF NOT EXISTS lakehouse.bronze.customer (
    customer_id   BIGINT      NOT NULL,
    first_name    STRING,
    last_name     STRING,
    email         STRING
)
USING DELTA
COMMENT 'Customer master record — migrated from Ab Initio dml/customer.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
