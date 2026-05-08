"""
Delta Lake schema for Customer record.

Source: dml/customer.dml
Migrated from Ab Initio DML to PySpark StructType + Delta Lake DDL.

Type Mapping:
  - decimal(",") -> LongType (integer identifier, no fractional part specified)
  - string(",")  -> StringType (variable-length text)
  - string("\n") -> StringType (last field, newline-delimited)
"""

from pyspark.sql.types import StructType, StructField, LongType, StringType


# PySpark StructType definition equivalent to customer.dml record layout
customer_schema = StructType([
    # customer_id: Ab Initio decimal(",") -> Spark LongType (integer key)
    StructField("customer_id", LongType(), nullable=False),
    # first_name: Ab Initio string(",") -> Spark StringType
    StructField("first_name", StringType(), nullable=True),
    # last_name: Ab Initio string(",") -> Spark StringType
    StructField("last_name", StringType(), nullable=True),
    # email: Ab Initio string("\n") -> Spark StringType (last field in record)
    StructField("email", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
CUSTOMER_DDL = """
-- Delta Lake table definition for Customer record
-- Source: dml/customer.dml
-- Delimiter: comma-separated, newline-terminated
CREATE TABLE IF NOT EXISTS catalog.bronze.customer (
    customer_id   BIGINT       NOT NULL  COMMENT 'Unique customer identifier (Ab Initio decimal)',
    first_name    STRING                 COMMENT 'Customer first name',
    last_name     STRING                 COMMENT 'Customer last name',
    email         STRING                 COMMENT 'Customer email address'
)
USING DELTA
COMMENT 'Customer master record - migrated from Ab Initio DML (customer.dml)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality'                    = 'bronze',
    'source.system'              = 'ab_initio',
    'source.dml'                 = 'customer.dml'
);
"""
