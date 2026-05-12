"""
Delta Lake schema for customer records.

Migrated from: dml/customer.dml
Ab Initio record layout → PySpark StructType + Delta Lake DDL.

Type Mapping Decisions:
  - decimal(",") customer_id  → LongType: Ab Initio decimal without precision is a generic
    integer identifier; LongType gives 64-bit range for surrogate keys.
  - string(",") first_name    → StringType: Direct mapping; Delta Lake does not enforce
    VARCHAR length, so no size constraint is needed.
  - string(",") last_name     → StringType: Same as first_name.
  - string("\n") email        → StringType: Newline delimiter is an Ab Initio serialization
    detail; irrelevant in columnar Delta format.
"""

from pyspark.sql.types import StructType, StructField, LongType, StringType


# PySpark StructType definition for the customer record
customer_schema = StructType([
    # customer_id: Ab Initio decimal(",") → LongType (integer key, no fractional part)
    StructField("customer_id", LongType(), nullable=False),
    # first_name: Ab Initio string(",") → StringType
    StructField("first_name", StringType(), nullable=True),
    # last_name: Ab Initio string(",") → StringType
    StructField("last_name", StringType(), nullable=True),
    # email: Ab Initio string("\n") → StringType
    StructField("email", StringType(), nullable=True),
])


# Delta Lake DDL — catalog.schema.table naming follows Unity Catalog convention
CUSTOMER_DDL = """
-- Delta Lake table for customer master records.
-- Migrated from Ab Initio DML: dml/customer.dml
-- Delimiter info (comma-separated) is an Ab Initio serialization detail;
-- Delta Lake uses columnar Parquet storage, so delimiters do not apply.
CREATE TABLE IF NOT EXISTS catalog.bronze.customer (
    customer_id   BIGINT       NOT NULL  COMMENT 'Unique customer identifier (Ab Initio decimal → BIGINT)',
    first_name    STRING                 COMMENT 'Customer first name',
    last_name     STRING                 COMMENT 'Customer last name',
    email         STRING                 COMMENT 'Customer email address'
)
USING DELTA
COMMENT 'Customer master table — migrated from Ab Initio dml/customer.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
