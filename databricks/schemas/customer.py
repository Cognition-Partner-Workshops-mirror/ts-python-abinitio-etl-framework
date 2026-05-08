"""
Delta Lake schema definition for Customer record.

Migrated from: dml/customer.dml
Ab Initio DML → PySpark StructType + Delta Lake DDL

Type Mapping Decisions:
  - decimal(",") customer_id  → LongType (integer identifier, no fractional part)
  - string(",") first_name    → StringType (variable-length text)
  - string(",") last_name     → StringType (variable-length text)
  - string("\n") email        → StringType (variable-length text)
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

# PySpark StructType definition equivalent to dml/customer.dml
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("first_name", StringType(), nullable=False),
    StructField("last_name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
])

# Delta Lake DDL — maps Ab Initio decimal→BIGINT, string→STRING
CUSTOMER_DDL = """
-- Delta Lake table definition for Customer record
-- Migrated from: dml/customer.dml
-- Ab Initio delimiter: comma-separated
CREATE TABLE IF NOT EXISTS catalog.schema.customer (
    customer_id   BIGINT       NOT NULL  COMMENT 'Ab Initio decimal → BIGINT (integer key)',
    first_name    STRING       NOT NULL  COMMENT 'Ab Initio string → STRING',
    last_name     STRING       NOT NULL  COMMENT 'Ab Initio string → STRING',
    email         STRING       NOT NULL  COMMENT 'Ab Initio string → STRING'
)
USING DELTA
COMMENT 'Customer master record — migrated from Ab Initio DML'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'gold',
    'source.system' = 'abinitio',
    'source.dml' = 'dml/customer.dml'
);
"""
