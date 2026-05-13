# customer_schema.py — Converted from dml/customer.dml
# Ab Initio DML → PySpark StructType + Delta Lake DDL
#
# Type mapping decisions:
#   decimal(",") customer_id → LongType (integer identifier, no fractional part)
#   string(",") first_name   → StringType (variable-length text)
#   string(",") last_name    → StringType (variable-length text)
#   string("\n") email       → StringType (variable-length text)

from pyspark.sql.types import StructType, StructField, LongType, StringType

# PySpark StructType definition matching the Ab Initio DML record layout
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
])

# Delta Lake DDL — maps Ab Initio decimal→BIGINT, string→STRING
CUSTOMER_DDL = """
-- Converted from: dml/customer.dml
-- Ab Initio record → Delta Lake table
CREATE TABLE IF NOT EXISTS lakehouse.bronze.customer (
    customer_id   BIGINT       NOT NULL  COMMENT 'Ab Initio decimal — unique customer identifier',
    first_name    STRING                 COMMENT 'Ab Initio string — customer first name',
    last_name     STRING                 COMMENT 'Ab Initio string — customer last name',
    email         STRING                 COMMENT 'Ab Initio string — customer email address'
)
USING DELTA
COMMENT 'Customer master table — migrated from Ab Initio DML record layout'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze',
    'source' = 'abinitio.dml.customer'
);
"""
