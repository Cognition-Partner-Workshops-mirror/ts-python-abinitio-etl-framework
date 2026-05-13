# account_status_schema.py — Converted from dml/account_status.dml
# Ab Initio DML → PySpark StructType + Delta Lake DDL
#
# Type mapping decisions:
#   decimal(",") id       → LongType (integer identifier)
#   void(",") padding1    → omitted — Ab Initio void fields are filler/alignment bytes
#   string(",") name      → StringType
#   void(",") padding2    → omitted — Ab Initio void fields are filler/alignment bytes
#   string("\n") status   → StringType
#
# Note: Ab Initio `void` type is used for padding bytes in fixed-width or delimited
# records. These fields carry no business data and are dropped in the Delta Lake schema.
# The ingestion notebook should skip these columns during CSV parsing.

from pyspark.sql.types import StructType, StructField, LongType, StringType

# PySpark StructType — void padding fields omitted (no business value)
ACCOUNT_STATUS_SCHEMA = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
])

# Delta Lake DDL — void fields dropped
ACCOUNT_STATUS_DDL = """
-- Converted from: dml/account_status.dml
-- Ab Initio void fields (padding1, padding2) omitted — no business data
-- Ingestion must skip void columns when parsing delimited source files
CREATE TABLE IF NOT EXISTS lakehouse.bronze.account_status (
    id       BIGINT    NOT NULL  COMMENT 'Ab Initio decimal — account status identifier',
    name     STRING              COMMENT 'Ab Initio string — account holder name',
    status   STRING              COMMENT 'Ab Initio string — account status code'
)
USING DELTA
COMMENT 'Account status table — migrated from Ab Initio DML; void padding fields dropped'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze',
    'source' = 'abinitio.dml.account_status'
);
"""
