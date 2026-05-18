# --------------------------------------------------------------------------
# customer.py — Databricks Delta Lake schema for Ab Initio customer.dml
#
# Source DML (dml/customer.dml):
#   record
#     decimal(",") customer_id;
#     string(",") first_name;
#     string(",") last_name;
#     string("\n") email;
#   end;
#
# Type Mapping Decisions:
#   - decimal → LongType: customer_id is an integer identifier, no fractional part
#   - string  → StringType: direct 1:1 mapping
# --------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, LongType, StringType

# PySpark StructType definition matching the Ab Initio customer.dml layout
customer_schema = StructType([
    StructField("customer_id", LongType(), nullable=False),   # Ab Initio: decimal(",") — integer ID
    StructField("first_name", StringType(), nullable=True),    # Ab Initio: string(",")
    StructField("last_name", StringType(), nullable=True),     # Ab Initio: string(",")
    StructField("email", StringType(), nullable=True),         # Ab Initio: string("\n")
])

# Delta Lake DDL — use in Databricks SQL or spark.sql()
CUSTOMER_DDL = """
-- Delta Lake table for customer records
-- Migrated from: dml/customer.dml
CREATE TABLE IF NOT EXISTS catalog.bronze.customer (
    customer_id   BIGINT       NOT NULL  COMMENT 'Unique customer identifier (Ab Initio: decimal)',
    first_name    STRING                 COMMENT 'Customer first name (Ab Initio: string)',
    last_name     STRING                 COMMENT 'Customer last name (Ab Initio: string)',
    email         STRING                 COMMENT 'Customer email address (Ab Initio: string)',
    _loaded_at    TIMESTAMP    NOT NULL  DEFAULT current_timestamp()  COMMENT 'Databricks ingestion timestamp',
    _source_file  STRING                 COMMENT 'Source file path for lineage tracking'
)
USING DELTA
COMMENT 'Customer master table — migrated from Ab Initio customer.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'bronze'
);
"""
