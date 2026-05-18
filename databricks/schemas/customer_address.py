# --------------------------------------------------------------------------
# customer_address.py — Databricks Delta Lake schema for Ab Initio customer_address.dml
#
# Source DML (dml/customer_address.dml):
#   include "common_address.dml";
#   record
#     decimal(",") customer_id;
#     string(",") name;
#     address_t address;          ← included type from common_address.dml
#     string("\n") phone;
#   end;
#
# Type Mapping Decisions:
#   - decimal → LongType: customer_id is an integer identifier
#   - string → StringType: direct 1:1 mapping
#   - address_t (included type) → nested StructType: mirrors the Ab Initio
#     include/type pattern; reuses address_schema from common_address.py
#   - The nested address struct is flattened in DDL as STRUCT<street, city, state, zip>
# --------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, LongType, StringType

# Import the reusable address struct (mirrors Ab Initio: include "common_address.dml")
from databricks.schemas.common_address import address_schema

# PySpark StructType definition matching customer_address.dml
customer_address_schema = StructType([
    StructField("customer_id", LongType(), nullable=False),       # Ab Initio: decimal(",")
    StructField("name", StringType(), nullable=True),             # Ab Initio: string(",")
    StructField("address", address_schema, nullable=True),        # Ab Initio: address_t (included type)
    StructField("phone", StringType(), nullable=True),            # Ab Initio: string("\n")
])

# Delta Lake DDL
CUSTOMER_ADDRESS_DDL = """
-- Delta Lake table for customer address records
-- Migrated from: dml/customer_address.dml (includes common_address.dml)
-- Note: Ab Initio include + named type (address_t) mapped to nested STRUCT
CREATE TABLE IF NOT EXISTS catalog.bronze.customer_address (
    customer_id   BIGINT        NOT NULL  COMMENT 'Customer FK (Ab Initio: decimal)',
    name          STRING                  COMMENT 'Customer name (Ab Initio: string)',
    address       STRUCT<
        street: STRING,
        city: STRING,
        state: STRING,
        zip: STRING
    >                                     COMMENT 'Address struct (Ab Initio: address_t from common_address.dml)',
    phone         STRING                  COMMENT 'Phone number (Ab Initio: string)',
    _loaded_at    TIMESTAMP     NOT NULL  DEFAULT current_timestamp()  COMMENT 'Databricks ingestion timestamp',
    _source_file  STRING                  COMMENT 'Source file path for lineage tracking'
)
USING DELTA
COMMENT 'Customer address table — migrated from Ab Initio customer_address.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'bronze'
);
"""
