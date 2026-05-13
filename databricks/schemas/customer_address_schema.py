# customer_address_schema.py — Converted from dml/customer_address.dml + dml/common_address.dml
# Ab Initio DML → PySpark StructType + Delta Lake DDL
#
# Type mapping decisions:
#   decimal(",") customer_id     → LongType (integer identifier)
#   string(",") name             → StringType (variable-length text)
#   address_t (nested type from common_address.dml):
#     string(",") street         → StringType
#     string(",") city           → StringType
#     string(",") state          → StringType
#     string(",") zip            → StringType
#   string("\n") phone           → StringType
#
# Note: Ab Initio's `include "common_address.dml"` and `type address_t` are flattened
# into a single Delta Lake table since Spark SQL handles nested structs but flat columns
# are simpler for downstream SQL queries. The original nested structure is preserved in
# the StructType as a StructField of StructType for compatibility.

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

# Nested address type — mirrors Ab Initio `type address_t = record`
ADDRESS_TYPE = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
])

# Full record — mirrors Ab Initio customer_address.dml with include
CUSTOMER_ADDRESS_SCHEMA = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("address", ADDRESS_TYPE, nullable=True),
    StructField("phone", StringType(), nullable=True),
])

# Delta Lake DDL — flattened version for SQL-friendly querying
CUSTOMER_ADDRESS_DDL = """
-- Converted from: dml/customer_address.dml + dml/common_address.dml
-- Ab Initio `include` directive resolved; address_t type inlined as struct
CREATE TABLE IF NOT EXISTS lakehouse.bronze.customer_address (
    customer_id   BIGINT       NOT NULL  COMMENT 'Ab Initio decimal — customer FK',
    name          STRING                 COMMENT 'Ab Initio string — customer name',
    address       STRUCT<
        street: STRING,
        city:   STRING,
        state:  STRING,
        zip:    STRING
    >                                    COMMENT 'Ab Initio address_t — nested address record from common_address.dml',
    phone         STRING                 COMMENT 'Ab Initio string — phone number'
)
USING DELTA
COMMENT 'Customer address table — migrated from Ab Initio DML with nested address_t type'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze',
    'source' = 'abinitio.dml.customer_address'
);
"""
