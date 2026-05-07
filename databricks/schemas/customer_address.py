# -----------------------------------------------------------------------------
# Delta Lake Schema: customer_address
# Source: dml/customer_address.dml
#
# Type Mapping:
#   include "common_address.dml" -> imports address_type_schema from common_address.py
#   decimal(",")                 -> LongType (integer key)
#   string(",")                  -> StringType
#   address_t address            -> StructType (nested struct from common_address)
#   string("\n")                 -> StringType
#
# Ab Initio `include` directives pull in named type definitions. In the Spark
# schema we import the reusable StructType and embed it as a nested column.
# -----------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, LongType, StringType

from databricks.schemas.common_address import address_type_schema

customer_address_schema = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("address", address_type_schema, nullable=True),
    StructField("phone", StringType(), nullable=True),
])

# Delta Lake DDL
CUSTOMER_ADDRESS_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.bronze.customer_address (
    customer_id  BIGINT    NOT NULL  COMMENT 'Primary key — mapped from Ab Initio decimal',
    name         STRING              COMMENT 'Customer name — mapped from Ab Initio string',
    address      STRUCT<
        street: STRING,
        city: STRING,
        state: STRING,
        zip: STRING
    >                                COMMENT 'Nested address — mapped from Ab Initio address_t type (include common_address.dml)',
    phone        STRING              COMMENT 'Phone number — mapped from Ab Initio string (record-terminating field)'
)
USING DELTA
COMMENT 'Customer address with nested address struct — migrated from dml/customer_address.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
