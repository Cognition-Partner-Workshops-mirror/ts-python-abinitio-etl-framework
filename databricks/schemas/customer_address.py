"""
Delta Lake schema definition for customer_address record.

Source: dml/customer_address.dml (includes common_address.dml)
Type Mapping:
  - decimal(",")    → LongType
  - string(",")     → StringType
  - address_t       → Embedded StructType from common_address
  - include "..."   → Python import of shared schema module

Note: Ab Initio `include` directive maps to a Python import of the
shared address StructType defined in common_address.py.
"""
from pyspark.sql.types import StructType, StructField, LongType, StringType

from databricks.schemas.common_address import address_schema

customer_address_schema = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("address", address_schema, nullable=True),
    StructField("phone", StringType(), nullable=True),
])

CUSTOMER_ADDRESS_DDL = """\
CREATE TABLE IF NOT EXISTS lakehouse.bronze.customer_address (
    customer_id  BIGINT   NOT NULL,
    name         STRING,
    address      STRUCT<
        street: STRING,
        city:   STRING,
        state:  STRING,
        zip:    STRING
    >,
    phone        STRING
)
USING DELTA
COMMENT 'Customer address — migrated from Ab Initio dml/customer_address.dml (includes common_address.dml)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
