"""
Delta Lake schema for Customer Address record.

Source: dml/customer_address.dml (includes common_address.dml)
Migrated from Ab Initio DML to PySpark StructType + Delta Lake DDL.

Type Mapping:
  - decimal(",")    -> LongType (integer identifier)
  - string(",")     -> StringType (name field)
  - address_t       -> StructType (embedded from common_address.dml)
  - string("\n")    -> StringType (phone, last field)

Notes:
  - Ab Initio 'include "common_address.dml"' is equivalent to importing
    the address_struct from common_address.py in PySpark.
  - The address_t type is embedded as a nested STRUCT column, preserving
    the hierarchical structure from the original DML.
"""

from pyspark.sql.types import StructType, StructField, LongType, StringType

# Import reusable address struct (Ab Initio: include "common_address.dml")
from databricks.schemas.common_address import address_struct


# PySpark StructType definition equivalent to customer_address.dml record layout
customer_address_schema = StructType([
    # customer_id: Ab Initio decimal(",") -> Spark LongType (FK to customer)
    StructField("customer_id", LongType(), nullable=False),
    # name: Ab Initio string(",") -> Spark StringType
    StructField("name", StringType(), nullable=True),
    # address: Ab Initio address_t (from common_address.dml) -> nested StructType
    StructField("address", address_struct, nullable=True),
    # phone: Ab Initio string("\n") -> Spark StringType
    StructField("phone", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
CUSTOMER_ADDRESS_DDL = """
-- Delta Lake table definition for Customer Address record
-- Source: dml/customer_address.dml (includes common_address.dml for address_t type)
-- The address_t reusable type is embedded as a STRUCT column
CREATE TABLE IF NOT EXISTS catalog.bronze.customer_address (
    customer_id   BIGINT       NOT NULL  COMMENT 'FK to customer table (Ab Initio decimal)',
    name          STRING                 COMMENT 'Customer name',
    address       STRUCT<
        street: STRING  COMMENT 'Street address',
        city:   STRING  COMMENT 'City name',
        state:  STRING  COMMENT 'State code',
        zip:    STRING  COMMENT 'ZIP/postal code'
    >                                    COMMENT 'Embedded address struct (Ab Initio address_t from common_address.dml)',
    phone         STRING                 COMMENT 'Customer phone number'
)
USING DELTA
COMMENT 'Customer address records with nested address struct - migrated from Ab Initio DML (customer_address.dml + common_address.dml)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality'                    = 'bronze',
    'source.system'              = 'ab_initio',
    'source.dml'                 = 'customer_address.dml'
);
"""
