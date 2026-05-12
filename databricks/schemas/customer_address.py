"""
Delta Lake schema for customer address records.

Migrated from: dml/customer_address.dml (includes dml/common_address.dml)
Ab Initio record layout → PySpark StructType + Delta Lake DDL.

Type Mapping Decisions:
  - decimal(",") customer_id   → LongType: Integer key, same rationale as customer.dml.
  - string(",") name           → StringType: Direct mapping.
  - address_t address          → StructType (nested): Ab Initio named type from
    common_address.dml is modeled as a Spark StructType. Delta Lake supports nested
    structs natively in Parquet format.
  - string("\n") phone         → StringType: Delimiter is serialization-only.

  Nested type address_t (from common_address.dml):
  - string(",") street → StringType
  - string(",") city   → StringType
  - string(",") state  → StringType
  - string(",") zip    → StringType
"""

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
)


# Reusable nested struct matching Ab Initio address_t from common_address.dml
address_struct = StructType([
    # street: Ab Initio string(",") → StringType
    StructField("street", StringType(), nullable=True),
    # city: Ab Initio string(",") → StringType
    StructField("city", StringType(), nullable=True),
    # state: Ab Initio string(",") → StringType
    StructField("state", StringType(), nullable=True),
    # zip: Ab Initio string(",") → StringType
    StructField("zip", StringType(), nullable=True),
])

# PySpark StructType definition for the customer_address record
customer_address_schema = StructType([
    # customer_id: Ab Initio decimal(",") → LongType
    StructField("customer_id", LongType(), nullable=False),
    # name: Ab Initio string(",") → StringType
    StructField("name", StringType(), nullable=True),
    # address: Ab Initio address_t (nested record from common_address.dml) → StructType
    StructField("address", address_struct, nullable=True),
    # phone: Ab Initio string("\n") → StringType
    StructField("phone", StringType(), nullable=True),
])


# Delta Lake DDL with nested STRUCT for the address type
CUSTOMER_ADDRESS_DDL = """
-- Delta Lake table for customer address records with nested address struct.
-- Migrated from Ab Initio DML: dml/customer_address.dml + dml/common_address.dml
-- The Ab Initio `include "common_address.dml"` directive is modeled as a nested STRUCT
-- in Delta Lake, preserving the logical grouping of address fields.
CREATE TABLE IF NOT EXISTS catalog.bronze.customer_address (
    customer_id   BIGINT       NOT NULL  COMMENT 'Customer FK (Ab Initio decimal → BIGINT)',
    name          STRING                 COMMENT 'Customer name',
    address       STRUCT<
        street: STRING COMMENT 'Street address',
        city:   STRING COMMENT 'City',
        state:  STRING COMMENT 'State code',
        zip:    STRING COMMENT 'ZIP/postal code'
    >                                    COMMENT 'Nested address (Ab Initio address_t from common_address.dml)',
    phone         STRING                 COMMENT 'Phone number'
)
USING DELTA
COMMENT 'Customer address table — migrated from Ab Initio dml/customer_address.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
