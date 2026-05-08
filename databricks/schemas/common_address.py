"""
Delta Lake schema for Common Address reusable type.

Source: dml/common_address.dml
Migrated from Ab Initio DML to PySpark StructType + Delta Lake DDL.

Type Mapping:
  - string(",") -> StringType (all address fields are comma-delimited strings)

Notes:
  - In Ab Initio, this is defined as a reusable type (type address_t = record)
    that can be included in other DML files via 'include' directive.
  - In PySpark, this is represented as a reusable StructType that can be
    embedded as a nested struct in other schemas (e.g., customer_address).
  - This file does NOT define a standalone Delta table; it is a reusable
    struct definition. See customer_address.py for a table that uses it.
"""

from pyspark.sql.types import StructType, StructField, StringType


# Reusable PySpark StructType for address fields
# Equivalent to Ab Initio "type address_t = record" in common_address.dml
address_struct = StructType([
    # street: Ab Initio string(",") -> Spark StringType
    StructField("street", StringType(), nullable=True),
    # city: Ab Initio string(",") -> Spark StringType
    StructField("city", StringType(), nullable=True),
    # state: Ab Initio string(",") -> Spark StringType
    StructField("state", StringType(), nullable=True),
    # zip: Ab Initio string(",") -> Spark StringType
    StructField("zip", StringType(), nullable=True),
])

# DDL fragment for embedding in other tables (not a standalone table)
ADDRESS_STRUCT_DDL = """
-- Reusable address struct definition
-- Source: dml/common_address.dml (type address_t)
-- Embed this as a STRUCT column in tables that include address fields
-- Example usage:
--   address STRUCT<
--       street: STRING COMMENT 'Street address',
--       city:   STRING COMMENT 'City name',
--       state:  STRING COMMENT 'State code',
--       zip:    STRING COMMENT 'ZIP/postal code'
--   > COMMENT 'Embedded address (from common_address.dml)'
"""
