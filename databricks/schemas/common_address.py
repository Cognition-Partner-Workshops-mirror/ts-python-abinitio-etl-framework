"""
Delta Lake schema definition for the reusable address_t type.

Source: dml/common_address.dml
Type Mapping:
  - type address_t = record ... end → StructType (reusable sub-struct)
  - string(",") → StringType

This module defines a shared StructType that can be embedded in any table
that includes an address (e.g. customer_address).
"""
from pyspark.sql.types import StructType, StructField, StringType

address_schema = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
])
