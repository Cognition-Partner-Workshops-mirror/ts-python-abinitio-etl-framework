# common_address_schema.py — Converted from dml/common_address.dml
# Ab Initio DML → PySpark StructType
#
# This file defines the reusable address_t type that is included by customer_address.dml.
# In Ab Initio, `type address_t = record` defines a reusable nested record type.
# In PySpark, this maps to a StructType that can be embedded in other schemas.
#
# Type mapping decisions:
#   string(",") street → StringType
#   string(",") city   → StringType
#   string(",") state  → StringType
#   string(",") zip    → StringType

from pyspark.sql.types import StructType, StructField, StringType

# Reusable address struct — mirrors Ab Initio `type address_t = record`
# Used by customer_address_schema.py (equivalent to Ab Initio `include` directive)
ADDRESS_TYPE = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
])

# No standalone DDL — this type is embedded in customer_address table.
# Provided here for documentation and reuse by other schemas.
COMMON_ADDRESS_DDL = """
-- Converted from: dml/common_address.dml
-- Ab Initio `type address_t = record` — reusable struct, not a standalone table.
-- Embedded as STRUCT<street:STRING, city:STRING, state:STRING, zip:STRING> in
-- consuming tables (e.g., customer_address).
"""
