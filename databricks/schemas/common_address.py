"""
Delta Lake reusable struct for the common address type.

Migrated from: dml/common_address.dml
Ab Initio named type (type address_t = record) → PySpark StructType.

This is a shared type definition included by other DML files (e.g., customer_address.dml).
In Databricks/Spark, this is represented as a reusable StructType that can be embedded
within other schemas.

Type Mapping Decisions:
  - string(",") street → StringType: Direct mapping.
  - string(",") city   → StringType: Direct mapping.
  - string(",") state  → StringType: Direct mapping.
  - string(",") zip    → StringType: Direct mapping.

  Note: Ab Initio's `type ... = record` construct defines a reusable named type.
  PySpark does not have named types, so this is simply a StructType variable that
  other schema modules import and embed as a nested field.
"""

from pyspark.sql.types import StructType, StructField, StringType


# Reusable StructType matching Ab Initio address_t from common_address.dml
# Other schema modules (e.g., customer_address.py) import this struct
address_type = StructType([
    # street: Ab Initio string(",") → StringType
    StructField("street", StringType(), nullable=True),
    # city: Ab Initio string(",") → StringType
    StructField("city", StringType(), nullable=True),
    # state: Ab Initio string(",") → StringType
    StructField("state", StringType(), nullable=True),
    # zip: Ab Initio string(",") → StringType
    StructField("zip", StringType(), nullable=True),
])
