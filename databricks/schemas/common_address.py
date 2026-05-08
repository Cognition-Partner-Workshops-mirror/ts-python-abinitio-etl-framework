"""
Delta Lake schema definition for Common Address reusable type.

Migrated from: dml/common_address.dml
Ab Initio DML → PySpark StructType

Type Mapping Decisions:
  - type address_t = record → StructType (reusable nested struct)
  - string(",") street      → StringType
  - string(",") city        → StringType
  - string(",") state       → StringType
  - string(",") zip         → StringType

Note: Ab Initio 'type ... = record' defines a reusable named type that can be
      included via 'include' directives. In PySpark, this becomes a StructType
      that can be embedded in parent schemas. No standalone Delta table is needed
      for this type — it is used as a column type within other tables.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

# Reusable PySpark StructType — equivalent to Ab Initio address_t type
# Embed this in parent schemas via: StructField("address", ADDRESS_TYPE, ...)
ADDRESS_TYPE = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
])

# No standalone DDL — this is a reusable type, not a table.
# When flattened into a parent table, use individual columns:
#   street STRING, city STRING, state STRING, zip STRING
# When nested, use:
#   address STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>
