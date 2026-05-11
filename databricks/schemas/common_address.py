"""
Delta Lake schema for common_address.dml — Reusable address type definition.

Source DML (Ab Initio):
    type address_t = record
      string(",") street;
      string(",") city;
      string(",") state;
      string(",") zip;
    end;

This DML defines a reusable type (address_t) rather than a standalone record.
In PySpark, this maps to a StructType that can be embedded as a nested column
in other schemas (e.g., customer_address). No standalone Delta table is created
for type definitions — they exist only as reusable StructType components.

Type Mapping Applied:
    Ab Initio string → Spark StringType
    (delimiter metadata is not carried into the schema — it governs serialization
    in Ab Initio but is irrelevant in Delta Lake's columnar Parquet storage)
"""

from pyspark.sql.types import StructType, StructField, StringType


# Reusable address struct — corresponds to Ab Initio "type address_t"
ADDRESS_T_SCHEMA = StructType([
    # Ab Initio: string(",") street → StringType (delimiter ignored in Delta)
    StructField("street", StringType(), nullable=True),
    # Ab Initio: string(",") city → StringType
    StructField("city", StringType(), nullable=True),
    # Ab Initio: string(",") state → StringType
    StructField("state", StringType(), nullable=True),
    # Ab Initio: string(",") zip → StringType
    StructField("zip", StringType(), nullable=True),
])

# No DDL for type-only definitions — address_t is embedded in other tables.
# See customer_address.py for the table that uses this type.
