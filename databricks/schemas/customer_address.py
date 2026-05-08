"""
Delta Lake schema definition for Customer Address record.

Migrated from: dml/customer_address.dml (includes dml/common_address.dml)
Ab Initio DML → PySpark StructType + Delta Lake DDL

Type Mapping Decisions:
  - decimal(",") customer_id  → LongType (integer identifier)
  - string(",") name          → StringType (variable-length text)
  - address_t (struct):
      - string(",") street    → StringType
      - string(",") city      → StringType
      - string(",") state     → StringType
      - string(",") zip       → StringType
  - string("\n") phone        → StringType

Note: Ab Initio 'include "common_address.dml"' defines a reusable type 'address_t'.
      In PySpark this is flattened into the parent struct OR represented as a nested
      StructType. We provide both options below — the DDL uses flattened columns for
      broad SQL compatibility, while the StructType preserves the nested structure.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

# Reusable address struct — equivalent to common_address.dml address_t type
ADDRESS_STRUCT = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
])

# PySpark StructType with nested address (preserves Ab Initio type hierarchy)
CUSTOMER_ADDRESS_SCHEMA = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("address", ADDRESS_STRUCT, nullable=True),
    StructField("phone", StringType(), nullable=True),
])

# Flattened variant for Delta Lake SQL queries (no nested structs)
CUSTOMER_ADDRESS_FLAT_SCHEMA = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
    StructField("phone", StringType(), nullable=True),
])

# Delta Lake DDL — flattened for SQL compatibility
CUSTOMER_ADDRESS_DDL = """
-- Delta Lake table definition for Customer Address record
-- Migrated from: dml/customer_address.dml + dml/common_address.dml
-- Ab Initio address_t type is flattened into top-level columns
CREATE TABLE IF NOT EXISTS catalog.schema.customer_address (
    customer_id   BIGINT       NOT NULL  COMMENT 'Ab Initio decimal → BIGINT (FK to customer)',
    name          STRING       NOT NULL  COMMENT 'Ab Initio string → STRING',
    street        STRING                 COMMENT 'Ab Initio address_t.street → STRING (flattened)',
    city          STRING                 COMMENT 'Ab Initio address_t.city → STRING (flattened)',
    state         STRING                 COMMENT 'Ab Initio address_t.state → STRING (flattened)',
    zip           STRING                 COMMENT 'Ab Initio address_t.zip → STRING (flattened)',
    phone         STRING                 COMMENT 'Ab Initio string → STRING'
)
USING DELTA
COMMENT 'Customer address record — migrated from Ab Initio DML (address_t flattened)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'gold',
    'source.system' = 'abinitio',
    'source.dml' = 'dml/customer_address.dml'
);
"""
