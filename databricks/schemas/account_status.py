"""
Delta Lake schema definition for Account Status record.

Migrated from: dml/account_status.dml
Ab Initio DML → PySpark StructType + Delta Lake DDL

Type Mapping Decisions:
  - decimal(",") id           → LongType (integer identifier)
  - void(",") padding1        → (dropped) Ab Initio void fields are filler bytes
  - string(",") name          → StringType (variable-length text)
  - void(",") padding2        → (dropped) Ab Initio void fields are filler bytes
  - string("\n") status       → StringType (enumerated status value)

Note: Ab Initio 'void' type represents padding/filler fields used in fixed-width
      mainframe record layouts. These have no semantic meaning and are dropped
      during migration to Delta Lake. The original field positions are documented
      in comments for traceability.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

# PySpark StructType — void (padding) fields from Ab Initio are dropped
ACCOUNT_STATUS_SCHEMA = StructType([
    StructField("id", LongType(), nullable=False),
    # padding1 (void) — dropped, was Ab Initio filler at position 2
    StructField("name", StringType(), nullable=False),
    # padding2 (void) — dropped, was Ab Initio filler at position 4
    StructField("status", StringType(), nullable=False),
])

# Delta Lake DDL — Ab Initio void/padding fields excluded
ACCOUNT_STATUS_DDL = """
-- Delta Lake table definition for Account Status record
-- Migrated from: dml/account_status.dml
-- Ab Initio void (padding) fields at positions 2 and 4 are dropped
CREATE TABLE IF NOT EXISTS catalog.schema.account_status (
    id        BIGINT   NOT NULL  COMMENT 'Ab Initio decimal → BIGINT (record key)',
    name      STRING   NOT NULL  COMMENT 'Ab Initio string → STRING (position 3, after padding1)',
    status    STRING   NOT NULL  COMMENT 'Ab Initio string → STRING (position 5, after padding2)'
)
USING DELTA
COMMENT 'Account status record — migrated from Ab Initio DML (void/padding fields dropped)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'gold',
    'source.system' = 'abinitio',
    'source.dml' = 'dml/account_status.dml'
);
"""
