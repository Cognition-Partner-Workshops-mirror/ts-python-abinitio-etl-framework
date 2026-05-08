"""
Delta Lake schema definition for Packed Account record.

Migrated from: dml/packed_account.dml
Ab Initio DML → PySpark StructType + Delta Lake DDL

Type Mapping Decisions:
  - packed_decimal(5) account_num    → DecimalType(5, 0) / BIGINT
  - packed_decimal("7.2") balance    → DecimalType(7, 2)
  - zoned_decimal(4) status_code     → DecimalType(4, 0) / INT
  - string(20) account_name          → StringType (fixed-width, max 20 chars)

CRITICAL NOTE — Packed/Zoned Decimal Handling:
  Ab Initio packed_decimal (COMP-3) stores two digits per byte plus a sign nibble.
  packed_decimal(5) = 5-digit number stored in 3 bytes.
  packed_decimal("7.2") = 7 digits total, 2 fractional, stored in 4 bytes.

  Zoned decimal stores one digit per byte with zone bits in the high nibble.
  zoned_decimal(4) = 4-digit number stored in 4 bytes.

  In Spark/Delta Lake, both are mapped to DecimalType with appropriate precision.
  During ingestion, raw packed/zoned bytes must be decoded before loading:
    - Use struct.unpack() or custom UDF to convert COMP-3 bytes to numeric
    - Apply implicit decimal point based on scale specification
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    DecimalType,
    StringType,
)

# PySpark StructType — packed/zoned decimal mapped to Spark DecimalType
PACKED_ACCOUNT_SCHEMA = StructType([
    StructField("account_num", DecimalType(5, 0), nullable=False),
    StructField("balance", DecimalType(7, 2), nullable=False),
    StructField("status_code", DecimalType(4, 0), nullable=False),
    StructField("account_name", StringType(), nullable=False),
])

# Delta Lake DDL — packed_decimal/zoned_decimal → DECIMAL with matching precision
PACKED_ACCOUNT_DDL = """
-- Delta Lake table definition for Packed Account record
-- Migrated from: dml/packed_account.dml
-- CRITICAL: packed_decimal (COMP-3) and zoned_decimal require byte-level decoding
--           at ingestion time before loading into Delta Lake
CREATE TABLE IF NOT EXISTS catalog.schema.packed_account (
    account_num    DECIMAL(5, 0)   NOT NULL  COMMENT 'Ab Initio packed_decimal(5) → DECIMAL(5,0) (COMP-3, 3 bytes)',
    balance        DECIMAL(7, 2)   NOT NULL  COMMENT 'Ab Initio packed_decimal("7.2") → DECIMAL(7,2) (COMP-3, 4 bytes)',
    status_code    DECIMAL(4, 0)   NOT NULL  COMMENT 'Ab Initio zoned_decimal(4) → DECIMAL(4,0) (1 digit/byte)',
    account_name   STRING          NOT NULL  COMMENT 'Ab Initio string(20) → STRING (was fixed-width 20 chars)'
)
USING DELTA
COMMENT 'Packed account record — migrated from Ab Initio DML (mainframe packed/zoned decimals)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'silver',
    'source.system' = 'abinitio',
    'source.dml' = 'dml/packed_account.dml'
);
"""
