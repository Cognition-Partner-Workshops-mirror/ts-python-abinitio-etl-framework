"""
Delta Lake schema for packed/zoned decimal account records.

Migrated from: dml/packed_account.dml
Ab Initio record layout → PySpark StructType + Delta Lake DDL.

Type Mapping Decisions:
  - packed_decimal(5) account_num     → DecimalType(5,0): Ab Initio packed_decimal is
    BCD (Binary Coded Decimal) encoding from mainframe systems (e.g., IBM COMP-3).
    Each byte stores two decimal digits; packed_decimal(5) holds 5 digits total with
    no fractional part. Spark DecimalType(5,0) preserves the exact precision.
    RISK: If source data has leading zeros (e.g., "00123"), they are preserved in
    DECIMAL but would be lost in BIGINT. Using DecimalType is the safer choice.

  - packed_decimal("7.2") balance     → DecimalType(7,2): 7 total digits, 2 fractional.
    The "7.2" notation directly maps to Spark precision and scale.

  - zoned_decimal(4) status_code      → DecimalType(4,0): Ab Initio zoned_decimal is
    EBCDIC zoned encoding (one digit per byte, zone nibble + digit nibble). Common in
    IBM mainframe COBOL copybooks. zoned_decimal(4) holds 4 digits.
    DecimalType(4,0) preserves the exact digit count.
    RISK: Zoned decimal can encode sign in the zone nibble of the last byte. Spark's
    DecimalType handles signed values natively, so no data loss.

  - string(20) account_name           → StringType: Ab Initio string(20) is a fixed-width
    string of 20 characters. Delta Lake STRING is variable-length; the fixed-width
    constraint is a storage optimization in mainframe layouts that doesn't apply to
    columnar Parquet. Trailing spaces from fixed-width records should be trimmed at
    ingestion time.
"""

from pyspark.sql.types import StructType, StructField, DecimalType, StringType


# PySpark StructType definition for the packed_account record
packed_account_schema = StructType([
    # account_num: Ab Initio packed_decimal(5) → DecimalType(5,0) (BCD mainframe encoding)
    StructField("account_num", DecimalType(5, 0), nullable=False),
    # balance: Ab Initio packed_decimal("7.2") → DecimalType(7,2) (preserves financial precision)
    StructField("balance", DecimalType(7, 2), nullable=True),
    # status_code: Ab Initio zoned_decimal(4) → DecimalType(4,0) (EBCDIC zoned encoding)
    StructField("status_code", DecimalType(4, 0), nullable=True),
    # account_name: Ab Initio string(20) → StringType (fixed-width → variable-length)
    StructField("account_name", StringType(), nullable=True),
])


# Delta Lake DDL with exact decimal precision for mainframe-origin packed/zoned fields
PACKED_ACCOUNT_DDL = """
-- Delta Lake table for mainframe-origin packed/zoned decimal account records.
-- Migrated from Ab Initio DML: dml/packed_account.dml
--
-- IMPORTANT: This DML uses mainframe-specific data types:
--   packed_decimal = IBM COMP-3 BCD encoding (2 digits per byte + sign nibble)
--   zoned_decimal  = EBCDIC zoned encoding (1 digit per byte, zone + digit nibbles)
-- Both are mapped to Spark DECIMAL with exact precision to avoid data loss.
-- Ingestion pipelines must decode BCD/EBCDIC bytes before loading into Delta Lake.
CREATE TABLE IF NOT EXISTS catalog.bronze.packed_account (
    account_num   DECIMAL(5,0)   NOT NULL  COMMENT 'Account number (packed_decimal(5) → DECIMAL(5,0), BCD encoded)',
    balance       DECIMAL(7,2)             COMMENT 'Account balance (packed_decimal(7.2) → DECIMAL(7,2))',
    status_code   DECIMAL(4,0)             COMMENT 'Status code (zoned_decimal(4) → DECIMAL(4,0), EBCDIC zoned)',
    account_name  STRING                   COMMENT 'Account name (fixed-width 20 chars → variable-length STRING)'
)
USING DELTA
COMMENT 'Packed/zoned decimal account table — migrated from Ab Initio dml/packed_account.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
