"""
Delta Lake schema for Packed Account record.

Source: dml/packed_account.dml
Migrated from Ab Initio DML to PySpark StructType + Delta Lake DDL.

Type Mapping:
  - packed_decimal(5)     -> DecimalType(5,0)  (COMP-3 mainframe packed BCD, 5 digits)
  - packed_decimal("7.2") -> DecimalType(7,2)  (COMP-3 with 2 decimal places)
  - zoned_decimal(4)      -> DecimalType(4,0)  (EBCDIC zoned numeric, 4 digits)
  - string(20)            -> StringType         (fixed-width 20-char field)

Notes:
  - packed_decimal (COMP-3) stores two digits per byte plus a sign nibble.
    In Ab Initio, this is read from binary mainframe files. In Spark, the
    value is decoded during ingestion and stored as a standard DECIMAL type.
  - zoned_decimal uses one byte per digit with the sign in the last byte's
    zone nibble. Same treatment as packed_decimal for Delta Lake storage.
  - string(20) is a fixed-width field. In Delta Lake, this becomes a
    variable-length STRING; leading/trailing spaces should be trimmed
    during ingestion with TRIM().
  - This record uses fixed-width (binary) layout, NOT delimited. Ingestion
    requires a custom binary reader or pre-processing step to convert the
    mainframe format to a parseable format before loading into Delta Lake.

RISK: Packed/zoned decimal conversion requires careful validation.
      Binary representation differences between EBCDIC and ASCII can cause
      data corruption if not handled correctly. See migration runbook for
      recommended validation approach.
"""

from pyspark.sql.types import (
    StructType, StructField, DecimalType, StringType,
)


# PySpark StructType definition equivalent to packed_account.dml record layout
packed_account_schema = StructType([
    # account_num: Ab Initio packed_decimal(5) -> Spark DecimalType(5,0)
    # COMP-3 encoded in source; decoded to standard decimal during ingestion
    StructField("account_num", DecimalType(5, 0), nullable=False),
    # balance: Ab Initio packed_decimal("7.2") -> Spark DecimalType(7,2)
    # COMP-3 with 2 decimal places for monetary precision
    StructField("balance", DecimalType(7, 2), nullable=True),
    # status_code: Ab Initio zoned_decimal(4) -> Spark DecimalType(4,0)
    # EBCDIC zoned numeric decoded during ingestion
    StructField("status_code", DecimalType(4, 0), nullable=True),
    # account_name: Ab Initio string(20) -> Spark StringType
    # Fixed-width 20-char field; TRIM() during ingestion recommended
    StructField("account_name", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
PACKED_ACCOUNT_DDL = """
-- Delta Lake table definition for Packed Account record
-- Source: dml/packed_account.dml
-- WARNING: Source data uses mainframe binary formats (COMP-3 packed decimal,
--          EBCDIC zoned decimal). A pre-processing step is required to convert
--          binary data to a parseable format before Delta Lake ingestion.
CREATE TABLE IF NOT EXISTS catalog.bronze.packed_account (
    account_num    DECIMAL(5,0)    NOT NULL  COMMENT 'Account number (Ab Initio packed_decimal(5), COMP-3 encoded)',
    balance        DECIMAL(7,2)              COMMENT 'Account balance (Ab Initio packed_decimal 7.2, COMP-3 encoded)',
    status_code    DECIMAL(4,0)              COMMENT 'Status code (Ab Initio zoned_decimal(4), EBCDIC zoned numeric)',
    account_name   STRING                    COMMENT 'Account name (Ab Initio string(20), fixed-width - TRIM on ingest)'
)
USING DELTA
COMMENT 'Packed/zoned decimal account records from mainframe - migrated from Ab Initio DML (packed_account.dml). Requires binary pre-processing.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality'                    = 'bronze',
    'source.system'              = 'ab_initio',
    'source.dml'                 = 'packed_account.dml',
    'source.format'              = 'mainframe_binary'
);
"""
