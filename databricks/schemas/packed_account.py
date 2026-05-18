# --------------------------------------------------------------------------
# packed_account.py — Databricks Delta Lake schema for Ab Initio packed_account.dml
#
# Source DML (dml/packed_account.dml):
#   record
#     packed_decimal(5) account_num;
#     packed_decimal("7.2") balance;
#     zoned_decimal(4) status_code;
#     string(20) account_name;
#   end;
#
# Type Mapping Decisions:
#   - packed_decimal(5) → DecimalType(5,0): COBOL/mainframe packed BCD format.
#     Packed decimals store two digits per byte plus a sign nibble.
#     packed_decimal(5) = 5 digits, no fractional part → DecimalType(5,0).
#     RISK: Raw binary packed data must be decoded during ingestion — see
#     ingestion notebook for the byte-level conversion logic.
#   - packed_decimal("7.2") → DecimalType(7,2): 7 total digits, 2 fractional.
#     Monetary value — same binary decoding required.
#   - zoned_decimal(4) → DecimalType(4,0): EBCDIC zoned decimal format.
#     Each digit occupies one byte (zone nibble + digit nibble).
#     zoned_decimal(4) = 4 digits, no fractional part.
#     RISK: EBCDIC encoding requires codepage-aware conversion.
#   - string(20) → StringType: fixed-width 20-char field, right-padded with spaces
#     in mainframe format. Trimming applied during ingestion.
#
# MIGRATION RISKS:
#   1. Binary format: packed/zoned decimals are NOT text — they require byte-level
#      decoding. The ingestion notebook uses struct.unpack for conversion.
#   2. Sign handling: packed decimals use 0xC (positive), 0xD (negative) sign nibbles.
#   3. EBCDIC vs ASCII: if source files are EBCDIC, codepage conversion needed first.
#   4. Fixed-width: string(20) is not delimited — positional parsing required.
# --------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, DecimalType, StringType

# PySpark StructType definition matching packed_account.dml
# Note: actual binary decoding happens in the ingestion layer, not schema definition
packed_account_schema = StructType([
    StructField("account_num", DecimalType(5, 0), nullable=False),   # Ab Initio: packed_decimal(5)
    StructField("balance", DecimalType(7, 2), nullable=True),        # Ab Initio: packed_decimal("7.2")
    StructField("status_code", DecimalType(4, 0), nullable=True),    # Ab Initio: zoned_decimal(4)
    StructField("account_name", StringType(), nullable=True),        # Ab Initio: string(20) — fixed-width
])

# Delta Lake DDL
PACKED_ACCOUNT_DDL = """
-- Delta Lake table for packed/zoned decimal account records
-- Migrated from: dml/packed_account.dml
-- IMPORTANT: Source data is mainframe binary format (packed/zoned decimal).
-- Raw files must be decoded during ingestion — see parallel_loader notebook.
-- packed_decimal → Spark DECIMAL with matching precision/scale
-- zoned_decimal → Spark DECIMAL (EBCDIC zoned format decoded at read time)
CREATE TABLE IF NOT EXISTS catalog.bronze.packed_account (
    account_num    DECIMAL(5,0)    NOT NULL  COMMENT 'Account number (Ab Initio: packed_decimal(5) — BCD binary)',
    balance        DECIMAL(7,2)              COMMENT 'Account balance (Ab Initio: packed_decimal(7.2) — BCD binary)',
    status_code    DECIMAL(4,0)              COMMENT 'Status code (Ab Initio: zoned_decimal(4) — EBCDIC zoned)',
    account_name   STRING                    COMMENT 'Account name (Ab Initio: string(20) — fixed-width, trimmed)',
    _loaded_at     TIMESTAMP       NOT NULL  DEFAULT current_timestamp()  COMMENT 'Databricks ingestion timestamp',
    _source_file   STRING                    COMMENT 'Source file path for lineage tracking'
)
USING DELTA
COMMENT 'Packed account table — migrated from Ab Initio packed_account.dml (mainframe binary formats)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'bronze'
);
"""
