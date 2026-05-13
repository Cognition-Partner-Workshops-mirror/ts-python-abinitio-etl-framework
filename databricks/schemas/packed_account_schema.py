# packed_account_schema.py — Converted from dml/packed_account.dml
# Ab Initio DML → PySpark StructType + Delta Lake DDL
#
# Type mapping decisions (mainframe legacy formats):
#   packed_decimal(5) account_num     → DecimalType(5, 0)
#       Packed decimal (COMP-3 / BCD) stores two digits per byte plus a sign nibble.
#       A packed_decimal(5) holds up to 5 digits. Maps to DECIMAL(5,0) in Spark.
#   packed_decimal("7.2") balance     → DecimalType(7, 2)
#       Precision 7 with scale 2. Maps directly to DECIMAL(7,2).
#   zoned_decimal(4) status_code      → DecimalType(4, 0)
#       Zoned decimal uses one byte per digit with the sign in the zone nibble of the
#       last byte. A zoned_decimal(4) holds 4 digits. Maps to DECIMAL(4,0).
#   string(20) account_name           → StringType
#       Fixed-width 20-byte string. In Delta Lake, stored as variable-length STRING.
#       Trailing spaces should be trimmed during ingestion.
#
# RISK: Packed/zoned decimal files are binary-encoded. Ingestion must use a binary reader
# (e.g., COBOL copybook parser or custom UDF) rather than plain CSV/text parsing.
# See docs/ABINITIO_TO_DATABRICKS_RUNBOOK.md for packed decimal handling guidance.

from pyspark.sql.types import (
    StructType,
    StructField,
    DecimalType,
    StringType,
)

# PySpark StructType — mainframe packed/zoned decimals → Spark DecimalType
PACKED_ACCOUNT_SCHEMA = StructType([
    StructField("account_num", DecimalType(5, 0), nullable=False),
    StructField("balance", DecimalType(7, 2), nullable=True),
    StructField("status_code", DecimalType(4, 0), nullable=True),
    StructField("account_name", StringType(), nullable=True),
])

# Delta Lake DDL — binary-encoded source requires special ingestion
PACKED_ACCOUNT_DDL = """
-- Converted from: dml/packed_account.dml
-- WARNING: Source data uses mainframe binary encoding (packed/zoned decimal).
-- Ingestion must decode packed_decimal (COMP-3/BCD) and zoned_decimal before loading.
-- See migration runbook for recommended decoding approach.
--
-- packed_decimal(5) → DECIMAL(5,0): 5-digit packed BCD integer
-- packed_decimal("7.2") → DECIMAL(7,2): 7-digit packed BCD with 2 decimal places
-- zoned_decimal(4) → DECIMAL(4,0): 4-digit zoned decimal integer
-- string(20) → STRING: fixed-width 20-byte field (trim trailing spaces on ingest)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.packed_account (
    account_num    DECIMAL(5, 0)   NOT NULL  COMMENT 'Ab Initio packed_decimal(5) — BCD-encoded account number',
    balance        DECIMAL(7, 2)             COMMENT 'Ab Initio packed_decimal("7.2") — BCD-encoded balance',
    status_code    DECIMAL(4, 0)             COMMENT 'Ab Initio zoned_decimal(4) — zoned decimal status code',
    account_name   STRING                    COMMENT 'Ab Initio string(20) — fixed-width name, trim trailing spaces'
)
USING DELTA
COMMENT 'Packed account table — migrated from mainframe binary-encoded Ab Initio DML. Requires binary decoder for ingestion.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze',
    'source' = 'abinitio.dml.packed_account'
);
"""
