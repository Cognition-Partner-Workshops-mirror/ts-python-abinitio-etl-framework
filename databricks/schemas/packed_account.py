# -----------------------------------------------------------------------------
# Delta Lake Schema: packed_account
# Source: dml/packed_account.dml
#
# Type Mapping:
#   packed_decimal(5)     -> DecimalType(5,0)
#     Packed decimal (COMP-3 / BCD) is a mainframe binary encoding. Each byte
#     holds two digits plus a sign nibble. The Ab Initio width (5) indicates
#     total digits. Spark DecimalType(5,0) preserves the same precision.
#
#   packed_decimal("7.2") -> DecimalType(7,2)
#     Packed decimal with explicit precision.scale notation.
#
#   zoned_decimal(4)      -> DecimalType(4,0)
#     Zoned decimal (DISPLAY / USAGE DISPLAY) stores one digit per byte with
#     EBCDIC zone bits. Width 4 = 4 digits. Mapped to DecimalType(4,0).
#
#   string(20)            -> StringType
#     Fixed-width string, 20 bytes. Spark StringType is variable-length;
#     trailing spaces from fixed-width fields should be trimmed during ingestion.
#
# RISK: Packed and zoned decimals require byte-level decoding during ingestion.
#       See databricks/notebooks/parallel_loader.py for the decoding UDF.
# -----------------------------------------------------------------------------
from pyspark.sql.types import (
    StructType, StructField, DecimalType, StringType,
)

packed_account_schema = StructType([
    StructField("account_num", DecimalType(5, 0), nullable=False),
    StructField("balance", DecimalType(7, 2), nullable=True),
    StructField("status_code", DecimalType(4, 0), nullable=True),
    StructField("account_name", StringType(), nullable=True),
])

# Delta Lake DDL
PACKED_ACCOUNT_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.bronze.packed_account (
    account_num   DECIMAL(5,0)    NOT NULL  COMMENT 'Account number — mapped from Ab Initio packed_decimal(5). COMP-3 BCD encoding decoded at ingestion.',
    balance       DECIMAL(7,2)              COMMENT 'Account balance — mapped from Ab Initio packed_decimal("7.2"). 7 total digits, 2 fractional.',
    status_code   DECIMAL(4,0)              COMMENT 'Status code — mapped from Ab Initio zoned_decimal(4). EBCDIC zoned encoding decoded at ingestion.',
    account_name  STRING                    COMMENT 'Account name — mapped from Ab Initio string(20). Fixed-width; trailing spaces trimmed.'
)
USING DELTA
COMMENT 'Packed/zoned decimal account records (mainframe origin) — migrated from dml/packed_account.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
