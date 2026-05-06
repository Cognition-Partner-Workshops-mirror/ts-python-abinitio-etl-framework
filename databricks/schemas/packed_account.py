"""
Delta Lake schema definition for packed_account record.

Source: dml/packed_account.dml
Type Mapping:
  - packed_decimal(5)     → DecimalType(5, 0) — mainframe packed BCD, 5 digits
  - packed_decimal("7.2") → DecimalType(7, 2) — mainframe packed BCD with scale
  - zoned_decimal(4)      → DecimalType(4, 0) — mainframe zoned (EBCDIC) decimal
  - string(20)            → StringType         — fixed-length string (20 bytes)

Risk: Packed and zoned decimals originate from mainframe COBOL/EBCDIC systems.
During migration the byte-level decoding must happen in the ingestion layer
(e.g. a custom UDF or pre-processing step) before data lands in Delta Lake.
Once decoded, standard Spark DecimalType is a lossless representation.
"""
from pyspark.sql.types import (
    StructType, StructField, DecimalType, StringType,
)

packed_account_schema = StructType([
    StructField("account_num", DecimalType(5, 0), nullable=False),
    StructField("balance", DecimalType(7, 2), nullable=True),
    StructField("status_code", DecimalType(4, 0), nullable=True),
    StructField("account_name", StringType(), nullable=True),
])

PACKED_ACCOUNT_DDL = """\
CREATE TABLE IF NOT EXISTS lakehouse.bronze.packed_account (
    account_num   DECIMAL(5,0)   NOT NULL,
    balance       DECIMAL(7,2),
    status_code   DECIMAL(4,0),
    account_name  STRING
)
USING DELTA
COMMENT 'Packed/zoned decimal account — migrated from Ab Initio dml/packed_account.dml (mainframe format)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
