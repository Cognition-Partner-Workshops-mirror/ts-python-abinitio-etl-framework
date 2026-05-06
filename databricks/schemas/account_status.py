"""
Delta Lake schema definition for account_status record.

Source: dml/account_status.dml
Type Mapping:
  - decimal(",") → LongType (integer identifier)
  - void(",")    → (skipped) — Ab Initio void fields are padding/filler bytes
  - string(",")  → StringType

Note: The `void` type in Ab Initio DML represents padding or filler bytes
used for fixed-width record alignment. These fields carry no business data
and are intentionally omitted from the Delta Lake schema.
"""
from pyspark.sql.types import StructType, StructField, LongType, StringType

account_status_schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
])

ACCOUNT_STATUS_DDL = """\
CREATE TABLE IF NOT EXISTS lakehouse.bronze.account_status (
    id      BIGINT   NOT NULL,
    name    STRING,
    status  STRING
)
USING DELTA
COMMENT 'Account status — migrated from Ab Initio dml/account_status.dml (void padding fields removed)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
