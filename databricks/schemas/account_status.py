"""
Delta Lake schema for account status records.

Migrated from: dml/account_status.dml
Ab Initio record layout → PySpark StructType + Delta Lake DDL.

Type Mapping Decisions:
  - decimal(",") id           → LongType: Integer key.
  - void(",") padding1        → SKIPPED: Ab Initio void type represents filler/padding
    bytes in fixed-width mainframe records. These have no semantic meaning and are
    dropped in the Delta Lake schema. Documented here for traceability.
  - string(",") name          → StringType: Direct mapping.
  - void(",") padding2        → SKIPPED: Same as padding1.
  - string("\n") status       → StringType: Account status code.

  Note: Ab Initio `void` fields are common in mainframe-origin record layouts where
  fixed-width records include filler bytes for alignment. These are intentionally
  excluded from the Delta Lake schema since columnar storage does not require padding.
"""

from pyspark.sql.types import StructType, StructField, LongType, StringType


# PySpark StructType definition for account_status record
# Ab Initio void (padding) fields are intentionally excluded
account_status_schema = StructType([
    # id: Ab Initio decimal(",") → LongType
    StructField("id", LongType(), nullable=False),
    # padding1: Ab Initio void(",") → SKIPPED (filler byte, no data meaning)
    # name: Ab Initio string(",") → StringType
    StructField("name", StringType(), nullable=True),
    # padding2: Ab Initio void(",") → SKIPPED (filler byte, no data meaning)
    # status: Ab Initio string("\n") → StringType
    StructField("status", StringType(), nullable=True),
])


# Delta Lake DDL — void/padding fields omitted from the target schema
ACCOUNT_STATUS_DDL = """
-- Delta Lake table for account status records.
-- Migrated from Ab Initio DML: dml/account_status.dml
-- Ab Initio void (padding/filler) fields padding1 and padding2 are intentionally
-- excluded. These are mainframe record-alignment bytes with no semantic meaning.
CREATE TABLE IF NOT EXISTS catalog.bronze.account_status (
    id       BIGINT   NOT NULL  COMMENT 'Account status identifier (Ab Initio decimal → BIGINT)',
    name     STRING             COMMENT 'Account holder name',
    status   STRING             COMMENT 'Account status code'
)
USING DELTA
COMMENT 'Account status table — migrated from Ab Initio dml/account_status.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
