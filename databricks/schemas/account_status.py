# --------------------------------------------------------------------------
# account_status.py — Databricks Delta Lake schema for Ab Initio account_status.dml
#
# Source DML (dml/account_status.dml):
#   record
#     decimal(",") id;
#     void(",") padding1;
#     string(",") name;
#     void(",") padding2;
#     string("\n") status;
#   end;
#
# Type Mapping Decisions:
#   - decimal → LongType: integer identifier
#   - void → DROPPED: Ab Initio void fields are filler/padding bytes used in
#     fixed-format mainframe files. They carry no data and are not migrated.
#     During ingestion, the corresponding CSV columns are read and discarded.
#   - string → StringType: direct 1:1 mapping
# --------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, LongType, StringType

# PySpark StructType — void/padding fields from Ab Initio are excluded
# The ingestion notebook must skip columns at positions 1 and 3 (0-indexed)
account_status_schema = StructType([
    StructField("id", LongType(), nullable=False),        # Ab Initio: decimal(",")
    # padding1 (Ab Initio: void) — DROPPED: filler field, no data value
    StructField("name", StringType(), nullable=True),     # Ab Initio: string(",")
    # padding2 (Ab Initio: void) — DROPPED: filler field, no data value
    StructField("status", StringType(), nullable=True),   # Ab Initio: string("\n")
])

# Delta Lake DDL
ACCOUNT_STATUS_DDL = """
-- Delta Lake table for account status records
-- Migrated from: dml/account_status.dml
-- Note: Ab Initio void (padding) fields are dropped during migration
CREATE TABLE IF NOT EXISTS catalog.bronze.account_status (
    id            BIGINT        NOT NULL  COMMENT 'Account status ID (Ab Initio: decimal)',
    name          STRING                  COMMENT 'Account holder name (Ab Initio: string)',
    status        STRING                  COMMENT 'Account status (Ab Initio: string)',
    _loaded_at    TIMESTAMP     NOT NULL  DEFAULT current_timestamp()  COMMENT 'Databricks ingestion timestamp',
    _source_file  STRING                  COMMENT 'Source file path for lineage tracking'
)
USING DELTA
COMMENT 'Account status table — migrated from Ab Initio account_status.dml (void fields dropped)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'bronze'
);
"""
