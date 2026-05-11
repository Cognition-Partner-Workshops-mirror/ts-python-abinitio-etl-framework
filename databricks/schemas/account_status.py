"""
Delta Lake schema for account_status.dml — Account status with void (padding) fields.

Source DML (Ab Initio):
    record
      decimal(",") id;
      void(",") padding1;
      string(",") name;
      void(",") padding2;
      string("\\n") status;
    end;

Type Mapping Applied:
    Ab Initio decimal (bare) → Spark LongType
    Ab Initio string → Spark StringType
    Ab Initio void → SKIPPED (not migrated)
        Rationale: "void" fields in Ab Initio are padding/filler bytes used to
        maintain fixed record offsets in flat-file layouts. They carry no business
        data. In Delta Lake (columnar Parquet), record offsets are irrelevant —
        each column is stored independently. Void fields are omitted entirely
        from the migrated schema.

        If downstream processes depend on field ordinal positions (e.g., legacy
        consumers reading by column index), add comments or use explicit column
        ordering in SELECT statements during migration.
"""

from pyspark.sql.types import StructType, StructField, LongType, StringType


# PySpark StructType definition for the account_status record
# Note: Ab Initio void fields (padding1, padding2) are intentionally omitted —
# they are flat-file padding with no business meaning.
ACCOUNT_STATUS_SCHEMA = StructType([
    # Ab Initio: decimal(",") id → LongType (bare decimal = integer ID)
    StructField("id", LongType(), nullable=False),
    # SKIPPED: void(",") padding1 — flat-file padding, no business data
    # SKIPPED: void(",") padding2 — flat-file padding, no business data
    # Ab Initio: string(",") name → StringType
    StructField("name", StringType(), nullable=True),
    # Ab Initio: string("\n") status → StringType
    StructField("status", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
ACCOUNT_STATUS_DDL = """\
-- Delta Lake table migrated from Ab Initio DML: account_status.dml
-- Account status record (void/padding fields removed)
--
-- Migration Notes:
--   - Two void fields (padding1, padding2) were present in the Ab Initio DML
--     for flat-file record alignment. These are omitted in Delta Lake because
--     columnar Parquet storage does not require fixed-width record padding.
CREATE TABLE IF NOT EXISTS account_status (
    id       BIGINT   NOT NULL  COMMENT 'Ab Initio decimal (bare) → BIGINT identifier',
    name     STRING             COMMENT 'Ab Initio string → STRING',
    status   STRING             COMMENT 'Ab Initio string → STRING'
)
USING DELTA
COMMENT 'Account status record (void padding removed) — migrated from Ab Initio DML (account_status.dml)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'migration.source'                 = 'abinitio',
    'migration.source_dml'             = 'dml/account_status.dml'
);
"""
