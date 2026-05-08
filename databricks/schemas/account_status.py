"""
Delta Lake schema for Account Status record.

Source: dml/account_status.dml
Migrated from Ab Initio DML to PySpark StructType + Delta Lake DDL.

Type Mapping:
  - decimal(",")  -> LongType (integer identifier)
  - void(",")     -> (dropped) Ab Initio void fields are padding/filler bytes
  - string(",")   -> StringType
  - string("\n")  -> StringType (last field, newline-terminated)

Notes:
  - Ab Initio void fields (padding1, padding2) are structural padding used in
    fixed-width and mainframe-originated files. These have no semantic value
    and are dropped in the Delta Lake schema.
  - If downstream processes depend on positional field ordering, the padding
    columns can be re-added as NULL string columns during ingestion.
"""

from pyspark.sql.types import StructType, StructField, LongType, StringType


# PySpark StructType definition equivalent to account_status.dml record layout
# Note: void fields (padding1, padding2) are intentionally omitted
account_status_schema = StructType([
    # id: Ab Initio decimal(",") -> Spark LongType (integer key)
    StructField("id", LongType(), nullable=False),
    # padding1: Ab Initio void(",") -> DROPPED (no semantic value)
    # name: Ab Initio string(",") -> Spark StringType
    StructField("name", StringType(), nullable=True),
    # padding2: Ab Initio void(",") -> DROPPED (no semantic value)
    # status: Ab Initio string("\n") -> Spark StringType
    StructField("status", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
ACCOUNT_STATUS_DDL = """
-- Delta Lake table definition for Account Status record
-- Source: dml/account_status.dml
-- Note: Ab Initio void/padding fields are dropped (padding1, padding2)
CREATE TABLE IF NOT EXISTS catalog.bronze.account_status (
    id       BIGINT       NOT NULL  COMMENT 'Account status identifier (Ab Initio decimal)',
    name     STRING                 COMMENT 'Account holder or status name',
    status   STRING                 COMMENT 'Account status code/description'
)
USING DELTA
COMMENT 'Account status records - migrated from Ab Initio DML (account_status.dml). Void padding fields dropped.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality'                    = 'bronze',
    'source.system'              = 'ab_initio',
    'source.dml'                 = 'account_status.dml'
);
"""
