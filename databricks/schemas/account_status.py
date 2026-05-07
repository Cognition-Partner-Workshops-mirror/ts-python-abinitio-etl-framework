# -----------------------------------------------------------------------------
# Delta Lake Schema: account_status
# Source: dml/account_status.dml
#
# Type Mapping:
#   decimal(",")  -> LongType (integer key)
#   void(",")     -> (skipped) Ab Initio void fields are padding/filler bytes
#                    used for fixed-width record alignment. They carry no data
#                    and are omitted from the Delta Lake schema.
#   string(",")   -> StringType
#   string("\n")  -> StringType
#
# Note: Two void fields (padding1, padding2) from the source DML are dropped.
# If downstream consumers depend on field ordinal positions, the ingestion
# notebook must account for these skipped columns during parsing.
# -----------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, LongType, StringType

account_status_schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
])

# Delta Lake DDL
ACCOUNT_STATUS_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.bronze.account_status (
    id      BIGINT    NOT NULL  COMMENT 'Primary key — mapped from Ab Initio decimal',
    name    STRING              COMMENT 'Account name — mapped from Ab Initio string. Void padding fields before/after this column are dropped.',
    status  STRING              COMMENT 'Account status — mapped from Ab Initio string (record-terminating field)'
)
USING DELTA
COMMENT 'Account status records (void padding fields omitted) — migrated from dml/account_status.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
