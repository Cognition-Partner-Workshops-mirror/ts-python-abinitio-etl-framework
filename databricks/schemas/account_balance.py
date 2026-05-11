"""
Delta Lake schema for account_balance.dml — Account balance with dates and decimals.

Source DML (Ab Initio):
    record
      decimal("|") account_id;
      string("|") account_holder;
      decimal("8.2", "|") balance;
      date("YYYY-MM-DD")(";") opened_date;
      string("\\n") branch;
    end;

Type Mapping Applied:
    Ab Initio decimal (bare, delimiter-only) → Spark LongType
        Rationale: "decimal("|")" has no precision/scale — just a pipe delimiter.
        Used as an integer identifier, so LongType is appropriate.
    Ab Initio decimal("8.2") → Spark DecimalType(8, 2)
        Rationale: The "8.2" format string specifies precision=8, scale=2.
        This maps directly to Spark's DecimalType for exact numeric representation
        of monetary values. Delta Lake stores DECIMAL as fixed-point in Parquet.
    Ab Initio date("YYYY-MM-DD") → Spark DateType
        Rationale: The format string is serialization metadata for flat files.
        Delta Lake uses Parquet's native DATE type (days since epoch), so the
        format is not needed in the schema — only during data ingestion/parsing.
    Ab Initio string → Spark StringType
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DecimalType,
    DateType,
)


# PySpark StructType definition for the account_balance record
ACCOUNT_BALANCE_SCHEMA = StructType([
    # Ab Initio: decimal("|") account_id → LongType (bare decimal = integer ID)
    StructField("account_id", LongType(), nullable=False),
    # Ab Initio: string("|") account_holder → StringType
    StructField("account_holder", StringType(), nullable=True),
    # Ab Initio: decimal("8.2", "|") balance → DecimalType(8, 2) (precision.scale)
    StructField("balance", DecimalType(8, 2), nullable=True),
    # Ab Initio: date("YYYY-MM-DD")(";") opened_date → DateType
    StructField("opened_date", DateType(), nullable=True),
    # Ab Initio: string("\n") branch → StringType
    StructField("branch", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
ACCOUNT_BALANCE_DDL = """\
-- Delta Lake table migrated from Ab Initio DML: account_balance.dml
-- Account balance with dates and decimals
CREATE TABLE IF NOT EXISTS account_balance (
    account_id      BIGINT        NOT NULL  COMMENT 'Ab Initio decimal (bare) → BIGINT identifier',
    account_holder  STRING                  COMMENT 'Ab Initio string → STRING',
    balance         DECIMAL(8,2)            COMMENT 'Ab Initio decimal("8.2") → DECIMAL(8,2) for monetary precision',
    opened_date     DATE                    COMMENT 'Ab Initio date("YYYY-MM-DD") → DATE (Parquet native)',
    branch          STRING                  COMMENT 'Ab Initio string → STRING'
)
USING DELTA
COMMENT 'Account balance record — migrated from Ab Initio DML (account_balance.dml)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'migration.source'                 = 'abinitio',
    'migration.source_dml'             = 'dml/account_balance.dml'
);
"""
