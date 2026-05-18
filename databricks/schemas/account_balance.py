# --------------------------------------------------------------------------
# account_balance.py — Databricks Delta Lake schema for Ab Initio account_balance.dml
#
# Source DML (dml/account_balance.dml):
#   record
#     decimal("|") account_id;
#     string("|") account_holder;
#     decimal("8.2", "|") balance;
#     date("YYYY-MM-DD")(";") opened_date;
#     string("\n") branch;
#   end;
#
# Type Mapping Decisions:
#   - decimal → LongType: account_id is an integer identifier
#   - decimal("8.2") → DecimalType(8,2): monetary balance with 2 decimal places
#   - date("YYYY-MM-DD") → DateType: Spark native date
#   - string → StringType: direct 1:1 mapping
#   - Note: pipe-delimited ("|") source format — handled by read options, not schema
# --------------------------------------------------------------------------
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType, DateType
)

# PySpark StructType definition matching the Ab Initio account_balance.dml layout
account_balance_schema = StructType([
    StructField("account_id", LongType(), nullable=False),        # Ab Initio: decimal("|")
    StructField("account_holder", StringType(), nullable=True),   # Ab Initio: string("|")
    StructField("balance", DecimalType(8, 2), nullable=True),     # Ab Initio: decimal("8.2")
    StructField("opened_date", DateType(), nullable=True),        # Ab Initio: date("YYYY-MM-DD")
    StructField("branch", StringType(), nullable=True),           # Ab Initio: string("\n")
])

# Delta Lake DDL
ACCOUNT_BALANCE_DDL = """
-- Delta Lake table for account balance records
-- Migrated from: dml/account_balance.dml
-- Note: source is pipe-delimited ("|"), handled by ingestion reader options
CREATE TABLE IF NOT EXISTS catalog.bronze.account_balance (
    account_id      BIGINT          NOT NULL  COMMENT 'Account identifier (Ab Initio: decimal)',
    account_holder  STRING                    COMMENT 'Account holder name (Ab Initio: string)',
    balance         DECIMAL(8,2)              COMMENT 'Account balance (Ab Initio: decimal 8.2)',
    opened_date     DATE                      COMMENT 'Account open date (Ab Initio: date YYYY-MM-DD)',
    branch          STRING                    COMMENT 'Branch name (Ab Initio: string)',
    _loaded_at      TIMESTAMP       NOT NULL  DEFAULT current_timestamp()  COMMENT 'Databricks ingestion timestamp',
    _source_file    STRING                    COMMENT 'Source file path for lineage tracking'
)
USING DELTA
COMMENT 'Account balance table — migrated from Ab Initio account_balance.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'bronze'
);
"""
