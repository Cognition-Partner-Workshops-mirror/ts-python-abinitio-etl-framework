# account_balance_schema.py — Converted from dml/account_balance.dml
# Ab Initio DML → PySpark StructType + Delta Lake DDL
#
# Type mapping decisions:
#   decimal("|") account_id              → LongType (integer identifier, pipe-delimited)
#   string("|") account_holder           → StringType (variable-length text)
#   decimal("8.2", "|") balance          → DecimalType(8,2) — Ab Initio precision/scale preserved
#   date("YYYY-MM-DD")(";") opened_date  → DateType — Ab Initio date format mapped to Spark DateType
#   string("\n") branch                  → StringType
#
# Note: The Ab Initio delimiter specifications ("|", ";", "\n") are ingestion metadata
# and do not affect the Delta Lake storage format.

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DecimalType,
    DateType,
)

# PySpark StructType — preserving Ab Initio precision for decimal("8.2")
ACCOUNT_BALANCE_SCHEMA = StructType([
    StructField("account_id", LongType(), nullable=False),
    StructField("account_holder", StringType(), nullable=True),
    StructField("balance", DecimalType(8, 2), nullable=True),
    StructField("opened_date", DateType(), nullable=True),
    StructField("branch", StringType(), nullable=True),
])

# Delta Lake DDL
ACCOUNT_BALANCE_DDL = """
-- Converted from: dml/account_balance.dml
-- Ab Initio decimal("8.2") → DECIMAL(8,2) preserving precision and scale
-- Ab Initio date("YYYY-MM-DD") → DATE type
CREATE TABLE IF NOT EXISTS lakehouse.bronze.account_balance (
    account_id      BIGINT         NOT NULL  COMMENT 'Ab Initio decimal — account identifier (pipe-delimited)',
    account_holder  STRING                   COMMENT 'Ab Initio string — account holder name',
    balance         DECIMAL(8, 2)            COMMENT 'Ab Initio decimal("8.2") — account balance with 2 decimal places',
    opened_date     DATE                     COMMENT 'Ab Initio date("YYYY-MM-DD") — account opening date',
    branch          STRING                   COMMENT 'Ab Initio string — branch name'
)
USING DELTA
COMMENT 'Account balance table — migrated from Ab Initio DML with precision-preserving decimal mapping'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze',
    'source' = 'abinitio.dml.account_balance'
);
"""
