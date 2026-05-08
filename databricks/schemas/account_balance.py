"""
Delta Lake schema definition for Account Balance record.

Migrated from: dml/account_balance.dml
Ab Initio DML → PySpark StructType + Delta Lake DDL

Type Mapping Decisions:
  - decimal("|") account_id           → LongType (integer identifier)
  - string("|") account_holder        → StringType (variable-length text)
  - decimal("8.2", "|") balance       → DecimalType(8,2) (fixed-precision monetary)
  - date("YYYY-MM-DD")(";") opened_date → DateType (ISO date format)
  - string("\n") branch               → StringType (variable-length text)

Note: Ab Initio decimal("8.2") means 8 digits total, 2 fractional. This maps
      directly to Spark DecimalType(8, 2) and Delta DECIMAL(8, 2).
      The pipe-delimited format is handled at ingestion time, not in the schema.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DecimalType,
    DateType,
)

# PySpark StructType — preserves Ab Initio precision for monetary fields
ACCOUNT_BALANCE_SCHEMA = StructType([
    StructField("account_id", LongType(), nullable=False),
    StructField("account_holder", StringType(), nullable=False),
    StructField("balance", DecimalType(8, 2), nullable=False),
    StructField("opened_date", DateType(), nullable=True),
    StructField("branch", StringType(), nullable=True),
])

# Delta Lake DDL — maps Ab Initio decimal("8.2") to DECIMAL(8,2)
ACCOUNT_BALANCE_DDL = """
-- Delta Lake table definition for Account Balance record
-- Migrated from: dml/account_balance.dml
-- Ab Initio delimiter: pipe-separated ("|")
CREATE TABLE IF NOT EXISTS catalog.schema.account_balance (
    account_id      BIGINT          NOT NULL  COMMENT 'Ab Initio decimal → BIGINT (account key)',
    account_holder  STRING          NOT NULL  COMMENT 'Ab Initio string → STRING',
    balance         DECIMAL(8, 2)   NOT NULL  COMMENT 'Ab Initio decimal("8.2") → DECIMAL(8,2) (monetary)',
    opened_date     DATE                      COMMENT 'Ab Initio date("YYYY-MM-DD") → DATE',
    branch          STRING                    COMMENT 'Ab Initio string → STRING'
)
USING DELTA
COMMENT 'Account balance record — migrated from Ab Initio DML'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'gold',
    'source.system' = 'abinitio',
    'source.dml' = 'dml/account_balance.dml'
);
"""
