"""
Delta Lake schema for account balance records.

Migrated from: dml/account_balance.dml
Ab Initio record layout → PySpark StructType + Delta Lake DDL.

Type Mapping Decisions:
  - decimal("|") account_id       → LongType: Integer key; pipe delimiter is serialization only.
  - string("|") account_holder    → StringType: Direct mapping.
  - decimal("8.2", "|") balance   → DecimalType(8,2): Ab Initio decimal with explicit
    precision (8 total digits, 2 fractional) maps directly to Spark DecimalType(8,2).
    This preserves the exact precision for financial calculations.
  - date("YYYY-MM-DD")(";")      → DateType: Ab Initio date with format string maps
    directly to Spark DateType. The format string is handled at ingestion time.
  - string("\n") branch           → StringType: Newline delimiter is serialization only.

  Note: The pipe ("|") and semicolon (";") delimiters in the Ab Initio DML are
  serialization details for the flat-file format. Delta Lake uses columnar Parquet
  storage, so delimiters are irrelevant in the target schema.
"""

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType, DateType,
)


# PySpark StructType definition for the account_balance record
account_balance_schema = StructType([
    # account_id: Ab Initio decimal("|") → LongType (integer key)
    StructField("account_id", LongType(), nullable=False),
    # account_holder: Ab Initio string("|") → StringType
    StructField("account_holder", StringType(), nullable=True),
    # balance: Ab Initio decimal("8.2") → DecimalType(8,2) (preserves financial precision)
    StructField("balance", DecimalType(8, 2), nullable=True),
    # opened_date: Ab Initio date("YYYY-MM-DD") → DateType
    StructField("opened_date", DateType(), nullable=True),
    # branch: Ab Initio string("\n") → StringType
    StructField("branch", StringType(), nullable=True),
])


# Delta Lake DDL with proper financial precision types
ACCOUNT_BALANCE_DDL = """
-- Delta Lake table for account balance records.
-- Migrated from Ab Initio DML: dml/account_balance.dml
-- The pipe ("|") and semicolon (";") delimiters are Ab Initio flat-file serialization
-- details that do not carry over to Delta Lake's columnar Parquet format.
CREATE TABLE IF NOT EXISTS catalog.bronze.account_balance (
    account_id      BIGINT         NOT NULL  COMMENT 'Account identifier (Ab Initio decimal → BIGINT)',
    account_holder  STRING                   COMMENT 'Account holder name',
    balance         DECIMAL(8,2)             COMMENT 'Account balance (Ab Initio decimal(8.2) → DECIMAL(8,2))',
    opened_date     DATE                     COMMENT 'Date account opened (Ab Initio date(YYYY-MM-DD) → DATE)',
    branch          STRING                   COMMENT 'Branch name'
)
USING DELTA
COMMENT 'Account balance table — migrated from Ab Initio dml/account_balance.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
