"""
Delta Lake schema for Account Balance record.

Source: dml/account_balance.dml
Migrated from Ab Initio DML to PySpark StructType + Delta Lake DDL.

Type Mapping:
  - decimal("|")                -> LongType (integer identifier, pipe-delimited)
  - string("|")                 -> StringType (pipe-delimited text)
  - decimal("8.2", "|")        -> DecimalType(8,2) (monetary balance with 2 decimal places)
  - date("YYYY-MM-DD")(";")    -> DateType (ISO date, semicolon-delimited)
  - string("\n")                -> StringType (last field, newline-terminated)

Notes:
  - Source file uses pipe (|) and semicolon (;) delimiters, not comma.
    Ingestion must configure the correct delimiter in the read options.
"""

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType, DateType,
)


# PySpark StructType definition equivalent to account_balance.dml record layout
account_balance_schema = StructType([
    # account_id: Ab Initio decimal("|") -> Spark LongType (integer key)
    StructField("account_id", LongType(), nullable=False),
    # account_holder: Ab Initio string("|") -> Spark StringType
    StructField("account_holder", StringType(), nullable=True),
    # balance: Ab Initio decimal("8.2", "|") -> Spark DecimalType(8,2)
    StructField("balance", DecimalType(8, 2), nullable=True),
    # opened_date: Ab Initio date("YYYY-MM-DD")(";") -> Spark DateType
    StructField("opened_date", DateType(), nullable=True),
    # branch: Ab Initio string("\n") -> Spark StringType
    StructField("branch", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
ACCOUNT_BALANCE_DDL = """
-- Delta Lake table definition for Account Balance record
-- Source: dml/account_balance.dml
-- Delimiter: pipe-separated with semicolon for date field, newline-terminated
CREATE TABLE IF NOT EXISTS catalog.bronze.account_balance (
    account_id       BIGINT          NOT NULL  COMMENT 'Unique account identifier (Ab Initio decimal)',
    account_holder   STRING                    COMMENT 'Account holder full name',
    balance          DECIMAL(8,2)              COMMENT 'Current account balance (Ab Initio decimal 8.2)',
    opened_date      DATE                      COMMENT 'Account opening date (YYYY-MM-DD)',
    branch           STRING                    COMMENT 'Branch name or code'
)
USING DELTA
COMMENT 'Account balance records - migrated from Ab Initio DML (account_balance.dml)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality'                    = 'bronze',
    'source.system'              = 'ab_initio',
    'source.dml'                 = 'account_balance.dml'
);
"""
