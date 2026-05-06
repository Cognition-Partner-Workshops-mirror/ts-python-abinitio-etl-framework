"""
Delta Lake schema definition for account_balance record.

Source: dml/account_balance.dml
Type Mapping:
  - decimal("|")           → LongType (integer key, no precision specified)
  - string("|")            → StringType
  - decimal("8.2", "|")    → DecimalType(8, 2) — explicit precision and scale
  - date("YYYY-MM-DD")(";") → DateType
"""
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType, DateType,
)

account_balance_schema = StructType([
    StructField("account_id", LongType(), nullable=False),
    StructField("account_holder", StringType(), nullable=True),
    StructField("balance", DecimalType(8, 2), nullable=True),
    StructField("opened_date", DateType(), nullable=True),
    StructField("branch", StringType(), nullable=True),
])

ACCOUNT_BALANCE_DDL = """\
CREATE TABLE IF NOT EXISTS lakehouse.bronze.account_balance (
    account_id      BIGINT        NOT NULL,
    account_holder  STRING,
    balance         DECIMAL(8,2),
    opened_date     DATE,
    branch          STRING
)
USING DELTA
COMMENT 'Account balance record — migrated from Ab Initio dml/account_balance.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
