# -----------------------------------------------------------------------------
# Delta Lake Schema: account_balance
# Source: dml/account_balance.dml
#
# Type Mapping:
#   decimal("|")          -> LongType (integer key, no precision/scale)
#   string("|")           -> StringType
#   decimal("8.2", "|")   -> DecimalType(8,2) (precision 8, scale 2)
#   date("YYYY-MM-DD")    -> DateType
#   string("\n")           -> StringType (record delimiter)
# -----------------------------------------------------------------------------
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

# Delta Lake DDL
ACCOUNT_BALANCE_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.bronze.account_balance (
    account_id      BIGINT          NOT NULL  COMMENT 'Primary key — mapped from Ab Initio decimal (no precision)',
    account_holder  STRING                    COMMENT 'Mapped from Ab Initio string',
    balance         DECIMAL(8,2)              COMMENT 'Mapped from Ab Initio decimal("8.2") — 8 digits, 2 fractional',
    opened_date     DATE                      COMMENT 'Mapped from Ab Initio date("YYYY-MM-DD")',
    branch          STRING                    COMMENT 'Mapped from Ab Initio string (record-terminating field)'
)
USING DELTA
COMMENT 'Account balance records — migrated from dml/account_balance.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
