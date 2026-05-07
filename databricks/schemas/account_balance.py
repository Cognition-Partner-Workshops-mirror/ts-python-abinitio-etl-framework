# Databricks notebook source
# MAGIC %md
# MAGIC # Account Balance Schema
# MAGIC **Migrated from:** `dml/account_balance.dml`
# MAGIC
# MAGIC ## Type Mapping
# MAGIC | DML Field       | Ab Initio Type               | Spark Type      | Notes                                           |
# MAGIC |-----------------|------------------------------|-----------------|--------------------------------------------------|
# MAGIC | account_id      | decimal("\|")                | BIGINT          | Integer key; pipe-delimited source               |
# MAGIC | account_holder  | string("\|")                 | STRING          | Direct mapping                                   |
# MAGIC | balance         | decimal("8.2", "\|")         | DECIMAL(8,2)    | Precision and scale preserved                    |
# MAGIC | opened_date     | date("YYYY-MM-DD")(";")      | DATE            | Ab Initio date format maps directly to DateType  |
# MAGIC | branch          | string("\n")                 | STRING          | Terminal field                                   |

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog.bronze.account_balance (
# MAGIC     account_id      BIGINT        NOT NULL  COMMENT 'Unique account identifier',
# MAGIC     account_holder  STRING                  COMMENT 'Name of the account holder',
# MAGIC     balance         DECIMAL(8,2)            COMMENT 'Current account balance',
# MAGIC     opened_date     DATE                    COMMENT 'Date the account was opened (YYYY-MM-DD)',
# MAGIC     branch          STRING                  COMMENT 'Branch identifier'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Account balance records - migrated from dml/account_balance.dml'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'bronze',
# MAGIC     'source.system' = 'ab_initio',
# MAGIC     'source.dml' = 'dml/account_balance.dml'
# MAGIC );
