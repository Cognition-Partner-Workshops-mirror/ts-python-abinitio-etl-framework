# Databricks notebook source
# MAGIC %md
# MAGIC # Packed Account Schema
# MAGIC **Migrated from:** `dml/packed_account.dml`
# MAGIC
# MAGIC ## Type Mapping
# MAGIC | DML Field     | Ab Initio Type        | Spark Type    | Notes                                                                   |
# MAGIC |---------------|-----------------------|---------------|-------------------------------------------------------------------------|
# MAGIC | account_num   | packed_decimal(5)     | DECIMAL(9,0)  | BCD packed: 5 bytes = 9 digits + sign nibble; no fractional part        |
# MAGIC | balance       | packed_decimal("7.2") | DECIMAL(7,2)  | 7 total digits, 2 fractional; precision/scale preserved                 |
# MAGIC | status_code   | zoned_decimal(4)      | DECIMAL(4,0)  | EBCDIC zoned: 4 bytes = 4 digits; integer only                         |
# MAGIC | account_name  | string(20)            | STRING        | Fixed-width 20-char field → variable-length STRING (RTRIM at ingestion) |
# MAGIC
# MAGIC ### Migration Decisions: Mainframe Numeric Formats
# MAGIC
# MAGIC **Packed Decimal (COMP-3 / BCD):**
# MAGIC Each byte holds two BCD digits except the last byte whose low nibble is the sign.
# MAGIC `packed_decimal(N)` = N bytes → (2*N - 1) digits of precision.
# MAGIC - `packed_decimal(5)` → 9 digits → `DECIMAL(9,0)`
# MAGIC - `packed_decimal("7.2")` → 7 total, 2 fractional → `DECIMAL(7,2)`
# MAGIC
# MAGIC **Zoned Decimal:**
# MAGIC Each byte holds one digit in the low nibble (zone in high nibble).
# MAGIC `zoned_decimal(N)` = N bytes → N digits.
# MAGIC - `zoned_decimal(4)` → 4 digits → `DECIMAL(4,0)`
# MAGIC
# MAGIC **Fixed-Width Strings:**
# MAGIC Ab Initio `string(20)` is a fixed 20-byte field, typically right-padded with spaces.
# MAGIC Mapped to Spark `STRING` with `RTRIM()` applied during ingestion to strip padding.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DecimalType, StringType

packed_account_schema = StructType([
    StructField("account_num", DecimalType(9, 0), nullable=False),
    StructField("balance", DecimalType(7, 2), nullable=True),
    StructField("status_code", DecimalType(4, 0), nullable=True),
    StructField("account_name", StringType(), nullable=True),
])

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog.bronze.packed_account (
# MAGIC     account_num   DECIMAL(9,0)  NOT NULL  COMMENT 'Account number (packed_decimal 5 bytes → 9 digits)',
# MAGIC     balance       DECIMAL(7,2)            COMMENT 'Account balance (packed_decimal 7.2)',
# MAGIC     status_code   DECIMAL(4,0)            COMMENT 'Status code (zoned_decimal 4 bytes → 4 digits)',
# MAGIC     account_name  STRING                  COMMENT 'Account name (fixed-width 20 chars; RTRIM applied at ingestion)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Packed/zoned decimal account records - migrated from dml/packed_account.dml'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'bronze',
# MAGIC     'source.system' = 'ab_initio',
# MAGIC     'source.dml' = 'dml/packed_account.dml'
# MAGIC );
