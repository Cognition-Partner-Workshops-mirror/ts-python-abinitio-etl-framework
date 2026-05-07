# Databricks notebook source
# MAGIC %md
# MAGIC # Account Status Schema
# MAGIC **Migrated from:** `dml/account_status.dml`
# MAGIC
# MAGIC ## Type Mapping
# MAGIC | DML Field   | Ab Initio Type   | Spark Type | Notes                                                    |
# MAGIC |-------------|------------------|------------|----------------------------------------------------------|
# MAGIC | id          | decimal(",")     | BIGINT     | Integer key                                              |
# MAGIC | padding1    | void(",")        | *(dropped)*| Ab Initio void = filler bytes; no semantic value         |
# MAGIC | name        | string(",")      | STRING     | Account holder name                                      |
# MAGIC | padding2    | void(",")        | *(dropped)*| Ab Initio void = filler bytes; dropped in migration      |
# MAGIC | status      | string("\n")     | STRING     | Account status code                                      |
# MAGIC
# MAGIC ### Migration Decision: Void Fields
# MAGIC Ab Initio `void` fields are padding/filler with no semantic value. They are
# MAGIC **dropped** during migration. If downstream consumers rely on field ordinal
# MAGIC positions, they must be updated to use named column access.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

account_status_schema = StructType([
    StructField("id", LongType(), nullable=False),
    # void field 'padding1' dropped - Ab Initio filler with no semantic value
    StructField("name", StringType(), nullable=True),
    # void field 'padding2' dropped - Ab Initio filler with no semantic value
    StructField("status", StringType(), nullable=True),
])

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog.bronze.account_status (
# MAGIC     id       BIGINT   NOT NULL  COMMENT 'Unique account identifier',
# MAGIC     name     STRING             COMMENT 'Account holder name',
# MAGIC     status   STRING             COMMENT 'Account status code (e.g., ACTIVE, CLOSED)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Account status records - migrated from dml/account_status.dml (void padding fields dropped)'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'bronze',
# MAGIC     'source.system' = 'ab_initio',
# MAGIC     'source.dml' = 'dml/account_status.dml'
# MAGIC );
