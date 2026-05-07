# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Schema
# MAGIC **Migrated from:** `dml/customer.dml`
# MAGIC
# MAGIC ## Type Mapping
# MAGIC | DML Field       | Ab Initio Type         | Spark Type    | Notes                                      |
# MAGIC |-----------------|------------------------|---------------|--------------------------------------------|
# MAGIC | customer_id     | decimal(",")           | BIGINT        | Integer key; no fractional part in source   |
# MAGIC | first_name      | string(",")            | STRING        | Direct mapping                             |
# MAGIC | last_name       | string(",")            | STRING        | Direct mapping                             |
# MAGIC | email           | string("\n")           | STRING        | Newline-delimited terminal field            |

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

customer_schema = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
])

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog.bronze.customer (
# MAGIC     customer_id   BIGINT       NOT NULL  COMMENT 'Unique customer identifier (Ab Initio decimal)',
# MAGIC     first_name    STRING                 COMMENT 'Customer first name',
# MAGIC     last_name     STRING                 COMMENT 'Customer last name',
# MAGIC     email         STRING                 COMMENT 'Customer email address'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Customer master record - migrated from dml/customer.dml'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'bronze',
# MAGIC     'source.system' = 'ab_initio',
# MAGIC     'source.dml' = 'dml/customer.dml'
# MAGIC );
