# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Address Schema
# MAGIC **Migrated from:** `dml/customer_address.dml`
# MAGIC
# MAGIC ## Type Mapping
# MAGIC | DML Field        | Ab Initio Type           | Spark Type                    | Notes                                                  |
# MAGIC |------------------|--------------------------|-------------------------------|--------------------------------------------------------|
# MAGIC | customer_id      | decimal(",")             | BIGINT                        | FK to customer table                                   |
# MAGIC | name             | string(",")              | STRING                        | Customer name                                          |
# MAGIC | address (struct)  | address_t (included type)| STRUCT<street,city,state,zip> | Reusable type from common_address.dml → nested struct  |
# MAGIC | phone            | string("\n")             | STRING                        | Terminal field                                         |
# MAGIC
# MAGIC ### Migration Decision: Include Directive
# MAGIC Ab Initio `include "common_address.dml"` imports the `address_t` type. In Spark
# MAGIC this is realized by embedding the `address_schema` StructType from
# MAGIC `common_address.py` as a nested struct column.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

# Import the reusable address struct from common_address schema
from databricks.schemas.common_address import address_schema

customer_address_schema = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("address", address_schema, nullable=True),
    StructField("phone", StringType(), nullable=True),
])

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog.bronze.customer_address (
# MAGIC     customer_id  BIGINT       NOT NULL  COMMENT 'FK to customer table',
# MAGIC     name         STRING                 COMMENT 'Customer name',
# MAGIC     address      STRUCT<
# MAGIC                      street: STRING,
# MAGIC                      city: STRING,
# MAGIC                      state: STRING,
# MAGIC                      zip: STRING
# MAGIC                  >                      COMMENT 'Address struct (from reusable type address_t in common_address.dml)',
# MAGIC     phone        STRING                 COMMENT 'Customer phone number'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Customer addresses with nested address struct - migrated from dml/customer_address.dml'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'bronze',
# MAGIC     'source.system' = 'ab_initio',
# MAGIC     'source.dml' = 'dml/customer_address.dml'
# MAGIC );
