# Databricks notebook source
# MAGIC %md
# MAGIC # Order Items Schema
# MAGIC **Migrated from:** `dml/order_items.dml`
# MAGIC
# MAGIC ## Type Mapping
# MAGIC | DML Field        | Ab Initio Type                    | Spark Type        | Notes                                                        |
# MAGIC |------------------|-----------------------------------|-------------------|--------------------------------------------------------------|
# MAGIC | order_id         | decimal(",")                      | BIGINT            | Integer key; no fractional part                              |
# MAGIC | item_count       | decimal(",")                      | INT               | Count of items; used as array length in Ab Initio            |
# MAGIC | item_names       | string(",")[item_count]            | ARRAY<STRING>     | Variable-length array; Ab Initio repeat count → Spark array  |
# MAGIC | item_quantities  | decimal(",")[item_count]           | ARRAY<INT>        | Variable-length array of quantities                          |
# MAGIC | order_status     | string("\n")                      | STRING            | Terminal field                                               |
# MAGIC
# MAGIC ### Migration Decision: Variable-Length Arrays
# MAGIC Ab Initio `[item_count]` repeat groups are flattened into Spark `ARRAY` columns.
# MAGIC The `item_count` column is retained for backward compatibility but is redundant
# MAGIC (derivable via `SIZE(item_names)`).

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType, ArrayType,
)

order_items_schema = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("item_count", IntegerType(), nullable=True),
    StructField("item_names", ArrayType(StringType()), nullable=True),
    StructField("item_quantities", ArrayType(IntegerType()), nullable=True),
    StructField("order_status", StringType(), nullable=True),
])

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog.bronze.order_items (
# MAGIC     order_id         BIGINT          NOT NULL  COMMENT 'Unique order identifier',
# MAGIC     item_count       INT                       COMMENT 'Number of line items (retained for compat; derivable via SIZE(item_names))',
# MAGIC     item_names       ARRAY<STRING>             COMMENT 'Item names array (from Ab Initio variable-length repeat group)',
# MAGIC     item_quantities  ARRAY<INT>                COMMENT 'Item quantities array (parallel to item_names)',
# MAGIC     order_status     STRING                    COMMENT 'Order status code'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Order line items - migrated from dml/order_items.dml'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'bronze',
# MAGIC     'source.system' = 'ab_initio',
# MAGIC     'source.dml' = 'dml/order_items.dml'
# MAGIC );
