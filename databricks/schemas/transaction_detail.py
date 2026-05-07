# Databricks notebook source
# MAGIC %md
# MAGIC # Transaction Detail Schema
# MAGIC **Migrated from:** `dml/transaction_detail.dml`
# MAGIC
# MAGIC ## Type Mapping
# MAGIC | DML Field                        | Ab Initio Type                          | Spark Type              | Notes                                                      |
# MAGIC |----------------------------------|-----------------------------------------|-------------------------|------------------------------------------------------------|
# MAGIC | txn_id                           | decimal(",")                            | BIGINT                  | Integer transaction key                                    |
# MAGIC | txn_timestamp                    | datetime("YYYY-MM-DD HH24:MI:SS")(",") | TIMESTAMP               | Direct mapping; Ab Initio format → Spark TimestampType     |
# MAGIC | customer_id                      | decimal(",")                            | BIGINT                  | FK to customer                                             |
# MAGIC | txn_type                         | decimal(",")                            | INT                     | Transaction type code (1=purchase, 2=refund)               |
# MAGIC | merchant_info.merchant_name      | string(",", null(""))                   | STRING                  | Nested record → struct field; empty string maps to NULL    |
# MAGIC | merchant_info.merchant_category  | string(",")                             | STRING                  | Merchant category                                          |
# MAGIC | merchant_info.amount             | decimal("10.2", ",")                    | DECIMAL(10,2)           | Precision/scale preserved from Ab Initio                   |
# MAGIC | item_count                       | decimal(",")                            | INT                     | Count for variable-length line_items                       |
# MAGIC | line_items[].sku                 | string(",")                             | STRING                  | Nested repeat group → ARRAY<STRUCT>                        |
# MAGIC | line_items[].quantity            | decimal(",")                            | INT                     | Item quantity                                              |
# MAGIC | line_items[].line_total          | decimal("8.2", ",")                     | DECIMAL(8,2)            | Line amount with precision                                 |
# MAGIC | refund_details.original_txn_id   | decimal(",")                            | BIGINT                  | Conditional record (txn_type==2) → nullable struct         |
# MAGIC | refund_details.refund_reason     | string(",")                             | STRING                  | Refund reason text                                         |
# MAGIC | channel                          | string("\n", null("UNKNOWN"))           | STRING                  | Default "UNKNOWN" handled via COALESCE at read time        |
# MAGIC
# MAGIC ### Migration Decisions
# MAGIC - **Nested records** (`merchant_info`, `refund_details`): Mapped to Spark `StructType` fields.
# MAGIC - **Conditional record** (`if txn_type == 2`): `refund_details` struct is nullable; NULL when txn_type != 2.
# MAGIC - **Variable-length repeat** (`line_items[item_count]`): Mapped to `ARRAY<STRUCT>`.
# MAGIC - **Null sentinels** (`null("")`, `null("UNKNOWN")`): Handled at ingestion via COALESCE / NULLIF transforms.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType,
    TimestampType, DecimalType, ArrayType,
)

merchant_info_schema = StructType([
    StructField("merchant_name", StringType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("amount", DecimalType(10, 2), nullable=True),
])

line_item_schema = StructType([
    StructField("sku", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("line_total", DecimalType(8, 2), nullable=True),
])

refund_details_schema = StructType([
    StructField("original_txn_id", LongType(), nullable=True),
    StructField("refund_reason", StringType(), nullable=True),
])

transaction_detail_schema = StructType([
    StructField("txn_id", LongType(), nullable=False),
    StructField("txn_timestamp", TimestampType(), nullable=True),
    StructField("customer_id", LongType(), nullable=True),
    StructField("txn_type", IntegerType(), nullable=True),
    StructField("merchant_info", merchant_info_schema, nullable=True),
    StructField("item_count", IntegerType(), nullable=True),
    StructField("line_items", ArrayType(line_item_schema), nullable=True),
    StructField("refund_details", refund_details_schema, nullable=True),
    StructField("channel", StringType(), nullable=True),
])

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog.bronze.transaction_detail (
# MAGIC     txn_id          BIGINT                NOT NULL  COMMENT 'Unique transaction identifier',
# MAGIC     txn_timestamp   TIMESTAMP                       COMMENT 'Transaction timestamp (YYYY-MM-DD HH24:MI:SS)',
# MAGIC     customer_id     BIGINT                          COMMENT 'FK to customer table',
# MAGIC     txn_type        INT                             COMMENT 'Transaction type: 1=purchase, 2=refund',
# MAGIC     merchant_info   STRUCT<
# MAGIC                         merchant_name: STRING,
# MAGIC                         merchant_category: STRING,
# MAGIC                         amount: DECIMAL(10,2)
# MAGIC                     >                               COMMENT 'Merchant details (nested record from Ab Initio)',
# MAGIC     item_count      INT                             COMMENT 'Number of line items',
# MAGIC     line_items      ARRAY<STRUCT<
# MAGIC                         sku: STRING,
# MAGIC                         quantity: INT,
# MAGIC                         line_total: DECIMAL(8,2)
# MAGIC                     >>                              COMMENT 'Order line items (variable-length repeat group)',
# MAGIC     refund_details  STRUCT<
# MAGIC                         original_txn_id: BIGINT,
# MAGIC                         refund_reason: STRING
# MAGIC                     >                               COMMENT 'Refund info; NULL when txn_type != 2 (conditional record)',
# MAGIC     channel         STRING                          COMMENT 'Transaction channel; COALESCE to handle null sentinel UNKNOWN'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Transaction details with nested merchant and line item structs - migrated from dml/transaction_detail.dml'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'bronze',
# MAGIC     'source.system' = 'ab_initio',
# MAGIC     'source.dml' = 'dml/transaction_detail.dml'
# MAGIC );
