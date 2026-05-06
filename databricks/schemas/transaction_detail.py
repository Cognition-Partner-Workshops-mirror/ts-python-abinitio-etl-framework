"""
Delta Lake schema definition for transaction_detail record.

Source: dml/transaction_detail.dml
Type Mapping:
  - decimal(",")                       → LongType (integer identifiers)
  - datetime("YYYY-MM-DD HH24:MI:SS") → TimestampType
  - nested record ... end merchant_info → StructType (merchant sub-struct)
  - record[item_count] ... end line_items → ArrayType(StructType) for repeating group
  - decimal("10.2", ",")               → DecimalType(10, 2)
  - decimal("8.2", ",")                → DecimalType(8, 2)
  - if (txn_type == 2) record ... end  → nullable StructType (conditional block)
  - string(",", null("UNKNOWN"))       → StringType with default 'UNKNOWN'

Note: The conditional `if (txn_type == 2)` block is modelled as a nullable
struct — the refund_details fields will be NULL for non-refund transactions.
Spark does not support conditional schema definitions natively.
"""
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType,
    DecimalType, ArrayType,
)

merchant_info_schema = StructType([
    StructField("merchant_name", StringType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("amount", DecimalType(10, 2), nullable=True),
])

line_item_schema = StructType([
    StructField("sku", StringType(), nullable=True),
    StructField("quantity", LongType(), nullable=True),
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
    StructField("txn_type", LongType(), nullable=True),
    StructField("merchant_info", merchant_info_schema, nullable=True),
    StructField("item_count", LongType(), nullable=True),
    StructField("line_items", ArrayType(line_item_schema), nullable=True),
    StructField("refund_details", refund_details_schema, nullable=True),
    StructField("channel", StringType(), nullable=True),
])

TRANSACTION_DETAIL_DDL = """\
CREATE TABLE IF NOT EXISTS lakehouse.bronze.transaction_detail (
    txn_id          BIGINT         NOT NULL,
    txn_timestamp   TIMESTAMP,
    customer_id     BIGINT,
    txn_type        BIGINT,
    merchant_info   STRUCT<
        merchant_name:     STRING,
        merchant_category: STRING,
        amount:            DECIMAL(10,2)
    >,
    item_count      BIGINT,
    line_items      ARRAY<STRUCT<
        sku:        STRING,
        quantity:   BIGINT,
        line_total: DECIMAL(8,2)
    >>,
    refund_details  STRUCT<
        original_txn_id: BIGINT,
        refund_reason:   STRING
    >,
    channel         STRING DEFAULT 'UNKNOWN'
)
USING DELTA
COMMENT 'Transaction detail — migrated from Ab Initio dml/transaction_detail.dml (nested/conditional records)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
