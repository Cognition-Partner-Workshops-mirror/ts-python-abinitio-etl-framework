# transaction_detail_schema.py — Converted from dml/transaction_detail.dml
# Ab Initio DML → PySpark StructType + Delta Lake DDL
#
# Type mapping decisions:
#   decimal(",") txn_id                          → LongType (transaction identifier)
#   datetime("YYYY-MM-DD HH24:MI:SS")(",")       → TimestampType (Ab Initio datetime format)
#   decimal(",") customer_id                     → LongType (FK to customer)
#   decimal(",") txn_type                        → IntegerType (transaction type code)
#
#   Nested record merchant_info:
#     string(",", null("")) merchant_name        → StringType (nullable, empty string = null)
#     string(",") merchant_category              → StringType
#     decimal("10.2", ",") amount                → DecimalType(10,2)
#
#   decimal(",") item_count                      → IntegerType
#   Nested array record[item_count] line_items:
#     string(",") sku                            → StringType
#     decimal(",") quantity                      → IntegerType
#     decimal("8.2", ",") line_total             → DecimalType(8,2)
#   → ArrayType(StructType(...))
#
#   Conditional record (if txn_type == 2) refund_details:
#     decimal(",") original_txn_id               → LongType (nullable — only present for refunds)
#     string(",") refund_reason                  → StringType (nullable)
#   → Flattened as nullable fields since Delta Lake doesn't support conditional schemas.
#
#   string("\n", null("UNKNOWN")) channel        → StringType (default "UNKNOWN" handled at ingestion)
#
# Note: Ab Initio conditional records (`if (txn_type == 2)`) have no direct equivalent in
# Delta Lake. Refund fields are included as nullable columns; non-refund rows will have NULLs.

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    DecimalType,
    TimestampType,
    ArrayType,
)

# Nested struct for merchant info — mirrors Ab Initio nested record
MERCHANT_INFO_TYPE = StructType([
    StructField("merchant_name", StringType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("amount", DecimalType(10, 2), nullable=True),
])

# Nested struct for line items — mirrors Ab Initio record[item_count]
LINE_ITEM_TYPE = StructType([
    StructField("sku", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("line_total", DecimalType(8, 2), nullable=True),
])

# Full transaction detail schema
TRANSACTION_DETAIL_SCHEMA = StructType([
    StructField("txn_id", LongType(), nullable=False),
    StructField("txn_timestamp", TimestampType(), nullable=True),
    StructField("customer_id", LongType(), nullable=True),
    StructField("txn_type", IntegerType(), nullable=True),
    # Nested merchant info record
    StructField("merchant_info", MERCHANT_INFO_TYPE, nullable=True),
    # Variable-length array of line items
    StructField("item_count", IntegerType(), nullable=True),
    StructField("line_items", ArrayType(LINE_ITEM_TYPE), nullable=True),
    # Conditional refund details — nullable (only populated when txn_type == 2)
    StructField("original_txn_id", LongType(), nullable=True),
    StructField("refund_reason", StringType(), nullable=True),
    # Channel with Ab Initio null default
    StructField("channel", StringType(), nullable=True),
])

# Delta Lake DDL
TRANSACTION_DETAIL_DDL = """
-- Converted from: dml/transaction_detail.dml
-- Complex schema with nested records, variable-length arrays, and conditional fields.
--
-- Ab Initio nested record → STRUCT<>
-- Ab Initio record[item_count] → ARRAY<STRUCT<>>
-- Ab Initio conditional record (if txn_type == 2) → nullable columns
-- Ab Initio null("UNKNOWN") → DEFAULT 'UNKNOWN' handled at ingestion layer
-- Ab Initio null("") → NULL mapping for empty strings
CREATE TABLE IF NOT EXISTS lakehouse.bronze.transaction_detail (
    txn_id            BIGINT         NOT NULL  COMMENT 'Ab Initio decimal — unique transaction ID',
    txn_timestamp     TIMESTAMP                COMMENT 'Ab Initio datetime("YYYY-MM-DD HH24:MI:SS") — transaction time',
    customer_id       BIGINT                   COMMENT 'Ab Initio decimal — FK to customer table',
    txn_type          INT                      COMMENT 'Ab Initio decimal — 1=purchase, 2=refund',

    -- Nested merchant info (Ab Initio nested record)
    merchant_info     STRUCT<
        merchant_name:     STRING COMMENT 'Ab Initio string with null("") — empty string maps to NULL',
        merchant_category: STRING,
        amount:            DECIMAL(10, 2) COMMENT 'Ab Initio decimal("10.2") — transaction amount'
    >                                          COMMENT 'Ab Initio nested record — merchant details',

    -- Variable-length line items (Ab Initio record[item_count])
    item_count        INT                      COMMENT 'Ab Initio decimal — number of line items',
    line_items        ARRAY<STRUCT<
        sku:        STRING,
        quantity:   INT,
        line_total: DECIMAL(8, 2) COMMENT 'Ab Initio decimal("8.2") — per-item total'
    >>                                         COMMENT 'Ab Initio record[item_count] — variable-length array of line items',

    -- Conditional refund details (Ab Initio: if txn_type == 2)
    original_txn_id   BIGINT                   COMMENT 'Ab Initio conditional field — only populated for refund transactions (txn_type=2)',
    refund_reason     STRING                   COMMENT 'Ab Initio conditional field — refund reason text',

    -- Channel with Ab Initio null handling
    channel           STRING                   COMMENT 'Ab Initio string with null("UNKNOWN") — defaults to UNKNOWN when absent'
)
USING DELTA
COMMENT 'Transaction detail table — complex migrated schema with nested structs, arrays, and conditional fields'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze',
    'source' = 'abinitio.dml.transaction_detail'
);
"""
