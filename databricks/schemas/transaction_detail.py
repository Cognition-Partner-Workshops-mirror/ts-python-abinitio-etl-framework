"""
Delta Lake schema for transaction detail records.

Migrated from: dml/transaction_detail.dml
Ab Initio record layout → PySpark StructType + Delta Lake DDL.

This is the most complex DML in the estate — it features nested records, variable-length
arrays of records, conditional (if/then) sub-records, and nullable fields.

Type Mapping Decisions:
  - decimal(",") txn_id                   → LongType: Transaction identifier.
  - datetime("YYYY-MM-DD HH24:MI:SS")    → TimestampType: Ab Initio datetime with format
    string maps directly to Spark TimestampType. The format string is a serialization
    detail; Spark handles timestamp parsing at read time.
  - decimal(",") customer_id              → LongType: FK to customer.
  - decimal(",") txn_type                 → IntegerType: 1=purchase, 2=refund.

  Nested record merchant_info:
  - string(",", null("")) merchant_name   → StringType (nullable): Ab Initio null("")
    maps to Spark NULL when the field is an empty string. Handled via ingestion logic.
  - string(",") merchant_category         → StringType
  - decimal("10.2", ",") amount           → DecimalType(10,2): Precision/scale preserved.

  Variable-length array of records line_items[item_count]:
  - Modeled as ArrayType(StructType) in Spark. Each element has sku, quantity, line_total.
  - decimal("8.2") line_total             → DecimalType(8,2)

  Conditional record refund_details (if txn_type == 2):
  - Modeled as a nullable StructType in Spark. Populated only for refund transactions.
  - NULL for non-refund rows (txn_type != 2).

  - string("\n", null("UNKNOWN")) channel → StringType (nullable): "UNKNOWN" sentinel
    mapped to NULL at ingestion time.
"""

from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType,
    TimestampType, DecimalType, ArrayType,
)


# Nested struct for merchant information
merchant_info_struct = StructType([
    # merchant_name: Ab Initio string with null("") → nullable StringType
    StructField("merchant_name", StringType(), nullable=True),
    # merchant_category: Ab Initio string(",") → StringType
    StructField("merchant_category", StringType(), nullable=True),
    # amount: Ab Initio decimal("10.2") → DecimalType(10,2)
    StructField("amount", DecimalType(10, 2), nullable=True),
])

# Nested struct for individual line items within the variable-length array
line_item_struct = StructType([
    # sku: Ab Initio string(",") → StringType
    StructField("sku", StringType(), nullable=True),
    # quantity: Ab Initio decimal(",") → IntegerType
    StructField("quantity", IntegerType(), nullable=True),
    # line_total: Ab Initio decimal("8.2") → DecimalType(8,2)
    StructField("line_total", DecimalType(8, 2), nullable=True),
])

# Conditional nested struct for refund details (only populated when txn_type == 2)
refund_details_struct = StructType([
    # original_txn_id: Ab Initio decimal(",") → LongType
    StructField("original_txn_id", LongType(), nullable=True),
    # refund_reason: Ab Initio string(",") → StringType
    StructField("refund_reason", StringType(), nullable=True),
])

# PySpark StructType definition for the full transaction_detail record
transaction_detail_schema = StructType([
    # txn_id: Ab Initio decimal(",") → LongType
    StructField("txn_id", LongType(), nullable=False),
    # txn_timestamp: Ab Initio datetime("YYYY-MM-DD HH24:MI:SS") → TimestampType
    StructField("txn_timestamp", TimestampType(), nullable=True),
    # customer_id: Ab Initio decimal(",") → LongType (FK to customer)
    StructField("customer_id", LongType(), nullable=True),
    # txn_type: Ab Initio decimal(",") → IntegerType (1=purchase, 2=refund)
    StructField("txn_type", IntegerType(), nullable=True),
    # merchant_info: Ab Initio nested record → StructType
    StructField("merchant_info", merchant_info_struct, nullable=True),
    # item_count: Ab Initio decimal(",") → IntegerType (array length hint)
    StructField("item_count", IntegerType(), nullable=True),
    # line_items: Ab Initio record[item_count] → ArrayType(StructType)
    StructField("line_items", ArrayType(line_item_struct), nullable=True),
    # refund_details: Ab Initio conditional record (if txn_type==2) → nullable StructType
    StructField("refund_details", refund_details_struct, nullable=True),
    # channel: Ab Initio string with null("UNKNOWN") → nullable StringType
    StructField("channel", StringType(), nullable=True),
])


# Delta Lake DDL with nested STRUCTs and ARRAY for the complex record
TRANSACTION_DETAIL_DDL = """
-- Delta Lake table for transaction detail records with nested/array/conditional structures.
-- Migrated from Ab Initio DML: dml/transaction_detail.dml
--
-- Key mapping decisions:
--   - Ab Initio nested records → Delta STRUCT<>
--   - Ab Initio record[count] (variable-length array of records) → Delta ARRAY<STRUCT<>>
--   - Ab Initio conditional record (if txn_type==2) → Nullable STRUCT (NULL for non-refunds)
--   - Ab Initio null("") and null("UNKNOWN") → handled as NULL at ingestion time
CREATE TABLE IF NOT EXISTS catalog.bronze.transaction_detail (
    txn_id            BIGINT         NOT NULL  COMMENT 'Transaction identifier',
    txn_timestamp     TIMESTAMP                COMMENT 'Transaction time (Ab Initio datetime)',
    customer_id       BIGINT                   COMMENT 'Customer FK',
    txn_type          INT                      COMMENT 'Transaction type: 1=purchase, 2=refund',
    merchant_info     STRUCT<
        merchant_name:     STRING COMMENT 'Merchant name (NULL if empty string in source)',
        merchant_category: STRING COMMENT 'Merchant category code',
        amount:            DECIMAL(10,2) COMMENT 'Transaction amount'
    >                                          COMMENT 'Nested merchant info (Ab Initio sub-record)',
    item_count        INT                      COMMENT 'Number of line items (redundant with array length)',
    line_items        ARRAY<STRUCT<
        sku:        STRING COMMENT 'Product SKU',
        quantity:   INT    COMMENT 'Quantity ordered',
        line_total: DECIMAL(8,2) COMMENT 'Line item total'
    >>                                         COMMENT 'Line items array (Ab Initio record[item_count])',
    refund_details    STRUCT<
        original_txn_id: BIGINT COMMENT 'Original transaction being refunded',
        refund_reason:   STRING COMMENT 'Reason for refund'
    >                                          COMMENT 'Refund info — NULL for non-refund txns (Ab Initio conditional record)',
    channel           STRING                   COMMENT 'Transaction channel (WEB/STORE/APP; NULL replaces UNKNOWN sentinel)'
)
USING DELTA
PARTITIONED BY (txn_type)
COMMENT 'Transaction detail table — migrated from Ab Initio dml/transaction_detail.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
