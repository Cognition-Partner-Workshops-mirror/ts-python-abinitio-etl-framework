# --------------------------------------------------------------------------
# transaction_detail.py — Databricks Delta Lake schema for Ab Initio transaction_detail.dml
#
# Source DML (dml/transaction_detail.dml):
#   record
#     decimal(",") txn_id;
#     datetime("YYYY-MM-DD HH24:MI:SS")(",") txn_timestamp;
#     decimal(",") customer_id;
#     decimal(",") txn_type;
#     record                                  ← nested struct: merchant_info
#       string(",", null("")) merchant_name;
#       string(",") merchant_category;
#       decimal("10.2", ",") amount;
#     end merchant_info;
#     decimal(",") item_count;
#     record[item_count]                      ← variable-length array of structs
#       string(",") sku;
#       decimal(",") quantity;
#       decimal("8.2", ",") line_total;
#     end line_items;
#     if (txn_type == 2)                      ← conditional record (refunds only)
#       record
#         decimal(",") original_txn_id;
#         string(",") refund_reason;
#       end refund_details;
#     string("\n", null("UNKNOWN")) channel;
#   end;
#
# Type Mapping Decisions:
#   - decimal → LongType: integer identifiers (txn_id, customer_id, txn_type)
#   - decimal("10.2") → DecimalType(10,2): fixed-precision monetary amount
#   - decimal("8.2") → DecimalType(8,2): line item totals
#   - datetime("YYYY-MM-DD HH24:MI:SS") → TimestampType: Spark native timestamp
#   - Nested record → StructType: Ab Initio nested record → Spark struct
#   - record[item_count] → ArrayType(StructType): variable-length array of structs
#   - Conditional record (if txn_type==2) → nullable StructType: always present in
#     schema but NULL for non-refund transactions; Spark has no conditional fields
#   - null("") / null("UNKNOWN") → Spark nullable with default handled in ETL logic
# --------------------------------------------------------------------------
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType,
    DecimalType, ArrayType
)

# Nested struct for merchant information (Ab Initio: nested record merchant_info)
merchant_info_schema = StructType([
    StructField("merchant_name", StringType(), nullable=True),      # null("") → nullable
    StructField("merchant_category", StringType(), nullable=True),
    StructField("amount", DecimalType(10, 2), nullable=True),       # decimal("10.2")
])

# Nested struct for order line items (Ab Initio: record[item_count] line_items)
line_item_schema = StructType([
    StructField("sku", StringType(), nullable=True),
    StructField("quantity", LongType(), nullable=True),
    StructField("line_total", DecimalType(8, 2), nullable=True),    # decimal("8.2")
])

# Conditional refund details struct (Ab Initio: if txn_type==2 → refund_details)
# In Spark, this is always present but NULL for non-refund transactions
refund_details_schema = StructType([
    StructField("original_txn_id", LongType(), nullable=True),
    StructField("refund_reason", StringType(), nullable=True),
])

# Full PySpark StructType for transaction_detail
transaction_detail_schema = StructType([
    StructField("txn_id", LongType(), nullable=False),
    StructField("txn_timestamp", TimestampType(), nullable=True),
    StructField("customer_id", LongType(), nullable=True),
    StructField("txn_type", LongType(), nullable=True),
    StructField("merchant_info", merchant_info_schema, nullable=True),
    StructField("item_count", LongType(), nullable=True),
    StructField("line_items", ArrayType(line_item_schema), nullable=True),
    StructField("refund_details", refund_details_schema, nullable=True),  # NULL when txn_type != 2
    StructField("channel", StringType(), nullable=True),                  # null("UNKNOWN") → handle in ETL
])

# Delta Lake DDL
TRANSACTION_DETAIL_DDL = """
-- Delta Lake table for transaction detail records
-- Migrated from: dml/transaction_detail.dml
-- Note: Ab Initio conditional record (if txn_type==2) mapped to nullable struct
-- Note: Ab Initio variable-length array record[item_count] mapped to ARRAY<STRUCT>
CREATE TABLE IF NOT EXISTS catalog.bronze.transaction_detail (
    txn_id           BIGINT          NOT NULL  COMMENT 'Transaction ID (Ab Initio: decimal)',
    txn_timestamp    TIMESTAMP                 COMMENT 'Transaction time (Ab Initio: datetime YYYY-MM-DD HH24:MI:SS)',
    customer_id      BIGINT                    COMMENT 'Customer FK (Ab Initio: decimal)',
    txn_type         BIGINT                    COMMENT 'Transaction type: 1=purchase, 2=refund (Ab Initio: decimal)',
    merchant_info    STRUCT<
        merchant_name: STRING,
        merchant_category: STRING,
        amount: DECIMAL(10,2)
    >                                          COMMENT 'Merchant details (Ab Initio: nested record)',
    item_count       BIGINT                    COMMENT 'Number of line items (Ab Initio: decimal)',
    line_items       ARRAY<STRUCT<
        sku: STRING,
        quantity: BIGINT,
        line_total: DECIMAL(8,2)
    >>                                         COMMENT 'Order line items (Ab Initio: record[item_count])',
    refund_details   STRUCT<
        original_txn_id: BIGINT,
        refund_reason: STRING
    >                                          COMMENT 'Refund info — NULL for non-refund txns (Ab Initio: conditional record if txn_type==2)',
    channel          STRING                    COMMENT 'Transaction channel — defaults to UNKNOWN (Ab Initio: string null=UNKNOWN)',
    _loaded_at       TIMESTAMP       NOT NULL  DEFAULT current_timestamp()  COMMENT 'Databricks ingestion timestamp',
    _source_file     STRING                    COMMENT 'Source file path for lineage tracking'
)
USING DELTA
COMMENT 'Transaction detail table — migrated from Ab Initio transaction_detail.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'bronze'
);
"""
