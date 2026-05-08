"""
Delta Lake schema for Transaction Detail record.

Source: dml/transaction_detail.dml
Migrated from Ab Initio DML to PySpark StructType + Delta Lake DDL.

Type Mapping:
  - decimal(",")                          -> LongType (integer identifiers)
  - datetime("YYYY-MM-DD HH24:MI:SS")    -> TimestampType
  - decimal("10.2", ",")                  -> DecimalType(10,2) (monetary amounts)
  - decimal("8.2", ",")                   -> DecimalType(8,2) (line item totals)
  - string(",", null(""))                 -> StringType (nullable with empty-string sentinel)
  - string("\n", null("UNKNOWN"))         -> StringType (nullable with "UNKNOWN" sentinel)

Notes:
  - Ab Initio nested records (merchant_info, line_items, refund_details) are
    flattened into the main struct with prefixed column names for Delta Lake.
  - The conditional record (if txn_type == 2) for refund_details is always
    present in the schema but populated only for refund transactions.
  - Variable-length line_items array uses ArrayType(StructType) in Spark.
"""

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType,
    DecimalType, ArrayType,
)


# Nested struct for merchant information (Ab Initio nested record "merchant_info")
_merchant_info_struct = StructType([
    # merchant_name: Ab Initio string(",", null("")) -> nullable StringType
    StructField("merchant_name", StringType(), nullable=True),
    # merchant_category: Ab Initio string(",") -> StringType
    StructField("merchant_category", StringType(), nullable=True),
    # amount: Ab Initio decimal("10.2", ",") -> DecimalType(10,2)
    StructField("amount", DecimalType(10, 2), nullable=True),
])

# Nested struct for line items (Ab Initio variable-length record array "line_items")
_line_item_struct = StructType([
    # sku: Ab Initio string(",") -> StringType
    StructField("sku", StringType(), nullable=True),
    # quantity: Ab Initio decimal(",") -> LongType
    StructField("quantity", LongType(), nullable=True),
    # line_total: Ab Initio decimal("8.2", ",") -> DecimalType(8,2)
    StructField("line_total", DecimalType(8, 2), nullable=True),
])

# Nested struct for refund details (Ab Initio conditional record "refund_details")
_refund_details_struct = StructType([
    # original_txn_id: Ab Initio decimal(",") -> LongType
    StructField("original_txn_id", LongType(), nullable=True),
    # refund_reason: Ab Initio string(",") -> StringType
    StructField("refund_reason", StringType(), nullable=True),
])


# PySpark StructType definition equivalent to transaction_detail.dml record layout
transaction_detail_schema = StructType([
    # txn_id: Ab Initio decimal(",") -> Spark LongType (transaction key)
    StructField("txn_id", LongType(), nullable=False),
    # txn_timestamp: Ab Initio datetime("YYYY-MM-DD HH24:MI:SS") -> TimestampType
    StructField("txn_timestamp", TimestampType(), nullable=True),
    # customer_id: Ab Initio decimal(",") -> LongType (FK to customer)
    StructField("customer_id", LongType(), nullable=True),
    # txn_type: Ab Initio decimal(",") -> LongType (1=purchase, 2=refund)
    StructField("txn_type", LongType(), nullable=True),
    # merchant_info: Ab Initio nested record -> Spark StructType
    StructField("merchant_info", _merchant_info_struct, nullable=True),
    # item_count: Ab Initio decimal(",") -> LongType (array length indicator)
    StructField("item_count", LongType(), nullable=True),
    # line_items: Ab Initio record[item_count] -> ArrayType(StructType)
    StructField("line_items", ArrayType(_line_item_struct), nullable=True),
    # refund_details: Ab Initio conditional record (if txn_type==2) -> nullable StructType
    StructField("refund_details", _refund_details_struct, nullable=True),
    # channel: Ab Initio string("\n", null("UNKNOWN")) -> StringType
    StructField("channel", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
TRANSACTION_DETAIL_DDL = """
-- Delta Lake table definition for Transaction Detail record
-- Source: dml/transaction_detail.dml
-- Complex nested record with conditional fields and variable-length arrays
CREATE TABLE IF NOT EXISTS catalog.bronze.transaction_detail (
    txn_id              BIGINT          NOT NULL  COMMENT 'Unique transaction identifier',
    txn_timestamp       TIMESTAMP                 COMMENT 'Transaction timestamp (YYYY-MM-DD HH24:MI:SS)',
    customer_id         BIGINT                    COMMENT 'FK to customer table',
    txn_type            BIGINT                    COMMENT 'Transaction type: 1=purchase, 2=refund',
    merchant_info       STRUCT<
        merchant_name:      STRING    COMMENT 'Merchant name (nullable, empty-string sentinel in Ab Initio)',
        merchant_category:  STRING    COMMENT 'Merchant category code',
        amount:             DECIMAL(10,2) COMMENT 'Transaction amount'
    >                                             COMMENT 'Nested merchant information (Ab Initio nested record)',
    item_count          BIGINT                    COMMENT 'Number of line items',
    line_items          ARRAY<STRUCT<
        sku:        STRING            COMMENT 'Item SKU code',
        quantity:   BIGINT            COMMENT 'Item quantity',
        line_total: DECIMAL(8,2)      COMMENT 'Line item total amount'
    >>                                            COMMENT 'Variable-length line items array (Ab Initio record[item_count])',
    refund_details      STRUCT<
        original_txn_id:  BIGINT      COMMENT 'Original transaction ID for refund',
        refund_reason:    STRING      COMMENT 'Reason for refund'
    >                                             COMMENT 'Conditional refund details (populated when txn_type=2)',
    channel             STRING                    COMMENT 'Transaction channel (default: UNKNOWN)'
)
USING DELTA
PARTITIONED BY (txn_type)
COMMENT 'Transaction details with nested structs and arrays - migrated from Ab Initio DML (transaction_detail.dml)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality'                    = 'bronze',
    'source.system'              = 'ab_initio',
    'source.dml'                 = 'transaction_detail.dml'
);
"""
