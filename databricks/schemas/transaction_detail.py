# -----------------------------------------------------------------------------
# Delta Lake Schema: transaction_detail
# Source: dml/transaction_detail.dml
#
# Type Mapping:
#   decimal(",")                       -> LongType (integer key)
#   datetime("YYYY-MM-DD HH24:MI:SS") -> TimestampType
#   string(",", null(""))              -> StringType (nullable with empty-string sentinel)
#   decimal("10.2", ",")               -> DecimalType(10,2)
#   decimal("8.2", ",")                -> DecimalType(8,2)
#   string("\n", null("UNKNOWN"))      -> StringType (nullable with "UNKNOWN" sentinel)
#
# Nested records:
#   Ab Initio nested `record ... end merchant_info` -> Spark StructType
#   Ab Initio `record[item_count] ... end line_items` -> ArrayType(StructType)
#   Ab Initio conditional `if (txn_type == 2) record ... end` -> nullable StructType
#     (always present in schema; NULL when condition not met)
# -----------------------------------------------------------------------------
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
    StructField("txn_type", IntegerType(), nullable=True),
    StructField("merchant_info", merchant_info_schema, nullable=True),
    StructField("item_count", IntegerType(), nullable=True),
    StructField("line_items", ArrayType(line_item_schema), nullable=True),
    StructField("refund_details", refund_details_schema, nullable=True),
    StructField("channel", StringType(), nullable=True),
])

# Delta Lake DDL
# Note: nested structs and arrays use Spark SQL syntax.
# The conditional `refund_details` is always present but NULL for non-refund txns.
TRANSACTION_DETAIL_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.bronze.transaction_detail (
    txn_id          BIGINT          NOT NULL  COMMENT 'Primary key — mapped from Ab Initio decimal',
    txn_timestamp   TIMESTAMP                 COMMENT 'Mapped from Ab Initio datetime("YYYY-MM-DD HH24:MI:SS")',
    customer_id     BIGINT                    COMMENT 'Foreign key to customer — mapped from Ab Initio decimal',
    txn_type        INT                       COMMENT 'Transaction type code (2=refund) — mapped from Ab Initio decimal',
    merchant_info   STRUCT<
        merchant_name: STRING,
        merchant_category: STRING,
        amount: DECIMAL(10,2)
    >                                         COMMENT 'Nested merchant record — mapped from Ab Initio nested record',
    item_count      INT                       COMMENT 'Line item count — mapped from Ab Initio decimal',
    line_items      ARRAY<STRUCT<
        sku: STRING,
        quantity: BIGINT,
        line_total: DECIMAL(8,2)
    >>                                        COMMENT 'Variable-length line items — mapped from Ab Initio record[item_count]',
    refund_details  STRUCT<
        original_txn_id: BIGINT,
        refund_reason: STRING
    >                                         COMMENT 'Conditional refund info — NULL when txn_type != 2. Mapped from Ab Initio if(txn_type==2) record',
    channel         STRING                    COMMENT 'Origination channel — null sentinel "UNKNOWN" mapped to SQL NULL'
)
USING DELTA
COMMENT 'Transaction detail with nested structs and conditional records — migrated from dml/transaction_detail.dml'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
"""
