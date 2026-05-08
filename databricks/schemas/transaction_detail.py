"""
Delta Lake schema definition for Transaction Detail record.

Migrated from: dml/transaction_detail.dml
Ab Initio DML → PySpark StructType + Delta Lake DDL

Type Mapping Decisions:
  - decimal(",") txn_id                      → LongType (integer identifier)
  - datetime("YYYY-MM-DD HH24:MI:SS")(",")  → TimestampType (full timestamp)
  - decimal(",") customer_id                 → LongType (FK to customer)
  - decimal(",") txn_type                    → IntegerType (1=purchase, 2=refund)
  - record merchant_info (nested struct):
      - string(",", null("")) merchant_name  → StringType (nullable, empty=null)
      - string(",") merchant_category        → StringType
      - decimal("10.2", ",") amount          → DecimalType(10,2) (monetary)
  - decimal(",") item_count                  → IntegerType (array length prefix)
  - record[item_count] line_items (array of structs):
      - string(",") sku                      → StringType
      - decimal(",") quantity                → IntegerType
      - decimal("8.2", ",") line_total       → DecimalType(8,2) (monetary)
  - if (txn_type == 2) refund_details (conditional record):
      - decimal(",") original_txn_id         → LongType (nullable, only for refunds)
      - string(",") refund_reason            → StringType (nullable, only for refunds)
  - string("\n", null("UNKNOWN")) channel    → StringType (default "UNKNOWN" → nullable)

Note: Ab Initio conditional records ('if (txn_type == 2)') have no direct Spark
      equivalent. The refund fields are included as nullable columns. A CHECK
      constraint or view can enforce the conditional logic at query time.

Note: Ab Initio null("") means empty string maps to NULL. In Spark, configure
      .option("nullValue", "") during CSV ingestion, or use a post-load transform.
"""

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

# Nested struct for merchant information
MERCHANT_INFO_STRUCT = StructType([
    StructField("merchant_name", StringType(), nullable=True),      # null("") → nullable
    StructField("merchant_category", StringType(), nullable=False),
    StructField("amount", DecimalType(10, 2), nullable=False),
])

# Nested struct for line items (array element)
LINE_ITEM_STRUCT = StructType([
    StructField("sku", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("line_total", DecimalType(8, 2), nullable=False),
])

# Nested struct for refund details (conditional — nullable in Delta)
REFUND_DETAILS_STRUCT = StructType([
    StructField("original_txn_id", LongType(), nullable=True),
    StructField("refund_reason", StringType(), nullable=True),
])

# Full PySpark StructType — preserves nested structure from Ab Initio DML
TRANSACTION_DETAIL_SCHEMA = StructType([
    StructField("txn_id", LongType(), nullable=False),
    StructField("txn_timestamp", TimestampType(), nullable=False),
    StructField("customer_id", LongType(), nullable=False),
    StructField("txn_type", IntegerType(), nullable=False),
    StructField("merchant_info", MERCHANT_INFO_STRUCT, nullable=False),
    StructField("item_count", IntegerType(), nullable=False),
    StructField("line_items", ArrayType(LINE_ITEM_STRUCT), nullable=True),
    # Conditional record: only populated when txn_type == 2 (refund)
    StructField("refund_details", REFUND_DETAILS_STRUCT, nullable=True),
    StructField("channel", StringType(), nullable=True),  # null("UNKNOWN") → nullable
])

# Delta Lake DDL — flattened for broad SQL compatibility
# Nested structs preserved where Delta/Spark SQL supports them natively
TRANSACTION_DETAIL_DDL = """
-- Delta Lake table definition for Transaction Detail record
-- Migrated from: dml/transaction_detail.dml
-- Contains nested structs, variable-length arrays, and conditional fields
CREATE TABLE IF NOT EXISTS catalog.schema.transaction_detail (
    txn_id            BIGINT                     NOT NULL  COMMENT 'Ab Initio decimal → BIGINT (transaction key)',
    txn_timestamp     TIMESTAMP                  NOT NULL  COMMENT 'Ab Initio datetime("YYYY-MM-DD HH24:MI:SS") → TIMESTAMP',
    customer_id       BIGINT                     NOT NULL  COMMENT 'Ab Initio decimal → BIGINT (FK to customer)',
    txn_type          INT                        NOT NULL  COMMENT 'Ab Initio decimal → INT (1=purchase, 2=refund)',

    -- Nested struct: merchant_info
    merchant_info     STRUCT<
                          merchant_name: STRING,
                          merchant_category: STRING,
                          amount: DECIMAL(10, 2)
                      >                          NOT NULL  COMMENT 'Ab Initio nested record → STRUCT',

    item_count        INT                        NOT NULL  COMMENT 'Ab Initio decimal → INT (array length prefix)',

    -- Variable-length array of structs: line_items[item_count]
    line_items        ARRAY<STRUCT<
                          sku: STRING,
                          quantity: INT,
                          line_total: DECIMAL(8, 2)
                      >>                                   COMMENT 'Ab Initio record[item_count] → ARRAY<STRUCT>',

    -- Conditional record: only populated when txn_type == 2
    refund_details    STRUCT<
                          original_txn_id: BIGINT,
                          refund_reason: STRING
                      >                                    COMMENT 'Ab Initio if(txn_type==2) → nullable STRUCT (refunds only)',

    channel           STRING                               COMMENT 'Ab Initio string null("UNKNOWN") → STRING (nullable)'
)
USING DELTA
COMMENT 'Transaction detail record — migrated from Ab Initio DML (nested structs, conditional fields)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'silver',
    'source.system' = 'abinitio',
    'source.dml' = 'dml/transaction_detail.dml'
);
"""
