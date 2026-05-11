"""
Delta Lake schema for transaction_detail.dml — Complex nested transaction records.

Source DML (Ab Initio):
    record
      decimal(",") txn_id;
      datetime("YYYY-MM-DD HH24:MI:SS")(",") txn_timestamp;
      decimal(",") customer_id;
      decimal(",") txn_type;

      record
        string(",", null("")) merchant_name;
        string(",") merchant_category;
        decimal("10.2", ",") amount;
      end merchant_info;

      decimal(",") item_count;
      record[item_count]
        string(",") sku;
        decimal(",") quantity;
        decimal("8.2", ",") line_total;
      end line_items;

      if (txn_type == 2)
        record
          decimal(",") original_txn_id;
          string(",") refund_reason;
        end refund_details;

      string("\\n", null("UNKNOWN")) channel;
    end;

Type Mapping Applied:
    Ab Initio decimal (bare) → Spark LongType
    Ab Initio datetime("YYYY-MM-DD HH24:MI:SS") → Spark TimestampType
        Rationale: The format string describes flat-file serialization. Delta Lake
        uses Parquet's native TIMESTAMP (microsecond precision). The "HH24:MI:SS"
        Ab Initio format maps naturally to 24-hour time.
    Ab Initio nested record → Spark StructType (embedded struct column)
        Rationale: Ab Initio's "record ... end name" pattern defines an inline
        nested record. In Delta Lake this becomes a STRUCT column, preserving the
        logical grouping without requiring a separate table.
    Ab Initio record[item_count] → Spark ArrayType(StructType(...))
        Rationale: The "[item_count]" modifier creates a variable-length array of
        nested records. In Delta Lake this maps to ARRAY<STRUCT<...>>.
    Ab Initio if (condition) record → Spark nullable StructType
        Rationale: Conditional records in Ab Initio are present only when the
        condition is true. In Delta Lake, this maps to a nullable STRUCT column —
        NULL when the condition is false, populated when true. The condition itself
        (txn_type == 2) becomes application-level logic, not schema-level.
    Ab Initio null("value") modifier → Spark nullable=True
        Rationale: The null("") and null("UNKNOWN") modifiers define sentinel values
        in Ab Initio flat files. In Delta Lake, these become proper SQL NULLs.
        Sentinel values are replaced with NULL during data migration.
    Ab Initio decimal("10.2") → Spark DecimalType(10, 2)
    Ab Initio decimal("8.2") → Spark DecimalType(8, 2)
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType,
    ArrayType,
)


# Nested struct: merchant_info sub-record
_MERCHANT_INFO_SCHEMA = StructType([
    # Ab Initio: string(",", null("")) merchant_name → StringType, nullable
    # null("") means empty string is the sentinel for NULL in Ab Initio
    StructField("merchant_name", StringType(), nullable=True),
    # Ab Initio: string(",") merchant_category → StringType
    StructField("merchant_category", StringType(), nullable=True),
    # Ab Initio: decimal("10.2", ",") amount → DecimalType(10, 2)
    StructField("amount", DecimalType(10, 2), nullable=True),
])

# Nested struct: line_items array element
_LINE_ITEM_SCHEMA = StructType([
    # Ab Initio: string(",") sku → StringType
    StructField("sku", StringType(), nullable=True),
    # Ab Initio: decimal(",") quantity → LongType (bare decimal)
    StructField("quantity", LongType(), nullable=True),
    # Ab Initio: decimal("8.2", ",") line_total → DecimalType(8, 2)
    StructField("line_total", DecimalType(8, 2), nullable=True),
])

# Conditional struct: refund_details (present only when txn_type == 2)
_REFUND_DETAILS_SCHEMA = StructType([
    # Ab Initio: decimal(",") original_txn_id → LongType
    StructField("original_txn_id", LongType(), nullable=True),
    # Ab Initio: string(",") refund_reason → StringType
    StructField("refund_reason", StringType(), nullable=True),
])

# PySpark StructType definition for the full transaction_detail record
TRANSACTION_DETAIL_SCHEMA = StructType([
    # Ab Initio: decimal(",") txn_id → LongType (record identifier)
    StructField("txn_id", LongType(), nullable=False),
    # Ab Initio: datetime("YYYY-MM-DD HH24:MI:SS") → TimestampType
    StructField("txn_timestamp", TimestampType(), nullable=True),
    # Ab Initio: decimal(",") customer_id → LongType
    StructField("customer_id", LongType(), nullable=True),
    # Ab Initio: decimal(",") txn_type → LongType
    StructField("txn_type", LongType(), nullable=True),
    # Ab Initio: nested record merchant_info → StructType
    StructField("merchant_info", _MERCHANT_INFO_SCHEMA, nullable=True),
    # Ab Initio: decimal(",") item_count → IntegerType (array length control)
    StructField("item_count", IntegerType(), nullable=True),
    # Ab Initio: record[item_count] line_items → ArrayType(StructType)
    StructField("line_items", ArrayType(_LINE_ITEM_SCHEMA), nullable=True),
    # Ab Initio: if (txn_type == 2) record refund_details → nullable StructType
    # NULL when txn_type != 2; populated for refund transactions
    StructField("refund_details", _REFUND_DETAILS_SCHEMA, nullable=True),
    # Ab Initio: string("\n", null("UNKNOWN")) channel → StringType, nullable
    # Sentinel "UNKNOWN" replaced with SQL NULL during migration
    StructField("channel", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
TRANSACTION_DETAIL_DDL = """\
-- Delta Lake table migrated from Ab Initio DML: transaction_detail.dml
-- Complex nested transaction records with sub-records, arrays, and conditional fields
--
-- Migration Notes:
--   - merchant_info: inline nested record → STRUCT column
--   - line_items: variable-length record array → ARRAY<STRUCT>
--   - refund_details: conditional record (if txn_type == 2) → nullable STRUCT
--     Application logic must populate refund_details only for refund transactions.
--   - channel: null("UNKNOWN") sentinel → SQL NULL (replace "UNKNOWN" during ingestion)
--   - merchant_name: null("") sentinel → SQL NULL (replace empty string during ingestion)
CREATE TABLE IF NOT EXISTS transaction_detail (
    txn_id          BIGINT          NOT NULL  COMMENT 'Ab Initio decimal (bare) → BIGINT transaction identifier',
    txn_timestamp   TIMESTAMP                 COMMENT 'Ab Initio datetime("YYYY-MM-DD HH24:MI:SS") → TIMESTAMP',
    customer_id     BIGINT                    COMMENT 'Ab Initio decimal (bare) → BIGINT foreign key',
    txn_type        BIGINT                    COMMENT 'Ab Initio decimal (bare) → BIGINT (1=purchase, 2=refund, etc.)',
    merchant_info   STRUCT<
                        merchant_name: STRING,
                        merchant_category: STRING,
                        amount: DECIMAL(10,2)
                    >                         COMMENT 'Ab Initio nested record → STRUCT (merchant_name nullable: sentinel="")',
    item_count      INT                       COMMENT 'Ab Initio decimal (bare) → INT array-length control field',
    line_items      ARRAY<STRUCT<
                        sku: STRING,
                        quantity: BIGINT,
                        line_total: DECIMAL(8,2)
                    >>                        COMMENT 'Ab Initio record[item_count] → ARRAY<STRUCT> variable-length',
    refund_details  STRUCT<
                        original_txn_id: BIGINT,
                        refund_reason: STRING
                    >                         COMMENT 'Ab Initio conditional record (if txn_type==2) → nullable STRUCT',
    channel         STRING                    COMMENT 'Ab Initio string with null("UNKNOWN") → STRING (sentinel→NULL)'
)
USING DELTA
COMMENT 'Transaction detail with nested structs and arrays — migrated from Ab Initio DML (transaction_detail.dml)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'migration.source'                 = 'abinitio',
    'migration.source_dml'             = 'dml/transaction_detail.dml'
);
"""
