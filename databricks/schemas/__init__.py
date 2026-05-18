# --------------------------------------------------------------------------
# Databricks Delta Lake schema definitions
# Migrated from Ab Initio DML record layouts (dml/ directory)
#
# Each module provides:
#   - A PySpark StructType for use with spark.read / DataFrame operations
#   - A Delta Lake DDL string for CREATE TABLE statements
#
# Type mapping reference (Ab Initio → Spark):
#   decimal          → LongType (integer IDs) or DecimalType (with precision)
#   decimal("p.s")   → DecimalType(p, s)
#   packed_decimal(n) → DecimalType(n, 0) — requires binary BCD decoding at read
#   packed_decimal("p.s") → DecimalType(p, s) — requires binary BCD decoding
#   zoned_decimal(n) → DecimalType(n, 0) — requires EBCDIC zoned decoding at read
#   string           → StringType
#   string(n)        → StringType — fixed-width, trimmed at read time
#   date("fmt")      → DateType
#   datetime("fmt")  → TimestampType
#   void             → DROPPED (padding/filler bytes)
#   record           → StructType (nested struct)
#   record[count]    → ArrayType(StructType) (variable-length array)
#   type T = record  → reusable StructType (shared across schemas)
#   if (condition)   → nullable StructType (always present, NULL when condition false)
# --------------------------------------------------------------------------
from databricks.schemas.customer import customer_schema, CUSTOMER_DDL
from databricks.schemas.order_items import order_items_schema, ORDER_ITEMS_DDL
from databricks.schemas.transaction_detail import transaction_detail_schema, TRANSACTION_DETAIL_DDL
from databricks.schemas.account_balance import account_balance_schema, ACCOUNT_BALANCE_DDL
from databricks.schemas.account_status import account_status_schema, ACCOUNT_STATUS_DDL
from databricks.schemas.common_address import address_schema
from databricks.schemas.customer_address import customer_address_schema, CUSTOMER_ADDRESS_DDL
from databricks.schemas.packed_account import packed_account_schema, PACKED_ACCOUNT_DDL

__all__ = [
    "customer_schema", "CUSTOMER_DDL",
    "order_items_schema", "ORDER_ITEMS_DDL",
    "transaction_detail_schema", "TRANSACTION_DETAIL_DDL",
    "account_balance_schema", "ACCOUNT_BALANCE_DDL",
    "account_status_schema", "ACCOUNT_STATUS_DDL",
    "address_schema",
    "customer_address_schema", "CUSTOMER_ADDRESS_DDL",
    "packed_account_schema", "PACKED_ACCOUNT_DDL",
]
