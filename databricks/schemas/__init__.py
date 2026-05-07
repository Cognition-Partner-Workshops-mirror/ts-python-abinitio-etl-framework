# Databricks Delta Lake schemas — converted from Ab Initio DML record layouts.
#
# Each module exports:
#   - A PySpark StructType schema (e.g., customer_schema)
#   - A Delta Lake CREATE TABLE DDL string (e.g., CUSTOMER_DDL)
#
# Type Mapping Reference (Ab Initio -> Spark):
#   decimal (no precision)    -> LongType / BIGINT
#   decimal("p.s")            -> DecimalType(p,s) / DECIMAL(p,s)
#   string                    -> StringType / STRING
#   date("YYYY-MM-DD")        -> DateType / DATE
#   datetime("...")            -> TimestampType / TIMESTAMP
#   packed_decimal(n)          -> DecimalType(n,0) / DECIMAL(n,0)
#   packed_decimal("p.s")      -> DecimalType(p,s) / DECIMAL(p,s)
#   zoned_decimal(n)           -> DecimalType(n,0) / DECIMAL(n,0)
#   void                       -> (omitted — padding/filler field)
#   type T = record            -> reusable StructType (nested)
#   record ... end name        -> StructType (nested struct column)
#   record[N] ... end name     -> ArrayType(StructType)
#   if (cond) record ... end   -> nullable StructType (NULL when condition false)
#   string[count_field]        -> ArrayType(StringType)
#   decimal[count_field]       -> ArrayType(LongType)

from databricks.schemas.customer import customer_schema, CUSTOMER_DDL
from databricks.schemas.account_balance import account_balance_schema, ACCOUNT_BALANCE_DDL
from databricks.schemas.order_items import order_items_schema, ORDER_ITEMS_DDL
from databricks.schemas.transaction_detail import transaction_detail_schema, TRANSACTION_DETAIL_DDL
from databricks.schemas.packed_account import packed_account_schema, PACKED_ACCOUNT_DDL
from databricks.schemas.account_status import account_status_schema, ACCOUNT_STATUS_DDL
from databricks.schemas.common_address import address_type_schema, COMMON_ADDRESS_DDL
from databricks.schemas.customer_address import customer_address_schema, CUSTOMER_ADDRESS_DDL

__all__ = [
    "customer_schema", "CUSTOMER_DDL",
    "account_balance_schema", "ACCOUNT_BALANCE_DDL",
    "order_items_schema", "ORDER_ITEMS_DDL",
    "transaction_detail_schema", "TRANSACTION_DETAIL_DDL",
    "packed_account_schema", "PACKED_ACCOUNT_DDL",
    "account_status_schema", "ACCOUNT_STATUS_DDL",
    "address_type_schema", "COMMON_ADDRESS_DDL",
    "customer_address_schema", "CUSTOMER_ADDRESS_DDL",
]
