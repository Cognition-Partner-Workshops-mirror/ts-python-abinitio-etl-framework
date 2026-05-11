"""
Delta Lake schema definitions migrated from Ab Initio DML record layouts.

Each module in this package corresponds to one .dml file from the legacy
Ab Initio ETL framework and exposes:
  - A PySpark StructType (*_SCHEMA) for use in DataFrame creation and validation
  - A Delta Lake DDL string (*_DDL) for table creation in Databricks SQL

Usage in a Databricks notebook:
    from databricks.schemas.customer import CUSTOMER_SCHEMA, CUSTOMER_DDL
    df = spark.read.schema(CUSTOMER_SCHEMA).csv("path/to/data")
    spark.sql(CUSTOMER_DDL)

Module index:
    common_address      - Reusable address_t StructType (no standalone table)
    customer            - Customer master record
    account_balance     - Account balance with dates and decimals
    order_items         - Variable-length order line items (arrays)
    transaction_detail  - Complex nested records, arrays, and conditional fields
    packed_account      - Mainframe packed/zoned decimal record
    account_status      - Account status (void padding fields removed)
    customer_address    - Customer address with embedded address_t struct

See TYPE_MAPPING.md for the complete Ab Initio → Spark type mapping reference.
"""

# Reusable type (no table)
from databricks.schemas.common_address import ADDRESS_T_SCHEMA

# Table schemas and DDL
from databricks.schemas.customer import CUSTOMER_SCHEMA, CUSTOMER_DDL
from databricks.schemas.account_balance import ACCOUNT_BALANCE_SCHEMA, ACCOUNT_BALANCE_DDL
from databricks.schemas.order_items import ORDER_ITEMS_SCHEMA, ORDER_ITEMS_DDL
from databricks.schemas.transaction_detail import (
    TRANSACTION_DETAIL_SCHEMA,
    TRANSACTION_DETAIL_DDL,
)
from databricks.schemas.packed_account import PACKED_ACCOUNT_SCHEMA, PACKED_ACCOUNT_DDL
from databricks.schemas.account_status import ACCOUNT_STATUS_SCHEMA, ACCOUNT_STATUS_DDL
from databricks.schemas.customer_address import CUSTOMER_ADDRESS_SCHEMA, CUSTOMER_ADDRESS_DDL

__all__ = [
    "ADDRESS_T_SCHEMA",
    "CUSTOMER_SCHEMA",
    "CUSTOMER_DDL",
    "ACCOUNT_BALANCE_SCHEMA",
    "ACCOUNT_BALANCE_DDL",
    "ORDER_ITEMS_SCHEMA",
    "ORDER_ITEMS_DDL",
    "TRANSACTION_DETAIL_SCHEMA",
    "TRANSACTION_DETAIL_DDL",
    "PACKED_ACCOUNT_SCHEMA",
    "PACKED_ACCOUNT_DDL",
    "ACCOUNT_STATUS_SCHEMA",
    "ACCOUNT_STATUS_DDL",
    "CUSTOMER_ADDRESS_SCHEMA",
    "CUSTOMER_ADDRESS_DDL",
]
