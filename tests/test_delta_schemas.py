"""Tests for Delta Lake schema definitions migrated from Ab Initio DML files.

Validates that each schema module:
  - Defines the correct StructType with expected fields, types, and nullability
  - Produces valid DDL strings with required keywords (CREATE TABLE, USING DELTA, etc.)
  - Correctly handles special Ab Initio constructs (nested records, arrays, void, etc.)
"""

import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    DecimalType,
    DateType,
    TimestampType,
    ArrayType,
)


# ---------------------------------------------------------------------------
# common_address — reusable address_t type
# ---------------------------------------------------------------------------
class TestCommonAddress:
    def test_address_t_is_struct_type(self):
        from databricks.schemas.common_address import ADDRESS_T_SCHEMA

        assert isinstance(ADDRESS_T_SCHEMA, StructType)

    def test_address_t_has_four_string_fields(self):
        from databricks.schemas.common_address import ADDRESS_T_SCHEMA

        assert len(ADDRESS_T_SCHEMA.fields) == 4
        for field in ADDRESS_T_SCHEMA.fields:
            assert isinstance(field.dataType, StringType)

    def test_address_t_field_names(self):
        from databricks.schemas.common_address import ADDRESS_T_SCHEMA

        names = [f.name for f in ADDRESS_T_SCHEMA.fields]
        assert names == ["street", "city", "state", "zip"]


# ---------------------------------------------------------------------------
# customer — simple flat record
# ---------------------------------------------------------------------------
class TestCustomer:
    def test_schema_field_count(self):
        from databricks.schemas.customer import CUSTOMER_SCHEMA

        assert len(CUSTOMER_SCHEMA.fields) == 4

    def test_customer_id_is_long_not_null(self):
        from databricks.schemas.customer import CUSTOMER_SCHEMA

        field = CUSTOMER_SCHEMA["customer_id"]
        assert isinstance(field.dataType, LongType)
        assert field.nullable is False

    def test_string_fields_are_nullable(self):
        from databricks.schemas.customer import CUSTOMER_SCHEMA

        for name in ["first_name", "last_name", "email"]:
            assert isinstance(CUSTOMER_SCHEMA[name].dataType, StringType)
            assert CUSTOMER_SCHEMA[name].nullable is True

    def test_ddl_contains_delta_keywords(self):
        from databricks.schemas.customer import CUSTOMER_DDL

        assert "CREATE TABLE IF NOT EXISTS customer" in CUSTOMER_DDL
        assert "USING DELTA" in CUSTOMER_DDL
        assert "BIGINT" in CUSTOMER_DDL
        assert "migration.source_dml" in CUSTOMER_DDL


# ---------------------------------------------------------------------------
# account_balance — dates and precision decimals
# ---------------------------------------------------------------------------
class TestAccountBalance:
    def test_schema_field_count(self):
        from databricks.schemas.account_balance import ACCOUNT_BALANCE_SCHEMA

        assert len(ACCOUNT_BALANCE_SCHEMA.fields) == 5

    def test_balance_is_decimal_8_2(self):
        from databricks.schemas.account_balance import ACCOUNT_BALANCE_SCHEMA

        field = ACCOUNT_BALANCE_SCHEMA["balance"]
        assert isinstance(field.dataType, DecimalType)
        assert field.dataType.precision == 8
        assert field.dataType.scale == 2

    def test_opened_date_is_date_type(self):
        from databricks.schemas.account_balance import ACCOUNT_BALANCE_SCHEMA

        field = ACCOUNT_BALANCE_SCHEMA["opened_date"]
        assert isinstance(field.dataType, DateType)

    def test_ddl_contains_decimal_spec(self):
        from databricks.schemas.account_balance import ACCOUNT_BALANCE_DDL

        assert "DECIMAL(8,2)" in ACCOUNT_BALANCE_DDL
        assert "DATE" in ACCOUNT_BALANCE_DDL


# ---------------------------------------------------------------------------
# order_items — variable-length arrays
# ---------------------------------------------------------------------------
class TestOrderItems:
    def test_item_names_is_array_of_string(self):
        from databricks.schemas.order_items import ORDER_ITEMS_SCHEMA

        field = ORDER_ITEMS_SCHEMA["item_names"]
        assert isinstance(field.dataType, ArrayType)
        assert isinstance(field.dataType.elementType, StringType)

    def test_item_quantities_is_array_of_long(self):
        from databricks.schemas.order_items import ORDER_ITEMS_SCHEMA

        field = ORDER_ITEMS_SCHEMA["item_quantities"]
        assert isinstance(field.dataType, ArrayType)
        assert isinstance(field.dataType.elementType, LongType)

    def test_item_count_is_integer(self):
        from databricks.schemas.order_items import ORDER_ITEMS_SCHEMA

        field = ORDER_ITEMS_SCHEMA["item_count"]
        assert isinstance(field.dataType, IntegerType)

    def test_ddl_contains_array_types(self):
        from databricks.schemas.order_items import ORDER_ITEMS_DDL

        assert "ARRAY<STRING>" in ORDER_ITEMS_DDL
        assert "ARRAY<BIGINT>" in ORDER_ITEMS_DDL


# ---------------------------------------------------------------------------
# transaction_detail — nested records, arrays of structs, conditional fields
# ---------------------------------------------------------------------------
class TestTransactionDetail:
    def test_txn_timestamp_is_timestamp(self):
        from databricks.schemas.transaction_detail import TRANSACTION_DETAIL_SCHEMA

        field = TRANSACTION_DETAIL_SCHEMA["txn_timestamp"]
        assert isinstance(field.dataType, TimestampType)

    def test_merchant_info_is_struct(self):
        from databricks.schemas.transaction_detail import TRANSACTION_DETAIL_SCHEMA

        field = TRANSACTION_DETAIL_SCHEMA["merchant_info"]
        assert isinstance(field.dataType, StructType)
        nested_names = [f.name for f in field.dataType.fields]
        assert "merchant_name" in nested_names
        assert "amount" in nested_names

    def test_merchant_amount_is_decimal_10_2(self):
        from databricks.schemas.transaction_detail import TRANSACTION_DETAIL_SCHEMA

        merchant = TRANSACTION_DETAIL_SCHEMA["merchant_info"].dataType
        amount_field = merchant["amount"]
        assert isinstance(amount_field.dataType, DecimalType)
        assert amount_field.dataType.precision == 10
        assert amount_field.dataType.scale == 2

    def test_line_items_is_array_of_struct(self):
        from databricks.schemas.transaction_detail import TRANSACTION_DETAIL_SCHEMA

        field = TRANSACTION_DETAIL_SCHEMA["line_items"]
        assert isinstance(field.dataType, ArrayType)
        assert isinstance(field.dataType.elementType, StructType)
        elem_names = [f.name for f in field.dataType.elementType.fields]
        assert elem_names == ["sku", "quantity", "line_total"]

    def test_refund_details_is_nullable_struct(self):
        """Conditional record (if txn_type == 2) maps to nullable STRUCT."""
        from databricks.schemas.transaction_detail import TRANSACTION_DETAIL_SCHEMA

        field = TRANSACTION_DETAIL_SCHEMA["refund_details"]
        assert isinstance(field.dataType, StructType)
        assert field.nullable is True
        nested_names = [f.name for f in field.dataType.fields]
        assert "original_txn_id" in nested_names
        assert "refund_reason" in nested_names

    def test_channel_is_nullable(self):
        """null("UNKNOWN") sentinel → nullable StringType."""
        from databricks.schemas.transaction_detail import TRANSACTION_DETAIL_SCHEMA

        field = TRANSACTION_DETAIL_SCHEMA["channel"]
        assert isinstance(field.dataType, StringType)
        assert field.nullable is True

    def test_ddl_contains_struct_and_array(self):
        from databricks.schemas.transaction_detail import TRANSACTION_DETAIL_DDL

        assert "STRUCT<" in TRANSACTION_DETAIL_DDL
        assert "ARRAY<STRUCT<" in TRANSACTION_DETAIL_DDL
        assert "USING DELTA" in TRANSACTION_DETAIL_DDL


# ---------------------------------------------------------------------------
# packed_account — mainframe packed/zoned decimal
# ---------------------------------------------------------------------------
class TestPackedAccount:
    def test_account_num_is_decimal_5_0(self):
        from databricks.schemas.packed_account import PACKED_ACCOUNT_SCHEMA

        field = PACKED_ACCOUNT_SCHEMA["account_num"]
        assert isinstance(field.dataType, DecimalType)
        assert field.dataType.precision == 5
        assert field.dataType.scale == 0

    def test_balance_is_decimal_7_2(self):
        from databricks.schemas.packed_account import PACKED_ACCOUNT_SCHEMA

        field = PACKED_ACCOUNT_SCHEMA["balance"]
        assert isinstance(field.dataType, DecimalType)
        assert field.dataType.precision == 7
        assert field.dataType.scale == 2

    def test_status_code_is_decimal_4_0(self):
        """zoned_decimal(4) → DecimalType(4, 0)."""
        from databricks.schemas.packed_account import PACKED_ACCOUNT_SCHEMA

        field = PACKED_ACCOUNT_SCHEMA["status_code"]
        assert isinstance(field.dataType, DecimalType)
        assert field.dataType.precision == 4
        assert field.dataType.scale == 0

    def test_account_name_is_string(self):
        """string(20) fixed-width → StringType (Parquet is variable-length)."""
        from databricks.schemas.packed_account import PACKED_ACCOUNT_SCHEMA

        field = PACKED_ACCOUNT_SCHEMA["account_name"]
        assert isinstance(field.dataType, StringType)

    def test_ddl_contains_mainframe_comments(self):
        from databricks.schemas.packed_account import PACKED_ACCOUNT_DDL

        assert "DECIMAL(5,0)" in PACKED_ACCOUNT_DDL
        assert "DECIMAL(7,2)" in PACKED_ACCOUNT_DDL
        assert "DECIMAL(4,0)" in PACKED_ACCOUNT_DDL
        assert "mainframe" in PACKED_ACCOUNT_DDL.lower()


# ---------------------------------------------------------------------------
# account_status — void fields skipped
# ---------------------------------------------------------------------------
class TestAccountStatus:
    def test_void_fields_omitted(self):
        """void fields (padding1, padding2) must not appear in schema."""
        from databricks.schemas.account_status import ACCOUNT_STATUS_SCHEMA

        field_names = [f.name for f in ACCOUNT_STATUS_SCHEMA.fields]
        assert "padding1" not in field_names
        assert "padding2" not in field_names

    def test_only_business_fields_remain(self):
        from databricks.schemas.account_status import ACCOUNT_STATUS_SCHEMA

        names = [f.name for f in ACCOUNT_STATUS_SCHEMA.fields]
        assert names == ["id", "name", "status"]

    def test_ddl_documents_void_removal(self):
        from databricks.schemas.account_status import ACCOUNT_STATUS_DDL

        assert "void" in ACCOUNT_STATUS_DDL.lower()
        assert "padding" in ACCOUNT_STATUS_DDL.lower()


# ---------------------------------------------------------------------------
# customer_address — includes common_address type reference
# ---------------------------------------------------------------------------
class TestCustomerAddress:
    def test_address_is_nested_struct(self):
        from databricks.schemas.customer_address import CUSTOMER_ADDRESS_SCHEMA

        field = CUSTOMER_ADDRESS_SCHEMA["address"]
        assert isinstance(field.dataType, StructType)

    def test_address_struct_matches_common_address(self):
        """The embedded struct must match the reusable ADDRESS_T_SCHEMA."""
        from databricks.schemas.customer_address import CUSTOMER_ADDRESS_SCHEMA
        from databricks.schemas.common_address import ADDRESS_T_SCHEMA

        embedded = CUSTOMER_ADDRESS_SCHEMA["address"].dataType
        assert embedded == ADDRESS_T_SCHEMA

    def test_schema_has_four_fields(self):
        from databricks.schemas.customer_address import CUSTOMER_ADDRESS_SCHEMA

        assert len(CUSTOMER_ADDRESS_SCHEMA.fields) == 4

    def test_ddl_contains_struct(self):
        from databricks.schemas.customer_address import CUSTOMER_ADDRESS_DDL

        assert "STRUCT<" in CUSTOMER_ADDRESS_DDL
        assert "street" in CUSTOMER_ADDRESS_DDL
        assert "common_address.dml" in CUSTOMER_ADDRESS_DDL


# ---------------------------------------------------------------------------
# Cross-schema: all DDL strings share consistent properties
# ---------------------------------------------------------------------------
class TestAllDDLConsistency:
    """Verify all DDL strings follow a consistent pattern."""

    @pytest.fixture(params=[
        "customer",
        "account_balance",
        "order_items",
        "transaction_detail",
        "packed_account",
        "account_status",
        "customer_address",
    ])
    def ddl_string(self, request):
        """Parameterized fixture that yields each DDL string."""
        module_map = {
            "customer": "databricks.schemas.customer",
            "account_balance": "databricks.schemas.account_balance",
            "order_items": "databricks.schemas.order_items",
            "transaction_detail": "databricks.schemas.transaction_detail",
            "packed_account": "databricks.schemas.packed_account",
            "account_status": "databricks.schemas.account_status",
            "customer_address": "databricks.schemas.customer_address",
        }
        import importlib

        mod = importlib.import_module(module_map[request.param])
        ddl_attr = f"{request.param.upper()}_DDL"
        return getattr(mod, ddl_attr)

    def test_uses_delta_format(self, ddl_string):
        assert "USING DELTA" in ddl_string

    def test_has_create_table(self, ddl_string):
        assert "CREATE TABLE IF NOT EXISTS" in ddl_string

    def test_has_table_comment(self, ddl_string):
        assert "COMMENT" in ddl_string

    def test_has_migration_source_property(self, ddl_string):
        assert "migration.source" in ddl_string

    def test_has_auto_optimize(self, ddl_string):
        assert "delta.autoOptimize.optimizeWrite" in ddl_string
