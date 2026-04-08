"""Tests for PSET manager and utility modules."""
import pytest
from pathlib import Path
from psets.pset_manager import PSETManager
from utils.dml_parser import DMLParser
from graphs.cdc_processor import CDCProcessor
import pandas as pd


@pytest.fixture
def pset_dir(tmp_path):
    return str(tmp_path)


@pytest.fixture
def pset_manager(pset_dir):
    return PSETManager(pset_dir)


class TestPSETManager:
    def test_load_pset_from_file(self, pset_manager, tmp_path):
        pset_file = tmp_path / "test_job.pset"
        pset_file.write_text("define SOURCE_PATH /data/dev/orders\ndefine BATCH_SIZE 50000\n")
        result = pset_manager._parse_pset(pset_file, "dev")
        assert result["SOURCE_PATH"] == "/data/dev/orders"
        assert result["BATCH_SIZE"] == "50000"

    def test_render_pset_produces_define_lines(self, pset_manager):
        params = {"SOURCE_PATH": "/data/orders", "BATCH_SIZE": "100000"}
        rendered = pset_manager.render_pset(params)
        assert "define SOURCE_PATH /data/orders" in rendered
        assert "define BATCH_SIZE 100000" in rendered

    def test_write_pset_creates_file(self, pset_manager, tmp_path):
        pset_manager.pset_dir = tmp_path
        params = {"SOURCE_PATH": "/data/orders"}
        output = pset_manager.write_pset("test_job", params, "uat")
        assert output.exists()
        content = output.read_text()
        assert "SOURCE_PATH" in content

    def test_diff_psets_detects_changes(self, pset_manager):
        a = {"SOURCE_PATH": "/data/dev/orders", "BATCH_SIZE": "50000"}
        b = {"SOURCE_PATH": "/data/prod/orders", "BATCH_SIZE": "50000", "NEW_PARAM": "value"}
        diff = pset_manager.diff_psets(a, b)
        assert "SOURCE_PATH" in diff["changed"]
        assert "NEW_PARAM" in diff["only_in_b"]


class TestDMLParser:
    SAMPLE_DML = """record order_record {
    integer(10) order_id;
    integer(10) customer_id;
    decimal(12,2) total_amount;
    string(50) status;
    date order_date;
}"""

    def test_parse_fields(self):
        parser = DMLParser()
        result = parser.parse_content(self.SAMPLE_DML, "order_record")
        assert result["field_count"] == 5
        assert result["fields"][0]["name"] == "order_id"
        assert result["fields"][0]["sql_type"] == "INTEGER(10)"

    def test_string_type_mapped(self):
        parser = DMLParser()
        result = parser.parse_content(self.SAMPLE_DML)
        status_field = next((f for f in result["fields"] if f["name"] == "status"), None)
        assert status_field is not None
        assert "VARCHAR" in status_field["sql_type"]

    def test_create_table_sql_generated(self):
        parser = DMLParser()
        schema = parser.parse_content(self.SAMPLE_DML, "order_record")
        sql = parser.to_create_table_sql(schema)
        assert "CREATE TABLE" in sql
        assert "ORDER_ID" in sql


class TestCDCProcessor:
    @pytest.fixture
    def source_df(self):
        return pd.DataFrame({
            "order_id": [1, 2, 3, 4],
            "status": ["ACTIVE", "PENDING", "ACTIVE", "NEW"],
            "amount": [100.0, 200.0, 300.0, 150.0],
        })

    @pytest.fixture
    def target_df(self):
        return pd.DataFrame({
            "order_id": [1, 2, 3],
            "status": ["ACTIVE", "SHIPPED", "ACTIVE"],  # order 2 changed
            "amount": [100.0, 200.0, 300.0],
        })

    def test_inserts_detected(self, source_df, target_df):
        cdc = CDCProcessor(key_columns=["order_id"])
        result = cdc.process(source_df, target_df)
        assert result["stats"]["inserts"] == 1  # order 4 is new

    def test_deletes_detected(self, source_df, target_df):
        cdc = CDCProcessor(key_columns=["order_id"])
        result = cdc.process(source_df, target_df)
        assert result["stats"]["deletes"] == 0

    def test_no_changes_when_identical(self, source_df):
        cdc = CDCProcessor(key_columns=["order_id"])
        result = cdc.process(source_df, source_df.copy())
        assert result["stats"]["inserts"] == 0
        assert result["stats"]["deletes"] == 0
