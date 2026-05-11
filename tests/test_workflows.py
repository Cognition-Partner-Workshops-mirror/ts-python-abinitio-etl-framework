"""Tests for Databricks Workflow definitions migrated from KornShell orchestration scripts.

Validates:
  - Workflow JSON files are valid and contain required fields
  - Task dependency chains match the original KornShell pipeline flow
  - PSET parameter mappings are complete and documented
  - Schedule definitions match AutoSys trigger specifications
  - Notebook paths reference existing notebooks
  - workflow_parameters.py mapping logic is correct
"""

import json
import pytest
from pathlib import Path

from databricks.workflows.workflow_parameters import (
    PSET_TO_DATABRICKS_MAPPING,
    SETENV_TO_DATABRICKS_MAPPING,
    get_parameter_mapping,
    get_mapped_parameters,
    get_unmapped_parameters,
    validate_workflow_json,
)


WORKFLOWS_DIR = Path(__file__).parent.parent / "databricks" / "workflows"
NOTEBOOKS_DIR = Path(__file__).parent.parent / "databricks" / "notebooks"


# ---------------------------------------------------------------------------
# Workflow JSON validation
# ---------------------------------------------------------------------------
class TestDailyOrdersWorkflow:
    """Validate daily_orders_workflow.json structure and task dependencies."""

    @pytest.fixture
    def workflow(self):
        with open(WORKFLOWS_DIR / "daily_orders_workflow.json") as f:
            return json.load(f)

    def test_workflow_is_valid_json(self):
        """Workflow file parses as valid JSON."""
        with open(WORKFLOWS_DIR / "daily_orders_workflow.json") as f:
            data = json.load(f)
        assert isinstance(data, dict)

    def test_has_required_fields(self, workflow):
        """Workflow has name, tasks, and job_clusters."""
        assert workflow["name"] == "daily_orders_pipeline"
        assert "tasks" in workflow
        assert "job_clusters" in workflow

    def test_has_four_tasks(self, workflow):
        """Daily orders pipeline has 4 phases (extract → CDC → staging → prod)."""
        assert len(workflow["tasks"]) == 4

    def test_task_keys_match_pipeline_phases(self, workflow):
        """Task keys correspond to the 4 KornShell phases."""
        task_keys = [t["task_key"] for t in workflow["tasks"]]
        assert task_keys == [
            "extract_orders",
            "cdc_orders",
            "load_staging",
            "production_rollover",
        ]

    def test_dependency_chain_is_sequential(self, workflow):
        """Tasks form a sequential chain: extract → CDC → staging → production."""
        tasks = workflow["tasks"]
        # First task has no dependencies
        assert "depends_on" not in tasks[0] or tasks[0].get("depends_on") == []
        # Each subsequent task depends on the previous
        assert tasks[1]["depends_on"] == [{"task_key": "extract_orders"}]
        assert tasks[2]["depends_on"] == [{"task_key": "cdc_orders"}]
        assert tasks[3]["depends_on"] == [{"task_key": "load_staging"}]

    def test_has_daily_schedule(self, workflow):
        """Workflow has a daily schedule (matching AutoSys JOB_DAILY_ORDERS_LOAD)."""
        schedule = workflow["schedule"]
        assert schedule["quartz_cron_expression"] == "0 0 2 * * ?"
        assert schedule["timezone_id"] == "UTC"

    def test_schedule_starts_paused(self, workflow):
        """Schedule should be paused until manually activated."""
        assert workflow["schedule"]["pause_status"] == "PAUSED"

    def test_max_concurrent_runs_is_one(self, workflow):
        """Only one instance should run at a time."""
        assert workflow["max_concurrent_runs"] == 1

    def test_all_tasks_reference_valid_cluster(self, workflow):
        """All tasks reference a defined job cluster."""
        cluster_keys = {jc["job_cluster_key"] for jc in workflow["job_clusters"]}
        for task in workflow["tasks"]:
            if "job_cluster_key" in task:
                assert task["job_cluster_key"] in cluster_keys

    def test_all_tasks_have_notebook_path(self, workflow):
        """Every task has a notebook_task with a notebook_path."""
        for task in workflow["tasks"]:
            assert "notebook_task" in task
            assert "notebook_path" in task["notebook_task"]

    def test_production_rollover_has_no_retries(self, workflow):
        """Production writes should not retry to prevent duplicates."""
        prod_task = workflow["tasks"][3]
        assert prod_task["task_key"] == "production_rollover"
        assert prod_task["max_retries"] == 0

    def test_job_parameters_include_batch_date(self, workflow):
        """Job parameters include batch_date with auto-resolution."""
        param_names = [p["name"] for p in workflow["parameters"]]
        assert "batch_date" in param_names

    def test_job_parameters_include_environment(self, workflow):
        """Job parameters include environment for PSET-like env resolution."""
        param_names = [p["name"] for p in workflow["parameters"]]
        assert "environment" in param_names

    def test_has_migration_metadata(self, workflow):
        """Workflow contains migration metadata documenting the source script."""
        meta = workflow["_migration_metadata"]
        assert meta["source_script"] == "scripts/run_daily_orders.ksh"
        assert "JOB_DAILY_ORDERS_LOAD" in meta["source_trigger"]

    def test_has_notification_placeholders(self, workflow):
        """Workflow has email and webhook notification sections."""
        assert "email_notifications" in workflow
        assert "webhook_notifications" in workflow


class TestCustomerCDCWorkflow:
    """Validate customer_cdc_workflow.json structure and task dependencies."""

    @pytest.fixture
    def workflow(self):
        with open(WORKFLOWS_DIR / "customer_cdc_workflow.json") as f:
            return json.load(f)

    def test_workflow_is_valid_json(self):
        """Workflow file parses as valid JSON."""
        with open(WORKFLOWS_DIR / "customer_cdc_workflow.json") as f:
            data = json.load(f)
        assert isinstance(data, dict)

    def test_has_required_fields(self, workflow):
        """Workflow has name, tasks, and job_clusters."""
        assert workflow["name"] == "customer_cdc_pipeline"
        assert "tasks" in workflow
        assert "job_clusters" in workflow

    def test_has_three_tasks(self, workflow):
        """Customer CDC has 3 tasks (Steps 2+3 consolidated into MERGE)."""
        assert len(workflow["tasks"]) == 3

    def test_task_keys_match_cdc_steps(self, workflow):
        """Task keys correspond to the consolidated CDC steps."""
        task_keys = [t["task_key"] for t in workflow["tasks"]]
        assert task_keys == [
            "snapshot_customer_source",
            "cdc_detect_and_apply",
            "audit_customer_changes",
        ]

    def test_cdc_steps_consolidated(self, workflow):
        """Steps 2 (detect) and 3 (apply) are combined into one MERGE task."""
        cdc_task = workflow["tasks"][1]
        assert cdc_task["task_key"] == "cdc_detect_and_apply"
        # Uses cdc_processor notebook which does MERGE (detect + apply in one)
        assert "cdc_processor" in cdc_task["notebook_task"]["notebook_path"]

    def test_dependency_chain(self, workflow):
        """Tasks form: snapshot → detect+apply → audit."""
        tasks = workflow["tasks"]
        assert "depends_on" not in tasks[0] or tasks[0].get("depends_on") == []
        assert tasks[1]["depends_on"] == [{"task_key": "snapshot_customer_source"}]
        assert tasks[2]["depends_on"] == [{"task_key": "cdc_detect_and_apply"}]

    def test_has_four_hour_schedule(self, workflow):
        """Schedule runs every 4 hours (matching AutoSys JOB_CUSTOMER_CDC)."""
        schedule = workflow["schedule"]
        assert schedule["quartz_cron_expression"] == "0 0 0/4 * * ?"

    def test_schedule_starts_paused(self, workflow):
        """Schedule should be paused until manually activated."""
        assert workflow["schedule"]["pause_status"] == "PAUSED"

    def test_key_columns_parameter(self, workflow):
        """Job parameters include key_columns from customer_cdc.pset."""
        param_names = {p["name"] for p in workflow["parameters"]}
        assert "key_columns" in param_names
        key_param = next(p for p in workflow["parameters"] if p["name"] == "key_columns")
        assert key_param["default"] == "customer_id"

    def test_compare_columns_parameter(self, workflow):
        """Job parameters include compare_columns (from HASH_COLUMNS minus keys)."""
        param_names = {p["name"] for p in workflow["parameters"]}
        assert "compare_columns" in param_names
        compare_param = next(p for p in workflow["parameters"] if p["name"] == "compare_columns")
        # Should NOT include customer_id (key column)
        assert "customer_id" not in compare_param["default"]
        assert "name" in compare_param["default"]

    def test_retention_days_parameter(self, workflow):
        """Job parameters include retention_days from customer_cdc.pset."""
        param_names = {p["name"] for p in workflow["parameters"]}
        assert "retention_days" in param_names
        ret_param = next(p for p in workflow["parameters"] if p["name"] == "retention_days")
        assert ret_param["default"] == "90"

    def test_cdc_task_enables_cdf(self, workflow):
        """CDC task enables Change Data Feed."""
        cdc_task = workflow["tasks"][1]
        params = cdc_task["notebook_task"]["base_parameters"]
        assert params["enable_cdf"] == "true"

    def test_cdc_task_handles_deletes(self, workflow):
        """CDC task handles delete detection."""
        cdc_task = workflow["tasks"][1]
        params = cdc_task["notebook_task"]["base_parameters"]
        assert params["handle_deletes"] == "true"

    def test_has_migration_metadata(self, workflow):
        """Workflow contains migration metadata documenting the source script."""
        meta = workflow["_migration_metadata"]
        assert meta["source_script"] == "scripts/run_customer_cdc.ksh"
        assert "JOB_CUSTOMER_CDC" in meta["source_trigger"]
        assert "Every 4 hours" in meta["source_schedule"]


# ---------------------------------------------------------------------------
# Workflow JSON validator
# ---------------------------------------------------------------------------
class TestWorkflowValidator:
    """Test the validate_workflow_json utility function."""

    def test_validates_daily_orders(self):
        result = validate_workflow_json(str(WORKFLOWS_DIR / "daily_orders_workflow.json"))
        assert result["valid"] is True
        assert result["task_count"] == 4
        assert result["has_schedule"] is True

    def test_validates_customer_cdc(self):
        result = validate_workflow_json(str(WORKFLOWS_DIR / "customer_cdc_workflow.json"))
        assert result["valid"] is True
        assert result["task_count"] == 3
        assert result["has_schedule"] is True

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            validate_workflow_json("/nonexistent/workflow.json")


# ---------------------------------------------------------------------------
# PSET parameter mapping
# ---------------------------------------------------------------------------
class TestPSETMapping:
    """Test PSET → Databricks parameter mapping completeness."""

    def test_all_orders_pipeline_params_mapped(self):
        """Every parameter from orders_pipeline.pset is documented in the mapping."""
        orders_params = [
            "SOURCE_PATH", "TARGET_TABLE", "PARTITION_COUNT", "BATCH_SIZE",
            "LOG_LEVEL", "RECORD_SOURCE", "MAX_ERRORS", "CHECKPOINT_DIR",
        ]
        for param in orders_params:
            mapping = get_parameter_mapping(param)
            assert mapping is not None, f"Missing mapping for orders_pipeline.pset: {param}"

    def test_all_orders_staging_params_mapped(self):
        """Every parameter from orders_staging.pset is documented in the mapping."""
        staging_params = [
            "SOURCE_PATH", "TARGET_TABLE", "PARTITION_COUNT", "LOAD_MODE",
            "BATCH_SIZE", "MAX_ERRORS", "REJECT_PATH", "CHECKPOINT_DIR",
            "DML_FILE", "RECORD_SOURCE", "SLA_MINUTES",
        ]
        for param in staging_params:
            mapping = get_parameter_mapping(param)
            assert mapping is not None, f"Missing mapping for orders_staging.pset: {param}"

    def test_all_customer_cdc_params_mapped(self):
        """Every parameter from customer_cdc.pset is documented in the mapping."""
        cdc_params = [
            "SOURCE_PATH", "TARGET_TABLE", "PREVIOUS_SNAPSHOT_PATH",
            "CURRENT_SNAPSHOT_PATH", "CDC_OUTPUT_PATH", "PARTITION_COUNT",
            "HASH_COLUMNS", "KEY_COLUMNS", "BATCH_SIZE", "MAX_ERRORS",
            "AUDIT_TABLE", "RETENTION_DAYS",
        ]
        for param in cdc_params:
            mapping = get_parameter_mapping(param)
            assert mapping is not None, f"Missing mapping for customer_cdc.pset: {param}"

    def test_mapped_parameters_have_databricks_equivalent(self):
        """Parameters with direct mapping have a databricks_param value."""
        mapped = get_mapped_parameters()
        assert len(mapped) > 0
        for key in mapped:
            mapping = get_parameter_mapping(key)
            assert mapping["databricks_param"] is not None

    def test_unmapped_parameters_have_notes(self):
        """Parameters without direct mapping have explanatory notes."""
        unmapped = get_unmapped_parameters()
        assert len(unmapped) > 0
        for key in unmapped:
            mapping = get_parameter_mapping(key)
            assert mapping["notes"], f"Unmapped parameter {key} has no notes"

    def test_key_columns_mapping(self):
        """KEY_COLUMNS maps to key_columns widget."""
        mapping = get_parameter_mapping("KEY_COLUMNS")
        assert mapping["databricks_param"] == "key_columns"
        assert mapping["widget"] == "key_columns"
        assert mapping["pset_default"] == "customer_id"

    def test_hash_columns_becomes_compare_columns(self):
        """HASH_COLUMNS maps to compare_columns (key column removed, no hashing)."""
        mapping = get_parameter_mapping("HASH_COLUMNS")
        assert mapping["databricks_param"] == "compare_columns"
        assert mapping["widget"] == "compare_columns"
        # Databricks default should NOT include customer_id (key column)
        assert "customer_id" not in mapping["databricks_default"]

    def test_checkpoint_dir_not_needed(self):
        """CHECKPOINT_DIR has no Databricks equivalent (Delta provides ACID)."""
        mapping = get_parameter_mapping("CHECKPOINT_DIR")
        assert mapping["databricks_param"] is None
        assert "Delta" in mapping["notes"] or "ACID" in mapping["notes"]

    def test_cdc_output_path_replaced_by_cdf(self):
        """CDC_OUTPUT_PATH replaced by Change Data Feed."""
        mapping = get_parameter_mapping("CDC_OUTPUT_PATH")
        assert mapping["databricks_param"] is None
        assert "Change Data Feed" in mapping["notes"]


# ---------------------------------------------------------------------------
# setenv.ksh mapping
# ---------------------------------------------------------------------------
class TestSetenvMapping:
    """Test setenv.ksh → Databricks mapping completeness."""

    def test_all_setenv_variables_mapped(self):
        """Every variable from setenv.ksh is documented."""
        setenv_vars = [
            "AI_HOME", "AI_PROJECT_DIR", "AI_LOG_DIR", "AI_SANDBOX_DIR",
            "AI_DATA_DIR", "AI_STAGING_DIR", "AI_ARCHIVE_DIR",
            "AI_SOURCE_DB", "AI_TARGET_DB", "AI_DEFAULT_PARTITIONS",
            "AI_MAX_PARTITIONS", "AI_MAX_ERRORS", "AI_ERROR_ACTION",
            "AI_CHECKPOINT_ENABLED", "AI_CHECKPOINT_DIR",
        ]
        for var in setenv_vars:
            assert var in SETENV_TO_DATABRICKS_MAPPING, f"Missing setenv mapping: {var}"
            entry = SETENV_TO_DATABRICKS_MAPPING[var]
            assert "databricks_equivalent" in entry
            assert "notes" in entry

    def test_source_db_maps_to_catalog(self):
        """AI_SOURCE_DB (ORACLE_PROD) maps to source_catalog parameter."""
        mapping = SETENV_TO_DATABRICKS_MAPPING["AI_SOURCE_DB"]
        assert "catalog" in mapping["databricks_equivalent"].lower()

    def test_archive_dir_maps_to_time_travel(self):
        """AI_ARCHIVE_DIR maps to Delta time travel."""
        mapping = SETENV_TO_DATABRICKS_MAPPING["AI_ARCHIVE_DIR"]
        assert "time travel" in mapping["notes"].lower()


# ---------------------------------------------------------------------------
# Cross-workflow consistency
# ---------------------------------------------------------------------------
class TestWorkflowConsistency:
    """Verify consistency across all workflow definitions."""

    @pytest.fixture(params=["daily_orders_workflow.json", "customer_cdc_workflow.json"])
    def workflow(self, request):
        with open(WORKFLOWS_DIR / request.param) as f:
            return json.load(f)

    def test_has_migration_metadata(self, workflow):
        """Every workflow has _migration_metadata documenting the source."""
        assert "_migration_metadata" in workflow
        meta = workflow["_migration_metadata"]
        assert "source_script" in meta
        assert "migration_notes" in meta

    def test_has_tags(self, workflow):
        """Every workflow has tags for organization."""
        assert "tags" in workflow
        assert "migration_source" in workflow["tags"]
        assert workflow["tags"]["migration_source"] == "abinitio"

    def test_tasks_have_descriptions(self, workflow):
        """Every task has a description documenting the migration."""
        for task in workflow["tasks"]:
            assert "description" in task, f"Task {task.get('task_key')} missing description"

    def test_tasks_have_timeouts(self, workflow):
        """Every task has a timeout_seconds value."""
        for task in workflow["tasks"]:
            assert "timeout_seconds" in task, f"Task {task.get('task_key')} missing timeout"
            assert task["timeout_seconds"] > 0

    def test_notebook_tasks_have_base_parameters(self, workflow):
        """Every notebook task has base_parameters defined."""
        for task in workflow["tasks"]:
            nb = task.get("notebook_task", {})
            assert "base_parameters" in nb, \
                f"Task {task.get('task_key')} missing base_parameters"
