"""
PSET → Databricks job parameter mapping for workflow migration.

Maps Ab Initio PSET (Parameter Set) template values to Databricks Workflow
job parameters and notebook widget base_parameters.

In Ab Initio, PSETs provided environment-aware configuration:
  - psets/pset_templates/orders_pipeline.pset → default/dev values
  - psets/dev/orders_pipeline.pset → dev overrides
  - psets/prod/orders_pipeline.pset → prod overrides

In Databricks, this maps to:
  - Job-level parameters with defaults (environment-independent)
  - Per-task base_parameters using {{job.parameters.*}} references
  - Environment-specific overrides via the 'environment' job parameter
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


# Ab Initio PSET parameter → Databricks job parameter / widget mapping.
# Each entry documents the source PSET key, target Databricks parameter,
# and the rationale for the mapping decision.
PSET_TO_DATABRICKS_MAPPING: Dict[str, Dict[str, Any]] = {
    # --- orders_pipeline.pset mappings ---
    "SOURCE_PATH": {
        "databricks_param": "source_path",
        "widget": "source_path",
        "pset_default": "/data/dev/raw/orders",
        "databricks_default": "/mnt/main/raw/orders/{{job.parameters.batch_date}}",
        "notes": "File-system path replaced with cloud storage mount. Date partitioning added.",
    },
    "TARGET_TABLE": {
        "databricks_param": "target_table",
        "widget": "target_table",
        "pset_default": "DEV.STAGING.ORDERS",
        "databricks_default": "{{catalog}}.{{environment}}_staging.orders",
        "notes": "Database.schema.table replaced with Unity Catalog three-level namespace.",
    },
    "PARTITION_COUNT": {
        "databricks_param": "partition_count",
        "widget": "partition_count",
        "pset_default": "4",
        "databricks_default": "8",
        "notes": "Increased from 4 to 8 for Spark. Ab Initio partitions were OS threads; "
                 "Spark partitions are distributed tasks across executors.",
    },
    "BATCH_SIZE": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "50000",
        "databricks_default": None,
        "notes": "Not needed in Spark — the engine handles batch sizing via partition splits "
                 "and shuffle.partitions. Spark processes full partitions, not fixed-size batches.",
    },
    "LOG_LEVEL": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "DEBUG",
        "databricks_default": None,
        "notes": "Controlled via Spark log4j configuration or cluster-level settings, "
                 "not per-job parameters.",
    },
    "RECORD_SOURCE": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "ORDER_SYSTEM_DEV",
        "databricks_default": None,
        "notes": "Lineage tracking replaced by Unity Catalog lineage and Delta TBLPROPERTIES "
                 "('migration.source' = 'abinitio').",
    },
    "MAX_ERRORS": {
        "databricks_param": "max_errors",
        "widget": None,
        "pset_default": "100",
        "databricks_default": "100",
        "notes": "Mapped to job-level parameter. Can be used in notebook logic for "
                 "data quality threshold checks.",
    },
    "CHECKPOINT_DIR": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "/tmp/abinitio/checkpoints/orders",
        "databricks_default": None,
        "notes": "Not needed — Delta Lake provides built-in ACID transactions and "
                 "Structured Streaming checkpoints. No manual checkpoint management required.",
    },

    # --- orders_staging.pset mappings ---
    "LOAD_MODE": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "MERGE",
        "databricks_default": None,
        "notes": "Implicit in Delta MERGE operations — the CDC notebook always uses MERGE. "
                 "Append/overwrite modes available via parallel_loader write_mode parameter.",
    },
    "REJECT_PATH": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "/data/rejects/orders",
        "databricks_default": None,
        "notes": "Data quality rejects handled via Spark's 'badRecordsPath' option or "
                 "Delta Lake expectations (data quality rules). Not a separate output path.",
    },
    "DML_FILE": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "/data/projects/enterprise_etl/dml/order_items.dml",
        "databricks_default": None,
        "notes": "Replaced by PySpark StructType definitions in databricks/schemas/. "
                 "Import ORDER_ITEMS_SCHEMA from databricks.schemas.order_items.",
    },
    "SLA_MINUTES": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "30",
        "databricks_default": None,
        "notes": "Mapped to task timeout_seconds (30 min = 1800s) in the workflow JSON. "
                 "SLA monitoring via Databricks SQL Alerts replaces monitoring/sla_tracker.py.",
    },

    # --- customer_cdc.pset mappings ---
    "PREVIOUS_SNAPSHOT_PATH": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "/data/snapshots/customer/previous",
        "databricks_default": None,
        "notes": "Not needed — Delta MERGE compares source against current table state directly. "
                 "No need to maintain separate 'previous' and 'current' snapshot files.",
    },
    "CURRENT_SNAPSHOT_PATH": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "/data/snapshots/customer/current",
        "databricks_default": None,
        "notes": "Replaced by customer_snapshot Delta table. Source data is loaded into a "
                 "Delta table that serves as the MERGE source.",
    },
    "CDC_OUTPUT_PATH": {
        "databricks_param": None,
        "widget": None,
        "pset_default": "/data/cdc/customer",
        "databricks_default": None,
        "notes": "Replaced by Delta Change Data Feed (CDF). Downstream consumers read changes "
                 "via spark.read.option('readChangeFeed', 'true').table(name) instead of "
                 "reading separate INSERT/UPDATE/DELETE flat files.",
    },
    "HASH_COLUMNS": {
        "databricks_param": "compare_columns",
        "widget": "compare_columns",
        "pset_default": "customer_id,name,address,phone,email,status",
        "databricks_default": "name,address,phone,email,status",
        "notes": "Key column (customer_id) removed from compare list — MERGE ON handles keys. "
                 "Remaining columns compared directly (no MD5 hashing needed).",
    },
    "KEY_COLUMNS": {
        "databricks_param": "key_columns",
        "widget": "key_columns",
        "pset_default": "customer_id",
        "databricks_default": "customer_id",
        "notes": "Maps directly to MERGE ON condition. Supports composite keys (comma-separated).",
    },
    "AUDIT_TABLE": {
        "databricks_param": "audit_table",
        "widget": None,
        "pset_default": "AUDIT.CUSTOMER_CHANGES",
        "databricks_default": "{{catalog}}.{{environment}}_audit.customer_changes",
        "notes": "Audit trail table — populated from CDF instead of Ab Initio audit graph output.",
    },
    "RETENTION_DAYS": {
        "databricks_param": "retention_days",
        "widget": None,
        "pset_default": "90",
        "databricks_default": "90",
        "notes": "Used for Delta table history retention: "
                 "ALTER TABLE SET TBLPROPERTIES ('delta.logRetentionDuration' = '90 days').",
    },
}

# Ab Initio setenv.ksh environment variables → Databricks equivalents
SETENV_TO_DATABRICKS_MAPPING: Dict[str, Dict[str, str]] = {
    "AI_HOME": {
        "databricks_equivalent": "N/A",
        "notes": "Ab Initio installation path. Not needed — Spark is the runtime.",
    },
    "AI_PROJECT_DIR": {
        "databricks_equivalent": "Databricks Repos / workspace path",
        "notes": "Project files stored in Repos (/Repos/org/project) or workspace.",
    },
    "AI_LOG_DIR": {
        "databricks_equivalent": "Databricks job run logs (automatic)",
        "notes": "Logging is automatic — stdout/stderr captured by driver logs.",
    },
    "AI_SANDBOX_DIR": {
        "databricks_equivalent": "N/A",
        "notes": "Sandbox execution replaced by Spark cluster job runs.",
    },
    "AI_DATA_DIR": {
        "databricks_equivalent": "Cloud storage mount or Unity Catalog volume",
        "notes": "/mnt/data or /Volumes/catalog/schema/volume.",
    },
    "AI_STAGING_DIR": {
        "databricks_equivalent": "Delta staging schema/tables",
        "notes": "catalog.environment_staging.table_name.",
    },
    "AI_ARCHIVE_DIR": {
        "databricks_equivalent": "Delta time travel",
        "notes": "No archive needed — Delta Lake retains full history via time travel.",
    },
    "AI_SOURCE_DB": {
        "databricks_equivalent": "source_catalog job parameter",
        "notes": "ORACLE_PROD → Unity Catalog source catalog.",
    },
    "AI_TARGET_DB": {
        "databricks_equivalent": "target_catalog job parameter",
        "notes": "TERADATA_DW → Unity Catalog target catalog.",
    },
    "AI_DEFAULT_PARTITIONS": {
        "databricks_equivalent": "spark.sql.shuffle.partitions / partition_count widget",
        "notes": "4 → 8 (Spark partitions are lighter than Ab Initio process partitions).",
    },
    "AI_MAX_PARTITIONS": {
        "databricks_equivalent": "Cluster autoscaling max_workers",
        "notes": "16 → cluster autoscaling handles dynamic partition scaling.",
    },
    "AI_MAX_ERRORS": {
        "databricks_equivalent": "max_errors job parameter",
        "notes": "100 → passed to notebooks for data quality threshold checks.",
    },
    "AI_ERROR_ACTION": {
        "databricks_equivalent": "Task retry policy / depends_on failure routing",
        "notes": "ABORT → task failure stops downstream tasks via depends_on chain.",
    },
    "AI_CHECKPOINT_ENABLED": {
        "databricks_equivalent": "Delta Lake ACID transactions",
        "notes": "Built-in — Delta ensures atomicity, no manual checkpointing needed.",
    },
    "AI_CHECKPOINT_DIR": {
        "databricks_equivalent": "N/A (or Structured Streaming checkpoint location)",
        "notes": "Only needed for Structured Streaming, not batch jobs.",
    },
}


def get_parameter_mapping(pset_key: str) -> Optional[Dict[str, Any]]:
    """Look up the Databricks mapping for a given PSET parameter key."""
    return PSET_TO_DATABRICKS_MAPPING.get(pset_key)


def get_mapped_parameters() -> List[str]:
    """Return PSET keys that have a direct Databricks parameter mapping."""
    return [
        key for key, mapping in PSET_TO_DATABRICKS_MAPPING.items()
        if mapping.get("databricks_param") is not None
    ]


def get_unmapped_parameters() -> List[str]:
    """Return PSET keys that have no direct Databricks equivalent (absorbed by platform)."""
    return [
        key for key, mapping in PSET_TO_DATABRICKS_MAPPING.items()
        if mapping.get("databricks_param") is None
    ]


def validate_workflow_json(workflow_path: str) -> Dict[str, Any]:
    """
    Validate a Databricks workflow JSON file has required fields.

    Returns dict with validation results and any warnings.
    """
    path = Path(workflow_path)
    if not path.exists():
        raise FileNotFoundError(f"Workflow file not found: {workflow_path}")

    with open(path) as f:
        workflow = json.load(f)

    errors = []
    warnings = []

    # Required top-level fields
    for field in ["name", "tasks"]:
        if field not in workflow:
            errors.append(f"Missing required field: {field}")

    # Validate tasks
    tasks = workflow.get("tasks", [])
    if not tasks:
        errors.append("Workflow has no tasks defined")

    task_keys = set()
    for task in tasks:
        task_key = task.get("task_key")
        if not task_key:
            errors.append("Task missing task_key")
            continue

        if task_key in task_keys:
            errors.append(f"Duplicate task_key: {task_key}")
        task_keys.add(task_key)

        # Validate depends_on references
        for dep in task.get("depends_on", []):
            dep_key = dep.get("task_key")
            if dep_key and dep_key not in task_keys:
                # Check if dependency is defined later (forward reference)
                all_task_keys = {t.get("task_key") for t in tasks}
                if dep_key not in all_task_keys:
                    errors.append(f"Task '{task_key}' depends on unknown task: '{dep_key}'")

        # Validate notebook_task
        nb_task = task.get("notebook_task")
        if nb_task:
            if not nb_task.get("notebook_path"):
                errors.append(f"Task '{task_key}' has notebook_task without notebook_path")

    # Validate schedule if present
    schedule = workflow.get("schedule")
    if schedule:
        if not schedule.get("quartz_cron_expression"):
            warnings.append("Schedule defined but no quartz_cron_expression")

    # Validate job clusters referenced by tasks exist
    defined_clusters = {jc["job_cluster_key"] for jc in workflow.get("job_clusters", [])}
    for task in tasks:
        cluster_ref = task.get("job_cluster_key")
        if cluster_ref and cluster_ref not in defined_clusters:
            errors.append(f"Task '{task.get('task_key')}' references undefined cluster: '{cluster_ref}'")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "task_count": len(tasks),
        "has_schedule": schedule is not None,
    }
