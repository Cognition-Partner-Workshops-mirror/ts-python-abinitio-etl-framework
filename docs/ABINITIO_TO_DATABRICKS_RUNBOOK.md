# Ab Initio → Databricks Lakehouse Migration Runbook

> Complete reference for migrating the enterprise Ab Initio ETL estate to Databricks Lakehouse architecture.

---

## Table of Contents

1. [Migration Overview](#1-migration-overview)
2. [Complete Concept Mapping](#2-complete-concept-mapping)
3. [Migration Execution Order](#3-migration-execution-order)
4. [Phase 1: DML → Delta Lake Schemas](#4-phase-1-dml--delta-lake-schemas)
5. [Phase 2: Graphs → PySpark Notebooks](#5-phase-2-graphs--pyspark-notebooks)
6. [Phase 3: KornShell → Databricks Workflows](#6-phase-3-kornshell--databricks-workflows)
7. [Phase 4: Monitoring → Databricks SQL Alerts](#7-phase-4-monitoring--databricks-sql-alerts)
8. [Risks and Mitigations](#8-risks-and-mitigations)
9. [Validation Checklist](#9-validation-checklist)
10. [Rollback Strategy](#10-rollback-strategy)
11. [Post-Migration Operations](#11-post-migration-operations)

---

## 1. Migration Overview

### Source Estate

| Component | Location | Description |
|-----------|----------|-------------|
| DML Record Layouts | `dml/` | 8 Ab Initio schema definition files (customer, orders, accounts, transactions) |
| Graph Patterns | `graphs/` | 2 Python execution orchestrators (parallel_loader, cdc_processor) |
| PSET Templates | `psets/pset_templates/` | 3 environment-aware configuration templates |
| KornShell Scripts | `scripts/` | 2 AutoSys-triggered batch pipelines + `setenv.ksh` environment setup |
| Monitoring | `monitoring/` | AutoSys API polling (job_monitor.py) + SLA tracking (sla_tracker.py) |

### Target Architecture

| Component | Location | Description |
|-----------|----------|-------------|
| Delta Lake Schemas | `databricks/schemas/` | PySpark StructType definitions + CREATE TABLE DDL |
| PySpark Notebooks | `databricks/notebooks/` | Databricks .py notebooks with widget parameterization |
| Workflow Definitions | `databricks/workflows/` | JSON job definitions with task DAGs and schedules |
| SQL Alerts & Dashboards | `databricks/monitoring/` | Alert queries + SLA compliance dashboard SQL |

### Key Platform Differences

| Capability | Ab Initio | Databricks |
|-----------|-----------|------------|
| **Runtime** | Co>Operating System on bare metal/VM | Apache Spark on managed clusters |
| **Parallelism** | OS-level process partitions (`-partition N`) | Distributed Spark tasks across executors |
| **Storage** | Flat files (delimited, fixed-width) | Delta Lake tables (Parquet + transaction log) |
| **Schema** | DML record layouts | PySpark StructType / Delta DDL |
| **CDC** | MD5 row hashing + compare records | Delta MERGE + Change Data Feed (CDF) |
| **Config** | PSET files per environment | Job parameters + notebook widgets |
| **Orchestration** | KornShell + AutoSys cron | Databricks Workflows (JSON) |
| **Monitoring** | AutoSys REST API + Slack webhooks | SQL Alerts + system.workflow tables |
| **Transactions** | Checkpoint/restart (manual) | Delta Lake ACID (automatic) |
| **Time Travel** | Archive directories | Delta Lake version history |
| **Lineage** | Manual (RECORD_SOURCE PSET) | Unity Catalog automatic lineage |

---

## 2. Complete Concept Mapping

### Core Concepts

| Ab Initio Concept | Databricks Equivalent | Migration Notes |
|---|---|---|
| **Graph (.mp)** | **Notebook (.py)** | Visual dataflow → PySpark code with `# COMMAND ----------` cells |
| **DML record layout** | **StructType / DDL** | Field-level type definitions; see `TYPE_MAPPING.md` |
| **PSET (Parameter Set)** | **Job parameters + widgets** | `dbutils.widgets` for runtime; `{{job.parameters.*}}` for workflow |
| **Partition** | **Spark partition** | Ab Initio: OS process. Spark: distributed task. Typically increase count. |
| **Air Sandbox** | **Databricks Workspace** | Development and execution environment |
| **`air sandbox run`** | **`notebook_task`** | Graph execution → notebook task in workflow |
| **`air_run` / `air_deploy`** | **Databricks CLI / Asset Bundle** | Deployment and promotion tooling |
| **EME (Enterprise Meta-Environment)** | **Databricks Repos** | Version-controlled code repository |
| **AutoSys job** | **Databricks Workflow** | Job scheduling and dependency management |
| **AutoSys cron** | **Quartz cron expression** | Schedule syntax differs; see mapping below |
| **Co>Operating System** | **Apache Spark** | Distributed runtime engine |
| **`set -e` (KornShell)** | **`depends_on` chain** | Error propagation: shell exit → task dependency failure |

### Data Type Mapping (Summary)

| Ab Initio Type | Spark Type | SQL Type | Notes |
|---|---|---|---|
| `string` | `StringType` | `STRING` | Direct mapping |
| `decimal` (bare) | `LongType` | `BIGINT` | Integer identifiers (no scale) |
| `decimal("p.s")` | `DecimalType(p,s)` | `DECIMAL(p,s)` | Exact numeric, monetary |
| `packed_decimal("p.s")` | `DecimalType(p,s)` | `DECIMAL(p,s)` | Mainframe format → standard decimal |
| `zoned_decimal("p.s")` | `DecimalType(p,s)` | `DECIMAL(p,s)` | Mainframe format → standard decimal |
| `date("YYYY-MM-DD")` | `DateType` | `DATE` | Direct mapping |
| `datetime` | `TimestampType` | `TIMESTAMP` | Direct mapping |
| `void` | *(omitted)* | *(omitted)* | Flat-file padding — no business data |
| Nested `record` | `StructType` | `STRUCT<...>` | Nested record → nested struct |
| `record[count]` | `ArrayType(StructType)` | `ARRAY<STRUCT<...>>` | Fixed-size array |
| `include` / type ref | Import StructType | Nested STRUCT | Reusable type definitions |
| Conditional `if` | Nullable `StructType` | Nullable `STRUCT` | Conditional presence → nullability |

> Full mapping with 28 type entries: `databricks/schemas/TYPE_MAPPING.md`

### PSET → Databricks Parameter Mapping (Summary)

| PSET Parameter | Databricks Equivalent | Status |
|---|---|---|
| `SOURCE_PATH` | `source_path` job parameter | **Mapped** |
| `TARGET_TABLE` | `target_table` job parameter | **Mapped** |
| `PARTITION_COUNT` | `partition_count` widget (increased from 4→8) | **Mapped** |
| `KEY_COLUMNS` | `key_columns` job parameter | **Mapped** |
| `HASH_COLUMNS` | `compare_columns` (minus key column) | **Mapped** (renamed) |
| `MAX_ERRORS` | `max_errors` job parameter | **Mapped** |
| `AUDIT_TABLE` | `audit_table` task parameter | **Mapped** |
| `RETENTION_DAYS` | `retention_days` job parameter | **Mapped** |
| `BATCH_SIZE` | *(absorbed)* | Spark handles batch sizing natively |
| `LOG_LEVEL` | *(absorbed)* | Cluster-level log4j configuration |
| `CHECKPOINT_DIR` | *(absorbed)* | Delta Lake ACID transactions |
| `LOAD_MODE` | *(absorbed)* | Implicit in Delta MERGE operations |
| `DML_FILE` | *(absorbed)* | Replaced by PySpark StructType imports |
| `SLA_MINUTES` | `timeout_seconds` in workflow JSON | **Mapped** (to task timeout) |
| `REJECT_PATH` | *(absorbed)* | `badRecordsPath` or Delta expectations |
| `RECORD_SOURCE` | *(absorbed)* | Unity Catalog lineage tracking |

> Full mapping with 26 PSET + 15 setenv entries: `databricks/workflows/workflow_parameters.py`

### setenv.ksh → Databricks Environment Mapping

| setenv Variable | Value | Databricks Equivalent |
|---|---|---|
| `AI_HOME` | `/opt/abinitio` | N/A — Spark is the runtime |
| `AI_PROJECT_DIR` | `/data/projects/enterprise_etl` | Databricks Repos workspace path |
| `AI_LOG_DIR` | `/data/logs/abinitio` | Automatic driver logs |
| `AI_DATA_DIR` | `/data/raw` | Cloud storage mount or Unity Catalog volume |
| `AI_STAGING_DIR` | `/data/staging` | `catalog.environment_staging` schema |
| `AI_ARCHIVE_DIR` | `/data/archive` | Delta time travel (no archive needed) |
| `AI_SOURCE_DB` | `ORACLE_PROD` | `source_catalog` job parameter → Unity Catalog |
| `AI_TARGET_DB` | `TERADATA_DW` | `target_catalog` job parameter → Unity Catalog |
| `AI_STAGING_DB` | `ORACLE_STG` | `environment_staging` schema |
| `AI_DEFAULT_PARTITIONS` | `4` | `spark.sql.shuffle.partitions` (increased to 8) |
| `AI_MAX_PARTITIONS` | `16` | Cluster autoscaling `max_workers` |
| `AI_MAX_ERRORS` | `100` | `max_errors` job parameter |
| `AI_ERROR_ACTION` | `ABORT` | Task failure stops downstream via `depends_on` |
| `AI_CHECKPOINT_ENABLED` | `true` | Delta ACID transactions (automatic) |
| `AI_CHECKPOINT_DIR` | `${AI_SANDBOX_DIR}/checkpoints` | N/A (or Structured Streaming checkpoint) |

### Monitoring Concept Mapping

| Ab Initio Monitoring | Databricks Equivalent | Source File |
|---|---|---|
| `JobMonitor.__init__(autosys_url, api_token)` | Databricks SQL Alert + system tables | `job_monitor.py:17-28` |
| `JobMonitor.get_job_status(job_name)` | `system.workflow.job_run_timeline` query | `job_monitor.py:30-41` |
| `JobMonitor.check_sla(job_name, expected_complete_by)` | `sla_breach_alert` SQL Alert | `job_monitor.py:43-65` |
| `JobMonitor.monitor_jobs(jobs, poll_interval, max_polls)` | `job_failure_alert` SQL Alert (5-min schedule) | `job_monitor.py:67-96` |
| `JobMonitor._send_alert(slack_webhook)` | Alert notification (email/Slack/PagerDuty) | `job_monitor.py:98-109` |
| `SLATracker.register_job(job_name, sla_window_end)` | Workflow `timeout_seconds` | `sla_tracker.py:29-41` |
| `SLATracker.record_completion(completed_at, status)` | Automatic — system tables record all runs | `sla_tracker.py:43-77` |
| `SLATracker.generate_report(days=7)` | `sla_compliance_summary` dashboard query | `sla_tracker.py:79-97` |
| SLA state file (`/tmp/abinitio_sla_state.json`) | `system.workflow.job_run_timeline` | `sla_tracker.py:14-24` |
| 90-day run_history retention | Unlimited history in system tables | `sla_tracker.py:71` |

### Schedule Mapping (AutoSys → Quartz Cron)

| Pipeline | AutoSys | Quartz Cron | Notes |
|---|---|---|---|
| Daily Orders | Daily (JOB_DAILY_ORDERS_LOAD) | `0 0 2 * * ?` | Daily at 02:00 UTC |
| Customer CDC | Every 4 hours (JOB_CUSTOMER_CDC) | `0 0 0/4 * * ?` | At minute 0 past every 4th hour |

---

## 3. Migration Execution Order

### Prerequisites

Before beginning the migration, ensure:

1. **Databricks workspace** provisioned with Unity Catalog enabled
2. **Catalogs created**: `main` (or equivalent) with schemas:
   - `dev_staging`, `dev_production`, `dev_audit`
   - `uat_staging`, `uat_production`, `uat_audit`
   - `prod_staging`, `prod_production`, `prod_audit`
3. **Cloud storage mounts** or Unity Catalog volumes for raw data (`/mnt/main/raw/`)
4. **Source connectivity** established (replacing ORACLE_PROD / TERADATA_DW connections)
5. **Databricks Repos** configured with access to this repository

### Execution Phases

Execute in this order — each phase builds on the previous:

```
Phase 1: DML → Delta Lake Schemas
  ├── Deploy StructType definitions to Databricks Repos
  ├── Execute DDL statements to create empty Delta tables
  └── Verify table structures in Unity Catalog

Phase 2: Graphs → PySpark Notebooks
  ├── Import notebooks to Databricks workspace
  ├── Configure widget defaults per environment
  ├── Validate notebook execution with sample data
  └── Verify schema enforcement against Phase 1 definitions

Phase 3: KornShell → Databricks Workflows
  ├── Create workflows via Databricks API (databricks jobs create --json)
  ├── Validate task dependency chains
  ├── Configure notification destinations (email, Slack)
  ├── Test with pause_status=PAUSED (manual trigger only)
  └── Switch to UNPAUSED when ready for scheduled execution

Phase 4: Monitoring → Databricks SQL Alerts
  ├── Create SQL alerts from alert_definitions.py queries
  ├── Configure alert notification destinations
  ├── Set up SLA compliance dashboard
  ├── Validate alerts fire on test failure scenarios
  └── Decommission AutoSys monitoring (job_monitor.py)
```

### Dependency Graph

```
[Phase 1: Schemas]
       │
       ▼
[Phase 2: Notebooks] ──── requires schemas for enforcement
       │
       ▼
[Phase 3: Workflows] ──── requires notebooks for task execution
       │
       ▼
[Phase 4: Monitoring] ─── requires workflows for system table data
```

---

## 4. Phase 1: DML → Delta Lake Schemas

### Files Created

| Source DML | Target Schema | Key Mapping Decision |
|---|---|---|
| `dml/customer.dml` | `databricks/schemas/customer.py` | Bare `decimal` → `BIGINT` (integer ID) |
| `dml/account_balance.dml` | `databricks/schemas/account_balance.py` | `decimal("8.2")` → `DECIMAL(8,2)` |
| `dml/order_items.dml` | `databricks/schemas/order_items.py` | Variable-length fields → `ARRAY<STRING>` |
| `dml/transaction_detail.dml` | `databricks/schemas/transaction_detail.py` | Nested `record` → `STRUCT`, conditional → nullable |
| `dml/packed_account.dml` | `databricks/schemas/packed_account.py` | `packed_decimal` → `DECIMAL(p,s)` |
| `dml/account_status.dml` | `databricks/schemas/account_status.py` | `void` fields omitted |
| `dml/customer_address.dml` | `databricks/schemas/customer_address.py` | `include` → imported `ADDRESS_T_SCHEMA` |
| `dml/common_address.dml` | `databricks/schemas/common_address.py` | Reusable type definition |

### Deployment Steps

```bash
# 1. Verify schemas load correctly
python -c "from databricks.schemas import CUSTOMER_SCHEMA; print(CUSTOMER_SCHEMA)"

# 2. In Databricks SQL, execute DDL for each table
# Example for customer table:
from databricks.schemas.customer import CUSTOMER_DDL
spark.sql(CUSTOMER_DDL)

# 3. Verify table exists in Unity Catalog
DESCRIBE TABLE EXTENDED main.dev_staging.customer;
```

### Validation

- 69 unit tests in `tests/test_delta_schemas.py`
- Each test validates: field count, field names, field types, nullability, DDL keywords

---

## 5. Phase 2: Graphs → PySpark Notebooks

### Mapping Summary

| Ab Initio Graph | Databricks Notebook | Key Transformation |
|---|---|---|
| `graphs/parallel_loader.py` | `databricks/notebooks/parallel_loader.py` | `ThreadPoolExecutor` + `air_run` subprocess → Spark `repartition()` + native DAG |
| `graphs/cdc_processor.py` | `databricks/notebooks/cdc_processor.py` | pandas + MD5 hash → Delta `MERGE INTO` + null-safe `<=>` |

### parallel_loader.py Migration Detail

| Ab Initio Component | Databricks Replacement |
|---|---|
| `PartitionManager.generate_ranges()` | `df.repartition(partition_count)` |
| `ThreadPoolExecutor(max_workers)` | Spark distributed task execution |
| `subprocess.run(air_run ...)` | `spark.read.schema(schema).format(fmt).load(path)` |
| Return code check (`result.returncode`) | Try/except with `AnalysisException` |
| Callback `on_complete(result)` | Spark event listeners (optional) |

### cdc_processor.py Migration Detail

| Ab Initio Component | Databricks Replacement |
|---|---|
| `CDCProcessor.__init__(key_columns, compare_columns)` | `CDCConfig` dataclass + `dbutils.widgets` |
| `CDCProcessor.process(source_df, target_df)` | `CDCProcessor.process()` with Delta MERGE |
| `_row_hash()` (MD5 of concatenated columns) | Null-safe `<=>` column comparison in MERGE |
| `source_keys - target_keys` (set diff for inserts) | `WHEN NOT MATCHED THEN INSERT` |
| Hash comparison for updates | `WHEN MATCHED AND (col1 <=> col2) THEN UPDATE` |
| `target_keys - source_keys` (set diff for deletes) | `WHEN NOT MATCHED BY SOURCE THEN DELETE` |
| Return `{'inserts': df, 'updates': df, 'deletes': df}` | MERGE metrics from `DeltaTable.history()` |
| No downstream change feed | Delta Change Data Feed (CDF) enabled |

### Deployment Steps

```bash
# 1. Import notebooks to Databricks workspace
databricks workspace import_dir databricks/notebooks /Repos/project/databricks/notebooks

# 2. Configure widget defaults in the notebook UI or via dbutils
dbutils.widgets.text("source_path", "/mnt/main/raw/orders")
dbutils.widgets.text("target_table", "main.dev_staging.orders")

# 3. Test with sample data (manual run)
# Run the parallel_loader notebook with small dataset
```

### Validation

- 36 unit tests in `tests/test_notebooks.py`
- Tests validate: Databricks format markers, class definitions, MERGE SQL logic, schema enforcement

---

## 6. Phase 3: KornShell → Databricks Workflows

### Pipeline Mapping

#### Daily Orders Pipeline

| KornShell Phase | Databricks Task | Notebook | Dependencies |
|---|---|---|---|
| Phase 1: `air sandbox run extract_orders.mp` | `extract_orders` | `parallel_loader` | *(none)* |
| Phase 2: `air sandbox run cdc_orders.mp -partition 4` | `cdc_orders` | `cdc_processor` | `extract_orders` |
| Phase 3: `air sandbox run load_staging_orders.mp` | `load_staging` | `parallel_loader` | `cdc_orders` |
| Phase 4: `air sandbox run prod_rollover_orders.mp` | `production_rollover` | `parallel_loader` | `load_staging` |

#### Customer CDC Pipeline

| KornShell Step | Databricks Task | Notebook | Dependencies |
|---|---|---|---|
| Step 1: `air sandbox run snapshot_customer.mp` | `snapshot_customer_source` | `parallel_loader` | *(none)* |
| Steps 2+3: `cdc_detect_customer.mp` + `apply_customer_changes.mp` | `cdc_detect_and_apply` | `cdc_processor` | `snapshot_customer_source` |
| Step 4: `air sandbox run audit_customer_changes.mp` | `audit_customer_changes` | `cdc_audit` (TBD) | `cdc_detect_and_apply` |

> **Consolidation note**: Ab Initio Steps 2 (detect via hash comparison) and 3 (apply changes from flat files) are merged into one task because Delta MERGE performs detection and application atomically.

### Deployment Steps

```bash
# 1. Create workflows via Databricks CLI
databricks jobs create --json @databricks/workflows/daily_orders_workflow.json
databricks jobs create --json @databricks/workflows/customer_cdc_workflow.json

# 2. Verify workflows appear in Databricks UI (Jobs tab)
# Both should show as PAUSED

# 3. Test with manual trigger (single run)
databricks jobs run-now --job-id <JOB_ID> --params '{"batch_date": "2024-01-01", "environment": "dev"}'

# 4. Review run results in Databricks UI

# 5. When validated, activate schedule
# Edit workflow JSON: "pause_status": "UNPAUSED"
databricks jobs reset --job-id <JOB_ID> --json @updated_workflow.json
```

### Validation

- 54 unit tests in `tests/test_workflows.py`
- Tests validate: JSON structure, task dependencies, schedule expressions, parameter mappings

---

## 7. Phase 4: Monitoring → Databricks SQL Alerts

### Alert Mapping

| Ab Initio Monitoring | Databricks Alert | Schedule | Severity |
|---|---|---|---|
| `JobMonitor.monitor_jobs()` → FAILURE | `job_failure_alert` | Every 5 min | Critical |
| `JobMonitor.check_sla()` → SLA_BREACH | `sla_breach_alert` | Every 60 min | High |
| `JobMonitor.monitor_jobs()` → poll timeout | `long_running_job_alert` | Every 10 min | Warning |
| `setenv.ksh` AI_MAX_ERRORS | `data_quality_alert` | Every 30 min | High |

### Dashboard Query Mapping

| sla_tracker.py Method | Dashboard Query | Visualization |
|---|---|---|
| `generate_report(days=7)` | `sla_compliance_summary` | Table with conditional formatting |
| `run_history[-90:]` | `sla_compliance_trend` | 90-day line chart per pipeline |
| *(new)* | `job_duration_heatmap` | Hour × day-of-week heatmap |
| `record_completion()` entries | `pipeline_run_detail` | Sortable detail table |
| `cdc_processor.py` stats dict | `cdc_change_volume` | Stacked bar chart (I/U/D) |

### Deployment Steps

```sql
-- 1. Create SQL Alerts in Databricks SQL workspace
-- Use the query_sql from each alert definition in alert_definitions.py
-- Configure trigger condition and schedule per definition

-- 2. Set up notification destinations
-- Databricks SQL → SQL Admin → Alert Destinations
-- Add: email distribution list, Slack webhook, PagerDuty integration

-- 3. Create dashboard
-- Databricks SQL → Dashboards → Create Dashboard
-- Add one widget per DASHBOARD_QUERIES entry
-- Configure auto-refresh schedules

-- 4. Test alerts
-- Manually fail a workflow run and verify alert fires
-- Verify Slack/email notifications arrive
```

### Decommissioning Legacy Monitoring

Once Databricks alerts are validated:
1. Disable AutoSys monitoring jobs (`JOB_MONITOR_*`)
2. Remove Slack webhook from `job_monitor.py` configuration
3. Archive `monitoring/` directory (do not delete — keep for reference)
4. Confirm SLA state file (`/tmp/abinitio_sla_state.json`) is no longer needed

---

## 8. Risks and Mitigations

### Critical Risks

| # | Risk | Impact | Likelihood | Mitigation |
|---|------|--------|------------|------------|
| R1 | **Packed decimal precision loss** | Data corruption in financial fields | Medium | `packed_decimal("p.s")` maps to `DECIMAL(p,s)` — precision preserved exactly. Validate with `dml/packed_account.dml` test data: `account_balance` (7.2) → `DECIMAL(7,2)`. Run `SELECT CAST('12345.67' AS DECIMAL(7,2))` to verify. |
| R2 | **CDC behavior difference** | Missing or duplicate change records | Medium | Ab Initio: hash-based comparison produces separate INSERT/UPDATE/DELETE files. Databricks: MERGE is atomic. Risk is during cutover when both systems run in parallel. **Mitigation**: Run both in parallel for 1 week, compare change counts using CDF `table_changes()` vs Ab Initio CDC output. |
| R3 | **Partition strategy mismatch** | Performance regression | Low | Ab Initio partitions = OS threads (heavy). Spark partitions = lightweight tasks. Default increased from 4→8. **Mitigation**: Monitor `job_duration_heatmap` dashboard after go-live; tune `spark.sql.shuffle.partitions` if needed. |
| R4 | **Schedule timing drift** | Jobs run at wrong time | Low | AutoSys uses server-local timezone; Databricks Quartz cron is timezone-explicit (UTC). **Mitigation**: Both workflows set `timezone_id: UTC`. Verify AutoSys source schedule is also UTC-based. |

### High Risks

| # | Risk | Impact | Likelihood | Mitigation |
|---|------|--------|------------|------------|
| R5 | **Null handling difference** | Incorrect update detection in CDC | Medium | Ab Initio: MD5 hash of `"None"` string. Databricks: null-safe `<=>` operator handles NULLs correctly. `<=>` returns TRUE when both sides are NULL (no false positives). **Mitigation**: Test with NULL-heavy sample data before cutover. |
| R6 | **PSET parameter gaps** | Notebooks missing configuration | Low | 26 PSET parameters fully documented. 8 mapped directly, 14 absorbed by platform with rationale. 4 parameters (AUDIT_TABLE, KEY_COLUMNS, etc.) have semantic changes documented. **Mitigation**: Review `workflow_parameters.py` mapping before deployment. |
| R7 | **Void field removal** | Downstream consumers expect fixed-width output | Medium | `void` fields (padding) are omitted in Delta schemas since they carry no business data. **Mitigation**: If downstream consumers expect fixed-width format, add a final export step that re-inserts padding. Document in data contract. |
| R8 | **Concurrent run conflicts** | Data corruption from overlapping runs | Low | Both workflows set `max_concurrent_runs: 1`. Delta MERGE provides ACID isolation. **Mitigation**: Monitor for concurrent write conflicts in Delta transaction log. |

### Medium Risks

| # | Risk | Impact | Likelihood | Mitigation |
|---|------|--------|------------|------------|
| R9 | **Monitoring gap during cutover** | Missed failures during transition | Medium | Keep AutoSys monitoring active until Databricks SQL Alerts are validated. Run both in parallel for minimum 1 week. |
| R10 | **SLA definition mismatch** | Alerts fire incorrectly | Low | Ab Initio: SLA defined by `sla_window_end` time (e.g. "06:00"). Databricks: SLA proxied by `timeout_seconds`. These are different measures. **Mitigation**: Configure `timeout_seconds` to match the SLA window from the batch start time. |
| R11 | **cdc_audit notebook not implemented** | Audit trail gap in customer CDC pipeline | High | The `audit_customer_changes` task references `/Repos/databricks/notebooks/cdc_audit` which is not yet implemented. **Mitigation**: Implement before enabling customer CDC workflow. Use `CDCProcessor.read_changes()` pattern from cdc_processor notebook. |

---

## 9. Validation Checklist

### Pre-Deployment Validation

```
Schema Validation:
  □ All 69 schema tests pass: pytest tests/test_delta_schemas.py -v
  □ DDL executes successfully in Databricks SQL
  □ Table structures match expected column types in Unity Catalog
  □ Nested STRUCT fields render correctly in Databricks table explorer

Notebook Validation:
  □ All 36 notebook tests pass: pytest tests/test_notebooks.py -v
  □ Notebooks import successfully into Databricks workspace
  □ Widgets render with correct defaults
  □ ParallelLoader reads from cloud storage path
  □ CDCProcessor MERGE executes against Delta table
  □ Schema enforcement rejects mismatched data

Workflow Validation:
  □ All 54 workflow tests pass: pytest tests/test_workflows.py -v
  □ Workflows created via CLI without errors
  □ Task dependency chains visible in Databricks UI DAG view
  □ Manual trigger succeeds with test parameters
  □ Notifications fire on deliberate failure

Monitoring Validation:
  □ All monitoring tests pass: pytest tests/test_monitoring.py -v
  □ SQL alerts created in Databricks SQL workspace
  □ Alert queries execute without errors against system tables
  □ Test failure triggers alert notification
  □ Dashboard renders with sample data
```

### Post-Deployment Validation

```
Parallel Run (1 week minimum):
  □ Both Ab Initio and Databricks pipelines run on same schedule
  □ Row counts match between Ab Initio target and Delta tables
  □ CDC change volumes align (compare stats dict vs CDF counts)
  □ SLA compliance matches between AutoSys monitor and SQL Alert
  □ No data quality threshold breaches (max_errors)

Performance Validation:
  □ Databricks pipeline duration ≤ Ab Initio baseline
  □ Cluster utilization reasonable (not over/under-provisioned)
  □ Delta OPTIMIZE and VACUUM running on schedule
  □ No write amplification from small files
```

---

## 10. Rollback Strategy

### Phase-Level Rollback

Each phase can be rolled back independently:

| Phase | Rollback Action | Impact |
|---|---|---|
| **Schemas** | `DROP TABLE` the Delta tables | No impact — schemas are additive |
| **Notebooks** | Remove from Databricks Repos | No impact — workflows won't run |
| **Workflows** | Set `pause_status: PAUSED` or delete via CLI | Immediate — workflows stop running |
| **Monitoring** | Delete SQL Alerts, re-enable AutoSys monitoring | Immediate — monitoring switches back |

### Full Rollback Procedure

```bash
# 1. Pause all Databricks workflows
databricks jobs list --output JSON | jq '.[].job_id' | \
  xargs -I {} databricks jobs reset --job-id {} --json '{"pause_status": "PAUSED"}'

# 2. Re-enable AutoSys jobs
# (AutoSys admin action — outside Databricks scope)

# 3. Re-enable AutoSys monitoring
# Re-point job_monitor.py Slack webhook
# Re-enable SLA tracker state file writing

# 4. (Optional) Drop Delta tables if needed
# Only if data was written and needs cleanup
```

### Point of No Return

The migration reaches its point of no return when:
- Production consumers start reading from Delta tables (instead of Ab Initio output files)
- AutoSys monitoring jobs are deleted (not just disabled)
- Ab Initio sandbox/project directory is archived

Until that point, the Ab Initio estate can be reactivated with minimal effort.

---

## 11. Post-Migration Operations

### Ongoing Maintenance

| Task | Frequency | Command / Action |
|---|---|---|
| Delta OPTIMIZE | Weekly | `OPTIMIZE catalog.schema.table` |
| Delta VACUUM | Weekly | `VACUUM catalog.schema.table RETAIN 168 HOURS` |
| Review SLA dashboard | Daily | Check `sla_compliance_summary` dashboard |
| Review alert history | Daily | Check Databricks SQL Alert history |
| Update cluster sizing | Monthly | Review `job_duration_heatmap` for trends |
| Rotate notification webhooks | Quarterly | Update Slack/PagerDuty integration keys |

### Future Enhancements

1. **Implement `cdc_audit` notebook** — Required for the `audit_customer_changes` task in the customer CDC workflow. Pattern: read CDF from customer_master, write to audit table.

2. **Databricks Asset Bundle** — Package workflows, notebooks, and schemas into a `databricks.yml` bundle for CI/CD deployment across environments.

3. **Unity Catalog Data Contracts** — Add column-level expectations and data quality rules to Delta tables (replacing Ab Initio reject-port logic).

4. **Structured Streaming** — For tables requiring real-time CDC (sub-4-hour latency), convert batch MERGE to Structured Streaming with `foreachBatch`.

5. **Delta Sharing** — For downstream consumers currently reading Ab Initio output files, expose Delta tables via Delta Sharing protocol.

---

## Appendix: File Inventory

### Databricks Artifacts Created

```
databricks/
├── __init__.py
├── schemas/
│   ├── __init__.py                    # Exports all schemas
│   ├── TYPE_MAPPING.md                # Complete type mapping reference
│   ├── customer.py                    # CUSTOMER_SCHEMA + CUSTOMER_DDL
│   ├── account_balance.py             # ACCOUNT_BALANCE_SCHEMA + DDL
│   ├── order_items.py                 # ORDER_ITEMS_SCHEMA + DDL
│   ├── transaction_detail.py          # TRANSACTION_DETAIL_SCHEMA + DDL
│   ├── packed_account.py              # PACKED_ACCOUNT_SCHEMA + DDL
│   ├── account_status.py              # ACCOUNT_STATUS_SCHEMA + DDL
│   ├── customer_address.py            # CUSTOMER_ADDRESS_SCHEMA + DDL
│   └── common_address.py              # ADDRESS_T_SCHEMA (reusable type)
├── notebooks/
│   ├── __init__.py
│   ├── parallel_loader.py             # Partition-based parallel ingestion
│   └── cdc_processor.py               # Delta MERGE CDC processor
├── workflows/
│   ├── __init__.py
│   ├── daily_orders_workflow.json      # Daily orders pipeline (4 tasks)
│   ├── customer_cdc_workflow.json      # Customer CDC pipeline (3 tasks)
│   ├── workflow_parameters.py          # PSET → Databricks parameter mapping
│   └── MIGRATION_GUIDE.md             # Workflow-specific migration guide
└── monitoring/
    ├── __init__.py
    ├── alert_definitions.py            # 4 SQL alert definitions
    └── sla_dashboard.py                # 5 dashboard query definitions

docs/
└── ABINITIO_TO_DATABRICKS_RUNBOOK.md   # This file

tests/
├── test_delta_schemas.py               # 69 schema tests
├── test_notebooks.py                   # 36 notebook tests
├── test_workflows.py                   # 54 workflow tests
└── test_monitoring.py                  # Monitoring tests
```

### Test Coverage

| Test Suite | Tests | Validates |
|---|---|---|
| `test_delta_schemas.py` | 69 | Field types, counts, DDL validity, nested structs |
| `test_notebooks.py` | 36 | Databricks format, class structure, SQL logic |
| `test_workflows.py` | 54 | JSON structure, dependencies, parameters, schedules |
| `test_monitoring.py` | TBD | Alert definitions, dashboard queries, validation |
| **Total new** | **159+** | |
