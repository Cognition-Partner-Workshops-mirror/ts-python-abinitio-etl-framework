# KornShell → Databricks Workflows Migration Guide

This document describes how the Ab Initio KornShell orchestration scripts were migrated to Databricks Workflow JSON definitions.

---

## Pipeline Mapping

| KornShell Script | Databricks Workflow | Schedule | Tasks |
|---|---|---|---|
| `scripts/run_daily_orders.ksh` | `daily_orders_workflow.json` | Daily at 02:00 UTC | 4 tasks: extract → CDC → staging → production |
| `scripts/run_customer_cdc.ksh` | `customer_cdc_workflow.json` | Every 4 hours | 3 tasks: snapshot → CDC+apply → audit |

---

## Orchestration Concept Mapping

| Ab Initio / KornShell | Databricks Equivalent | Notes |
|---|---|---|
| AutoSys job trigger | Databricks Workflow schedule (Quartz cron) | `quartz_cron_expression` in workflow JSON |
| `set -e` (fail on error) | Task `depends_on` chain | Downstream tasks don't run if upstream fails |
| `air sandbox run graph.mp` | `notebook_task` execution | Each `air_run` becomes a notebook task |
| `-pset path.pset` | `base_parameters` on notebook task | PSET values become widget defaults |
| `-param KEY=VALUE` | Job-level `parameters` with `{{job.parameters.*}}` | Dynamic parameter resolution |
| `-partition N` | Spark native partitioning (`repartition(N)`) | No explicit partition parameter needed |
| `-log path.log` | Automatic driver/executor logs | Databricks captures stdout/stderr automatically |
| `RC=$?; if [ $RC -ne 0 ]` | Task failure triggers | Failed tasks stop the depends_on chain |
| `tee -a run.log` | Job run history | Full execution history in Databricks UI |
| Sequential `air_run` calls | `depends_on` task dependencies | Explicit DAG instead of linear script |
| `. setenv.ksh` | Cluster-level `spark_conf` + job `parameters` | Environment variables → Spark config + parameters |

---

## Daily Orders Pipeline (`run_daily_orders.ksh`)

### Original KornShell Flow
```
Phase 1: air sandbox run extract_orders.mp      -pset orders_extract.pset
Phase 2: air sandbox run cdc_orders.mp           -pset orders_cdc.pset      -partition 4
Phase 3: air sandbox run load_staging_orders.mp   -pset orders_staging.pset
Phase 4: air sandbox run prod_rollover_orders.mp  -pset orders_prod.pset
```

### Databricks Workflow Tasks
```
extract_orders → cdc_orders → load_staging → production_rollover
```

| KornShell Phase | Workflow Task | Notebook | Key Change |
|---|---|---|---|
| Phase 1: Extract | `extract_orders` | `parallel_loader` | File read → Delta table (ACID writes) |
| Phase 2: CDC | `cdc_orders` | `cdc_processor` | Hash comparison → Delta MERGE |
| Phase 3: Staging | `load_staging` | `parallel_loader` | File copy → Delta-to-Delta load |
| Phase 4: Production | `production_rollover` | `parallel_loader` | No retries (atomic writes prevent inconsistency) |

### Error Handling Migration
- **KornShell:** `set -e` + `if [ $RC -ne 0 ]; then exit $RC; fi`
- **Databricks:** Task failure automatically stops downstream `depends_on` tasks. `max_retries` configures retry policy per task. Production rollover has `max_retries: 0` to prevent duplicate writes.

---

## Customer CDC Pipeline (`run_customer_cdc.ksh`)

### Original KornShell Flow
```
Step 1: air sandbox run snapshot_customer.mp       -pset customer_snapshot.pset
Step 2: air sandbox run cdc_detect_customer.mp     -pset customer_cdc.pset     -partition 8
Step 3: air sandbox run apply_customer_changes.mp  -pset customer_apply.pset
Step 4: air sandbox run audit_customer_changes.mp  -pset customer_audit.pset
```

### Databricks Workflow Tasks
```
snapshot_customer_source → cdc_detect_and_apply → audit_customer_changes
```

**Key optimization:** Steps 2 + 3 are consolidated into one task (`cdc_detect_and_apply`). In Ab Initio, CDC detection (Step 2) wrote separate INSERT/UPDATE/DELETE files, then apply (Step 3) loaded them into the target. Delta Lake MERGE does both detection and apply in a single atomic operation.

| KornShell Step | Workflow Task | Notebook | Key Change |
|---|---|---|---|
| Step 1: Snapshot | `snapshot_customer_source` | `parallel_loader` | Flat-file snapshot → Delta table |
| Step 2+3: Detect + Apply | `cdc_detect_and_apply` | `cdc_processor` | 2 graphs → 1 MERGE statement |
| Step 4: Audit | `audit_customer_changes` | `cdc_audit` (TBD) | CDC files → Change Data Feed |

### Schedule Migration
- **AutoSys:** `JOB_CUSTOMER_CDC` every 4 hours
- **Databricks:** `"quartz_cron_expression": "0 0 0/4 * * ?"` (minute 0, every 4th hour)

---

## PSET → Databricks Parameter Mapping

### Parameters With Direct Mapping

| PSET Key | Databricks Parameter | Widget | Default Change |
|---|---|---|---|
| `SOURCE_PATH` | `source_path` | `source_path` | File path → cloud mount path |
| `TARGET_TABLE` | `target_table` | `target_table` | DB.SCHEMA.TABLE → catalog.schema.table |
| `PARTITION_COUNT` | `partition_count` | `partition_count` | 4 → 8 (Spark partitions are lighter) |
| `KEY_COLUMNS` | `key_columns` | `key_columns` | Direct mapping |
| `HASH_COLUMNS` | `compare_columns` | `compare_columns` | Key column removed; no hashing |
| `MAX_ERRORS` | `max_errors` | — | Direct mapping (100) |
| `AUDIT_TABLE` | `audit_table` | — | DB.TABLE → catalog.schema.table |
| `RETENTION_DAYS` | `retention_days` | — | Direct mapping (90) |

### Parameters Absorbed by Platform (No Databricks Equivalent)

| PSET Key | Why No Mapping | Platform Feature |
|---|---|---|
| `BATCH_SIZE` | Spark handles batch sizing via partition splits | Automatic |
| `LOG_LEVEL` | Cluster-level log4j configuration | Spark config |
| `RECORD_SOURCE` | Unity Catalog lineage tracking | Automatic |
| `CHECKPOINT_DIR` | Delta Lake ACID transactions | Built-in |
| `LOAD_MODE` | Implicit in MERGE vs append write mode | Notebook logic |
| `REJECT_PATH` | `badRecordsPath` option or DQ expectations | Spark option |
| `DML_FILE` | PySpark StructType in `databricks/schemas/` | Python import |
| `SLA_MINUTES` | `timeout_seconds` on workflow task | Workflow config |
| `PREVIOUS_SNAPSHOT_PATH` | Delta MERGE compares directly | Built-in |
| `CURRENT_SNAPSHOT_PATH` | Delta table serves as snapshot | Delta table |
| `CDC_OUTPUT_PATH` | Change Data Feed (CDF) | Built-in |

### setenv.ksh → Databricks Equivalents

| Shell Variable | Value | Databricks Equivalent |
|---|---|---|
| `AI_HOME` | `/opt/abinitio` | N/A (Spark is the runtime) |
| `AI_PROJECT_DIR` | `/data/projects/enterprise_etl` | Databricks Repos workspace path |
| `AI_LOG_DIR` | `/data/logs/abinitio` | Automatic job run logs |
| `AI_DATA_DIR` | `/data/raw` | Cloud storage mount (`/mnt/`) or Volume |
| `AI_STAGING_DIR` | `/data/staging` | Delta staging schema |
| `AI_ARCHIVE_DIR` | `/data/archive` | Delta time travel (no archive needed) |
| `AI_SOURCE_DB` | `ORACLE_PROD` | `source_catalog` parameter |
| `AI_TARGET_DB` | `TERADATA_DW` | `target_catalog` parameter |
| `AI_DEFAULT_PARTITIONS` | `4` | `spark.sql.shuffle.partitions` (8) |
| `AI_MAX_PARTITIONS` | `16` | Cluster autoscaling `max_workers` |
| `AI_MAX_ERRORS` | `100` | `max_errors` job parameter |
| `AI_ERROR_ACTION` | `ABORT` | Task failure stops depends_on chain |
| `AI_CHECKPOINT_ENABLED` | `true` | Delta ACID (automatic) |

---

## Deploying Workflows

### Using Databricks CLI
```bash
# Deploy the daily orders workflow
databricks jobs create --json @databricks/workflows/daily_orders_workflow.json

# Deploy the customer CDC workflow
databricks jobs create --json @databricks/workflows/customer_cdc_workflow.json
```

### Using Databricks Asset Bundles (DAB)
For production deployments, wrap these workflow definitions in a Databricks Asset Bundle (`databricks.yml`) for CI/CD integration. This replaces the Ab Initio ServiceNow/UrbanCode deployment pipeline.

### Activation
Both workflows are created with `"pause_status": "PAUSED"`. After validation:
1. Review task parameters match your environment (catalog names, paths)
2. Update `email_notifications` and `webhook_notifications`
3. Set `pause_status` to `"UNPAUSED"` to activate scheduling

---

## Key Architectural Improvements

1. **Atomic operations:** Delta MERGE provides ACID guarantees. No partial writes, no manual checkpoints.
2. **Consolidated steps:** CDC detect + apply → single MERGE statement (2 graphs → 1 task).
3. **Built-in CDC propagation:** Change Data Feed replaces flat-file CDC output.
4. **Time travel:** Delta history replaces manual archive/snapshot management.
5. **Declarative DAG:** `depends_on` replaces sequential shell script execution.
6. **Automatic retry:** Per-task retry policy replaces manual RC check + re-run.
7. **Centralized monitoring:** Databricks UI + SQL Alerts replace AutoSys + custom monitoring.

See `workflow_parameters.py` for the complete programmatic mapping reference.
