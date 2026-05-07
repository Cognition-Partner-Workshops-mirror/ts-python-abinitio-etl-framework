# Ab Initio to Databricks Lakehouse Migration Runbook

## 1. Overview

This document describes the complete migration of the Ab Initio ETL estate to Databricks Lakehouse architecture. It covers concept mapping, artifact inventory, execution order, type mapping decisions, and risks.

### Source Estate

| Component | Location | Count | Description |
|-----------|----------|-------|-------------|
| DML Record Layouts | `dml/` | 8 files | Ab Initio schema definitions (flat, nested, packed decimal) |
| Graph Patterns | `graphs/` | 2 modules | Parallel loader + CDC processor |
| PSET Templates | `psets/pset_templates/` | 3 files | Environment-aware pipeline configuration |
| KornShell Scripts | `scripts/` | 3 files | AutoSys-triggered batch orchestration |
| Monitoring | `monitoring/` | 2 modules | Job status polling + SLA tracking |
| Utilities | `utils/` | 1 module | DML parser for schema extraction |

### Target Architecture

| Component | Location | Description |
|-----------|----------|-------------|
| Delta Lake Schemas | `databricks/schemas/` | PySpark StructType definitions + CREATE TABLE DDL |
| PySpark Notebooks | `databricks/notebooks/` | Parallel ingestion + CDC MERGE notebooks |
| Databricks Workflows | `databricks/workflows/` | Job definitions with task dependencies |
| SQL Alerts & Dashboards | `databricks/monitoring/` | Failure alerts + SLA compliance dashboard |

---

## 2. Complete Concept Mapping

| Ab Initio Concept | Component | Databricks Equivalent | Notes |
|---|---|---|---|
| **Graph (.mp)** | Visual dataflow program | Databricks Notebook / Spark Job | One notebook per graph; Spark DAG replaces visual dataflow |
| **DML record layout** | Schema definition | PySpark `StructType` + Delta DDL | See Section 3 for type mapping |
| **PSET (Parameter Set)** | Runtime config per env | Databricks job parameters + widgets | `dbutils.widgets` for interactive; job params for automation |
| **`air sandbox run`** | Graph execution CLI | `dbutils.notebook.run()` / Jobs API | No subprocess spawning needed |
| **`m_partition`** | Parallel execution unit | `repartition()` / `partitionBy()` | Spark handles parallelism natively |
| **Partition ranges** | Manual record splitting | Spark partitioning (hash/range) | `PartitionManager.generate_ranges()` replaced by Catalyst |
| **`ThreadPoolExecutor`** | OS-level parallelism | Spark DAG scheduler | No manual thread management |
| **CDC (hash compare)** | Row-level MD5 comparison | Delta Lake `MERGE INTO` | Single atomic operation replaces 4-step compare+apply |
| **AutoSys / Control-M** | Job scheduler | Databricks Workflows | Cron schedule + task dependencies in JSON |
| **KornShell (.ksh)** | Pipeline orchestration | Workflow JSON definition | Linear phase dependencies → DAG task dependencies |
| **`setenv.ksh`** | Environment variables | Job parameters + cluster config | Paths, connections, defaults externalized |
| **PSET `define`** | Parameter declaration | Widget defaults / job params | `define KEY VALUE` → `"default": "value"` |
| **PSET env override** | Per-env config files | Databricks job parameter overrides | `psets/dev/` vs `psets/prod/` → environment-specific job configs |
| **AutoSys polling** | `get_job_status` API | `system.workflow.job_run_timeline` | System table replaces REST polling |
| **Slack webhook alert** | `_send_alert()` | Databricks SQL Alert notifications | Native notification destinations |
| **SLA tracking (JSON)** | File-based state | Databricks SQL dashboard query | System tables eliminate file I/O |
| **`AI_MAX_ERRORS`** | Error threshold | `max_errors` widget + accumulator | Spark accumulator tracks errors |
| **`AI_ERROR_ACTION`** | ABORT/CONTINUE/SKIP | Notebook exit with status JSON | `dbutils.notebook.exit()` for controlled failure |
| **Checkpoint/restart** | `AI_CHECKPOINT_DIR` | Delta transaction log | Delta's ACID guarantees eliminate manual checkpointing |
| **Co>Operating System** | Runtime engine | Spark cluster | Cluster config replaces CoOp installation |
| **EME (code repo)** | Ab Initio code mgmt | Databricks Repos / Git integration | Notebooks versioned in Git |
| **`air_deploy`** | Deployment CLI | Databricks Asset Bundles / CI/CD | `databricks bundle deploy` |
| **ServiceNow CR** | Change management | Databricks + external CI/CD | PR-based workflow replaces ServiceNow CRs |
| **Nested record** | `record ... end` block | `StructType` / `STRUCT<>` | Nested structs preserve hierarchy |
| **Variable-length array** | `field[count]` | `ArrayType` / `ARRAY<>` | Repeat groups → Spark arrays |
| **Conditional record** | `if (condition)` | Nullable struct column | NULL when condition is false |
| **Type definition** | `type name = record` | Reusable `StructType` variable | Python import for reuse |
| **Include directive** | `include "file.dml"` | Python import | `from .common_address import address_schema` |
| **Void field** | Padding/filler bytes | *(dropped)* | No semantic value; dropped in migration |

---

## 3. DML Type Mapping Reference

### Standard Types

| Ab Initio DML Type | Example | Spark Type | Delta DDL Type | Decision Rationale |
|---|---|---|---|---|
| `decimal(",")` | `customer_id` | `LongType` | `BIGINT` | Integer keys with no fractional component |
| `decimal("P.S", delim)` | `decimal("8.2", "\|")` | `DecimalType(P, S)` | `DECIMAL(P,S)` | Precision and scale preserved exactly |
| `string(delim)` | `string(",")` | `StringType` | `STRING` | Direct mapping |
| `string(N)` | `string(20)` | `StringType` | `STRING` | Fixed-width → variable-length; RTRIM at ingestion |
| `date("fmt")(delim)` | `date("YYYY-MM-DD")` | `DateType` | `DATE` | Direct mapping |
| `datetime("fmt")(delim)` | `datetime("YYYY-MM-DD HH24:MI:SS")` | `TimestampType` | `TIMESTAMP` | Direct mapping |
| `void(delim)` | `void(",")` | *(dropped)* | *(dropped)* | Padding with no semantic value |

### Mainframe Numeric Types

| Ab Initio DML Type | Example | Spark Type | Decision Rationale |
|---|---|---|---|
| `packed_decimal(N)` | `packed_decimal(5)` | `DecimalType(9, 0)` | BCD: N bytes → (2N-1) digits. 5 bytes = 9 digits + sign nibble |
| `packed_decimal("P.S")` | `packed_decimal("7.2")` | `DecimalType(7, 2)` | Explicit precision.scale notation preserved directly |
| `zoned_decimal(N)` | `zoned_decimal(4)` | `DecimalType(4, 0)` | EBCDIC zoned: 1 byte = 1 digit. 4 bytes = 4 digits |

### Complex Structures

| Ab Initio DML Pattern | Example | Spark Equivalent |
|---|---|---|
| Nested `record ... end name` | `merchant_info` in transaction_detail | `StructField("name", StructType([...]))` |
| Variable-length `field[count]` | `string(",")[item_count] item_names` | `ArrayType(StringType())` |
| Nested repeat `record[count]` | `record[item_count] ... end line_items` | `ArrayType(StructType([...]))` |
| Conditional `if (expr) record` | `if (txn_type == 2) ... refund_details` | Nullable `StructType` (NULL when condition false) |
| Type definition `type T = record` | `type address_t = record` | Reusable `StructType` variable (Python import) |
| Include directive `include "f.dml"` | `include "common_address.dml"` | `from .common_address import address_schema` |
| Null sentinel `null("val")` | `string(",", null(""))` | `COALESCE` / `NULLIF` at ingestion |

---

## 4. Artifact Inventory

### Schemas (`databricks/schemas/`)

| File | Source DML | Key Features |
|---|---|---|
| `customer.py` | `dml/customer.dml` | Simple flat record |
| `order_items.py` | `dml/order_items.dml` | Variable-length arrays (`ARRAY<STRING>`, `ARRAY<INT>`) |
| `transaction_detail.py` | `dml/transaction_detail.dml` | Nested structs, arrays of structs, conditional record |
| `account_balance.py` | `dml/account_balance.dml` | `DECIMAL(8,2)`, `DATE` type, pipe-delimited source |
| `account_status.py` | `dml/account_status.dml` | Void field handling (padding dropped) |
| `common_address.py` | `dml/common_address.dml` | Reusable type definition (no standalone table) |
| `customer_address.py` | `dml/customer_address.dml` | Include directive → nested struct import |
| `packed_account.py` | `dml/packed_account.dml` | Packed/zoned decimal (mainframe formats) |

### Notebooks (`databricks/notebooks/`)

| File | Source Graph | Key Features |
|---|---|---|
| `parallel_loader.py` | `graphs/parallel_loader.py` | Widget-parameterized ingestion, `repartition()`, error thresholds, audit columns |
| `cdc_processor.py` | `graphs/cdc_processor.py` | Delta MERGE with hash-based change detection, CDF audit, full/incremental modes |

### Workflows (`databricks/workflows/`)

| File | Source Script | Schedule | Tasks |
|---|---|---|---|
| `daily_orders_workflow.json` | `scripts/run_daily_orders.ksh` | Daily 02:00 UTC | extract → CDC → staging → production (4 tasks) |
| `customer_cdc_workflow.json` | `scripts/run_customer_cdc.ksh` | Every 4 hours | snapshot → CDC detect → apply/optimize → audit (4 tasks) |

### Monitoring (`databricks/monitoring/`)

| File | Source Module | Purpose |
|---|---|---|
| `job_failure_alert.sql` | `monitoring/job_monitor.py` | SQL alert for failed runs (replaces AutoSys polling + Slack webhook) |
| `sla_compliance_dashboard.sql` | `monitoring/sla_tracker.py` | 90-day SLA compliance dashboard (replaces JSON state file) |

---

## 5. Migration Execution Order

Execute the migration in this order to satisfy dependencies:

### Phase 1: Foundation (Day 1-2)

1. **Create Unity Catalog structure**
   ```sql
   CREATE CATALOG IF NOT EXISTS catalog;
   CREATE SCHEMA IF NOT EXISTS catalog.bronze;
   CREATE SCHEMA IF NOT EXISTS catalog.silver;
   CREATE SCHEMA IF NOT EXISTS catalog.gold;
   CREATE SCHEMA IF NOT EXISTS catalog.audit;
   ```

2. **Deploy schema definitions** — Run each notebook in `databricks/schemas/` to create Delta tables:
   - `common_address.py` (reusable type — no table, but must be available for import)
   - All other schema files (order does not matter)

3. **Verify table creation**
   ```sql
   SHOW TABLES IN catalog.bronze;
   DESCRIBE EXTENDED catalog.bronze.customer;
   ```

### Phase 2: Core Notebooks (Day 3-4)

4. **Deploy notebooks** to Databricks Repos:
   - `databricks/notebooks/parallel_loader.py`
   - `databricks/notebooks/cdc_processor.py`

5. **Smoke test with sample data**:
   - Upload `data/sample/customers.dat` and `data/sample/orders.dat` to DBFS/cloud storage
   - Run `parallel_loader` with sample source path
   - Run `cdc_processor` against the loaded data

### Phase 3: Workflows (Day 5-6)

6. **Create workflows** via Databricks Jobs API or UI:
   - Import `databricks/workflows/daily_orders_workflow.json`
   - Import `databricks/workflows/customer_cdc_workflow.json`
   - Both are created in `PAUSED` state

7. **Configure job parameters** per environment:
   - Update `source_path`, `target_table`, cluster sizing for DEV/UAT/PROD
   - This replaces the PSET per-environment override pattern (`psets/dev/`, `psets/prod/`)

8. **Test workflows end-to-end** in DEV:
   - Trigger manual run of `daily_orders_pipeline`
   - Trigger manual run of `customer_cdc_pipeline`
   - Verify all tasks complete successfully

### Phase 4: Monitoring (Day 7)

9. **Deploy SQL alerts and dashboard**:
   - Create SQL query from `databricks/monitoring/job_failure_alert.sql`
   - Create SQL alert with 5-minute schedule
   - Create dashboard from `databricks/monitoring/sla_compliance_dashboard.sql`
   - Configure notification destinations (Slack, email)

10. **Validate alerting** — Trigger a deliberate failure and confirm alert fires.

### Phase 5: Parallel Run (Day 8-14)

11. **Run Ab Initio and Databricks in parallel** for 1-2 weeks:
    - Compare row counts and checksums between legacy target (Teradata) and Delta tables
    - Validate SLA compliance in both systems
    - Monitor for data discrepancies

### Phase 6: Cutover (Day 15+)

12. **Cutover checklist**:
    - [ ] All data reconciliation checks pass
    - [ ] SLA compliance meets or exceeds Ab Initio baseline
    - [ ] Alert notifications confirmed working
    - [ ] Downstream consumers updated to read from Delta tables
    - [ ] AutoSys jobs disabled
    - [ ] Ab Initio graphs archived
    - [ ] Workflow schedules unpaused (`PAUSED` → `UNPAUSED`)

---

## 6. PSET to Databricks Parameter Mapping

### Orders Pipeline (`psets/pset_templates/orders_pipeline.pset`)

| PSET Parameter | Workflow Parameter | Widget Name | Default |
|---|---|---|---|
| `SOURCE_PATH` | `source_path` | `source_path` | `/data/raw/orders` |
| `TARGET_TABLE` | `target_table` | `target_table` | `catalog.bronze.orders` |
| `PARTITION_COUNT` | `partition_count` | `partition_count` | `4` |
| `BATCH_SIZE` | *(Spark auto-tuned)* | — | Spark handles batch sizing |
| `LOG_LEVEL` | `log_level` | `log_level` | `INFO` |
| `RECORD_SOURCE` | *(metadata column)* | — | Added as `_source` audit column |
| `MAX_ERRORS` | `max_errors` | `max_errors` | `100` |
| `CHECKPOINT_DIR` | `checkpoint_dir` | `checkpoint_dir` | Delta tx log replaces this |

### Customer CDC (`psets/pset_templates/customer_cdc.pset`)

| PSET Parameter | Workflow Parameter | Widget Name | Default |
|---|---|---|---|
| `SOURCE_PATH` | `source_path` | `source_path` | `/data/raw/customer` |
| `TARGET_TABLE` | `target_table` | `target_table` | `catalog.silver.customer_master` |
| `PREVIOUS_SNAPSHOT_PATH` | *(not needed)* | — | Delta versioning replaces snapshot files |
| `CURRENT_SNAPSHOT_PATH` | *(not needed)* | — | Delta versioning replaces snapshot files |
| `CDC_OUTPUT_PATH` | *(not needed)* | — | MERGE applies changes in-place |
| `PARTITION_COUNT` | `partition_count` | `partition_count` | `8` |
| `HASH_COLUMNS` | `hash_columns` | `hash_columns` | `customer_id,name,address,phone,email,status` |
| `KEY_COLUMNS` | `key_columns` | `key_columns` | `customer_id` |
| `BATCH_SIZE` | `batch_size` | `batch_size` | `100000` |
| `MAX_ERRORS` | `max_errors` | `max_errors` | `50` |
| `AUDIT_TABLE` | `audit_table` | `audit_table` | `catalog.audit.customer_cdc_log` |
| `RETENTION_DAYS` | `retention_days` | `retention_days` | `90` |

### Environment Overrides

The Ab Initio pattern of per-environment PSET directories (`psets/dev/`, `psets/uat/`, `psets/prod/`) is replaced by:

1. **Databricks job parameter overrides** — each environment has its own job definition (or the same job with different parameter defaults).
2. **Unity Catalog namespacing** — `dev_catalog.bronze.orders` vs `prod_catalog.bronze.orders`.
3. **Cluster policies** — DEV uses smaller clusters; PROD uses autoscaling.

---

## 7. Risks and Mitigations

### High Risk

| Risk | Impact | Mitigation |
|---|---|---|
| **Packed decimal precision loss** | Financial data corruption if BCD→Decimal conversion loses digits | Use `DecimalType` with exact precision: `packed_decimal(5)` → `DECIMAL(9,0)`. Validate with checksums against source. See `databricks/schemas/packed_account.py` for mapping. |
| **Zoned decimal EBCDIC encoding** | Incorrect numeric values if zone nibbles not stripped | Pre-process EBCDIC data through a conversion layer before Spark ingestion. Test with `data/sample/` files. |
| **CDC semantic gap** | Ab Initio 4-step CDC (snapshot→compare→apply→audit) vs Delta MERGE (single atomic op) may produce different intermediate states | Run parallel comparison for 2 weeks. Delta MERGE is more consistent (atomic) but downstream consumers expecting intermediate files need adjustment. |
| **Partition strategy differences** | Ab Initio explicit range-based partitions vs Spark hash partitions may produce different data distribution | Monitor partition skew via `spark.sql("DESCRIBE DETAIL table").select("numFiles", "sizeInBytes")`. Use `ZORDER BY` for query-aligned partitioning. |

### Medium Risk

| Risk | Impact | Mitigation |
|---|---|---|
| **Void field removal** | Downstream consumers relying on field ordinal positions will break | Document dropped fields in schema comments. Update all consumers to use named column access. |
| **Null sentinel differences** | Ab Initio `null("")` and `null("UNKNOWN")` may not match Spark NULL handling | Apply `NULLIF` / `COALESCE` transforms during ingestion. Test with edge cases. |
| **Variable-length array flattening** | Ab Initio `[item_count]` repeat groups stored as flat delimited fields vs Spark `ARRAY` columns | ETL consumers that parse flat files need to be updated to read array columns. Provide a flattening view for backward compat if needed. |
| **SLA window interpretation** | Ab Initio SLA based on AutoSys completion time vs Databricks based on workflow end time | Verify clock sync between systems. Use UTC consistently. SLA definitions in dashboard query should match the original `SLATracker.register_job()` values. |
| **BATCH_SIZE parameter removal** | Ab Initio explicit batching vs Spark auto-tuned shuffle | Spark's Catalyst optimizer handles batch sizing. If specific batch control is needed, use `spark.sql.shuffle.partitions` cluster config. |

### Low Risk

| Risk | Impact | Mitigation |
|---|---|---|
| **Checkpoint/restart differences** | Ab Initio file-based checkpoints vs Delta transaction log | Delta's ACID guarantees provide stronger recovery. No manual checkpoint management needed. |
| **Log format differences** | Ab Initio logs to `${AI_LOG_DIR}` vs Databricks driver logs | Configure log4j in cluster init scripts if specific log format is required. Databricks provides built-in log viewer. |
| **Workflow schedule timezone** | AutoSys may use local timezone vs Databricks UTC | All workflow schedules set to UTC. Verify original AutoSys timezone and adjust if needed. |

---

## 8. Rollback Plan

If critical issues are found during parallel run:

1. **Pause Databricks workflows** (schedules are created in `PAUSED` state)
2. **Re-enable AutoSys jobs** (no changes made to Ab Initio graphs)
3. **Investigate discrepancies** using the audit tables (`catalog.audit.*`)
4. **Fix and re-deploy** — Delta tables can be rolled back via time travel:
   ```sql
   RESTORE TABLE catalog.bronze.customer TO VERSION AS OF <version>;
   ```

---

## 9. Post-Migration Cleanup

After successful cutover and stabilization (30 days recommended):

- [ ] Archive Ab Initio graphs and PSETs
- [ ] Decommission AutoSys jobs
- [ ] Remove legacy `setenv.ksh` environment variables from servers
- [ ] Update data lineage documentation
- [ ] Remove `monitoring/job_monitor.py` AutoSys API integration
- [ ] Clean up snapshot directories (`/data/snapshots/`)
- [ ] Update ServiceNow CMDB to reflect new Databricks platform
