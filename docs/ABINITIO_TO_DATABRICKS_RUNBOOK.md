# Ab Initio → Databricks Lakehouse Migration Runbook

## Overview

This document describes the migration of the enterprise Ab Initio ETL framework to Databricks Lakehouse architecture. It covers concept mappings, execution order, type conversions, and risk mitigations.

**Source estate:** Ab Initio Co>Operating System ETL with AutoSys orchestration, DML schemas, PSET configuration, and KornShell batch pipelines.

**Target architecture:** Databricks Lakehouse with PySpark notebooks, Delta Lake tables, Databricks Workflows, and SQL alerts.

---

## 1. Complete Concept Mapping Table

| # | Ab Initio Concept | Legacy Location | Databricks Equivalent | Migrated Location |
|---|---|---|---|---|
| 1 | **Graph (.mp)** — visual dataflow program | `graphs/` | PySpark Notebook | `databricks/notebooks/` |
| 2 | **DML record layout** — schema definition | `dml/*.dml` | PySpark StructType + Delta DDL | `databricks/schemas/` |
| 3 | **PSET** — runtime parameter set | `psets/pset_templates/*.pset` | Databricks job parameters + widgets | `databricks/workflows/*.json` `parameters` section |
| 4 | **Partition (m_partition)** — parallel execution unit | `PartitionManager` in `graphs/parallel_loader.py` | Spark `repartition()` / Delta `PARTITIONED BY` | Built-in to Spark execution |
| 5 | **CDC (Compare Records by Key)** — hash-based change detection | `graphs/cdc_processor.py` | Delta Lake `MERGE` + Change Data Feed | `databricks/notebooks/cdc_processor.py` |
| 6 | **AutoSys job** — scheduled execution | `scripts/run_daily_orders.ksh`, `run_customer_cdc.ksh` | Databricks Workflow JSON | `databricks/workflows/daily_orders_workflow.json`, `customer_cdc_workflow.json` |
| 7 | **KornShell orchestration** — multi-phase batch pipeline | `scripts/*.ksh` | Databricks Workflow task dependencies | Workflow `tasks[].depends_on` |
| 8 | **setenv.ksh** — environment variables | `scripts/setenv.ksh` | Spark conf + job parameters + cluster env vars | Workflow `job_clusters[].spark_conf` |
| 9 | **AutoSys monitoring** — job failure alerts | `monitoring/job_monitor.py` | Databricks SQL Alerts | `databricks/monitoring/job_failure_alert.sql` |
| 10 | **SLA tracker** — compliance reporting | `monitoring/sla_tracker.py` | Databricks SQL Dashboard | `databricks/monitoring/sla_compliance_dashboard.sql` |
| 11 | **air sandbox run** — execute graph | Shell command in `.ksh` scripts | `dbutils.notebook.run()` / Workflow task | Workflow `notebook_task` configuration |
| 12 | **Co>Operating System** — runtime engine | `/opt/abinitio/coop` | Spark cluster | Databricks job cluster |
| 13 | **ServiceNow/UrbanCode deployment** | `deployment/deploy_manager.py` | Databricks Asset Bundles + CI/CD | Out of scope (recommended: `databricks.yml` bundle) |
| 14 | **Pre-deployment validation** | `deployment/change_validator.py` | CI/CD pipeline checks | Out of scope (recommended: GitHub Actions) |
| 15 | **Oracle (source DB)** | `setenv.ksh: AI_SOURCE_DB=ORACLE_PROD` | Databricks-to-Oracle JDBC connector / Lakehouse Federation | Connection configured in Unity Catalog |
| 16 | **Teradata (target DW)** | `setenv.ksh: AI_TARGET_DB=TERADATA_DW` | Delta Lake tables (bronze/silver/gold) | Replaces Teradata entirely |
| 17 | **Checkpoint/restart** | `setenv.ksh: AI_CHECKPOINT_ENABLED` | Delta Lake transactional writes (ACID) | Built-in to Delta Lake |
| 18 | **Error handling (MAX_ERRORS, ERROR_ACTION)** | PSET + `setenv.ksh` | Notebook-level error handling + job retries | Widget `max_errors` + workflow `max_retries` |
| 19 | **Log files** | `${AI_LOG_DIR}/...` | Databricks driver logs + Spark UI | Built-in to Databricks |
| 20 | **include directive** | `dml/customer_address.dml: include "common_address.dml"` | Python module import | `from databricks.schemas.common_address import address_type` |

---

## 2. Type Mapping Reference (Ab Initio DML → Spark/Delta)

| Ab Initio DML Type | Example | Spark Type | Delta DDL Type | Notes |
|---|---|---|---|---|
| `decimal` (no precision) | `decimal(",") customer_id` | `LongType` | `BIGINT` | Generic integer identifier; 64-bit range |
| `decimal("p.s")` | `decimal("8.2") balance` | `DecimalType(8,2)` | `DECIMAL(8,2)` | Exact precision preserved for financial data |
| `decimal("p.s", delim)` | `decimal("10.2", ",") amount` | `DecimalType(10,2)` | `DECIMAL(10,2)` | Delimiter is serialization-only |
| `string` / `string(delim)` | `string(",") first_name` | `StringType` | `STRING` | Delimiter ignored in columnar format |
| `string(n)` | `string(20) account_name` | `StringType` | `STRING` | Fixed-width → variable-length; trim trailing spaces |
| `date("fmt")` | `date("YYYY-MM-DD")` | `DateType` | `DATE` | Format handled at ingestion |
| `datetime("fmt")` | `datetime("YYYY-MM-DD HH24:MI:SS")` | `TimestampType` | `TIMESTAMP` | Format handled at ingestion |
| `packed_decimal(n)` | `packed_decimal(5)` | `DecimalType(5,0)` | `DECIMAL(5,0)` | IBM COMP-3 BCD; requires byte decoding |
| `packed_decimal("p.s")` | `packed_decimal("7.2")` | `DecimalType(7,2)` | `DECIMAL(7,2)` | BCD with fractional digits |
| `zoned_decimal(n)` | `zoned_decimal(4)` | `DecimalType(4,0)` | `DECIMAL(4,0)` | EBCDIC zoned; requires byte decoding |
| `void` | `void(",") padding1` | *Skipped* | *Omitted* | Mainframe filler — no semantic meaning |
| `record ... end name` | Nested `merchant_info` | `StructType` | `STRUCT<...>` | Nested record → nested struct |
| `record[count] ... end` | `record[item_count] line_items` | `ArrayType(StructType)` | `ARRAY<STRUCT<...>>` | Variable-length array of records |
| `if (cond) record ... end` | `if (txn_type==2) refund_details` | Nullable `StructType` | Nullable `STRUCT<...>` | NULL when condition is false |
| `type name = record` | `type address_t = record` | Reusable `StructType` variable | N/A (inline STRUCT) | Named type → Python module import |
| `null("sentinel")` | `string(",", null(""))` | Nullable `StringType` | `STRING` | Sentinel value mapped to NULL at ingestion |

---

## 3. File-by-File Migration Map

### 3.1 DML Schemas → Delta Lake Schemas

| Source DML | Target Schema | Key Decisions |
|---|---|---|
| `dml/customer.dml` | `databricks/schemas/customer.py` | Simple flat record; decimal→BIGINT for keys |
| `dml/common_address.dml` | `databricks/schemas/common_address.py` | Reusable named type → importable StructType |
| `dml/customer_address.dml` | `databricks/schemas/customer_address.py` | Include directive → nested STRUCT |
| `dml/order_items.dml` | `databricks/schemas/order_items.py` | Variable-length arrays → ARRAY types |
| `dml/transaction_detail.dml` | `databricks/schemas/transaction_detail.py` | Complex: nested records, arrays, conditionals |
| `dml/account_balance.dml` | `databricks/schemas/account_balance.py` | Financial precision: decimal("8.2")→DECIMAL(8,2) |
| `dml/account_status.dml` | `databricks/schemas/account_status.py` | Void/padding fields dropped |
| `dml/packed_account.dml` | `databricks/schemas/packed_account.py` | Mainframe types: packed_decimal, zoned_decimal |

### 3.2 Graphs → PySpark Notebooks

| Source Graph | Target Notebook | Key Changes |
|---|---|---|
| `graphs/parallel_loader.py` | `databricks/notebooks/parallel_loader.py` | Thread pool → Spark partitions; PSET → widgets |
| `graphs/cdc_processor.py` | `databricks/notebooks/cdc_processor.py` | Pandas hash CDC → Delta MERGE + CDF |

### 3.3 KornShell → Databricks Workflows

| Source Script | Target Workflow | Key Changes |
|---|---|---|
| `scripts/run_daily_orders.ksh` | `databricks/workflows/daily_orders_workflow.json` | 4-phase pipeline → 4 workflow tasks with dependencies |
| `scripts/run_customer_cdc.ksh` | `databricks/workflows/customer_cdc_workflow.json` | 4-step CDC → 3 tasks (MERGE consolidates steps 1-3) |

### 3.4 Monitoring → Databricks SQL

| Source Module | Target SQL | Key Changes |
|---|---|---|
| `monitoring/job_monitor.py` | `databricks/monitoring/job_failure_alert.sql` | AutoSys API polling → system.lakeflow query |
| `monitoring/sla_tracker.py` | `databricks/monitoring/sla_compliance_dashboard.sql` | JSON state file → SQL dashboard query |

---

## 4. Execution Order

The migration should be executed in this order to manage dependencies and enable incremental validation.

### Phase 1: Schema Foundation (Week 1)

1. **Deploy Delta Lake schemas** — Create all tables using the DDL in `databricks/schemas/`.
   ```sql
   -- Execute each schema's DDL in Databricks SQL
   -- Order: common_address (dependency) → customer → customer_address → others
   ```
2. **Validate schemas** — Verify StructType definitions match expected column types.
3. **Create Unity Catalog entries** — Register tables in `catalog.bronze.*`.

### Phase 2: Notebook Deployment (Week 1-2)

4. **Deploy parallel_loader notebook** — Import to Databricks Repos.
5. **Deploy cdc_processor notebook** — Import to Databricks Repos.
6. **Validate notebooks** — Run with sample data (`data/sample/`) against dev catalog.

### Phase 3: Workflow Configuration (Week 2)

7. **Create daily_orders_workflow** — Deploy JSON via Databricks Jobs API or CLI.
8. **Create customer_cdc_workflow** — Deploy JSON via Databricks Jobs API or CLI.
9. **Configure job parameters** — Map PSET values to production parameters.
10. **Test workflows end-to-end** — Run manually against dev environment.

### Phase 4: Monitoring Setup (Week 2-3)

11. **Create job failure alert** — Import SQL to Databricks SQL and configure alert schedule.
12. **Create SLA compliance dashboard** — Import SQL to Databricks SQL and configure widgets.
13. **Configure notification channels** — Set up Slack webhook and email destinations.

### Phase 5: Dual-Run Validation (Week 3-4)

14. **Run Ab Initio and Databricks pipelines in parallel** — Compare outputs for data parity.
15. **Validate CDC results** — Compare MERGE output against legacy CDC inserts/updates/deletes.
16. **Validate SLA metrics** — Compare Databricks dashboard against legacy SLA tracker reports.

### Phase 6: Cutover (Week 4-5)

17. **Disable AutoSys jobs** — Pause `JOB_DAILY_ORDERS_LOAD` and `JOB_CUSTOMER_CDC`.
18. **Enable Databricks workflows** — Unpause schedules.
19. **Monitor first 48 hours** — Watch alerts and SLA dashboard closely.
20. **Decommission legacy** — Archive Ab Initio graphs and KornShell scripts.

---

## 5. PSET → Job Parameter Migration

### orders_pipeline.pset → daily_orders_workflow.json

| PSET Parameter | PSET Value (dev) | Workflow Parameter | Workflow Default |
|---|---|---|---|
| `SOURCE_PATH` | `/data/dev/raw/orders` | `source_path` | `/mnt/raw/orders` |
| `TARGET_TABLE` | `DEV.STAGING.ORDERS` | `target_table` | `catalog.bronze.orders` |
| `PARTITION_COUNT` | `4` | `partition_count` | `4` |
| `BATCH_SIZE` | `50000` | `batch_size` | `50000` |
| `LOG_LEVEL` | `DEBUG` | `log_level` | `INFO` |
| `MAX_ERRORS` | `100` | `max_errors` | `100` |
| `CHECKPOINT_DIR` | `/tmp/abinitio/checkpoints/orders` | N/A | Delta Lake ACID (built-in) |
| `RECORD_SOURCE` | `ORDER_SYSTEM_DEV` | N/A | Captured in `_source_file` audit column |

### customer_cdc.pset → customer_cdc_workflow.json

| PSET Parameter | PSET Value | Workflow Parameter | Workflow Default |
|---|---|---|---|
| `SOURCE_PATH` | `/data/raw/customer` | `source_path` | `/mnt/raw/customer` |
| `TARGET_TABLE` | `STAGING.CUSTOMER_MASTER` | `target_table` | `catalog.bronze.customer_master` |
| `KEY_COLUMNS` | `customer_id` | `key_columns` | `customer_id` |
| `HASH_COLUMNS` | `customer_id,name,...` | `hash_columns` | `customer_id,name,...` |
| `PARTITION_COUNT` | `8` | `partition_count` | `8` |
| `MAX_ERRORS` | `50` | `max_errors` | `50` |
| `AUDIT_TABLE` | `AUDIT.CUSTOMER_CHANGES` | `audit_table` | `catalog.audit.customer_changes` |
| `RETENTION_DAYS` | `90` | `retention_days` | `90` |
| `PREVIOUS_SNAPSHOT_PATH` | `/data/snapshots/customer/previous` | N/A | Delta Lake time travel (built-in) |
| `CURRENT_SNAPSHOT_PATH` | `/data/snapshots/customer/current` | N/A | Delta Lake versioning (built-in) |
| `CDC_OUTPUT_PATH` | `/data/cdc/customer` | N/A | Change Data Feed (built-in) |
| `BATCH_SIZE` | `100000` | N/A | Spark handles batching internally |

### setenv.ksh → Cluster/Job Configuration

| Environment Variable | Value | Databricks Equivalent |
|---|---|---|
| `AI_HOME` | `/opt/abinitio` | Databricks Runtime (pre-installed) |
| `AI_PROJECT_DIR` | `/data/projects/enterprise_etl` | Databricks Repos path |
| `AI_LOG_DIR` | `/data/logs/abinitio` | Databricks driver logs (automatic) |
| `AI_SOURCE_DB` | `ORACLE_PROD` | Unity Catalog connection |
| `AI_TARGET_DB` | `TERADATA_DW` | Delta Lake tables (replaces Teradata) |
| `AI_DEFAULT_PARTITIONS` | `4` | `spark.sql.shuffle.partitions` |
| `AI_MAX_PARTITIONS` | `16` | Cluster auto-scaling max workers |
| `AI_MAX_ERRORS` | `100` | Widget parameter `max_errors` |
| `AI_ERROR_ACTION` | `ABORT` | Notebook exception handling |
| `AI_CHECKPOINT_ENABLED` | `true` | Delta Lake ACID transactions (always on) |

---

## 6. Risks and Mitigations

### 6.1 Packed Decimal Handling

**Risk:** Ab Initio `packed_decimal` and `zoned_decimal` types encode data as BCD (Binary Coded Decimal) and EBCDIC zoned encoding respectively. These are byte-level mainframe formats that cannot be read directly by Spark.

**Mitigation:**
- The Delta Lake schema uses `DecimalType(p,s)` which stores exact decimal values.
- **Ingestion pipeline must include a byte-decoding step** before loading into Delta.
- Options: (a) Use a COBOL copybook parser library (e.g., `cobrix` for Spark), (b) Pre-convert with a mainframe utility before landing in cloud storage, or (c) Use Databricks' built-in mainframe connector if available.
- The `packed_account.dml` schema is the only DML using these types — scope is limited.

### 6.2 Partition Strategy Differences

**Risk:** Ab Initio uses explicit partition ranges (`start_record`/`end_record`) managed by `PartitionManager`. Spark uses hash-based or range-based partitioning that may produce different data distribution.

**Mitigation:**
- Spark's `repartition(N)` is used to match the Ab Initio partition count where specified.
- For ordered processing requirements (rare in ETL), use `repartitionByRange()` with explicit key columns.
- The `partition_count` parameter is preserved in all workflows to maintain operational parity.
- Validate: Compare row counts per partition between Ab Initio and Spark during dual-run.

### 6.3 CDC Semantic Differences

**Risk:** Ab Initio CDC uses pandas-based row hashing with set difference operations. Delta Lake MERGE uses SQL join semantics. Edge cases (duplicate keys, null handling) may produce different results.

**Mitigation:**
- SHA-256 hashing replaces MD5 for stronger collision resistance.
- Delta MERGE's `whenNotMatchedBySourceDelete()` handles deletes that Ab Initio computed as `target_keys - source_keys`.
- Test CDC parity with the existing sample data during Phase 5 (dual-run).
- The two known failing tests (`test_inserts_detected`, `test_deletes_detected`) are due to a pandas indexing bug in the legacy code, not a logic issue — the Delta MERGE approach avoids this entirely.

### 6.4 Null Sentinel Handling

**Risk:** Ab Initio DML uses `null("sentinel")` syntax (e.g., `null("")`, `null("UNKNOWN")`) to represent missing values. If not handled at ingestion, sentinel strings will appear as data rather than NULLs.

**Mitigation:**
- Ingestion notebooks should include `when(col("field") == "sentinel", lit(None))` transformations.
- Document all sentinel values: `""` (empty string) for `merchant_name`, `"UNKNOWN"` for `channel` in `transaction_detail.dml`.
- Add data quality checks in the loading notebooks to flag unexpected sentinel values.

### 6.5 Variable-Length Array Serialization

**Risk:** Ab Initio's `type[count]` arrays are serialized as repeated delimited fields. Spark's `ARRAY<>` type is stored as a nested Parquet structure. Ingestion must parse the flat delimited format into arrays.

**Mitigation:**
- The `order_items.dml` uses `item_names[item_count]` and `item_quantities[item_count]` — these require a custom parser at ingestion time.
- Recommended approach: Read as flat CSV, then use `split()` + `transform()` to convert delimited strings to arrays.
- The `item_count` field is preserved for validation (`assert size(item_names) == item_count`).

### 6.6 Schedule Alignment

**Risk:** AutoSys and Databricks Workflows use different scheduling engines. Timing differences could cause data gaps or overlaps during the dual-run validation phase.

**Mitigation:**
- Daily orders: AutoSys job vs. Databricks cron `0 0 2 * * ?` (02:00 UTC) — verify both run at the same time.
- Customer CDC: AutoSys every-4-hours vs. Databricks cron `0 0 0/4 * * ?` — verify alignment.
- During dual-run, stagger schedules by 30 minutes to avoid source system contention.
- Use `max_concurrent_runs: 1` to prevent overlapping runs.

### 6.7 Financial Precision

**Risk:** Ab Initio `decimal("8.2")` and `decimal("10.2")` types have exact precision semantics. Spark's `DoubleType` would lose precision for financial calculations.

**Mitigation:**
- All financial fields use `DecimalType(p,s)` / `DECIMAL(p,s)` — NOT `DoubleType`.
- `account_balance.dml`: `balance` → `DECIMAL(8,2)`.
- `transaction_detail.dml`: `amount` → `DECIMAL(10,2)`, `line_total` → `DECIMAL(8,2)`.
- `packed_account.dml`: `balance` → `DECIMAL(7,2)`.

### 6.8 Void/Padding Field Removal

**Risk:** Dropping Ab Initio `void` (padding) fields changes the effective column positions if any downstream consumer relies on positional column access.

**Mitigation:**
- `account_status.dml` has two `void` fields (`padding1`, `padding2`) that are dropped in the Delta schema.
- Downstream consumers should use named column access, not positional.
- Document the column mapping: `id(0), [padding1 dropped], name(1), [padding2 dropped], status(2)` → `id(0), name(1), status(2)`.

### 6.9 Monitoring Fidelity

**Risk:** The legacy `JobMonitor` polled AutoSys API every 60 seconds with up to 60 polls. Databricks SQL Alerts have a minimum 5-minute schedule granularity — slower detection.

**Mitigation:**
- Configure the job failure alert to run every 5 minutes (Databricks SQL minimum).
- Supplement with Databricks webhook notifications (`on_failure`) in each workflow — these fire immediately on job failure, matching the real-time alerting behavior of `_send_alert()`.
- The SLA compliance dashboard replaces the file-based state tracking with a SQL query against `system.lakeflow` — no state file management needed.

---

## 7. Artifacts Produced

| Directory | Contents | Count |
|---|---|---|
| `databricks/schemas/` | PySpark StructType + Delta DDL for each DML | 8 files |
| `databricks/notebooks/` | PySpark notebooks for graph patterns | 2 files |
| `databricks/workflows/` | Databricks Workflow JSON definitions | 2 files |
| `databricks/monitoring/` | SQL alert + SLA dashboard queries | 2 files |
| `docs/` | This migration runbook | 1 file |

---

## 8. Post-Migration Recommendations

1. **Databricks Asset Bundles**: Package the entire `databricks/` directory as a DAB project (`databricks.yml`) for CI/CD deployment, replacing the legacy `deployment/deploy_manager.py` and `change_validator.py`.

2. **Unity Catalog Governance**: Register all Delta tables in Unity Catalog with appropriate access controls. The `catalog.bronze.*` naming in the DDL follows the medallion architecture convention.

3. **Delta Live Tables (DLT)**: For more complex pipelines, consider converting the notebooks to DLT pipelines for built-in data quality expectations and lineage tracking.

4. **Terraform/Pulumi**: Manage Databricks workspace resources (clusters, jobs, alerts) as infrastructure-as-code for environment parity across dev/uat/prod.

5. **Data Quality Framework**: Add `CONSTRAINT` expectations to Delta tables or use DLT `expect()` functions to replace the Ab Initio `MAX_ERRORS` threshold pattern with declarative quality rules.
