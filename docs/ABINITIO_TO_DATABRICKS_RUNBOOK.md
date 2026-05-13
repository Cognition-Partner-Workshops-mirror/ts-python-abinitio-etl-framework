# Ab Initio → Databricks Lakehouse Migration Runbook

## Table of Contents

1. [Overview](#overview)
2. [Complete Concept Mapping](#complete-concept-mapping)
3. [Type Mapping Reference](#type-mapping-reference)
4. [Artifact Inventory](#artifact-inventory)
5. [Execution Order](#execution-order)
6. [Detailed Migration Steps](#detailed-migration-steps)
7. [Risks and Mitigations](#risks-and-mitigations)
8. [Validation Checklist](#validation-checklist)
9. [Rollback Plan](#rollback-plan)

---

## Overview

This runbook documents the migration from an Ab Initio Co>Operating System ETL estate to Databricks Lakehouse architecture. The source estate consists of:

- **8 DML record layouts** defining schemas for customers, orders, accounts, and transactions
- **2 graph execution patterns** (parallel loader, CDC processor)
- **3 PSET templates** for environment-aware configuration
- **3 KornShell scripts** for AutoSys-triggered batch orchestration
- **2 monitoring modules** for SLA tracking and job alerting

The target architecture uses Delta Lake for storage, PySpark for processing, Databricks Workflows for orchestration, and Databricks SQL for monitoring.

### Architecture Comparison

```
┌─────────────────────────────────────┐     ┌─────────────────────────────────────┐
│         Ab Initio Estate            │     │      Databricks Lakehouse           │
├─────────────────────────────────────┤     ├─────────────────────────────────────┤
│ DML Record Layouts (8 files)        │ ──► │ PySpark StructType + Delta DDL      │
│ Graph Patterns (.mp files)          │ ──► │ PySpark Notebooks                   │
│ KornShell Scripts (AutoSys)         │ ──► │ Databricks Workflows (JSON)         │
│ PSET Templates (3 files)            │ ──► │ Job Parameters + Widget Defaults    │
│ AutoSys Job Scheduler               │ ──► │ Databricks Cron Triggers            │
│ JobMonitor (AutoSys API + Slack)    │ ──► │ Databricks SQL Alerts               │
│ SLATracker (JSON state file)        │ ──► │ Databricks SQL Dashboard            │
│ Oracle/Teradata Targets             │ ──► │ Delta Lake (Bronze/Silver/Gold)      │
│ ServiceNow Change Management        │ ──► │ Databricks Repos + Git Integration  │
│ UrbanCode Deployment                │ ──► │ Databricks Asset Bundles            │
└─────────────────────────────────────┘     └─────────────────────────────────────┘
```

---

## Complete Concept Mapping

| Ab Initio Concept | Source File(s) | Databricks Equivalent | Target File(s) |
|---|---|---|---|
| **DML Record Layout** | `dml/*.dml` | PySpark StructType + Delta Lake DDL | `databricks/schemas/*_schema.py` |
| **Graph (.mp file)** | `graphs/parallel_loader.py` | PySpark Notebook | `databricks/notebooks/parallel_loader.py` |
| **CDC Graph Pattern** | `graphs/cdc_processor.py` | Delta Lake MERGE + Change Data Feed | `databricks/notebooks/cdc_processor.py` |
| **PSET (Parameter Set)** | `psets/pset_templates/*.pset` | Databricks Job Parameters + Widgets | `databricks/workflows/*.json` → `parameters` |
| **PSET Manager** | `psets/pset_manager.py` | Databricks Widgets (`dbutils.widgets`) | Embedded in notebooks |
| **KornShell Orchestration** | `scripts/run_daily_orders.ksh` | Databricks Workflow JSON | `databricks/workflows/daily_orders_workflow.json` |
| **KornShell Orchestration** | `scripts/run_customer_cdc.ksh` | Databricks Workflow JSON | `databricks/workflows/customer_cdc_workflow.json` |
| **setenv.ksh** | `scripts/setenv.ksh` | Cluster env vars + Spark config | Cluster policies / init scripts |
| **AutoSys Job Schedule** | AutoSys JOB_DAILY_ORDERS_LOAD | Databricks Cron Trigger | Workflow `schedule` field |
| **AutoSys 4-hour Schedule** | AutoSys JOB_CUSTOMER_CDC | Databricks Cron Trigger `0 0 0/4 * * ?` | Workflow `schedule` field |
| **`air sandbox run`** | KornShell scripts | `dbutils.notebook.run()` / Workflow task | Notebook tasks in workflows |
| **`-partition N` flag** | KornShell scripts | `.repartition(N)` / `num_workers` | Notebook param + cluster config |
| **`air_run` CLI** | `parallel_loader.py` | Spark's native distributed execution | No equivalent needed |
| **`air_deploy` CLI** | `deployment/deploy_manager.py` | Databricks Asset Bundles / Repos | Git-based deployment |
| **PartitionManager** | `graphs/parallel_loader.py` | Spark shuffle partitions + `.repartition()` | Built into Spark |
| **ThreadPoolExecutor** | `graphs/parallel_loader.py` | Spark's distributed execution model | Not needed |
| **MD5 Row Hash** | `graphs/cdc_processor.py` | `sha2(concat_ws(...), 256)` | `cdc_processor.py` notebook |
| **pandas DataFrame** | `graphs/cdc_processor.py` | PySpark DataFrame | Notebooks |
| **JobMonitor (AutoSys API)** | `monitoring/job_monitor.py` | Databricks SQL Alerts on system tables | `databricks/monitoring/job_failure_alerts.sql` |
| **SLATracker (JSON state)** | `monitoring/sla_tracker.py` | Databricks SQL Dashboard queries | `databricks/monitoring/sla_compliance_dashboard.sql` |
| **Slack Webhook Alerts** | `monitoring/job_monitor.py` | Databricks Alert notification destinations | Alert config in SQL UI |
| **ServiceNow Integration** | `deployment/deploy_manager.py` | Databricks REST API + webhooks | Out of scope (external) |
| **ChangeValidator** | `deployment/change_validator.py` | Databricks Asset Bundle validation | CI/CD pipeline |
| **`void` DML Type** | `dml/account_status.dml` | Omitted (padding bytes) | Schema skips void fields |
| **`packed_decimal` DML Type** | `dml/packed_account.dml` | `DecimalType(p,s)` + binary decoder | `packed_account_schema.py` |
| **`zoned_decimal` DML Type** | `dml/packed_account.dml` | `DecimalType(p,s)` + binary decoder | `packed_account_schema.py` |
| **Nested Record** | `dml/transaction_detail.dml` | Spark `StructType` / Delta `STRUCT<>` | `transaction_detail_schema.py` |
| **Record Array `[count]`** | `dml/transaction_detail.dml` | Spark `ArrayType(StructType)` | `transaction_detail_schema.py` |
| **Conditional Record `if`** | `dml/transaction_detail.dml` | Nullable columns | `transaction_detail_schema.py` |
| **DML `include` Directive** | `dml/customer_address.dml` | Python import / shared StructType | `common_address_schema.py` |
| **DML `type` Definition** | `dml/common_address.dml` | Reusable `StructType` constant | `common_address_schema.py` |
| **`null()` DML Annotation** | `dml/transaction_detail.dml` | `coalesce()` / `fillna()` at ingestion | Notebook-level handling |
| **EME (Enterprise Meta-Env)** | Ab Initio Co>Op System | Databricks Unity Catalog | N/A |
| **Air Sandbox** | Development environment | Databricks Workspace (dev) | N/A |
| **Checkpoint/Restart** | `setenv.ksh` | Delta Lake transactions (ACID) | Built into Delta |

---

## Type Mapping Reference

### Standard Types

| Ab Initio DML Type | Example | PySpark Type | Delta Lake SQL Type | Notes |
|---|---|---|---|---|
| `decimal` | `decimal(",") id` | `LongType()` | `BIGINT` | Integer identifier, no fractional part |
| `decimal("p.s")` | `decimal("8.2") balance` | `DecimalType(8,2)` | `DECIMAL(8,2)` | Precision and scale preserved |
| `decimal("10.2")` | `decimal("10.2") amount` | `DecimalType(10,2)` | `DECIMAL(10,2)` | Large monetary amounts |
| `string` | `string(",") name` | `StringType()` | `STRING` | Variable-length text |
| `string(N)` | `string(20) account_name` | `StringType()` | `STRING` | Fixed-width → variable; trim trailing spaces |
| `date("fmt")` | `date("YYYY-MM-DD")` | `DateType()` | `DATE` | Format handled at ingestion |
| `datetime("fmt")` | `datetime("YYYY-MM-DD HH24:MI:SS")` | `TimestampType()` | `TIMESTAMP` | Format handled at ingestion |
| `integer` | `integer(10) order_id` | `IntegerType()` / `LongType()` | `INT` / `BIGINT` | Size determines choice |
| `double` | `double balance` | `DoubleType()` | `DOUBLE` | IEEE 754 floating point |
| `long` | `long txn_id` | `LongType()` | `BIGINT` | 64-bit integer |
| `void` | `void(",") padding` | *(omitted)* | *(omitted)* | Padding/filler bytes — no business data |

### Mainframe Legacy Types

| Ab Initio DML Type | Example | PySpark Type | Delta Lake SQL Type | Decoding Required |
|---|---|---|---|---|
| `packed_decimal(N)` | `packed_decimal(5) acct_num` | `DecimalType(5,0)` | `DECIMAL(5,0)` | **Yes** — COMP-3/BCD binary |
| `packed_decimal("p.s")` | `packed_decimal("7.2") bal` | `DecimalType(7,2)` | `DECIMAL(7,2)` | **Yes** — COMP-3/BCD binary |
| `zoned_decimal(N)` | `zoned_decimal(4) status` | `DecimalType(4,0)` | `DECIMAL(4,0)` | **Yes** — EBCDIC zoned |

### Complex Types

| Ab Initio DML Pattern | Example | PySpark Type | Delta Lake SQL Type |
|---|---|---|---|
| Nested `record...end name` | `record...end merchant_info` | `StructType([...])` | `STRUCT<...>` |
| Array `type[count]` | `string[item_count] names` | `ArrayType(StringType())` | `ARRAY<STRING>` |
| Record array `record[count]` | `record[item_count]...end` | `ArrayType(StructType([...]))` | `ARRAY<STRUCT<...>>` |
| Conditional `if (expr)` | `if (txn_type == 2) record...end` | Nullable fields | Nullable columns |
| Type definition `type X = record` | `type address_t = record` | Reusable `StructType` | Inline `STRUCT<>` |
| Include directive | `include "common_address.dml"` | Python import | N/A |
| Null annotation `null("val")` | `string(null("UNKNOWN"))` | Coalesce at read time | `DEFAULT` or ingestion |

---

## Artifact Inventory

### Schemas (`databricks/schemas/`)

| File | Source DML | Key Features |
|---|---|---|
| `customer_schema.py` | `dml/customer.dml` | Simple record — decimal, string types |
| `customer_address_schema.py` | `dml/customer_address.dml` + `dml/common_address.dml` | Include directive, nested address_t type |
| `common_address_schema.py` | `dml/common_address.dml` | Reusable type definition |
| `order_items_schema.py` | `dml/order_items.dml` | Variable-length arrays `[item_count]` |
| `account_balance_schema.py` | `dml/account_balance.dml` | Precision decimal, date type |
| `account_status_schema.py` | `dml/account_status.dml` | Void padding fields (dropped) |
| `packed_account_schema.py` | `dml/packed_account.dml` | Packed/zoned decimal (mainframe binary) |
| `transaction_detail_schema.py` | `dml/transaction_detail.dml` | Nested records, arrays, conditional fields, null annotations |

### Notebooks (`databricks/notebooks/`)

| File | Source Graph | Key Features |
|---|---|---|
| `parallel_loader.py` | `graphs/parallel_loader.py` | Partition-based ingestion, widget params, metadata columns |
| `cdc_processor.py` | `graphs/cdc_processor.py` | Delta MERGE, Change Data Feed, hash-based comparison |

### Workflows (`databricks/workflows/`)

| File | Source Script | Schedule | Tasks |
|---|---|---|---|
| `daily_orders_workflow.json` | `scripts/run_daily_orders.ksh` | Daily at 2:00 AM UTC | 4 tasks: extract → CDC → staging → production |
| `customer_cdc_workflow.json` | `scripts/run_customer_cdc.ksh` | Every 4 hours | 4 tasks: snapshot → detect+apply → audit → vacuum |

### Monitoring (`databricks/monitoring/`)

| File | Source Module | Purpose |
|---|---|---|
| `job_failure_alerts.sql` | `monitoring/job_monitor.py` | Failure, long-running, SLA breach alerts |
| `sla_compliance_dashboard.sql` | `monitoring/sla_tracker.py` | Compliance summary, trends, run details, health scorecard |

---

## Execution Order

The migration should be executed in the following order to minimise risk and enable incremental validation:

### Phase 1: Foundation (Week 1-2)

```
Step 1.1  Create Unity Catalog namespace
          → CREATE CATALOG lakehouse; CREATE SCHEMA bronze; CREATE SCHEMA silver; CREATE SCHEMA gold; CREATE SCHEMA staging; CREATE SCHEMA audit;

Step 1.2  Deploy Delta Lake table DDL
          → Execute all DDL from databricks/schemas/*_schema.py
          → Order: common_address → customer → customer_address → account_balance → account_status → packed_account → order_items → transaction_detail

Step 1.3  Validate schemas
          → DESCRIBE TABLE EXTENDED for each table
          → Verify column types match type mapping reference
```

### Phase 2: Processing Logic (Week 2-3)

```
Step 2.1  Import notebooks to Databricks Repos
          → databricks/notebooks/parallel_loader.py
          → databricks/notebooks/cdc_processor.py

Step 2.2  Test parallel_loader with sample data
          → Upload data/sample/customers.dat to /mnt/raw/test/
          → Run notebook with test parameters
          → Validate row counts and schema match

Step 2.3  Test cdc_processor with sample data
          → Create initial load from data/sample/customers.dat
          → Modify a few rows and re-run
          → Validate INSERT/UPDATE/DELETE counts
```

### Phase 3: Orchestration (Week 3-4)

```
Step 3.1  Deploy workflow definitions
          → Use Databricks CLI or API to create workflows from JSON
          → databricks jobs create --json @daily_orders_workflow.json
          → databricks jobs create --json @customer_cdc_workflow.json

Step 3.2  Configure job parameters
          → Map PSET values to production paths and tables
          → Update source_path, target_table, partition_count per environment

Step 3.3  Test workflows end-to-end
          → Manual trigger with test data
          → Verify all task dependencies execute correctly
          → Check error handling (simulate failure in one task)
```

### Phase 4: Monitoring (Week 4)

```
Step 4.1  Create SQL alerts
          → Import job_failure_alerts.sql queries into Databricks SQL
          → Configure alert schedules (5 min, 10 min, 15 min)
          → Set up notification destinations (Slack, PagerDuty, email)

Step 4.2  Create SLA dashboard
          → Import sla_compliance_dashboard.sql as dashboard widgets
          → Configure lookback parameter defaults
          → Share with data engineering team

Step 4.3  Parallel run period
          → Run both Ab Initio and Databricks pipelines simultaneously
          → Compare outputs for data accuracy
          → Monitor SLA compliance on both systems
```

### Phase 5: Cutover (Week 5-6)

```
Step 5.1  Production cutover
          → Unpause Databricks workflow schedules
          → Disable AutoSys job triggers
          → Monitor first 3 production runs closely

Step 5.2  Decommission Ab Initio components
          → Archive KornShell scripts
          → Disable AutoSys jobs
          → Retain DML files as documentation
```

---

## Detailed Migration Steps

### PSET → Databricks Parameter Mapping

The following table maps every PSET parameter to its Databricks workflow equivalent:

#### orders_pipeline.pset

| PSET Parameter | Value | Databricks Mapping |
|---|---|---|
| `SOURCE_PATH` | `/data/dev/raw/orders` | Job parameter `source_path` → `/mnt/raw/orders` |
| `TARGET_TABLE` | `DEV.STAGING.ORDERS` | Job parameter `target_table` → `lakehouse.bronze.orders` |
| `PARTITION_COUNT` | `4` | Job parameter `partition_count` + cluster `num_workers` |
| `BATCH_SIZE` | `50000` | Spark config `spark.sql.files.maxRecordsPerFile` |
| `LOG_LEVEL` | `DEBUG` | Spark config `spark.driver.extraJavaOptions=-Dlog4j.configuration=...` |
| `RECORD_SOURCE` | `ORDER_SYSTEM_DEV` | Ingestion metadata column `_record_source` |
| `MAX_ERRORS` | `100` | Job parameter `max_errors` |
| `CHECKPOINT_DIR` | `/tmp/abinitio/checkpoints/orders` | Delta Lake transactions (automatic) |

#### customer_cdc.pset

| PSET Parameter | Value | Databricks Mapping |
|---|---|---|
| `SOURCE_PATH` | `/data/raw/customer` | Job parameter `source_path` |
| `TARGET_TABLE` | `STAGING.CUSTOMER_MASTER` | Job parameter `target_table` → `lakehouse.bronze.customer` |
| `PREVIOUS_SNAPSHOT_PATH` | `/data/snapshots/customer/previous` | Delta Lake time travel (`VERSION AS OF`) |
| `CURRENT_SNAPSHOT_PATH` | `/data/snapshots/customer/current` | Current Delta table state |
| `CDC_OUTPUT_PATH` | `/data/cdc/customer` | Change Data Feed (`readChangeFeed`) |
| `PARTITION_COUNT` | `8` | Job parameter + cluster config |
| `HASH_COLUMNS` | `customer_id,name,...` | Job parameter `hash_columns` → notebook `compare_columns` |
| `KEY_COLUMNS` | `customer_id` | Job parameter `key_columns` → MERGE ON condition |
| `BATCH_SIZE` | `100000` | Spark config |
| `MAX_ERRORS` | `50` | Job parameter |
| `AUDIT_TABLE` | `AUDIT.CUSTOMER_CHANGES` | `lakehouse.audit.customer_changes` |
| `RETENTION_DAYS` | `90` | VACUUM RETAIN 2160 HOURS |

#### orders_staging.pset

| PSET Parameter | Value | Databricks Mapping |
|---|---|---|
| `SOURCE_PATH` | `/data/cdc/orders/delta` | Source Delta table from CDC step |
| `TARGET_TABLE` | `STAGING.ORDERS` | `lakehouse.silver.orders_staging` |
| `TARGET_SCHEMA` | `STAGING` | Unity Catalog schema `silver` |
| `LOAD_MODE` | `MERGE` | Notebook `load_mode` widget → `merge` |
| `REJECT_PATH` | `/data/rejects/orders` | Delta table `lakehouse.staging.orders_rejects` |
| `DML_FILE` | `/data/projects/.../order_items.dml` | PySpark schema import from `order_items_schema.py` |
| `SLA_MINUTES` | `30` | Dashboard SLA deadline + alert threshold |

### setenv.ksh → Databricks Configuration

| KornShell Variable | Value | Databricks Equivalent |
|---|---|---|
| `AI_HOME` | `/opt/abinitio` | N/A (Databricks Runtime) |
| `AI_PROJECT_DIR` | `/data/projects/enterprise_etl` | `/Repos/data-engineering/` |
| `AI_LOG_DIR` | `/data/logs/abinitio` | Built-in Databricks job run logs |
| `AI_DATA_DIR` | `/data/raw` | `/mnt/raw/` or ADLS/S3 path |
| `AI_STAGING_DIR` | `/data/staging` | `lakehouse.staging.*` tables |
| `AI_SOURCE_DB` | `ORACLE_PROD` | External location or Lakehouse Federation |
| `AI_TARGET_DB` | `TERADATA_DW` | `lakehouse` Unity Catalog |
| `AI_DEFAULT_PARTITIONS` | `4` | `spark.sql.shuffle.partitions = 4` |
| `AI_MAX_PARTITIONS` | `16` | Cluster autoscale max workers |
| `AI_MAX_ERRORS` | `100` | Job parameter `max_errors` |
| `AI_ERROR_ACTION` | `ABORT` | Workflow task `on_failure: FAIL` (default) |
| `AI_CHECKPOINT_ENABLED` | `true` | Delta Lake ACID (automatic) |

---

## Risks and Mitigations

### Risk 1: Packed Decimal Handling (HIGH)

**Risk:** The `packed_account.dml` uses `packed_decimal` and `zoned_decimal` types which are binary-encoded mainframe formats. Standard CSV/text readers cannot parse these.

**Impact:** Data corruption or ingestion failure for account data.

**Mitigation:**
- Use a COBOL copybook parser library (e.g., `cobrix` for Spark) to read binary files
- Alternative: Pre-convert packed decimal files to CSV using a mainframe utility before ingestion
- Validate converted values against known test data before production cutover
- Add data quality checks: `CONSTRAINT valid_balance CHECK (balance BETWEEN -9999999.99 AND 9999999.99)`

### Risk 2: Partition Strategy Differences (MEDIUM)

**Risk:** Ab Initio uses explicit record-range partitioning (`start_record`, `end_record`), while Spark uses hash/round-robin partitioning. This may produce different data distribution.

**Impact:** Performance differences; potential skew if data has hot keys.

**Mitigation:**
- For key-based workloads, use `.repartition("customer_id")` for even distribution
- Monitor partition sizes via `df.groupBy(spark_partition_id()).count()`
- Use adaptive query execution (AQE): `spark.sql.adaptive.enabled = true`
- Tune `spark.sql.shuffle.partitions` to match expected data volume

### Risk 3: Conditional Record Schema (MEDIUM)

**Risk:** The `transaction_detail.dml` uses `if (txn_type == 2)` for conditional fields. Delta Lake tables have fixed schemas — all rows must have the same columns.

**Impact:** Refund fields will be NULL for non-refund transactions, increasing storage slightly.

**Mitigation:**
- Refund fields are nullable in the Delta schema
- Add a CHECK constraint: `CONSTRAINT valid_refund CHECK (txn_type != 2 OR original_txn_id IS NOT NULL)`
- Document the conditional semantics for downstream consumers
- Consider separate refund detail table if storage becomes a concern at scale

### Risk 4: Variable-Length Array Semantics (MEDIUM)

**Risk:** Ab Initio `record[item_count]` arrays are count-prefixed; Spark arrays are self-describing. The `item_count` field becomes redundant.

**Impact:** Downstream code relying on `item_count` may diverge from actual array size.

**Mitigation:**
- Retain `item_count` for backward compatibility
- Add ingestion validation: `assert df.filter(col("item_count") != size(col("item_names"))).count() == 0`
- Document that `SIZE(item_names)` is the authoritative count

### Risk 5: CDC Consistency During Cutover (HIGH)

**Risk:** Running both Ab Initio and Databricks pipelines during the parallel-run period may cause duplicate processing or missed changes.

**Impact:** Data inconsistency between old and new systems.

**Mitigation:**
- Run pipelines against separate targets (Ab Initio → Oracle/Teradata, Databricks → Delta Lake)
- Compare outputs daily using row counts and hash aggregates
- Do NOT point both pipelines at the same target tables
- Use a specific cutover date/time where Ab Initio processes the last batch and Databricks picks up from the next one

### Risk 6: SLA Window Interpretation (LOW)

**Risk:** The SLATracker uses UTC times (`sla_window_end: "06:00"`), but business users may expect local time zones.

**Impact:** False SLA breach alerts or missed breaches.

**Mitigation:**
- Databricks workflows use `timezone_id: "UTC"` — align dashboard queries accordingly
- Document SLA times in UTC in all alert definitions
- Add timezone conversion in dashboard for business user consumption

### Risk 7: Void Field Handling (LOW)

**Risk:** `void` padding fields in `account_status.dml` are dropped in the Delta schema. Source files still contain these columns at specific positions.

**Impact:** Column position mismatch when reading source CSV files.

**Mitigation:**
- CSV reader must use `_c0`, `_c2`, `_c4` (skip positions 1, 3) or use explicit schema with void positions
- Alternative: Use a pre-processing step that strips void columns before Delta ingestion
- Document void positions in schema comments

---

## Validation Checklist

### Schema Validation

- [ ] All 8 DML files have corresponding `*_schema.py` files
- [ ] Delta tables created successfully with `CREATE TABLE` DDL
- [ ] Column types match the type mapping reference table
- [ ] `packed_account` DecimalType precision matches original packed_decimal specs
- [ ] `transaction_detail` nested STRUCT and ARRAY types verified
- [ ] `account_status` void fields correctly omitted
- [ ] `customer_address` address_t struct matches `common_address.dml` fields

### Data Validation

- [ ] Sample data (`data/sample/*.dat`) loads successfully into Delta tables
- [ ] Row counts match between source files and Delta tables
- [ ] Decimal precision preserved (compare balance values to 2 decimal places)
- [ ] Date/timestamp parsing correct (verify with `SELECT * WHERE opened_date = '2024-01-15'`)
- [ ] NULL handling matches Ab Initio `null()` annotations
- [ ] Array fields correctly parsed (verify `SIZE(item_names) == item_count`)

### Processing Validation

- [ ] Parallel loader ingests data with correct partition count
- [ ] CDC processor detects INSERTs for new records
- [ ] CDC processor detects UPDATEs for modified records
- [ ] CDC processor detects DELETEs for removed records
- [ ] Change Data Feed captures all MERGE operations
- [ ] Error handling works (simulate bad data, verify notebook exits with error)

### Workflow Validation

- [ ] Daily orders workflow tasks execute in correct order (extract → CDC → staging → prod)
- [ ] Customer CDC workflow runs all 4 steps
- [ ] Job parameters correctly propagated to notebook widgets
- [ ] Task dependency failure cascades (if extract fails, CDC does not run)
- [ ] Email notifications fire on task failure

### Monitoring Validation

- [ ] Job failure alert triggers within 15 minutes of a failure
- [ ] Long-running job alert triggers when duration > 2x average
- [ ] SLA breach alert triggers when deadline is missed
- [ ] Dashboard shows accurate compliance percentages
- [ ] Dashboard trends align with actual run history

---

## Rollback Plan

If critical issues are discovered during or after cutover:

1. **Re-enable AutoSys jobs** — unpause JOB_DAILY_ORDERS_LOAD and JOB_CUSTOMER_CDC
2. **Pause Databricks workflows** — set `pause_status: "PAUSED"` on both workflows
3. **Verify Ab Initio processing** — confirm next scheduled batch completes successfully
4. **Investigate Databricks issue** — check workflow run logs, notebook outputs, Delta history
5. **Re-attempt cutover** once root cause is fixed and validated
