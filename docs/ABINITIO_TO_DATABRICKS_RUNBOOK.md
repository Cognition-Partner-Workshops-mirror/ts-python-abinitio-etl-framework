# Ab Initio → Databricks Lakehouse Migration Runbook

This document provides a complete guide for migrating the Ab Initio ETL framework to Databricks Lakehouse architecture. It covers concept mapping, execution order, type mapping decisions, risks, and mitigations.

---

## Table of Contents

1. [Complete Concept Mapping](#1-complete-concept-mapping)
2. [Type Mapping: Ab Initio DML → Spark/Delta](#2-type-mapping-ab-initio-dml--sparkdelta)
3. [Artifact Inventory](#3-artifact-inventory)
4. [Execution Order](#4-execution-order)
5. [PSET → Databricks Parameter Mapping](#5-pset--databricks-parameter-mapping)
6. [Risks and Mitigations](#6-risks-and-mitigations)
7. [Validation Checklist](#7-validation-checklist)
8. [Rollback Plan](#8-rollback-plan)

---

## 1. Complete Concept Mapping

| Ab Initio Concept | Source File(s) | Databricks Equivalent | Target File(s) |
|---|---|---|---|
| **Graph (.mp)** — Visual dataflow program | `graphs/parallel_loader.py`, `graphs/cdc_processor.py` | **Databricks Notebook** — PySpark notebook with cells | `databricks/notebooks/parallel_loader.py`, `databricks/notebooks/cdc_processor.py` |
| **DML** — Record layout definition | `dml/*.dml` (8 files) | **PySpark StructType** + **Delta Lake DDL** | `databricks/schemas/*.py` (8 files) |
| **PSET** — Runtime parameters per environment | `psets/pset_templates/*.pset` (3 files) | **Databricks Job Parameters** + **Widget defaults** | `databricks/workflows/*.json` (parameters section) |
| **Partition** — Parallel execution unit | `PartitionManager.generate_ranges()` | **Spark partitions** — `repartition()` / `spark.sql.shuffle.partitions` | Spark config in notebooks and workflow cluster specs |
| **CDC** — Change Data Capture via row hashing | `CDCProcessor` (MD5 hash comparison) | **Delta Lake MERGE** + **Change Data Feed (CDF)** | `databricks/notebooks/cdc_processor.py` |
| **AutoSys / Control-M** — Job scheduler | `scripts/run_daily_orders.ksh`, `scripts/run_customer_cdc.ksh` | **Databricks Workflows** — JSON job definitions with schedules | `databricks/workflows/daily_orders_workflow.json`, `databricks/workflows/customer_cdc_workflow.json` |
| **KornShell orchestration** — Batch pipeline scripts | `scripts/*.ksh` (3 files) | **Workflow task dependencies** — DAG of notebook tasks | Task `depends_on` chains in workflow JSON |
| **Co>Operating System** — Ab Initio runtime engine | `setenv.ksh` (AI_HOME, AB_HOME) | **Spark cluster** — managed by Databricks Runtime | Job cluster configuration in workflow JSON |
| **air sandbox run** — Execute graph in sandbox | `air sandbox run graph.mp -pset ... -partition N` | **dbutils.notebook.run()** / **Jobs API** | Notebook tasks in workflow definitions |
| **Reject files** — Bad record routing | Ab Initio reject ports on graph components | **DataFrame filter + Delta write** — separate good/bad records | `validate_and_split()` in parallel_loader notebook |
| **Checkpoint/restart** — Recovery mechanism | `AI_CHECKPOINT_ENABLED`, `AI_CHECKPOINT_DIR` | **Delta Lake ACID transactions** — atomic writes, no partial state | Built into Delta Lake `write.format("delta")` |
| **AutoSys API monitoring** — Job status polling | `monitoring/job_monitor.py` (REST API + Slack) | **Databricks SQL Alerts** — scheduled queries on system tables | `databricks/monitoring/job_failure_alerts.sql` |
| **SLA tracking** — 90-day compliance reports | `monitoring/sla_tracker.py` (JSON state file) | **Databricks SQL Dashboard** — real-time queries, no state file | `databricks/monitoring/sla_compliance_dashboard.sql` |
| **ServiceNow/UrbanCode** — Deployment pipeline | `deployment/deploy_manager.py`, `deployment/change_validator.py` | **Databricks Asset Bundles** + **CI/CD** (GitHub Actions/Azure DevOps) | Out of scope for this migration phase |
| **DML include directives** — Reusable type definitions | `include "common_address.dml"` → `address_t` | **Shared StructType modules** — Python imports | `databricks/schemas/common_address.py` → imported by `customer_address.py` |
| **void fields** — Padding/filler bytes | `void(",") padding1` in `account_status.dml` | **Dropped** — no semantic meaning in Delta Lake | Documented in schema comments |
| **packed_decimal / zoned_decimal** — Mainframe numerics | `packed_account.dml` | **DecimalType(p, s)** — requires byte-level decoding at ingestion | `databricks/schemas/packed_account.py` |

---

## 2. Type Mapping: Ab Initio DML → Spark/Delta

### Scalar Types

| Ab Initio DML Type | Example | PySpark Type | Delta Lake SQL | Notes |
|---|---|---|---|---|
| `decimal(",")` | `decimal(",") customer_id` | `LongType()` | `BIGINT` | Integer identifiers — no fractional part |
| `decimal("P.S")` | `decimal("8.2") balance` | `DecimalType(P, S)` | `DECIMAL(P, S)` | Fixed-precision monetary values |
| `string(",")` | `string(",") name` | `StringType()` | `STRING` | Variable-length text |
| `string(N)` | `string(20) account_name` | `StringType()` | `STRING` | Fixed-width → variable-length (max N chars) |
| `date("fmt")` | `date("YYYY-MM-DD")` | `DateType()` | `DATE` | ISO format preserved |
| `datetime("fmt")` | `datetime("YYYY-MM-DD HH24:MI:SS")` | `TimestampType()` | `TIMESTAMP` | Full timestamp with seconds |
| `integer` | (not in current DMLs) | `IntegerType()` | `INT` | 32-bit integer |
| `long` | (not in current DMLs) | `LongType()` | `BIGINT` | 64-bit integer |
| `double` | (not in current DMLs) | `DoubleType()` | `DOUBLE` | 64-bit floating point |

### Special Types

| Ab Initio DML Type | Example | PySpark Type | Delta Lake SQL | Notes |
|---|---|---|---|---|
| `packed_decimal(N)` | `packed_decimal(5) account_num` | `DecimalType(N, 0)` | `DECIMAL(N, 0)` | COMP-3: 2 digits/byte + sign nibble. **Requires byte-level decoding at ingestion.** |
| `packed_decimal("P.S")` | `packed_decimal("7.2") balance` | `DecimalType(P, S)` | `DECIMAL(P, S)` | COMP-3 with fractional part. Implicit decimal point. |
| `zoned_decimal(N)` | `zoned_decimal(4) status_code` | `DecimalType(N, 0)` | `DECIMAL(N, 0)` | 1 digit/byte with zone bits. **Requires zone-stripping at ingestion.** |
| `void(",")` | `void(",") padding1` | *(dropped)* | *(dropped)* | Filler bytes — no semantic meaning. |
| `string(delim, null("X"))` | `string(",", null("")) merchant_name` | `StringType()` (nullable) | `STRING` | Empty string → NULL. Use `.option("nullValue", "")` at ingestion. |
| `string(delim, null("X"))` | `string("\n", null("UNKNOWN")) channel` | `StringType()` (nullable) | `STRING` | Sentinel value → NULL or keep as default. |

### Compound Types

| Ab Initio DML Type | Example | PySpark Type | Delta Lake SQL | Notes |
|---|---|---|---|---|
| `type X = record ... end` | `type address_t = record` | `StructType([...])` | `STRUCT<...>` | Reusable named type → Python module import |
| `record ... end name` (nested) | `record ... end merchant_info` | `StructField("name", StructType([...]))` | `STRUCT<...>` | Nested struct within parent |
| `type[N]` (fixed array) | (not in current DMLs) | `ArrayType(...)` | `ARRAY<...>` | Fixed-length array |
| `type[count_field]` (variable array) | `string(",")[item_count] item_names` | `ArrayType(StringType())` | `ARRAY<STRING>` | Length-prefixed by count field |
| `record[count_field] ... end` (array of structs) | `record[item_count] ... end line_items` | `ArrayType(StructType([...]))` | `ARRAY<STRUCT<...>>` | Variable-length array of nested records |
| `if (cond) record ... end` (conditional) | `if (txn_type == 2) record ... end refund_details` | `StructField("name", StructType(...), nullable=True)` | Nullable `STRUCT<...>` | Conditional field → nullable column in Delta |

---

## 3. Artifact Inventory

### Schemas (`databricks/schemas/`)

| File | Source DML | Tables/Types |
|---|---|---|
| `customer.py` | `dml/customer.dml` | `CUSTOMER_SCHEMA`, `CUSTOMER_DDL` |
| `customer_address.py` | `dml/customer_address.dml` + `dml/common_address.dml` | `CUSTOMER_ADDRESS_SCHEMA`, `CUSTOMER_ADDRESS_FLAT_SCHEMA`, `CUSTOMER_ADDRESS_DDL` |
| `order_items.py` | `dml/order_items.dml` | `ORDER_ITEMS_SCHEMA`, `ORDER_ITEMS_DDL` |
| `account_balance.py` | `dml/account_balance.dml` | `ACCOUNT_BALANCE_SCHEMA`, `ACCOUNT_BALANCE_DDL` |
| `account_status.py` | `dml/account_status.dml` | `ACCOUNT_STATUS_SCHEMA`, `ACCOUNT_STATUS_DDL` |
| `packed_account.py` | `dml/packed_account.dml` | `PACKED_ACCOUNT_SCHEMA`, `PACKED_ACCOUNT_DDL` |
| `common_address.py` | `dml/common_address.dml` | `ADDRESS_TYPE` (reusable StructType) |
| `transaction_detail.py` | `dml/transaction_detail.dml` | `TRANSACTION_DETAIL_SCHEMA`, `TRANSACTION_DETAIL_DDL` + nested structs |

### Notebooks (`databricks/notebooks/`)

| File | Source Graph | Key Functionality |
|---|---|---|
| `parallel_loader.py` | `graphs/parallel_loader.py` | Partition-based ingestion, validation, Delta write |
| `cdc_processor.py` | `graphs/cdc_processor.py` | Delta MERGE, Change Data Feed, audit trail |

### Workflows (`databricks/workflows/`)

| File | Source Script | Schedule |
|---|---|---|
| `daily_orders_workflow.json` | `scripts/run_daily_orders.ksh` | Daily at 02:00 UTC |
| `customer_cdc_workflow.json` | `scripts/run_customer_cdc.ksh` | Every 4 hours |

### Monitoring (`databricks/monitoring/`)

| File | Source | Purpose |
|---|---|---|
| `job_failure_alerts.sql` | `monitoring/job_monitor.py` | SQL alerts for failures, long-running jobs, consecutive failures |
| `sla_compliance_dashboard.sql` | `monitoring/sla_tracker.py` | SLA compliance summary, 90-day trend, CDC statistics |

---

## 4. Execution Order

### Phase 1: Infrastructure Setup (Day 1-2)

```
1.1  Create Unity Catalog: catalog, schemas (bronze, silver, gold, audit)
1.2  Configure cloud storage mounts (/mnt/raw/, /mnt/staging/)
1.3  Create shared job clusters (etl_cluster, cdc_cluster)
1.4  Set up Databricks SQL warehouse for monitoring queries
1.5  Configure alert destinations (Slack webhook, email)
```

### Phase 2: Schema Deployment (Day 2-3)

```
2.1  Deploy reusable types:
       → Run DDL from databricks/schemas/common_address.py (no table — type only)

2.2  Deploy core tables (no dependencies):
       → Run DDL from databricks/schemas/customer.py
       → Run DDL from databricks/schemas/account_balance.py
       → Run DDL from databricks/schemas/account_status.py
       → Run DDL from databricks/schemas/packed_account.py

2.3  Deploy dependent tables:
       → Run DDL from databricks/schemas/customer_address.py (depends on address type)
       → Run DDL from databricks/schemas/order_items.py
       → Run DDL from databricks/schemas/transaction_detail.py

2.4  Create audit tables:
       → catalog.audit.orders_cdc_log
       → catalog.audit.customer_cdc_log
       → catalog.audit.customer_cdc_log_summary

2.5  Validate all tables: DESCRIBE TABLE EXTENDED for each
```

### Phase 3: Notebook Deployment (Day 3-4)

```
3.1  Import notebooks to Databricks workspace:
       → /Repos/data-engineering/abinitio-migration/databricks/notebooks/parallel_loader
       → /Repos/data-engineering/abinitio-migration/databricks/notebooks/cdc_processor

3.2  Dry-run parallel_loader with sample data (data/sample/orders.dat)
3.3  Dry-run cdc_processor with sample customer data (data/sample/customers.dat)
3.4  Validate:
       → Delta table created with correct schema
       → Change Data Feed enabled
       → Audit records written
```

### Phase 4: Workflow Deployment (Day 4-5)

```
4.1  Deploy daily_orders_workflow.json via Databricks Jobs API:
       POST /api/2.1/jobs/create with workflow JSON
4.2  Deploy customer_cdc_workflow.json via Databricks Jobs API
4.3  Trigger manual run of each workflow (paused schedule)
4.4  Validate:
       → All 4 tasks in daily_orders complete in order
       → All 4 tasks in customer_cdc complete in order
       → Task values passed between tasks correctly
4.5  Unpause schedules after validation
```

### Phase 5: Monitoring Setup (Day 5-6)

```
5.1  Create Databricks SQL alerts from job_failure_alerts.sql:
       → ab_initio_migration_job_failures (5-min refresh)
       → ab_initio_migration_long_running_jobs (10-min refresh)
       → ab_initio_migration_consecutive_failures (30-min refresh)
5.2  Create SQL Dashboard from sla_compliance_dashboard.sql:
       → SLA Compliance Summary widget
       → SLA Compliance Trend widget
       → Recent Job Runs widget
       → CDC Statistics widget
5.3  Configure alert destinations (Slack, email, PagerDuty)
5.4  Verify alerts fire correctly with a test failure
```

### Phase 6: Parallel Run (Day 6-20)

```
6.1  Run Ab Initio and Databricks pipelines in parallel
6.2  Compare outputs:
       → Row counts match
       → Key column values match
       → Decimal precision preserved (especially packed_decimal)
6.3  Monitor SLA compliance on both systems
6.4  Document any discrepancies
```

### Phase 7: Cutover (Day 20-25)

```
7.1  Final parallel run comparison
7.2  Disable Ab Initio AutoSys jobs
7.3  Unpause all Databricks workflows
7.4  Monitor first 48 hours closely
7.5  Decommission Ab Initio sandbox
```

---

## 5. PSET → Databricks Parameter Mapping

### orders_pipeline.pset → daily_orders_workflow.json

| PSET Parameter | PSET Value (Dev) | Databricks Job Parameter | Databricks Default |
|---|---|---|---|
| `SOURCE_PATH` | `/data/dev/raw/orders` | `source_path` | `/mnt/raw/orders` |
| `TARGET_TABLE` | `DEV.STAGING.ORDERS` | `target_table` | `catalog.gold.orders` |
| `PARTITION_COUNT` | `4` | `partition_count` | `4` |
| `BATCH_SIZE` | `50000` | `batch_size` | `50000` |
| `LOG_LEVEL` | `DEBUG` | `log_level` (widget) | `INFO` |
| `RECORD_SOURCE` | `ORDER_SYSTEM_DEV` | `_source_system` (audit col) | `abinitio_migration` |
| `MAX_ERRORS` | `100` | `max_errors` | `100` |
| `CHECKPOINT_DIR` | `/tmp/abinitio/checkpoints/orders` | *(not needed — Delta ACID)* | — |

### customer_cdc.pset → customer_cdc_workflow.json

| PSET Parameter | PSET Value (Dev) | Databricks Job Parameter | Databricks Default |
|---|---|---|---|
| `SOURCE_PATH` | `/data/raw/customer` | `source_path` | `/mnt/raw/customer` |
| `TARGET_TABLE` | `STAGING.CUSTOMER_MASTER` | `target_table` | `catalog.gold.customer_master` |
| `KEY_COLUMNS` | `customer_id` | `key_columns` | `customer_id` |
| `HASH_COLUMNS` | `customer_id,name,...` | `compare_columns` | Same list |
| `PARTITION_COUNT` | `8` | `partition_count` | `8` |
| `AUDIT_TABLE` | `AUDIT.CUSTOMER_CHANGES` | `audit_table` | `catalog.audit.customer_cdc_log` |
| `RETENTION_DAYS` | `90` | `retention_days` | `90` |
| `PREVIOUS_SNAPSHOT_PATH` | `/data/snapshots/customer/previous` | *(not needed — Delta time travel)* | — |
| `CURRENT_SNAPSHOT_PATH` | `/data/snapshots/customer/current` | *(not needed — Delta versioning)* | — |
| `CDC_OUTPUT_PATH` | `/data/cdc/customer` | *(not needed — MERGE is atomic)* | — |

### Environment Variables (setenv.ksh) → Cluster/Workspace Config

| Shell Variable | Value | Databricks Equivalent | Notes |
|---|---|---|---|
| `AI_HOME` | `/opt/abinitio` | Databricks Runtime | Managed by platform |
| `AI_PROJECT_DIR` | `/data/projects/enterprise_etl` | Workspace Repos path | `/Repos/data-engineering/abinitio-migration` |
| `AI_LOG_DIR` | `/data/logs/abinitio` | Cluster log delivery | S3/ADLS log destination |
| `AI_SOURCE_DB` | `ORACLE_PROD` | Unity Catalog external connection | Configure via External Locations |
| `AI_TARGET_DB` | `TERADATA_DW` | Unity Catalog Delta tables | `catalog.gold.*` |
| `AI_DEFAULT_PARTITIONS` | `4` | `spark.sql.shuffle.partitions` | Set in cluster Spark config |
| `AI_MAX_PARTITIONS` | `16` | Autoscaling worker range | `min_workers=4, max_workers=16` |
| `AI_MAX_ERRORS` | `100` | `max_errors` job parameter | Per-workflow parameter |
| `AI_ERROR_ACTION` | `ABORT` | Exception handling in notebooks | `raise RuntimeError(...)` |
| `AI_CHECKPOINT_ENABLED` | `true` | *(not needed)* | Delta Lake ACID replaces checkpointing |

---

## 6. Risks and Mitigations

### R1: Packed Decimal (COMP-3) Data Loss

| | |
|---|---|
| **Risk** | Ab Initio `packed_decimal` stores data in COMP-3 binary format (2 digits per byte + sign nibble). If raw EBCDIC/binary files are read as text, numeric values will be corrupted. |
| **Affected** | `dml/packed_account.dml` — `account_num`, `balance`, `status_code` |
| **Mitigation** | Implement a PySpark UDF or use `struct.unpack()` to decode COMP-3 bytes before loading. Validate decoded values against Ab Initio output. Test with known packed decimal values (e.g., `12345` in COMP-3 = `0x01 0x23 0x45 0x0C`). |
| **Validation** | Compare 100% of `packed_account` rows between Ab Initio and Databricks output. |

### R2: Zoned Decimal Precision

| | |
|---|---|
| **Risk** | `zoned_decimal(4)` stores one digit per byte with zone bits (0xF0-0xF9 for unsigned). Zone-stripping must preserve the sign nibble correctly. |
| **Affected** | `dml/packed_account.dml` — `status_code` |
| **Mitigation** | Implement zone-stripping logic in an ingestion UDF. Test positive, negative, and unsigned values. |
| **Validation** | Bit-level comparison of decoded values vs. Ab Initio parsed output. |

### R3: Partition Strategy Differences

| | |
|---|---|
| **Risk** | Ab Initio uses range-based partitioning (`start_record=X, end_record=Y`), while Spark uses hash-based partitioning by default. This can cause different data distribution and potentially different processing order. |
| **Affected** | All parallel operations, especially `parallel_loader` |
| **Mitigation** | Use `repartition(N, col("partition_key"))` for deterministic partitioning when order matters. For most ETL workloads, hash partitioning is equivalent or better. |
| **Validation** | Compare total record counts and checksums. Order-dependent operations should be explicitly sorted. |

### R4: Conditional Record Fields

| | |
|---|---|
| **Risk** | Ab Initio DML supports conditional records (`if (txn_type == 2) record ... end`). These have no direct equivalent in Spark schemas — conditional fields become nullable columns that must be NULL for non-matching records. |
| **Affected** | `dml/transaction_detail.dml` — `refund_details` struct |
| **Mitigation** | Modeled as nullable `StructType` in PySpark schema. Add a view or CHECK constraint to enforce: `refund_details IS NULL OR txn_type = 2`. |
| **Validation** | Query `WHERE txn_type != 2 AND refund_details IS NOT NULL` should return 0 rows. |

### R5: Variable-Length Array Handling

| | |
|---|---|
| **Risk** | Ab Initio arrays like `string(",")[item_count]` are length-prefixed in the data stream. During ingestion, the array must be parsed correctly using `item_count` as the delimiter. |
| **Affected** | `dml/order_items.dml` — `item_names`, `item_quantities` |
| **Mitigation** | Parse delimited arrays during CSV ingestion using `split()` function. Validate `SIZE(item_names) == item_count` post-load. |
| **Validation** | `SELECT COUNT(*) FROM order_items WHERE SIZE(item_names) != item_count` should return 0. |

### R6: Null Sentinel Values

| | |
|---|---|
| **Risk** | Ab Initio uses `null("")` (empty string = NULL) and `null("UNKNOWN")` (sentinel = NULL). These must be handled during ingestion to avoid storing sentinel values as real data. |
| **Affected** | `dml/transaction_detail.dml` — `merchant_name` (null("")), `channel` (null("UNKNOWN")) |
| **Mitigation** | Configure `.option("nullValue", "")` for empty-string nulls. Post-load transform for custom sentinels: `WHEN channel = 'UNKNOWN' THEN NULL`. |
| **Validation** | Compare NULL counts between Ab Initio and Databricks for affected columns. |

### R7: Date/DateTime Format Parsing

| | |
|---|---|
| **Risk** | Ab Initio uses Oracle-style format strings (`YYYY-MM-DD HH24:MI:SS`) while Spark uses Java SimpleDateFormat (`yyyy-MM-dd HH:mm:ss`). Incorrect format mapping will cause parse failures. |
| **Affected** | `dml/account_balance.dml` (date), `dml/transaction_detail.dml` (datetime) |
| **Mitigation** | Format strings are mapped in the ingestion notebooks. The StructType definitions use `DateType()` and `TimestampType()` which handle ISO formats natively. For non-ISO formats, use `to_timestamp(col, "format")`. |
| **Validation** | Spot-check 100 date/timestamp values between systems. |

### R8: Ab Initio void (Padding) Field Offsets

| | |
|---|---|
| **Risk** | Dropping `void` fields changes the positional layout of the record. If downstream systems rely on field position rather than name, this could break integrations. |
| **Affected** | `dml/account_status.dml` — `padding1`, `padding2` |
| **Mitigation** | Delta Lake uses named columns, not positional access. All downstream queries should reference columns by name. Document the positional change in schema comments. |
| **Validation** | Verify all downstream queries use column names (no `SELECT *` with positional assumptions). |

### R9: Concurrent Schedule Overlap

| | |
|---|---|
| **Risk** | The customer CDC workflow runs every 4 hours. If a run takes longer than 4 hours, the next scheduled run could overlap. Ab Initio handled this via AutoSys job dependencies. |
| **Affected** | `customer_cdc_workflow.json` |
| **Mitigation** | Set `max_concurrent_runs: 1` in the workflow definition (already configured). Databricks will queue the next run until the current one completes. |
| **Validation** | Test by triggering two runs within 5 minutes — second should queue, not fail. |

### R10: Loss of Ab Initio Checkpoint/Restart

| | |
|---|---|
| **Risk** | Ab Initio supports checkpoint/restart for long-running graphs — a failed job can resume from the last checkpoint. Delta Lake's ACID transactions replace this with atomic writes, but a failed write loses all progress. |
| **Affected** | Large batch loads (daily_orders Phase 4: production rollover) |
| **Mitigation** | Break large loads into micro-batches using `batch_size` parameter. Each batch is an atomic Delta transaction. On failure, only the current batch is lost — previous batches are committed. Also leverage Databricks Workflows `max_retries` for automatic retry. |
| **Validation** | Simulate a mid-load failure and verify that committed batches are intact. |

---

## 7. Validation Checklist

### Schema Validation

- [ ] All 7 Delta tables created with correct column names and types
- [ ] `packed_account` decimal precision matches Ab Initio (5,0), (7,2), (4,0)
- [ ] `transaction_detail` nested structs match expected structure
- [ ] `order_items` ARRAY columns accept variable-length data
- [ ] `account_status` has no padding columns (void fields dropped)
- [ ] Change Data Feed enabled on all tables (`delta.enableChangeDataFeed = true`)

### Data Validation

- [ ] Row counts match between Ab Initio and Databricks for all tables
- [ ] Decimal precision preserved (no rounding errors on monetary fields)
- [ ] NULL handling correct (empty string → NULL, sentinel → NULL)
- [ ] Date/timestamp values match (no timezone offset issues)
- [ ] Variable-length arrays have correct element counts
- [ ] Conditional fields (refund_details) are NULL for non-refund transactions

### Pipeline Validation

- [ ] daily_orders_workflow: all 4 tasks execute in sequence (extract → CDC → staging → production)
- [ ] customer_cdc_workflow: all 4 tasks execute in sequence (snapshot → detect → audit → cleanup)
- [ ] Task values passed correctly between tasks via `dbutils.notebook.exit()`
- [ ] Error handling: intentionally fail a task and verify downstream tasks don't run
- [ ] Retry logic: verify `max_retries=1` works on transient failures

### Monitoring Validation

- [ ] Job failure alert fires within 5 minutes of a failed run
- [ ] Long-running job alert fires when a job exceeds its SLA threshold
- [ ] SLA compliance dashboard shows correct percentages
- [ ] CDC statistics dashboard shows insert/update/delete counts
- [ ] Slack webhook delivers alerts correctly

---

## 8. Rollback Plan

If critical issues are found during parallel run:

1. **Pause** all Databricks workflow schedules immediately
2. **Re-enable** Ab Initio AutoSys jobs (they should still be in HOLD status)
3. **Preserve** Databricks data for debugging (do not DROP tables)
4. **Investigate** using:
   - Delta Lake time travel: `SELECT * FROM table VERSION AS OF N`
   - Change Data Feed: `SELECT * FROM table_changes('table', start_version)`
   - Databricks SQL Dashboard for SLA and failure metrics
5. **Fix** the issue in the Databricks artifacts
6. **Re-run** from the failed phase (not from scratch — Delta state is preserved)
