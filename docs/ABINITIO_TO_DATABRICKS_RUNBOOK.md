# Ab Initio to Databricks Lakehouse Migration Runbook

## 1. Overview

This runbook documents the migration of the enterprise Ab Initio ETL framework to Databricks Lakehouse architecture. It covers the complete concept mapping, execution plan, artifact inventory, and risk mitigations.

### 1.1 Migration Scope

| Category | Source (Ab Initio) | Target (Databricks) | Artifact Count |
|---|---|---|---|
| **Schemas** | `dml/*.dml` (8 DML files) | `databricks/schemas/*.py` (StructType + DDL) | 8 |
| **Graph Logic** | `graphs/*.py` (parallel loader, CDC) | `databricks/notebooks/*.py` (PySpark notebooks) | 2 |
| **Orchestration** | `scripts/*.ksh` (KornShell + AutoSys) | `databricks/workflows/*.json` (Workflow definitions) | 2 |
| **Monitoring** | `monitoring/*.py` (AutoSys API + SLA) | `databricks/monitoring/*.sql` (SQL alerts + dashboard) | 2 |
| **Configuration** | `psets/pset_templates/*.pset` | Databricks job parameters + notebook widgets | N/A (embedded) |

---

## 2. Complete Concept Mapping Table

### 2.1 Core Platform Concepts

| Ab Initio Concept | Description | Databricks Equivalent | Migration Notes |
|---|---|---|---|
| **Graph (.mp)** | Visual ETL dataflow program | **Databricks Notebook** (PySpark) | Each graph maps to a notebook with equivalent logic |
| **Co>Operating System** | Ab Initio runtime engine | **Spark Cluster** | Managed by Databricks; no install required |
| **DML (Data Manipulation Language)** | Record layout/schema definition | **PySpark StructType** + Delta DDL | See Section 3 for type mapping details |
| **PSET (Parameter Set)** | Runtime parameters per environment | **Job Parameters** + Notebook Widgets | `define KEY VALUE` -> `dbutils.widgets.text("key", "value")` |
| **Partition (m_partition)** | Unit of parallel execution | **Spark `repartition()`** | Spark handles partitioning natively across executors |
| **air sandbox run** | Execute graph in sandbox | **Notebook task** in Workflow | `notebook_task.notebook_path` in workflow JSON |
| **air_run CLI** | Command-line graph execution | **`dbx execute`** / Jobs API | Or `databricks jobs run-now` via REST API |
| **Air Sandbox** | Local dev/test environment | **Databricks Workspace** | Repos + interactive notebook development |
| **EME (Enterprise Meta-Environment)** | Code repository | **Databricks Repos** + Git | Version control via Git integration |
| **air_deploy** | Deployment utility | **Databricks Asset Bundles** | `databricks bundle deploy` for CI/CD |

### 2.2 Data Processing Concepts

| Ab Initio Concept | Description | Databricks Equivalent | Migration Notes |
|---|---|---|---|
| **CDC (Change Data Capture)** | Hash-based record comparison | **Delta Lake MERGE** + Change Data Feed | `MERGE INTO ... WHEN MATCHED / NOT MATCHED` |
| **Compare Records by Key** | Ab Initio CDC component | **Delta MERGE conditions** | `target.key = source.key AND target._hash != source._hash` |
| **Row hash (MD5)** | Change detection hash | **`sha2(concat_ws())` in Spark** | SHA-256 for stronger collision resistance |
| **Parallel Loader** | Multi-partition graph executor | **Spark distributed read** | `df.repartition(N)` replaces explicit partition ranges |
| **PartitionManager** | Partition range calculator | **Spark partitioner** | Automatic with `repartition()` / partition pruning |
| **Checkpoint/Restart** | Job recovery mechanism | **Delta Lake transactions** | ACID guarantees; failed writes are automatically rolled back |

### 2.3 Orchestration & Operations

| Ab Initio Concept | Description | Databricks Equivalent | Migration Notes |
|---|---|---|---|
| **AutoSys** | Enterprise job scheduler | **Databricks Workflows** | Cron-based scheduling with task dependencies |
| **KornShell scripts** | Batch orchestration | **Workflow JSON definitions** | Sequential `air sandbox run` -> task dependency chain |
| **setenv.ksh** | Environment variable setup | **Job parameters** + cluster env vars | `export AI_*` vars -> `job_parameters[].default` |
| **AutoSys JOB_** prefix | Job naming convention | **Workflow `tags.original_autosys_job`** | Preserved in tags for traceability |
| **Control-M** | Alternative scheduler | **Databricks Workflows** | Same mapping as AutoSys |
| **ServiceNow CRs** | Change management | **Databricks Asset Bundles** + CI/CD | PR-based deployment with approval gates |
| **UrbanCode Deploy** | Deployment automation | **`databricks bundle deploy`** | CLI-based deployment from CI/CD pipeline |

### 2.4 Monitoring & Alerting

| Ab Initio Concept | Description | Databricks Equivalent | Migration Notes |
|---|---|---|---|
| **JobMonitor** (AutoSys API polling) | Job status monitoring | **Databricks SQL Alerts** | Query `system.lakeflow.job_run_timeline` |
| **SLATracker** (JSON state file) | 90-day SLA compliance | **SQL Dashboard query** | No external state file needed; uses system tables |
| **Slack webhook alerts** | Failure notifications | **Workflow webhook_notifications** | Configure Slack/PagerDuty webhook ID |
| **AutoSys REST API** | Job status API | **Databricks REST API** | `/api/2.1/jobs/runs/list` for programmatic access |
| **SLA window end time** | Deadline tracking | **Workflow health rules** | `RUN_DURATION_SECONDS > threshold` |
| **90-day run_history** | Historical retention | **System table retention** | `system.lakeflow` retains 90 days by default |

### 2.5 Data Types (DML -> Spark)

| Ab Initio DML Type | Example | Spark Type | Delta SQL Type | Notes |
|---|---|---|---|---|
| `decimal` (no precision) | `decimal(",") id` | `LongType` | `BIGINT` | Integer identifier |
| `decimal("P.S")` | `decimal("8.2") balance` | `DecimalType(P,S)` | `DECIMAL(8,2)` | Fixed-precision monetary |
| `string` | `string(",") name` | `StringType` | `STRING` | Variable-length text |
| `string(N)` | `string(20) name` | `StringType` | `STRING` | Fixed-width; TRIM on ingest |
| `date("fmt")` | `date("YYYY-MM-DD")` | `DateType` | `DATE` | ISO date format |
| `datetime("fmt")` | `datetime("YYYY-MM-DD HH24:MI:SS")` | `TimestampType` | `TIMESTAMP` | Full timestamp |
| `packed_decimal(N)` | `packed_decimal(5)` | `DecimalType(N,0)` | `DECIMAL(5,0)` | COMP-3 mainframe; needs binary decode |
| `packed_decimal("P.S")` | `packed_decimal("7.2")` | `DecimalType(P,S)` | `DECIMAL(7,2)` | COMP-3 with scale |
| `zoned_decimal(N)` | `zoned_decimal(4)` | `DecimalType(N,0)` | `DECIMAL(4,0)` | EBCDIC zoned; needs decode |
| `void` | `void(",") padding` | _(dropped)_ | _(dropped)_ | Padding bytes; no semantic value |
| `record` (nested) | `record ... end info` | `StructType` | `STRUCT<...>` | Nested struct |
| `type[count]` (array) | `string[count] items` | `ArrayType(...)` | `ARRAY<...>` | Variable-length array |
| `if (condition)` | `if (type == 2) record` | Nullable `StructType` | Nullable `STRUCT` | Conditional; always present, null when N/A |

---

## 3. DML Schema Migration Details

### 3.1 Schema Artifact Map

| Source DML | Target Schema File | Key Decisions |
|---|---|---|
| `dml/customer.dml` | `databricks/schemas/customer.py` | `decimal` -> `LongType` (integer key, no fractional part) |
| `dml/order_items.dml` | `databricks/schemas/order_items.py` | Variable-length arrays `[item_count]` -> `ArrayType` |
| `dml/transaction_detail.dml` | `databricks/schemas/transaction_detail.py` | Nested records -> `StructType`; conditional record -> nullable struct; partitioned by `txn_type` |
| `dml/account_balance.dml` | `databricks/schemas/account_balance.py` | Pipe delimiter noted; `decimal("8.2")` -> `DecimalType(8,2)` |
| `dml/account_status.dml` | `databricks/schemas/account_status.py` | `void` padding fields dropped |
| `dml/common_address.dml` | `databricks/schemas/common_address.py` | Reusable type `address_t` -> shared `StructType` (no standalone table) |
| `dml/customer_address.dml` | `databricks/schemas/customer_address.py` | `include` directive -> Python import of `address_struct` |
| `dml/packed_account.dml` | `databricks/schemas/packed_account.py` | `packed_decimal` / `zoned_decimal` -> `DecimalType` with binary pre-processing |

### 3.2 Type Mapping Decisions

1. **`decimal` without precision** (e.g., `decimal(",") customer_id`): Mapped to `LongType`/`BIGINT`. These are integer identifiers with no fractional component. `LongType` provides the same range as Ab Initio's unbounded decimal for ID values.

2. **`decimal("P.S")` with precision** (e.g., `decimal("8.2") balance`): Mapped to `DecimalType(P,S)`/`DECIMAL(P,S)`. Preserves exact precision for monetary calculations. No floating-point approximation.

3. **`packed_decimal` (COMP-3)**: Mapped to `DecimalType` but **requires a binary pre-processing step** during ingestion. Ab Initio reads COMP-3 natively; Spark does not. A custom UDF or pre-processing script must decode the binary BCD representation before loading.

4. **`zoned_decimal`**: Same treatment as `packed_decimal`. EBCDIC zoned numeric encoding must be decoded during ingestion.

5. **`void` padding fields**: Dropped entirely. These are structural filler bytes from mainframe-originated files with no business value. If positional compatibility is needed, they can be re-added as NULL string columns.

6. **`string(N)` fixed-width**: Mapped to variable-length `StringType`. Apply `TRIM()` during ingestion to remove trailing spaces from fixed-width source records.

7. **Variable-length arrays `[count]`**: Mapped to `ArrayType`. The count field is retained for compatibility but is redundant since `size(array_col)` can derive it.

8. **Nested records**: Mapped to `StructType` fields. Preserves the hierarchical structure from DML rather than flattening to avoid information loss.

9. **Conditional records `if (condition)`**: Mapped to nullable `StructType`. The field is always present in the schema but null when the condition is not met (e.g., `refund_details` is null when `txn_type != 2`).

10. **Reusable types `type name =`**: Mapped to shared Python modules. `common_address.dml`'s `address_t` becomes an importable `address_struct` in `common_address.py`.

---

## 4. Execution Order for Migration

### Phase 1: Foundation (Week 1-2)

| Step | Action | Depends On | Validation |
|---|---|---|---|
| 1.1 | Deploy Delta Lake schemas (`databricks/schemas/`) | Unity Catalog setup | `DESCRIBE TABLE` returns expected columns |
| 1.2 | Create bronze/silver/gold catalog schemas | Databricks workspace | `SHOW SCHEMAS IN catalog` |
| 1.3 | Execute all `CREATE TABLE` DDL statements | 1.1, 1.2 | Tables visible in Unity Catalog |
| 1.4 | Configure job parameters (replace PSETs) | Workspace | Widgets resolve correctly in notebooks |

### Phase 2: Core Pipeline Migration (Week 3-4)

| Step | Action | Depends On | Validation |
|---|---|---|---|
| 2.1 | Deploy `parallel_loader` notebook | 1.3 | Test with sample data from `data/sample/` |
| 2.2 | Deploy `cdc_processor` notebook | 1.3 | Test MERGE with known inserts/updates/deletes |
| 2.3 | Validate Delta CDF output | 2.2 | `table_changes()` returns expected change records |
| 2.4 | Run parallel data comparison | 2.1, 2.2 | Row counts match Ab Initio output |

### Phase 3: Orchestration Migration (Week 5)

| Step | Action | Depends On | Validation |
|---|---|---|---|
| 3.1 | Deploy `daily_orders_workflow.json` | 2.1, 2.2 | Workflow appears in Jobs UI; dry run succeeds |
| 3.2 | Deploy `customer_cdc_workflow.json` | 2.1, 2.2 | Workflow appears in Jobs UI; dry run succeeds |
| 3.3 | Configure webhook notifications | 3.1, 3.2 | Test alert fires on simulated failure |
| 3.4 | Parallel run: Ab Initio + Databricks | 3.1, 3.2 | Compare output tables for data parity |

### Phase 4: Monitoring & Cutover (Week 6)

| Step | Action | Depends On | Validation |
|---|---|---|---|
| 4.1 | Deploy SQL alert (`job_failure_alert.sql`) | 3.1, 3.2 | Alert fires on test failure |
| 4.2 | Deploy SLA dashboard (`sla_compliance_dashboard.sql`) | 3.4 | Dashboard shows compliance metrics |
| 4.3 | Enable workflow schedules (unpause) | 3.4 verification | Scheduled runs execute successfully |
| 4.4 | Decommission Ab Initio pipelines | 4.3 (7-day soak) | No regressions in 7-day parallel period |

### Phase 5: Cleanup (Week 7+)

| Step | Action | Depends On | Validation |
|---|---|---|---|
| 5.1 | Remove AutoSys job definitions | 4.4 | AutoSys confirms jobs deleted |
| 5.2 | Archive Ab Initio sandbox and graphs | 4.4 | Backup verified |
| 5.3 | Update documentation and runbooks | 5.1, 5.2 | Team review complete |

---

## 5. Risks and Mitigations

### 5.1 Critical Risks

#### R1: Packed/Zoned Decimal Handling
- **Risk**: COMP-3 (packed_decimal) and EBCDIC zoned_decimal formats are binary representations that Spark cannot read natively. Incorrect decoding will cause silent data corruption.
- **Impact**: HIGH — Financial data (balances, amounts) will be wrong.
- **Mitigation**:
  1. Build a pre-processing step using the `cobrix` Spark library (open-source COBOL/mainframe reader) or a custom PySpark UDF that implements BCD decoding.
  2. Validate every decoded value against Ab Initio output for the same source file.
  3. Run a checksum comparison: `SUM(balance)` from Ab Initio vs Databricks for 100% of records.
  4. Keep Ab Initio running in parallel for packed_account data for at least 2 weeks.
- **Affected artifact**: `databricks/schemas/packed_account.py`

#### R2: Partition Strategy Differences
- **Risk**: Ab Initio uses explicit record-range partitioning (`start_record=N, end_record=M`). Spark uses hash-based or column-based partitioning. Different partition boundaries may cause different ordering or grouping of records.
- **Impact**: MEDIUM — ETL logic that depends on record ordering within partitions may produce different results.
- **Mitigation**:
  1. Add explicit `ORDER BY` clauses in notebooks where ordering matters.
  2. Validate total record counts and aggregate checksums (SUM, COUNT DISTINCT) match.
  3. Avoid relying on partition-internal ordering for business logic.
- **Affected artifacts**: `databricks/notebooks/parallel_loader.py`

#### R3: CDC Semantic Differences
- **Risk**: Ab Initio CDC uses in-memory Pandas comparison with MD5 hashing. Delta Lake MERGE uses SQL-based matching. Edge cases (NULL handling, floating-point comparison, string encoding) may differ.
- **Impact**: MEDIUM — Some rows may be classified differently (insert vs update).
- **Mitigation**:
  1. Use `coalesce(col, lit("__NULL__"))` in hash computation to ensure NULL consistency.
  2. Cast all columns to STRING before hashing (matches Ab Initio's `astype(str)` behaviour).
  3. Run parallel CDC on the same source snapshot and compare insert/update/delete counts.
  4. SHA-256 (Databricks) is used instead of MD5 (Ab Initio) — collision probability is lower, so this is a safe change.
- **Affected artifact**: `databricks/notebooks/cdc_processor.py`

### 5.2 Operational Risks

#### R4: Schedule Timezone Handling
- **Risk**: AutoSys job schedules may be in local timezone; Databricks Workflows use explicit timezone IDs. Incorrect timezone mapping could cause pipelines to run at the wrong time.
- **Impact**: MEDIUM — SLA breaches if pipeline runs at the wrong hour.
- **Mitigation**:
  1. Workflows are configured with `timezone_id: "UTC"`. Verify this matches the existing AutoSys schedule.
  2. All workflows are deployed in PAUSED state to prevent accidental execution.
  3. Run at least 3 manual triggers before enabling the schedule.

#### R5: PSET Parameter Parity
- **Risk**: Some PSET parameters may have environment-specific overrides (dev/uat/prod) that are not captured in the default template files.
- **Impact**: LOW — Pipeline may run with dev defaults in production.
- **Mitigation**:
  1. Extract all PSET overrides from every environment using `PSETManager.diff_psets()`.
  2. Map environment-specific values to Databricks job parameter overrides.
  3. Use Databricks job environment-specific configurations for dev/staging/prod.

#### R6: Monitoring Coverage Gap During Cutover
- **Risk**: During the parallel-run period, alerts from both AutoSys and Databricks may fire for the same issue, causing confusion. Or one system may miss alerts the other catches.
- **Impact**: LOW — Alert fatigue or missed alerts during transition.
- **Mitigation**:
  1. Prefix all Databricks alerts with `[DATABRICKS]` and AutoSys alerts with `[LEGACY]`.
  2. Route to a dedicated `#migration-alerts` channel during the parallel period.
  3. Daily review of both alert streams for the first 2 weeks.

### 5.3 Data Quality Risks

#### R7: Delimiter Handling
- **Risk**: DML files use different delimiters (comma, pipe, semicolon, newline). If the wrong delimiter is configured during ingestion, fields will be misaligned.
- **Impact**: HIGH — Data corruption.
- **Mitigation**:
  1. Each schema file documents the source delimiter in comments.
  2. Ingestion notebooks must configure `option("delimiter", ...)` matching the DML delimiter.
  3. Validate first 100 rows visually after each initial load.

#### R8: NULL Sentinel Values
- **Risk**: Ab Initio DML uses `null("")` and `null("UNKNOWN")` sentinels. These must be mapped to SQL NULL values in Delta Lake, not stored as literal strings.
- **Impact**: MEDIUM — Incorrect NULL semantics in downstream queries.
- **Mitigation**:
  1. Configure `option("nullValue", "")` for empty-string sentinels.
  2. Add post-load transformation: `when(col("channel") == "UNKNOWN", lit(None))`.
  3. Document sentinel values in schema file comments.

---

## 6. Artifact Inventory

### 6.1 Databricks Schemas (`databricks/schemas/`)

| File | Source DML | Table Name | Key Types |
|---|---|---|---|
| `__init__.py` | — | — | Package init with type mapping reference |
| `customer.py` | `customer.dml` | `catalog.bronze.customer` | LongType, StringType |
| `order_items.py` | `order_items.dml` | `catalog.bronze.order_items` | LongType, ArrayType |
| `transaction_detail.py` | `transaction_detail.dml` | `catalog.bronze.transaction_detail` | TimestampType, StructType, ArrayType, DecimalType |
| `account_balance.py` | `account_balance.dml` | `catalog.bronze.account_balance` | DecimalType(8,2), DateType |
| `account_status.py` | `account_status.dml` | `catalog.bronze.account_status` | LongType (void fields dropped) |
| `common_address.py` | `common_address.dml` | _(reusable struct, no table)_ | Shared StructType for address |
| `customer_address.py` | `customer_address.dml` | `catalog.bronze.customer_address` | Nested address StructType |
| `packed_account.py` | `packed_account.dml` | `catalog.bronze.packed_account` | DecimalType (packed/zoned, needs binary decode) |

### 6.2 PySpark Notebooks (`databricks/notebooks/`)

| File | Source Graph | Purpose |
|---|---|---|
| `parallel_loader.py` | `graphs/parallel_loader.py` | Partition-based parallel ingestion with Spark repartition |
| `cdc_processor.py` | `graphs/cdc_processor.py` | Delta Lake MERGE with SHA-256 change detection and CDF |

### 6.3 Databricks Workflows (`databricks/workflows/`)

| File | Source Script | Schedule | Tasks |
|---|---|---|---|
| `daily_orders_workflow.json` | `scripts/run_daily_orders.ksh` | Daily 02:00 UTC | extract -> CDC -> staging -> production (4 tasks) |
| `customer_cdc_workflow.json` | `scripts/run_customer_cdc.ksh` | Every 4 hours | snapshot -> CDC merge -> audit validation (3 tasks) |

### 6.4 Monitoring (`databricks/monitoring/`)

| File | Source Module | Purpose |
|---|---|---|
| `job_failure_alert.sql` | `monitoring/job_monitor.py` | SQL alert for failed job detection (replaces AutoSys API polling) |
| `sla_compliance_dashboard.sql` | `monitoring/sla_tracker.py` | SLA compliance dashboard (replaces JSON state file tracking) |

---

## 7. PSET to Job Parameter Mapping

### 7.1 orders_pipeline.pset → daily_orders_workflow.json

| PSET Parameter | PSET Default | Workflow Parameter | Workflow Default |
|---|---|---|---|
| `SOURCE_PATH` | `/data/dev/raw/orders` | `source_base_path` | `abfss://raw@datalake.dfs.core.windows.net/orders` |
| `TARGET_TABLE` | `DEV.STAGING.ORDERS` | `staging_table` | `catalog.staging.orders` |
| `PARTITION_COUNT` | `4` | `partition_count` | `4` |
| `BATCH_SIZE` | `50000` | `batch_size` | `50000` |
| `MAX_ERRORS` | `100` | `max_errors` | `100` |
| `LOG_LEVEL` | `DEBUG` | _(cluster config)_ | Spark log4j configuration |
| `RECORD_SOURCE` | `ORDER_SYSTEM_DEV` | _(not mapped)_ | Lineage via Unity Catalog |
| `CHECKPOINT_DIR` | `/tmp/abinitio/checkpoints/orders` | _(not needed)_ | Delta Lake ACID handles recovery |

### 7.2 customer_cdc.pset → customer_cdc_workflow.json

| PSET Parameter | PSET Default | Workflow Parameter | Workflow Default |
|---|---|---|---|
| `SOURCE_PATH` | `/data/raw/customer` | `source_path` | `abfss://raw@datalake.dfs.core.windows.net/customer` |
| `TARGET_TABLE` | `STAGING.CUSTOMER_MASTER` | `target_table` | `catalog.silver.customer_master` |
| `KEY_COLUMNS` | `customer_id` | `key_columns` | `customer_id` |
| `HASH_COLUMNS` | `customer_id,name,...` | `hash_columns` | `first_name,last_name,email` |
| `PARTITION_COUNT` | `8` | `partition_count` | `8` |
| `BATCH_SIZE` | `100000` | `batch_size` | `100000` |
| `MAX_ERRORS` | `50` | `max_errors` | `50` |
| `PREVIOUS_SNAPSHOT_PATH` | `/data/snapshots/customer/previous` | _(not needed)_ | Delta Lake versioning handles snapshots |
| `CDC_OUTPUT_PATH` | `/data/cdc/customer` | _(not needed)_ | Delta CDF provides change feed |
| `AUDIT_TABLE` | `AUDIT.CUSTOMER_CHANGES` | _(CDF replaces)_ | `table_changes()` function |
| `RETENTION_DAYS` | `90` | _(system default)_ | System table retains 90 days |

---

## 8. Post-Migration Validation Checklist

- [ ] All 8 Delta tables created with correct schemas (`DESCRIBE TABLE` matches DML)
- [ ] Sample data from `data/sample/` loads correctly into bronze tables
- [ ] Parallel loader notebook processes all partition counts (4, 8, 16)
- [ ] CDC processor correctly identifies inserts, updates, and deletes
- [ ] Delta Change Data Feed returns expected change records
- [ ] Daily orders workflow completes all 4 phases in sequence
- [ ] Customer CDC workflow runs successfully on a 4-hour schedule
- [ ] Job failure alert fires within 5 minutes of a simulated failure
- [ ] SLA dashboard shows compliance metrics for the last 7 days
- [ ] Packed/zoned decimal values match Ab Initio output (100% validation)
- [ ] Row counts match between Ab Initio and Databricks for all tables
- [ ] Aggregate checksums (SUM of monetary columns) match within tolerance
- [ ] All workflows deployed in PAUSED state; enabled only after validation
- [ ] Webhook notifications configured and tested
