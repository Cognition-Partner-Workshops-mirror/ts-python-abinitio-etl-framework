# Ab Initio → Databricks Lakehouse Migration Runbook

## 1. Overview

This runbook documents the migration of an enterprise Ab Initio ETL estate to Databricks Lakehouse architecture. The migration covers schema definitions (DML), execution graphs, orchestration scripts, and monitoring infrastructure.

### Scope

| Component | Source (Ab Initio) | Target (Databricks) | Directory |
|---|---|---|---|
| Record layouts | `dml/*.dml` (8 files) | PySpark StructType + Delta DDL | `databricks/schemas/` |
| Graph execution | `graphs/parallel_loader.py`, `graphs/cdc_processor.py` | PySpark notebooks | `databricks/notebooks/` |
| Orchestration | `scripts/run_daily_orders.ksh`, `scripts/run_customer_cdc.ksh` | Databricks Workflow JSON | `databricks/workflows/` |
| Monitoring | `monitoring/job_monitor.py`, `monitoring/sla_tracker.py` | Databricks SQL Alerts + Dashboard | `databricks/monitoring/` |
| Configuration | `psets/pset_templates/*.pset` | Databricks job parameters + widgets | Embedded in workflows |

---

## 2. Complete Concept Mapping Table

| Ab Initio Concept | Description | Databricks Equivalent | Notes |
|---|---|---|---|
| **Graph (.mp)** | Visual dataflow program | Databricks Notebook / Spark Job | 1 graph → 1 notebook |
| **DML** | Record layout definition (schema) | PySpark `StructType` + Delta `CREATE TABLE` | See type mapping below |
| **PSET** | Runtime parameters per environment | Job parameters + notebook widgets | `dbutils.widgets` in notebooks |
| **PSET environment override** | `psets/{dev\|uat\|prod}/` directories | Databricks job parameter defaults per environment | Configure per workflow |
| **Partition (m_partition)** | Parallel execution unit (subprocess per partition) | Spark partitions / `repartition()` | Spark manages automatically |
| **air sandbox run** | Execute graph in sandbox | `dbutils.notebook.run()` or Jobs API | No subprocess needed |
| **air_run -partition=N** | Run graph on specific partition | `df.repartition(N)` | Declarative, not imperative |
| **CDC (Compare Records by Key)** | Hash-based row comparison | Delta Lake `MERGE` + Change Data Feed | Single MERGE replaces 3 graphs |
| **AutoSys job** | Scheduled job trigger | Databricks Workflow schedule (cron) | Quartz cron expressions |
| **AutoSys dependency chain** | Job A → Job B → Job C | Workflow task `depends_on` | DAG in workflow JSON |
| **KornShell script** | Orchestration wrapper | Databricks Workflow JSON definition | Shell logic → task dependencies |
| **`set -e` (abort on error)** | Fail-fast error handling | Task `max_retries` + failure notifications | More granular per-task control |
| **Co>Operating System** | Ab Initio runtime engine | Spark cluster (Databricks Runtime) | Managed infrastructure |
| **Checkpoint/restart** | File-based recovery point | Delta Lake ACID transactions | Implicit — no config needed |
| **Error port / reject file** | Bad record handling | `badRecordsPath` option + `_corrupt_record` column | Spark PERMISSIVE mode |
| **AutoSys API monitoring** | REST API for job status polling | `system.lakeflow.job_run_timeline` system table | SQL-queryable, no API needed |
| **Slack webhook alerts** | Failure notification | Databricks webhook notifications on workflows | Built into workflow JSON |
| **SLA tracking (JSON state)** | 90-day compliance history in flat file | Databricks SQL dashboard against system tables | No state file needed |
| **ServiceNow / UrbanCode** | Deployment automation | Databricks Asset Bundles / CI/CD | Not in scope for this migration |
| **`include` directive** | DML file inclusion (shared types) | Python module import | `from databricks.schemas.common_address import address_schema` |
| **`type T = record`** | Named reusable record type | Reusable `StructType` variable | Shared across schema modules |
| **void field** | Padding/filler bytes | Dropped (not migrated) | No data value — skip during ingestion |
| **null("value") directive** | Default value for nulls | Handled in ETL logic (`coalesce()`) | Spark has no schema-level defaults for reads |

---

## 3. DML → Spark Type Mapping Reference

| Ab Initio Type | Spark Type | Precision Handling | Example |
|---|---|---|---|
| `decimal` (no precision) | `LongType` | Integer identifiers | `decimal(",") customer_id` → `LongType` |
| `decimal("p.s")` | `DecimalType(p, s)` | Fixed-precision monetary | `decimal("8.2")` → `DecimalType(8, 2)` |
| `packed_decimal(n)` | `DecimalType(n, 0)` | **Binary BCD** — requires byte-level decoding | `packed_decimal(5)` → `DecimalType(5, 0)` |
| `packed_decimal("p.s")` | `DecimalType(p, s)` | **Binary BCD** with fractional | `packed_decimal("7.2")` → `DecimalType(7, 2)` |
| `zoned_decimal(n)` | `DecimalType(n, 0)` | **EBCDIC zoned** — requires codepage conversion | `zoned_decimal(4)` → `DecimalType(4, 0)` |
| `string` (delimited) | `StringType` | Direct mapping | `string(",")` → `StringType` |
| `string(n)` (fixed-width) | `StringType` | Trim padding at read time | `string(20)` → `StringType` (+ `trim()`) |
| `date("fmt")` | `DateType` | Format handled by reader | `date("YYYY-MM-DD")` → `DateType` |
| `datetime("fmt")` | `TimestampType` | Format handled by reader | `datetime("YYYY-MM-DD HH24:MI:SS")` → `TimestampType` |
| `void` | **Dropped** | Padding bytes — no data value | `void(",") padding1` → not migrated |
| Nested `record` | `StructType` | Nested struct | `record ... end merchant_info` → `StructType([...])` |
| `record[count]` | `ArrayType(StructType)` | Variable-length array of structs | `record[item_count] ... end line_items` → `ArrayType(StructType)` |
| `type T = record` | Reusable `StructType` | Shared type via Python import | `type address_t = record` → `address_schema` variable |
| Conditional `if (expr)` | Nullable `StructType` | Always present, NULL when condition false | `if (txn_type == 2) record ... end` → nullable struct |

---

## 4. File-by-File Migration Map

### 4.1 DML Schemas → `databricks/schemas/`

| Source DML | Target Module | Key Decisions |
|---|---|---|
| `dml/customer.dml` | `databricks/schemas/customer.py` | Straightforward — all string/decimal fields |
| `dml/order_items.dml` | `databricks/schemas/order_items.py` | Variable-length arrays `[item_count]` → `ArrayType` |
| `dml/transaction_detail.dml` | `databricks/schemas/transaction_detail.py` | Most complex: nested structs, arrays, conditional record |
| `dml/account_balance.dml` | `databricks/schemas/account_balance.py` | Pipe-delimited, `decimal("8.2")` → `DecimalType(8,2)` |
| `dml/account_status.dml` | `databricks/schemas/account_status.py` | `void` padding fields dropped |
| `dml/common_address.dml` | `databricks/schemas/common_address.py` | Reusable type — no standalone table |
| `dml/customer_address.dml` | `databricks/schemas/customer_address.py` | Includes `common_address.dml` → Python import |
| `dml/packed_account.dml` | `databricks/schemas/packed_account.py` | **High risk**: packed/zoned decimal binary formats |

### 4.2 Graphs → `databricks/notebooks/`

| Source | Target | Key Changes |
|---|---|---|
| `graphs/parallel_loader.py` | `databricks/notebooks/parallel_loader.py` | ThreadPoolExecutor → Spark native parallelism; `air_run` subprocess → DataFrame operations |
| `graphs/cdc_processor.py` | `databricks/notebooks/cdc_processor.py` | Pandas hash-based CDC → Delta Lake MERGE; 3 separate output DataFrames → single atomic MERGE |

### 4.3 Scripts → `databricks/workflows/`

| Source | Target | Key Changes |
|---|---|---|
| `scripts/run_daily_orders.ksh` | `databricks/workflows/daily_orders_workflow.json` | 4-phase sequential KornShell → 4-task DAG with `depends_on` |
| `scripts/run_customer_cdc.ksh` | `databricks/workflows/customer_cdc_workflow.json` | 4-step KornShell → single notebook task (MERGE replaces 4 graphs) |
| `scripts/setenv.ksh` | Embedded in workflow parameters | Environment variables → job parameters + Databricks secrets |

### 4.4 Monitoring → `databricks/monitoring/`

| Source | Target | Key Changes |
|---|---|---|
| `monitoring/job_monitor.py` | `databricks/monitoring/job_failure_alert.sql` | AutoSys API polling → SQL query on `system.lakeflow.job_run_timeline` |
| `monitoring/sla_tracker.py` | `databricks/monitoring/sla_compliance_dashboard.sql` | JSON state file → SQL dashboard with system table queries |

---

## 5. Execution Order for Migration

### Phase 1: Foundation (Week 1)

1. **Create Unity Catalog structure**
   ```sql
   CREATE CATALOG IF NOT EXISTS catalog;
   CREATE SCHEMA IF NOT EXISTS catalog.bronze;
   CREATE SCHEMA IF NOT EXISTS catalog.silver;
   CREATE SCHEMA IF NOT EXISTS catalog.gold;
   CREATE SCHEMA IF NOT EXISTS catalog.audit;
   ```

2. **Deploy Delta Lake schemas** — Run all DDL statements from `databricks/schemas/*.py`
   - Order: `common_address.py` first (shared type), then all others (no dependencies between them)
   - Verify: `DESCRIBE TABLE EXTENDED catalog.bronze.<table>` for each table

3. **Configure Databricks secrets** — Map `setenv.ksh` environment variables
   ```
   AI_SOURCE_DB=ORACLE_PROD  → Databricks secret scope: etl/source_db_connection
   AI_TARGET_DB=TERADATA_DW  → Replaced by Delta Lake (no external DW target)
   AI_MAX_ERRORS=100         → Job parameter (default in workflow JSON)
   ```

### Phase 2: Notebooks (Week 2)

4. **Deploy notebooks** to Databricks workspace
   - Import `databricks/notebooks/parallel_loader.py` → `/Repos/data-engineering/databricks/notebooks/parallel_loader`
   - Import `databricks/notebooks/cdc_processor.py` → `/Repos/data-engineering/databricks/notebooks/cdc_processor`

5. **Unit test notebooks** with sample data
   - Use `data/sample/` files from this repo as test inputs
   - Run parallel_loader with `write_mode=overwrite` against bronze tables
   - Run cdc_processor with test insert/update/delete scenarios

### Phase 3: Workflows (Week 3)

6. **Deploy Databricks Workflows** via Jobs API or Asset Bundles
   - `databricks/workflows/daily_orders_workflow.json` → daily schedule
   - `databricks/workflows/customer_cdc_workflow.json` → every-4-hours schedule
   - **Start with `pause_status: PAUSED`** for validation

7. **Parallel run validation** (2 weeks)
   - Run Ab Initio and Databricks pipelines in parallel
   - Compare output record counts and row-level hashes
   - Validate SLA compliance on Databricks side

### Phase 4: Monitoring (Week 4)

8. **Deploy SQL alerts** — Create Databricks SQL alert from `job_failure_alert.sql`
   - Schedule: every 5 minutes
   - Destination: Slack channel + email

9. **Deploy SLA dashboard** — Create Databricks SQL Dashboard from `sla_compliance_dashboard.sql`
   - Add visualizations: compliance table, trend chart, duration heatmap

### Phase 5: Cutover (Week 5)

10. **Unpause Databricks workflows** — Set `pause_status: UNPAUSED`
11. **Disable AutoSys jobs** — Pause JOB_DAILY_ORDERS_LOAD and JOB_CUSTOMER_CDC
12. **Decommission Ab Initio** — After 2-week soak period with no issues

---

## 6. Risks and Mitigations

### 6.1 High Risk: Packed/Zoned Decimal Handling

**Risk:** `packed_account.dml` uses mainframe binary formats (`packed_decimal`, `zoned_decimal`) that are NOT text-readable. Raw binary files cannot be loaded with `spark.read.csv()`.

**Mitigation:**
- Implement custom UDF for packed decimal decoding (BCD format: 2 digits/byte, sign nibble)
- Implement custom UDF for zoned decimal decoding (EBCDIC: zone nibble + digit nibble per byte)
- Validate against known test values before production cutover
- If source system can export as text/CSV, prefer that over binary format

```python
# Example packed decimal decoder for Spark UDF
import struct
def decode_packed_decimal(raw_bytes, precision, scale):
    # Each byte = 2 digits, last nibble = sign (0xC=+, 0xD=-)
    digits = []
    for b in raw_bytes[:-1]:
        digits.append(b >> 4)
        digits.append(b & 0x0F)
    digits.append(raw_bytes[-1] >> 4)
    sign = -1 if (raw_bytes[-1] & 0x0F) == 0x0D else 1
    value = int(''.join(str(d) for d in digits))
    return sign * value / (10 ** scale)
```

### 6.2 Medium Risk: Partition Strategy Differences

**Risk:** Ab Initio uses explicit record-range partitions (start_record → end_record), while Spark uses hash/range partitioning. Different partitioning may cause data skew or different output file layouts.

**Mitigation:**
- Use `repartition(N)` to approximate Ab Initio partition count
- Monitor partition sizes via `spark.sql("DESCRIBE DETAIL table").select("numFiles")`
- Run `OPTIMIZE` after loads to compact small files
- For skewed keys, use `repartition(N, col("key"))` for hash-based distribution

### 6.3 Medium Risk: CDC Semantics Mismatch

**Risk:** Ab Initio CDC uses full-snapshot comparison (hash of ALL non-key columns). Delta MERGE uses explicit column matching. Subtle differences in NULL handling or floating-point precision could produce different change sets.

**Mitigation:**
- Run parallel validation: Ab Initio CDC output vs Delta MERGE output
- Compare row counts for inserts/updates/deletes over 2-week parallel period
- Use `compare_columns` parameter to explicitly match Ab Initio `HASH_COLUMNS` PSET
- Enable Delta Change Data Feed (`delta.enableChangeDataFeed = true`) for downstream audit

### 6.4 Low Risk: Schedule Timing Drift

**Risk:** AutoSys job timing may differ slightly from Databricks cron due to cluster startup overhead (1-3 minutes) and different timezone handling.

**Mitigation:**
- Set Databricks schedule 5 minutes earlier than Ab Initio to account for cluster spin-up
- Use job clusters (not all-purpose) for predictable startup times
- Monitor via SLA dashboard during parallel run period

### 6.5 Low Risk: Error Handling Granularity

**Risk:** Ab Initio `set -e` aborts the entire pipeline on any failure. Databricks Workflows allow per-task retry and conditional execution, which is more granular but requires explicit configuration.

**Mitigation:**
- Configure `max_retries: 1` on critical tasks (extract, CDC)
- Set `retry_on_timeout: true` for transient failures
- Use `depends_on` to ensure downstream tasks don't run after upstream failure
- Email/Slack notifications configured on all workflows

### 6.6 Low Risk: PSET Environment Resolution

**Risk:** Ab Initio PSETs support `define` + environment variable expansion (`$VAR`). Databricks job parameters are static strings — no dynamic expansion.

**Mitigation:**
- Use Databricks secrets for sensitive values (`{{secrets/scope/key}}`)
- Use workflow parameter references (`{{job.parameters.name}}`) for runtime values
- For truly dynamic values (e.g., `BATCH_DATE=$(date)`), compute in notebook code

---

## 7. PSET → Databricks Parameter Mapping

### orders_pipeline.pset → daily_orders_workflow.json

| PSET Parameter | Value | Databricks Parameter | Notes |
|---|---|---|---|
| `SOURCE_PATH` | `/data/dev/raw/orders` | `source_path` = `s3://data-lake-raw/orders/` | Local path → cloud storage |
| `TARGET_TABLE` | `DEV.STAGING.ORDERS` | `target_table` = `catalog.silver.orders` | Teradata target → Delta table |
| `PARTITION_COUNT` | `4` | `partition_count` = `4` | Direct mapping |
| `BATCH_SIZE` | `50000` | N/A | Spark manages batch sizes automatically |
| `LOG_LEVEL` | `DEBUG` | Spark log4j config | Set in cluster spark_conf |
| `RECORD_SOURCE` | `ORDER_SYSTEM_DEV` | N/A | Tracked via `_source_file` column |
| `MAX_ERRORS` | `100` | `max_errors` = `100` | Direct mapping |
| `CHECKPOINT_DIR` | `/tmp/abinitio/checkpoints/orders` | N/A | Delta ACID replaces checkpoints |

### customer_cdc.pset → customer_cdc_workflow.json

| PSET Parameter | Value | Databricks Parameter | Notes |
|---|---|---|---|
| `SOURCE_PATH` | `/data/raw/customer` | `source_path` = `s3://data-lake-raw/customer/` | Local → cloud |
| `TARGET_TABLE` | `STAGING.CUSTOMER_MASTER` | `target_table` = `catalog.silver.customer_master` | Direct mapping |
| `KEY_COLUMNS` | `customer_id` | `key_columns` = `customer_id` | MERGE join condition |
| `HASH_COLUMNS` | `customer_id,name,...` | `compare_columns` | Optional — MERGE handles change detection |
| `PARTITION_COUNT` | `8` | `partition_count` = `8` | Direct mapping |
| `BATCH_SIZE` | `100000` | N/A | Spark manages automatically |
| `MAX_ERRORS` | `50` | `max_errors` = `50` | Direct mapping |
| `AUDIT_TABLE` | `AUDIT.CUSTOMER_CHANGES` | `audit_table` = `catalog.audit.customer_changes` | Direct mapping |
| `RETENTION_DAYS` | `90` | Delta table property `delta.logRetentionDuration` = `90 days` | Set via TBLPROPERTIES |

---

## 8. Rollback Plan

If the Databricks migration fails during cutover:

1. **Re-enable AutoSys jobs** — Unpause JOB_DAILY_ORDERS_LOAD and JOB_CUSTOMER_CDC
2. **Pause Databricks workflows** — Set `pause_status: PAUSED`
3. **Verify Ab Initio pipeline** — Run one manual cycle and confirm success
4. **Root cause analysis** — Investigate Databricks failure using:
   - Workflow run logs (Spark UI)
   - `system.lakeflow.job_run_timeline` for error details
   - Delta table history (`DESCRIBE HISTORY table`)
5. **Fix and retry** — Address root cause, update notebooks/workflows, restart parallel validation

---

## 9. Post-Migration Checklist

- [ ] All 7 Delta Lake tables created in Unity Catalog (bronze layer)
- [ ] parallel_loader notebook tested with sample data
- [ ] cdc_processor notebook tested with insert/update/delete scenarios
- [ ] daily_orders_workflow deployed and running on schedule
- [ ] customer_cdc_workflow deployed and running every 4 hours
- [ ] Job failure SQL alert active and tested
- [ ] SLA compliance dashboard deployed with visualizations
- [ ] 2-week parallel run completed with matching record counts
- [ ] AutoSys jobs disabled
- [ ] Ab Initio servers decommissioned (after 2-week soak)
