# Ab Initio to Databricks Lakehouse — Migration Runbook

## 1. Concept Mapping

| # | Ab Initio Concept | Source File(s) | Databricks Equivalent | Target File(s) | Notes |
|---|---|---|---|---|---|
| 1 | **Graph (.mp)** | `graphs/parallel_loader.py`, `graphs/cdc_processor.py` | **Databricks Notebook** (PySpark) | `databricks/notebooks/parallel_loader.py`, `databricks/notebooks/cdc_processor.py` | Each graph maps to one notebook. `air_run` CLI replaced by Spark session. |
| 2 | **DML Record Layout** | `dml/*.dml` (8 files) | **PySpark StructType + Delta DDL** | `databricks/schemas/*.py` (8 files + `__init__.py`) | One Python module per DML file. Exports both StructType and CREATE TABLE DDL. |
| 3 | **PSET (Parameter Set)** | `psets/pset_templates/*.pset` | **Databricks Job Parameters + Widgets** | `databricks/workflows/*.json` (parameters section), notebook `dbutils.widgets` | PSET `define KEY value` maps to workflow `parameters[].default`. |
| 4 | **Partition (m_partition)** | `PartitionManager`, `-partition N` flag | **Spark `repartition(N)`** | Notebook `repartition()` calls | Ab Initio spawns N OS processes; Spark distributes N partitions across executors. |
| 5 | **KornShell orchestration** | `scripts/run_daily_orders.ksh`, `scripts/run_customer_cdc.ksh` | **Databricks Workflow JSON** | `databricks/workflows/daily_orders_workflow.json`, `databricks/workflows/customer_cdc_workflow.json` | Multi-phase KSH pipelines map to task DAGs with `depends_on`. |
| 6 | **setenv.ksh** | `scripts/setenv.ksh` | **Cluster spark_conf + job parameters** | Workflow JSON `job_clusters[].new_cluster.spark_conf` | Environment variables (AI_HOME, AI_MAX_ERRORS) become job parameters or cluster config. |
| 7 | **AutoSys scheduler** | Cron triggers (daily, 4-hourly) | **Databricks Workflow schedule** | `schedule.quartz_cron_expression` in workflow JSON | AutoSys `JOB_DAILY_ORDERS_LOAD` → `0 0 2 * * ?`. `JOB_CUSTOMER_CDC` → `0 0 0/4 * * ?`. |
| 8 | **CDC (hash-based)** | `CDCProcessor` (MD5 row hash) | **Delta Lake MERGE + Change Data Feed** | `databricks/notebooks/cdc_processor.py` | Hash comparison preserved for backward compat; MERGE handles apply atomically. |
| 9 | **air sandbox run** | `air sandbox run graph.mp -pset ... -param ...` | **Notebook execution via Workflow** | Workflow tasks invoke notebooks with `base_parameters` | Parameters passed via `{{job.parameters.*}}` template syntax. |
| 10 | **AutoSys monitoring + Slack** | `monitoring/job_monitor.py` | **Databricks SQL Alerts + notification destinations** | `databricks/monitoring/job_failure_alert.sql` | Polling loop replaced by scheduled SQL query with row-count trigger. |
| 11 | **SLA tracker (JSON state)** | `monitoring/sla_tracker.py` | **Databricks SQL Dashboard** | `databricks/monitoring/sla_compliance_dashboard.sql` | JSON file state replaced by Delta table `lakehouse.ops.sla_definitions` + SQL aggregation. |
| 12 | **Co>Operating System** | Ab Initio runtime engine | **Spark cluster** | Workflow `job_clusters` | Cluster lifecycle managed by Databricks; no daemon process to maintain. |
| 13 | **Ab Initio `void` fields** | `dml/account_status.dml` | **(omitted)** | `databricks/schemas/account_status.py` | Padding fields dropped. Documented in schema comments. |
| 14 | **packed_decimal / zoned_decimal** | `dml/packed_account.dml` | **DecimalType(p,s)** | `databricks/schemas/packed_account.py` | Binary decoding required at ingestion time (see Risks section). |
| 15 | **Nested record / conditional record** | `dml/transaction_detail.dml` | **StructType / ArrayType / nullable StructType** | `databricks/schemas/transaction_detail.py` | `if(cond) record` → always-present nullable struct. |
| 16 | **DML `type` + `include`** | `dml/common_address.dml`, `dml/customer_address.dml` | **Shared StructType import** | `databricks/schemas/common_address.py` imported by `customer_address.py` | Python import replaces Ab Initio `include`. |

---

## 2. Migration Artifacts Summary

### 2.1 Delta Lake Schemas (`databricks/schemas/`)

| Source DML | Target Module | Schema Variable | DDL Variable | Key Decisions |
|---|---|---|---|---|
| `customer.dml` | `customer.py` | `customer_schema` | `CUSTOMER_DDL` | `decimal` (no precision) → `LongType`/`BIGINT` |
| `account_balance.dml` | `account_balance.py` | `account_balance_schema` | `ACCOUNT_BALANCE_DDL` | `decimal("8.2")` → `DecimalType(8,2)` |
| `order_items.dml` | `order_items.py` | `order_items_schema` | `ORDER_ITEMS_DDL` | Variable-length arrays → `ArrayType` |
| `transaction_detail.dml` | `transaction_detail.py` | `transaction_detail_schema` | `TRANSACTION_DETAIL_DDL` | Nested/conditional records → nested `StructType` |
| `packed_account.dml` | `packed_account.py` | `packed_account_schema` | `PACKED_ACCOUNT_DDL` | `packed_decimal`/`zoned_decimal` → `DecimalType` |
| `account_status.dml` | `account_status.py` | `account_status_schema` | `ACCOUNT_STATUS_DDL` | `void` padding fields omitted |
| `common_address.dml` | `common_address.py` | `address_type_schema` | `COMMON_ADDRESS_DDL` | Reusable type definition (no standalone table) |
| `customer_address.dml` | `customer_address.py` | `customer_address_schema` | `CUSTOMER_ADDRESS_DDL` | `include` → Python import of shared type |

### 2.2 PySpark Notebooks (`databricks/notebooks/`)

| Notebook | Replaces | Key Capabilities |
|---|---|---|
| `parallel_loader.py` | `graphs/parallel_loader.py` (PartitionManager + ParallelLoader) | Partition-based reads, error thresholds, reject path, ingestion metadata columns |
| `cdc_processor.py` | `graphs/cdc_processor.py` (CDCProcessor) | Delta MERGE with hash-based change detection, Change Data Feed metrics, audit trail, retention via VACUUM |

### 2.3 Databricks Workflows (`databricks/workflows/`)

| Workflow | Replaces | Schedule | Tasks |
|---|---|---|---|
| `daily_orders_workflow.json` | `scripts/run_daily_orders.ksh` | Daily at 02:00 UTC | extract_orders → cdc_orders → load_staging → load_production |
| `customer_cdc_workflow.json` | `scripts/run_customer_cdc.ksh` | Every 4 hours | run_customer_cdc (single task, all steps in notebook) |

### 2.4 Monitoring (`databricks/monitoring/`)

| Artifact | Replaces | Type |
|---|---|---|
| `job_failure_alert.sql` | `monitoring/job_monitor.py` (AutoSys polling + Slack) | Databricks SQL Alert (5-min schedule) |
| `sla_compliance_dashboard.sql` | `monitoring/sla_tracker.py` (JSON state file) | Databricks SQL Dashboard + SLA breach alert |

---

## 3. Type Mapping Reference

| Ab Initio DML Type | Example | Spark Type | Delta Lake SQL | Notes |
|---|---|---|---|---|
| `decimal` (no precision) | `decimal(",") customer_id` | `LongType` | `BIGINT` | Unqualified decimals treated as integer keys |
| `decimal("p.s")` | `decimal("8.2") balance` | `DecimalType(p,s)` | `DECIMAL(8,2)` | Precision and scale preserved |
| `string` | `string(",") name` | `StringType` | `STRING` | Variable-length; delimiter is metadata only |
| `string(N)` | `string(20) account_name` | `StringType` | `STRING` | Fixed-width → trim trailing spaces at ingestion |
| `date("fmt")` | `date("YYYY-MM-DD")` | `DateType` | `DATE` | Format used during CSV parsing |
| `datetime("fmt")` | `datetime("YYYY-MM-DD HH24:MI:SS")` | `TimestampType` | `TIMESTAMP` | `HH24:MI:SS` → Java `HH:mm:ss` at parse time |
| `packed_decimal(N)` | `packed_decimal(5) acct_num` | `DecimalType(N,0)` | `DECIMAL(5,0)` | COMP-3 BCD — requires byte-level decode UDF |
| `packed_decimal("p.s")` | `packed_decimal("7.2") balance` | `DecimalType(p,s)` | `DECIMAL(7,2)` | Same decode, explicit fractional digits |
| `zoned_decimal(N)` | `zoned_decimal(4) status_code` | `DecimalType(N,0)` | `DECIMAL(4,0)` | EBCDIC zoned — requires byte-level decode UDF |
| `void` | `void(",") padding1` | *(omitted)* | *(omitted)* | Padding/filler — dropped from target schema |
| `type T = record` | `type address_t = record` | `StructType` (reusable) | `STRUCT<...>` | Shared type imported across modules |
| `record ... end name` | nested `merchant_info` | `StructType` (column) | `STRUCT<...>` | Nested struct column |
| `record[N] ... end name` | `record[item_count] line_items` | `ArrayType(StructType)` | `ARRAY<STRUCT<...>>` | Variable-length array of structs |
| `if (cond) record` | `if (txn_type == 2) refund_details` | nullable `StructType` | nullable `STRUCT<...>` | Always present; NULL when condition false |
| `type[count]` | `string(",")[item_count]` | `ArrayType(StringType)` | `ARRAY<STRING>` | Variable-length array |
| `null("sentinel")` | `string(",", null(""))` | `StringType` (nullable) | `STRING` | Sentinel value mapped to SQL NULL at ingestion |

---

## 4. Execution Order

### Phase 1: Foundation (Week 1)

1. **Create Unity Catalog namespace**
   ```sql
   CREATE CATALOG IF NOT EXISTS lakehouse;
   CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;
   CREATE SCHEMA IF NOT EXISTS lakehouse.silver;
   CREATE SCHEMA IF NOT EXISTS lakehouse.gold;
   CREATE SCHEMA IF NOT EXISTS lakehouse.ops;
   ```

2. **Deploy Delta Lake schemas** — Execute DDL from each `databricks/schemas/*.py` module.
   Run in order:
   - `common_address.py` (shared type, no DDL to execute)
   - All other schemas (no inter-table dependencies)

3. **Create operational tables**
   ```sql
   -- Pipeline run log (written by notebooks)
   CREATE TABLE IF NOT EXISTS lakehouse.ops.pipeline_run_log (
       run_id          STRING DEFAULT uuid(),
       job_name        STRING,
       overall_status  STRING,
       error           STRING,
       duration_seconds DOUBLE,
       batch_date      STRING,
       run_timestamp   TIMESTAMP DEFAULT current_timestamp(),
       records_written BIGINT,
       partition_count INT,
       inserts         BIGINT,
       updates         BIGINT,
       deletes         BIGINT
   ) USING DELTA;
   ```

4. **Deploy SLA definitions** — Run `databricks/monitoring/sla_compliance_dashboard.sql` Part 1.

### Phase 2: Parallel Ingestion (Week 2)

5. **Deploy parallel_loader notebook** → Import `databricks/notebooks/parallel_loader.py` into Databricks Repos.

6. **Deploy daily_orders_workflow** → Use Databricks Jobs API or UI to create the workflow from `databricks/workflows/daily_orders_workflow.json`.

7. **Run validation** — Execute workflow manually with a small test dataset. Compare record counts against the legacy Ab Initio output.

### Phase 3: CDC Processing (Week 3)

8. **Deploy cdc_processor notebook** → Import `databricks/notebooks/cdc_processor.py`.

9. **Deploy customer_cdc_workflow** → Create from `databricks/workflows/customer_cdc_workflow.json`.

10. **Run CDC validation** — Execute with known insert/update/delete test cases. Compare CDC metrics against the legacy CDCProcessor output.

### Phase 4: Monitoring & Cutover (Week 4)

11. **Create Databricks SQL Alerts**
    - `job_failure_alert.sql` → Schedule every 5 minutes, trigger on rows > 0.
    - `sla_compliance_dashboard.sql` Part 3 (SLA breach) → Schedule every 15 minutes.

12. **Create SLA Dashboard** — `sla_compliance_dashboard.sql` Part 2 as a dashboard widget.

13. **Parallel run period** — Run both Ab Initio and Databricks pipelines for 2 weeks. Compare outputs daily.

14. **Cutover** — Disable AutoSys jobs, unpause Databricks workflow schedules.

---

## 5. Risks and Mitigations

### 5.1 Packed/Zoned Decimal Handling

| Risk | Impact | Mitigation |
|---|---|---|
| `packed_decimal` (COMP-3) and `zoned_decimal` fields require byte-level decoding from mainframe binary format | Data corruption if decoded incorrectly; silent precision loss | **Write a PySpark UDF** that implements COMP-3 BCD decoding. Test exhaustively with known mainframe extracts. Consider using the `ebcdic-parser` library or Cobrix Spark connector for raw mainframe files. |
| EBCDIC-to-ASCII conversion | Sign nibble in packed decimals may be misinterpreted | Validate sign handling (+/-) and half-byte boundary alignment with reference test data. Document the exact byte-to-decimal conversion algorithm. |

### 5.2 Partition Strategy Differences

| Risk | Impact | Mitigation |
|---|---|---|
| Ab Initio partitions are record-range-based (start/end offsets); Spark partitions are hash/round-robin | Different record distribution per partition; performance characteristics may vary | Use `repartition(N)` for general parallelism. For range-sensitive operations, use `repartitionByRange()` with the key column. Monitor skew via Spark UI. |
| Ab Initio `m_partition` guarantees ordered partitions; Spark does not | Order-dependent operations may produce different results | Identify order-dependent logic in graphs. Add explicit `.orderBy()` before any operation that depends on record ordering. |

### 5.3 Error Handling Behavioral Differences

| Risk | Impact | Mitigation |
|---|---|---|
| Ab Initio `AI_ERROR_ACTION=ABORT` stops a single partition; Spark exception fails the entire job | More aggressive failure behavior in Spark | Use `badRecordsPath` for corrupt records and a configurable error threshold (`MAX_ERRORS` parameter) to match Ab Initio behavior. Reject records written to `/mnt/rejects/`. |
| Ab Initio checkpoint/restart resumes from last good record; Spark has no native equivalent | Long-running jobs must restart from scratch on failure | Use Delta Lake idempotent writes (`mergeSchema`). For very large loads, partition source data by date and process incrementally. Workflow retry (`max_retries: 1`) handles transient failures. |

### 5.4 Scheduling and Dependency Gaps

| Risk | Impact | Mitigation |
|---|---|---|
| AutoSys cross-job dependencies (e.g., `JOB_DAILY_ORDERS_LOAD` → downstream consumers) are not captured in this migration | Downstream jobs may break if they depend on AutoSys success signals | Inventory all AutoSys job dependencies. Recreate cross-workflow dependencies using Databricks multi-task workflows or external orchestration (Airflow). |
| PSET environment overlays (`psets/dev/`, `psets/uat/`, `psets/prod/`) not yet replicated | Production workflow may use dev defaults | Create environment-specific Databricks job overrides or use cluster-level init scripts to set environment-appropriate parameter defaults. |

### 5.5 Data Fidelity

| Risk | Impact | Mitigation |
|---|---|---|
| Ab Initio `void` fields are positional padding; dropping them changes column ordinals | Downstream consumers relying on column position (not name) will break | Communicate schema changes to all downstream teams. Provide a column-name-to-position mapping in the migration docs. Prefer column-name-based access in all new code. |
| Ab Initio `null("")` sentinel means empty string = NULL; Spark treats empty string and NULL differently | Silent data quality differences | Explicitly map sentinel values to NULL at ingestion using `F.when(F.col(c) == "", None)`. Document all sentinel-to-NULL mappings. |
| Variable-length arrays (`type[count]`) stored differently in Delta (JSON-encoded in Parquet) vs Ab Initio (inline sequential fields) | Serialization differences may affect downstream parsing | Validate array round-trip fidelity with edge cases (empty arrays, max-length arrays). |

### 5.6 Operational Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Databricks SQL Alerts have minimum 1-minute granularity; AutoSys polling was 60s | Comparable latency for alerting | Acceptable — no action needed. |
| SLA state was previously in a JSON file (portable, manual); now in Delta table (requires Databricks access) | Operational teams need Databricks SQL access | Grant `SELECT` on `lakehouse.ops.*` to the operations team. Provide a read-only dashboard URL. |
| Slack webhook URL is now configured in Databricks notification destinations, not in code | Configuration drift between environments | Store webhook URLs in Databricks Secrets. Reference secrets in notification destination config. |

---

## 6. Post-Migration Checklist

- [ ] All 8 DML schemas deployed as Delta tables
- [ ] `pipeline_run_log` and `sla_definitions` operational tables created
- [ ] `parallel_loader` notebook deployed and tested with sample data
- [ ] `cdc_processor` notebook deployed and tested with known CDC scenarios
- [ ] `daily_orders_workflow` created and executed successfully (manual run)
- [ ] `customer_cdc_workflow` created and scheduled (4-hour interval)
- [ ] Job failure SQL alert configured (5-min schedule, Slack notification)
- [ ] SLA compliance dashboard created and shared with operations team
- [ ] SLA breach alert configured (15-min schedule)
- [ ] Parallel run period completed (minimum 2 weeks)
- [ ] Record counts validated: Ab Initio output vs Databricks output
- [ ] CDC metrics validated: insert/update/delete counts match
- [ ] Packed decimal decoding UDF tested against reference mainframe data
- [ ] AutoSys jobs disabled
- [ ] Databricks workflow schedules unpaused
- [ ] Stakeholder sign-off obtained
