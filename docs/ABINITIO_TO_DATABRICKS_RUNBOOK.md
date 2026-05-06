# Ab Initio → Databricks Lakehouse Migration Runbook

## Table of Contents

1. [Concept Mapping](#1-concept-mapping)
2. [Type Mapping (DML → Spark/Delta)](#2-type-mapping-dml--sparkdelta)
3. [Artifact Inventory](#3-artifact-inventory)
4. [Execution Order](#4-execution-order)
5. [PSET → Job Parameter Migration](#5-pset--job-parameter-migration)
6. [Risks and Mitigations](#6-risks-and-mitigations)
7. [Rollback Plan](#7-rollback-plan)
8. [Validation Checklist](#8-validation-checklist)

---

## 1. Concept Mapping

| Ab Initio Concept | Legacy Artifact | Databricks Equivalent | Migrated Artifact |
|---|---|---|---|
| **Graph (.mp)** | `graphs/parallel_loader.py` | PySpark Notebook | `databricks/notebooks/parallel_loader.py` |
| **Graph (.mp)** | `graphs/cdc_processor.py` | PySpark Notebook + Delta MERGE | `databricks/notebooks/cdc_processor.py` |
| **DML Record Layout** | `dml/*.dml` (8 files) | PySpark StructType + Delta DDL | `databricks/schemas/*.py` (8 files) |
| **PSET (Parameter Set)** | `psets/pset_templates/*.pset` | Databricks Job Parameters + Widgets | `databricks/workflows/*.json` `parameters` section |
| **PSET env override** | `psets/{dev,uat,prod}/` | Unity Catalog namespacing + Secrets | Terraform / Asset Bundles (future) |
| **Partition (m_partition)** | `air_run -partition=N` | `df.repartition(N)` | Built into notebooks |
| **CDC (hash compare)** | `CDCProcessor._row_hash()` | `Delta MERGE + md5(concat_ws(...))` | `databricks/notebooks/cdc_processor.py` |
| **AutoSys job** | `scripts/run_daily_orders.ksh` | Databricks Workflow (Jobs API) | `databricks/workflows/daily_orders_workflow.json` |
| **AutoSys job** | `scripts/run_customer_cdc.ksh` | Databricks Workflow (Jobs API) | `databricks/workflows/customer_cdc_workflow.json` |
| **setenv.ksh** | `scripts/setenv.ksh` | Job cluster config + Spark conf | Embedded in workflow JSON `job_clusters` |
| **air sandbox run** | Shell subprocess calls | Notebook task in workflow | Workflow task `notebook_task` |
| **Co>Operating System** | Ab Initio runtime engine | Spark cluster | Databricks job cluster |
| **AutoSys monitoring** | `monitoring/job_monitor.py` | Databricks SQL Alert | `databricks/monitoring/job_failure_alert.sql` |
| **SLA tracking** | `monitoring/sla_tracker.py` | Databricks SQL Dashboard | `databricks/monitoring/sla_compliance_dashboard.sql` |
| **Slack alerts** | `JobMonitor._send_alert()` | Databricks Alert notifications | Alert destination config |
| **ServiceNow/UrbanCode** | `deployment/deploy_manager.py` | Databricks Asset Bundles + CI/CD | Future phase |
| **include directive** | `include "common_address.dml"` | Python import | `from databricks.schemas.common_address import address_schema` |
| **void (padding)** | `void(",") padding1` | (omitted) | Padding fields dropped from schema |
| **Conditional record** | `if (txn_type == 2) record...` | Nullable StructType | Nullable struct in `transaction_detail.py` |

---

## 2. Type Mapping (DML → Spark/Delta)

| Ab Initio DML Type | Example | PySpark Type | Delta SQL Type | Notes |
|---|---|---|---|---|
| `decimal(",")` | `decimal(",") customer_id` | `LongType` | `BIGINT` | No precision → integer key |
| `decimal("P.S", delim)` | `decimal("8.2", "\|") balance` | `DecimalType(P, S)` | `DECIMAL(P,S)` | Explicit precision/scale preserved |
| `string(delim)` | `string(",") name` | `StringType` | `STRING` | Variable-length string |
| `string(N)` | `string(20) account_name` | `StringType` | `STRING` | Fixed-length → variable in Spark |
| `date("fmt")` | `date("YYYY-MM-DD")` | `DateType` | `DATE` | Format handled at read time |
| `datetime("fmt")` | `datetime("YYYY-MM-DD HH24:MI:SS")` | `TimestampType` | `TIMESTAMP` | Format handled at read time |
| `packed_decimal(N)` | `packed_decimal(5) account_num` | `DecimalType(N, 0)` | `DECIMAL(N,0)` | Requires byte-level decoding first |
| `packed_decimal("P.S")` | `packed_decimal("7.2") balance` | `DecimalType(P, S)` | `DECIMAL(P,S)` | Requires byte-level decoding first |
| `zoned_decimal(N)` | `zoned_decimal(4) status_code` | `DecimalType(N, 0)` | `DECIMAL(N,0)` | EBCDIC → ASCII conversion needed |
| `void(delim)` | `void(",") padding1` | *(omitted)* | *(omitted)* | Padding fields carry no data |
| `type T = record...end` | `type address_t = record...` | `StructType` (module) | `STRUCT<...>` | Reusable embedded type |
| `record...end name` | `record...end merchant_info` | `StructType` (nested) | `STRUCT<...>` | Nested struct field |
| `record[N]...end name` | `record[item_count]...end` | `ArrayType(StructType)` | `ARRAY<STRUCT<...>>` | Variable-length repeating group |
| `field[N]` | `string(",")[item_count]` | `ArrayType(ElementType)` | `ARRAY<type>` | Variable-length array |
| `null("val")` | `string(",", null("UNKNOWN"))` | Default value | `DEFAULT 'val'` | NULL sentinel → SQL default |

---

## 3. Artifact Inventory

### Schemas (`databricks/schemas/`)

| File | Source DML | Key Features |
|---|---|---|
| `customer.py` | `dml/customer.dml` | Simple flat record |
| `account_balance.py` | `dml/account_balance.dml` | Decimal precision, DateType |
| `order_items.py` | `dml/order_items.dml` | Variable-length arrays (`ArrayType`) |
| `transaction_detail.py` | `dml/transaction_detail.dml` | Nested structs, arrays, conditional record |
| `packed_account.py` | `dml/packed_account.dml` | Packed/zoned decimal (mainframe) |
| `account_status.py` | `dml/account_status.dml` | Void padding field removal |
| `common_address.py` | `dml/common_address.dml` | Reusable StructType (shared type) |
| `customer_address.py` | `dml/customer_address.dml` | Include directive → Python import |

### Notebooks (`databricks/notebooks/`)

| File | Source Graph | Key Features |
|---|---|---|
| `parallel_loader.py` | `graphs/parallel_loader.py` | Partition-based ingestion, DQ checks, checkpoint |
| `cdc_processor.py` | `graphs/cdc_processor.py` | Delta MERGE, CDF, audit trail |

### Workflows (`databricks/workflows/`)

| File | Source Script | Schedule |
|---|---|---|
| `daily_orders_workflow.json` | `scripts/run_daily_orders.ksh` | Daily at 02:00 UTC |
| `customer_cdc_workflow.json` | `scripts/run_customer_cdc.ksh` | Every 4 hours |
| `pset_parameter_mapping.md` | `psets/pset_templates/*.pset` | Documentation |

### Monitoring (`databricks/monitoring/`)

| File | Source | Purpose |
|---|---|---|
| `job_failure_alert.sql` | `monitoring/job_monitor.py` | SQL Alert for failed/timed-out jobs |
| `sla_compliance_dashboard.sql` | `monitoring/sla_tracker.py` | 7-day + 90-day SLA compliance queries |

---

## 4. Execution Order

The migration should be executed in the following sequence:

### Phase 1: Schema Foundation

1. **Create Unity Catalog resources**
   - Create catalog: `lakehouse`
   - Create schemas: `bronze`, `silver`, `gold`, `staging`, `audit`
2. **Deploy Delta tables** using DDL from `databricks/schemas/*.py`
   - Execute each `*_DDL` string against the target catalog
   - Verify table creation with `DESCRIBE TABLE EXTENDED`

### Phase 2: Notebook Deployment

3. **Import notebooks** into Databricks workspace
   - Upload `databricks/notebooks/parallel_loader.py`
   - Upload `databricks/notebooks/cdc_processor.py`
   - Verify widget parameters render correctly in the notebook UI

### Phase 3: Data Migration (Backfill)

4. **Initial full load** — run `parallel_loader` notebook for each table
   - Use production source paths
   - Verify record counts match source system
5. **Validate schemas** — compare Delta table schemas against DML definitions

### Phase 4: CDC Cutover

6. **Deploy CDC notebook** and run initial sync
   - First run creates the target table (full load)
   - Subsequent runs perform incremental MERGE
7. **Validate CDC** — insert/update/delete test records in source, verify Delta

### Phase 5: Workflow Activation

8. **Create Databricks Workflows** using the JSON definitions
   - `databricks/workflows/daily_orders_workflow.json`
   - `databricks/workflows/customer_cdc_workflow.json`
9. **Parallel run period** — run both Ab Initio and Databricks pipelines
   simultaneously for 1-2 weeks, comparing outputs

### Phase 6: Monitoring Cutover

10. **Deploy SQL alerts** — `databricks/monitoring/job_failure_alert.sql`
11. **Deploy SLA dashboard** — `databricks/monitoring/sla_compliance_dashboard.sql`
12. **Configure alert destinations** — email + Slack webhook

### Phase 7: Decommission

13. **Disable AutoSys jobs** — pause `JOB_DAILY_ORDERS_LOAD` and `JOB_CUSTOMER_CDC`
14. **Archive Ab Initio graphs** — mark legacy code as deprecated
15. **Update runbooks** — point operations team to Databricks dashboards

---

## 5. PSET → Job Parameter Migration

See `databricks/workflows/pset_parameter_mapping.md` for the complete
parameter-by-parameter mapping.

**Key strategy changes:**

| Ab Initio Approach | Databricks Approach |
|---|---|
| File-based PSET per environment | Job parameters + Unity Catalog namespacing |
| `define SOURCE_PATH /data/raw/...` | Widget default + job parameter override |
| PSET directory override (`psets/prod/`) | Terraform/Asset Bundle per environment |
| Connection strings in PSET | Databricks Secret Scopes |
| `CHECKPOINT_DIR` local filesystem | Cloud storage checkpoint path |

---

## 6. Risks and Mitigations

### R1: Packed Decimal / Zoned Decimal Handling

| | |
|---|---|
| **Risk** | `packed_decimal` and `zoned_decimal` types in `packed_account.dml` represent mainframe BCD-encoded binary data. Spark cannot natively read packed/zoned bytes. |
| **Impact** | Data corruption if raw bytes are loaded without decoding. |
| **Mitigation** | Implement a pre-processing step (custom UDF or upstream ETL) that decodes packed/zoned bytes to standard numeric values before writing to Delta Lake. The schema (`DecimalType`) is correct for the *decoded* values. Test with known test vectors from the mainframe team. |
| **Severity** | **High** |

### R2: Partition Strategy Differences

| | |
|---|---|
| **Risk** | Ab Initio uses record-range-based partitioning (`-start_record=N -end_record=M`), while Spark partitions by data distribution (`repartition(N)`). |
| **Impact** | Different partition boundaries may cause data skew or ordering differences. |
| **Mitigation** | Use `repartition(N, col)` with a well-distributed key column to achieve balanced partitions. Monitor partition sizes via Spark UI. The `partition_count` parameter is preserved for parity. |
| **Severity** | **Medium** |

### R3: CDC Semantics — Hash-Based vs. Delta MERGE

| | |
|---|---|
| **Risk** | Ab Initio CDC compares two full snapshots (previous vs. current). Delta MERGE operates on the live table state. Edge cases may differ (e.g., duplicate keys, late-arriving records). |
| **Impact** | CDC counts may not match exactly during parallel run period. |
| **Mitigation** | Run both systems in parallel for 1-2 weeks. Compare INSERT/UPDATE/DELETE counts daily. The `audit_table` captures Databricks-side metrics for comparison. |
| **Severity** | **Medium** |

### R4: Variable-Length Array Handling

| | |
|---|---|
| **Risk** | Ab Initio `field[count]` arrays are preceded by a count field. In Spark `ArrayType`, the length is implicit. The `item_count` field is redundant but retained for compatibility. |
| **Impact** | Minor schema mismatch if downstream consumers depend on `item_count`. |
| **Mitigation** | Retain `item_count` field during migration. Add a data quality check: `assert array_size(item_names) == item_count`. Remove in a future cleanup phase once consumers are updated. |
| **Severity** | **Low** |

### R5: Conditional Record Fields

| | |
|---|---|
| **Risk** | Ab Initio `if (txn_type == 2) record...end` conditionally includes fields. In Delta Lake, the `refund_details` struct is always present but nullable. |
| **Impact** | Storage overhead (null struct columns); downstream consumers must check for NULL instead of field absence. |
| **Mitigation** | Document that `refund_details IS NULL` when `txn_type != 2`. This is idiomatic for Spark/Delta and poses no functional risk. |
| **Severity** | **Low** |

### R6: SLA Timing Differences

| | |
|---|---|
| **Risk** | Databricks cluster startup time (2-5 min) adds latency vs. always-running Ab Initio Co>Operating System. |
| **Impact** | Tight SLA windows (e.g., 30 min for staging load) may be breached during migration. |
| **Mitigation** | Use Databricks Pools (pre-warmed instances) to reduce cluster startup time. Adjust SLA windows by 5 minutes during transition. Monitor via the SLA compliance dashboard. |
| **Severity** | **Medium** |

### R7: Void Field Removal

| | |
|---|---|
| **Risk** | `void` padding fields in `account_status.dml` are omitted from the Delta schema. If any downstream process references field positions (ordinal-based access), it will break. |
| **Impact** | Downstream ETL or reporting that uses column index instead of name. |
| **Mitigation** | Verify all downstream consumers use column names, not ordinal positions. The DML→schema mapping documentation explicitly notes the removal. |
| **Severity** | **Low** |

---

## 7. Rollback Plan

If critical issues are discovered during migration:

1. **Pause Databricks Workflows** — disable scheduled triggers
2. **Re-enable AutoSys jobs** — unpause `JOB_DAILY_ORDERS_LOAD` and `JOB_CUSTOMER_CDC`
3. **Preserve Delta tables** — do not drop migrated tables (useful for debugging)
4. **Time travel** — use Delta Lake time travel (`VERSION AS OF`) to inspect pre-migration state

---

## 8. Validation Checklist

- [ ] All 8 Delta tables created with correct schemas
- [ ] `parallel_loader` notebook runs successfully with sample data
- [ ] `cdc_processor` notebook correctly detects INSERTs, UPDATEs, DELETEs
- [ ] Daily orders workflow executes all 4 tasks in dependency order
- [ ] Customer CDC workflow runs every 4 hours
- [ ] Job failure alert fires for simulated failures
- [ ] SLA dashboard shows compliance data for the parallel run period
- [ ] Record counts match between Ab Initio and Databricks for each table
- [ ] Packed decimal test vectors decode correctly
- [ ] Variable-length arrays have correct element counts
- [ ] Audit trail records are written for every CDC run
- [ ] Alert notifications reach Slack and email destinations
