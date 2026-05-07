# Ab Initio ETL Framework — Knowledge Base

## 1. Architecture Overview

### System Purpose

Enterprise Ab Initio ETL framework managing large-scale data integration between **Oracle** (source systems) and **Teradata** (data warehouse target). The framework orchestrates batch data pipelines via **AutoSys/Control-M** job scheduling, with deployment gated through **ServiceNow** change management and **UrbanCode Deploy**.

This codebase is the **migration source** for a planned transition to Databricks Lakehouse (PySpark / Delta Lake / Unity Catalog).

### Technology Stack

| Layer | Technology |
|---|---|
| ETL Engine | Ab Initio Co>Operating System |
| Job Orchestration | AutoSys / Control-M |
| Source Database | Oracle (ORACLE_PROD) |
| Target Data Warehouse | Teradata (TERADATA_DW) |
| Staging Database | Oracle (ORACLE_STG) |
| Deployment | UrbanCode Deploy + ServiceNow (change management) |
| Languages | Python 3.x (utilities/orchestration), KornShell (batch scripts), Ab Initio DML (schemas) |
| Monitoring | AutoSys REST API + Slack webhooks |
| Python Dependencies | requests 2.32, pydantic 2.7, pandas 2.2, pyyaml 6.0, python-dotenv 1.0, croniter 2.0, pytest 8.2 |

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     AutoSys / Control-M                             │
│                  (Job Scheduling & Triggering)                      │
└──────────┬──────────────────────────────────┬───────────────────────┘
           │                                  │
           ▼                                  ▼
┌─────────────────────┐           ┌─────────────────────────┐
│  run_daily_orders.ksh│           │  run_customer_cdc.ksh   │
│  (Daily batch)       │           │  (Every 4 hours)        │
└──────────┬──────────┘           └──────────┬──────────────┘
           │                                  │
           ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  setenv.ksh (Environment Bootstrap)                  │
│   AI_HOME, AB_HOME, PATH, DB connections, partition defaults        │
└──────────┬──────────────────────────────────┬───────────────────────┘
           │                                  │
           ▼                                  ▼
┌─────────────────────┐           ┌─────────────────────────┐
│  ParallelLoader      │           │  CDCProcessor            │
│  (Python orchestrator│           │  (Hash-based CDC)        │
│   for air_run)       │           │                          │
└──────────┬──────────┘           └──────────┬──────────────┘
           │                                  │
           ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│               Ab Initio Co>Operating System                         │
│         air_run / air sandbox run (Graph Execution)                 │
│         air_deploy (Environment Promotion)                          │
└──────────┬──────────────────────────────────┬───────────────────────┘
           │                                  │
           ▼                                  ▼
┌─────────────────────┐           ┌─────────────────────────┐
│  Oracle (Source)     │           │  Teradata (DW Target)   │
│  ORACLE_PROD         │           │  TERADATA_DW            │
│  ORACLE_STG          │           │                          │
└─────────────────────┘           └─────────────────────────┘
```

### Module Inventory

| Module | Path | Purpose |
|---|---|---|
| Graph Orchestration | `graphs/` | Python wrappers for Ab Initio graph execution (parallel loader, CDC) |
| DML Schemas | `dml/` | Ab Initio record layout definitions (8 schema files) |
| PSET Management | `psets/` | Environment-aware parameter set loading, rendering, diffing |
| Shell Orchestration | `scripts/` | KornShell entrypoints triggered by AutoSys for batch pipelines |
| Job Monitoring | `monitoring/` | AutoSys API polling, SLA compliance tracking, Slack alerting |
| Deployment Automation | `deployment/` | ServiceNow CR validation, UrbanCode graph deployment |
| DML Utilities | `utils/` | DML-to-SQL parser for schema extraction and DDL generation |
| Sample Data | `data/sample/` | Pipe- and comma-delimited test datasets (3 files) |
| Tests | `tests/` | pytest suite covering PSET manager, DML parser, CDC processor |

---

## 2. Data Model — DML Record Layouts

Ab Initio DML files define the **record layouts** (schemas) for all data flowing through ETL graphs. Each file specifies field types, delimiters, and structural metadata.

### Schema Inventory

| DML File | Record Description | Delimiter | Key Features |
|---|---|---|---|
| `customer.dml` | Customer master record | Comma (`,`) | 4 fields: customer_id, first_name, last_name, email |
| `account_balance.dml` | Account balance with dates | Pipe (`\|`) | Typed decimal(`8.2`), date(`YYYY-MM-DD`), semicolon-terminated date field |
| `order_items.dml` | Variable-length order line items | Comma (`,`) | **Variable-length arrays**: `item_names[item_count]`, `item_quantities[item_count]` |
| `transaction_detail.dml` | Complex nested transactions | Comma (`,`) | **Nested records** (merchant_info), **variable-length nested arrays** (line_items), **conditional record** (refund_details when txn_type==2), null handling |
| `packed_account.dml` | Mainframe packed/zoned decimal | Fixed-width | `packed_decimal(5)`, `packed_decimal("7.2")`, `zoned_decimal(4)` — mainframe binary formats |
| `account_status.dml` | Account status with void padding | Comma (`,`) | `void(",")` padding fields — filler bytes in legacy layouts |
| `common_address.dml` | Reusable address type definition | Comma (`,`) | **Named type** (`type address_t = record`) — reusable across DML files |
| `customer_address.dml` | Customer with embedded address | Comma (`,`) | **Include directive** (`include "common_address.dml"`), embedded type reference |

### DML Type Taxonomy

| DML Type | Description | SQL Equivalent | Example |
|---|---|---|---|
| `decimal` | Numeric (default precision) | `DECIMAL` | `decimal(",") customer_id` |
| `decimal("8.2")` | Numeric with precision/scale | `DECIMAL(8,2)` | `decimal("8.2", "\|") balance` |
| `string` | Variable-length text | `VARCHAR` | `string(",") first_name` |
| `date("YYYY-MM-DD")` | Date with format mask | `DATE` | `date("YYYY-MM-DD")(";") opened_date` |
| `datetime("YYYY-MM-DD HH24:MI:SS")` | Timestamp with format | `TIMESTAMP` | `datetime(...)(",") txn_timestamp` |
| `packed_decimal(N)` | Mainframe packed decimal | `DECIMAL` | `packed_decimal(5) account_num` |
| `zoned_decimal(N)` | Mainframe zoned decimal | `DECIMAL` | `zoned_decimal(4) status_code` |
| `void` | Padding / filler bytes | (omitted) | `void(",") padding1` |

### Advanced DML Features Used

1. **Variable-length arrays**: `string(",")[item_count] item_names` — array size determined by preceding field
2. **Nested records**: `record ... end merchant_info` — sub-record within a parent record
3. **Conditional records**: `if (txn_type == 2) record ... end refund_details` — present only when condition met
4. **Null handling**: `null("")` and `null("UNKNOWN")` — null sentinel values
5. **Type definitions**: `type address_t = record ... end` — reusable named types
6. **Include directives**: `include "common_address.dml"` — DML file composition
7. **Mainframe formats**: `packed_decimal` / `zoned_decimal` — EBCDIC-origin binary encodings

---

## 3. Graph Execution Patterns

### ParallelLoader (`graphs/parallel_loader.py`)

Orchestrates parallel execution of Ab Initio graphs across data partitions — the Python equivalent of the Ab Initio `m_partition` component.

**Classes:**

| Class | Responsibility |
|---|---|
| `PartitionManager` | Splits total record count into N equal partition ranges |
| `ParallelLoader` | Manages ThreadPoolExecutor to run `air_run` CLI in parallel per partition |

**Execution Flow:**
1. `PartitionManager.generate_ranges(total_records)` divides data into N chunks
2. `ParallelLoader.run_graph()` submits each partition to a `ThreadPoolExecutor`
3. Each partition invokes `air_run -g <graph> -pset <pset> -partition=N -start_record=X -end_record=Y`
4. Results aggregated: success/failure counts, per-partition timing, overall status
5. Optional `on_complete` callback fires per partition completion

**Configuration:**
- Default `air_root`: `/usr/local/abinitio`
- Default `max_workers`: 4 (threads)
- Default partition `timeout`: 3600 seconds (1 hour)
- Graceful degradation: if `air_run` binary not found, simulates execution (for CI/CD testing)

### CDCProcessor (`graphs/cdc_processor.py`)

Implements Change Data Capture via row-level MD5 hashing — equivalent to the Ab Initio "Compare Records by Key" component.

**Algorithm:**
1. Compute MD5 hash of all non-key columns for each row in source and target DataFrames
2. Set key columns as index
3. **Inserts**: keys in source but not in target
4. **Deletes**: keys in target but not in source
5. **Updates**: keys in both, but hash differs (data changed)

**Configuration:**
- `key_columns`: list of columns forming the composite key
- `compare_columns`: optional — defaults to all non-key columns
- Uses pandas DataFrames as input/output
- Hash function: MD5 with `||` delimiter between column values

---

## 4. PSET (Parameter Set) Management

### PSETManager (`psets/pset_manager.py`)

Manages Ab Initio PSETs — externalised runtime parameters that allow changing job behaviour (paths, credentials, partition counts) without modifying graph code.

**Capabilities:**

| Method | Description |
|---|---|
| `load_pset(name, env)` | Load PSET with environment fallback: `{dir}/{env}/{name}.pset` → `{dir}/{name}.pset` |
| `_parse_pset(file, env)` | Parse `define KEY VALUE` and `KEY=VALUE` formats, expand `$ENV_VARS` |
| `render_pset(params, overrides)` | Generate PSET file content from a dict |
| `write_pset(name, params, env)` | Write rendered PSET to environment-specific subdirectory |
| `diff_psets(a, b)` | Compare two PSET dicts: additions, removals, changed values |

**PSET File Format:**
```
# Comment line
define PARAM_NAME value
PARAM_NAME=value
```

### PSET Templates

| PSET File | Pipeline | Key Parameters |
|---|---|---|
| `orders_pipeline.pset` | Daily orders load | SOURCE_PATH, TARGET_TABLE (DEV.STAGING.ORDERS), PARTITION_COUNT=4, BATCH_SIZE=50000, MAX_ERRORS=100 |
| `orders_staging.pset` | Orders staging merge | LOAD_MODE=MERGE, DML_FILE reference, REJECT_PATH, SLA_MINUTES=30 |
| `customer_cdc.pset` | Customer CDC | PREVIOUS/CURRENT_SNAPSHOT_PATH, CDC_OUTPUT_PATH, HASH_COLUMNS, KEY_COLUMNS, PARTITION_COUNT=8, RETENTION_DAYS=90 |

**Environment Resolution:** PSETs support per-environment overrides via directory hierarchy (`psets/dev/`, `psets/uat/`, `psets/prod/`) with fallback to the base template.

---

## 5. KornShell Orchestration

### Environment Bootstrap (`scripts/setenv.ksh`)

Sourced by all pipeline scripts. Establishes the Ab Initio runtime environment:

| Variable | Value | Purpose |
|---|---|---|
| `AI_HOME` | `/opt/abinitio` | Ab Initio installation root |
| `AI_PROJECT_DIR` | `/data/projects/enterprise_etl` | Project graph/PSET root |
| `AI_LOG_DIR` | `/data/logs/abinitio` | Centralised log directory |
| `AI_SANDBOX_DIR` | `/data/sandbox` | Local sandbox for execution |
| `AB_HOME` | `${AI_HOME}/coop` | Co>Operating System path |
| `AI_SOURCE_DB` | `ORACLE_PROD` | Source database alias |
| `AI_TARGET_DB` | `TERADATA_DW` | Target data warehouse alias |
| `AI_STAGING_DB` | `ORACLE_STG` | Staging database alias |
| `AI_DEFAULT_PARTITIONS` | `4` | Default parallelism |
| `AI_MAX_PARTITIONS` | `16` | Max parallelism cap |
| `AI_MAX_ERRORS` | `100` | Error threshold before abort |
| `AI_ERROR_ACTION` | `ABORT` | Error behaviour (ABORT/CONTINUE/SKIP) |
| `AI_CHECKPOINT_ENABLED` | `true` | Checkpoint/restart support |

### Daily Orders Pipeline (`scripts/run_daily_orders.ksh`)

**Trigger:** AutoSys job `JOB_DAILY_ORDERS_LOAD` (daily)

**Phases:**
1. **Extract** — `extract_orders.mp` with `orders_extract.pset` + BATCH_DATE parameter
2. **CDC** — `cdc_orders.mp` with `orders_cdc.pset`, 4 partitions
3. **Staging Load** — `load_staging_orders.mp` with `orders_staging.pset`
4. **Production Rollover** — `prod_rollover_orders.mp` with `orders_prod.pset`

**Error Handling:** `set -e` (fail fast on first non-zero exit), per-phase return code check with FATAL logging on extraction failure.

### Customer CDC Pipeline (`scripts/run_customer_cdc.ksh`)

**Trigger:** AutoSys job `JOB_CUSTOMER_CDC` (every 4 hours)

**Steps:**
1. **Snapshot** — `snapshot_customer.mp` — capture current state from source
2. **CDC Detect** — `cdc_detect_customer.mp` — hash-based comparison, 8 partitions
3. **Apply Changes** — `apply_customer_changes.mp` — merge changes to target
4. **Audit Trail** — `audit_customer_changes.mp` — record all changes for compliance

---

## 6. Monitoring & SLA Tracking

### JobMonitor (`monitoring/job_monitor.py`)

Integrates with the **AutoSys REST API** to monitor Ab Initio job execution.

**Capabilities:**

| Method | Description |
|---|---|
| `get_job_status(job_name)` | Fetch current status from `GET /api/v1/jobs/{name}/status` |
| `check_sla(job_name, deadline)` | Evaluate SLA compliance; alert on breach |
| `monitor_jobs(jobs, interval, max_polls)` | Poll multiple jobs until all complete or timeout |
| `_send_alert(job_name, status, type)` | Send Slack webhook alert (`:red_circle:` for failure, `:warning:` for SLA breach) |

**Configuration:** AutoSys URL, Bearer token, optional Slack webhook URL.

### SLATracker (`monitoring/sla_tracker.py`)

Tracks 90-day SLA compliance history and generates compliance reports.

**Capabilities:**

| Method | Description |
|---|---|
| `register_job(name, sla_window_end, criticality)` | Register a job with its SLA deadline (e.g. "06:00" UTC) |
| `record_completion(name, completed_at, status, duration)` | Record run outcome; evaluate SLA met/missed |
| `generate_report(days)` | Generate compliance report: run count, SLA %, avg duration per job |

**State Management:** JSON file at `/tmp/abinitio_sla_state.json`, retains last 90 days of run history per job.

---

## 7. Deployment Automation

### DeployManager (`deployment/deploy_manager.py`)

Automates Ab Initio graph deployment across environments with **ServiceNow** change management gating.

**Deployment Flow:**
1. Validate ServiceNow Change Request is in `approved` or `implement` state
2. For each graph: invoke `air_deploy -source-env <src> -target-env <tgt> -graph <path> -validate-after`
3. Aggregate results (success/partial_failure)
4. Update ServiceNow CR with deployment outcome via PATCH

**Integration Points:**
- ServiceNow: `GET /api/now/table/change_request` (status check), `PATCH` (update work notes)
- UrbanCode Deploy: via `air_deploy` CLI

### ChangeValidator (`deployment/change_validator.py`)

Pre-deployment static analysis — runs before any graph is promoted to production.

**Validation Checks:**

| Check | Blocking | Description |
|---|---|---|
| `graph_file_exists` | Yes | Verify graph file exists on filesystem |
| `no_dev_paths_in_prod` | Yes | Reject graphs containing `/dev/`, `/test/`, `DEV.`, etc. in prod deployments |
| `naming_convention` | No (warning) | Graph files should end with `.mp` or `.mf` |

---

## 8. DML Parser Utility

### DMLParser (`utils/dml_parser.py`)

Parses Ab Initio DML files into structured metadata and generates SQL DDL.

**Type Mapping:**

| DML Type | SQL Type |
|---|---|
| `integer` | `INTEGER` |
| `decimal` | `DECIMAL` |
| `string` | `VARCHAR` |
| `date` | `DATE` |
| `datetime` | `TIMESTAMP` |
| `double` | `DOUBLE` |
| `long` | `BIGINT` |

**Capabilities:**
- `parse_file(path)` / `parse_content(content)`: Extract field names, types, and sizes
- `to_create_table_sql(schema, dialect)`: Generate `CREATE TABLE IF NOT EXISTS` DDL

**Limitations (current):**
- Does not parse nested records, variable-length arrays, conditional records, or void fields
- Does not handle `packed_decimal` / `zoned_decimal` types
- Does not resolve `include` directives or `type` definitions
- Only generates Snowflake-dialect DDL (no Teradata/Spark output)

---

## 9. Sample Data

| File | Format | Delimiter | Records | Content |
|---|---|---|---|---|
| `customers.dat` | CSV | Comma | 10 | customer_id, first/last name, email, street, city, state, zip, status |
| `orders.dat` | DSV | Pipe (`\|`) | 10 | order_id (ORD-YYYY-NNNN), customer_id, date, status, item_count, total, currency |
| `transactions.dat` | DSV | Pipe (`\|`) | 5 | txn_id, timestamp, customer_id, txn_type, merchant, category, amount, SKU, qty, line_total, channel |

---

## 10. Test Coverage

Single test file: `tests/test_pset_manager.py` (113 lines, 10 test cases)

| Test Class | Tests | Module Under Test |
|---|---|---|
| `TestPSETManager` | 4 tests — load, render, write, diff | `psets/pset_manager.py` |
| `TestDMLParser` | 3 tests — parse fields, type mapping, SQL generation | `utils/dml_parser.py` |
| `TestCDCProcessor` | 3 tests — insert detection, delete detection, no-change identity | `graphs/cdc_processor.py` |

**Modules without tests:** `graphs/parallel_loader.py`, `monitoring/job_monitor.py`, `monitoring/sla_tracker.py`, `deployment/deploy_manager.py`, `deployment/change_validator.py`

---

## 11. Databricks Migration Mapping

| Ab Initio Concept | Current Implementation | Databricks Target |
|---|---|---|
| Graph (.mp) | `air sandbox run` via KSH scripts | Databricks Notebook / Spark Job |
| DML record layout | `.dml` files in `dml/` | PySpark `StructType` / Delta table DDL |
| PSET | `.pset` files + `PSETManager` | Databricks Job parameters / Widgets / Secrets |
| Partition | `air_run -partition=N` | Spark `repartition()` / shuffle partitions |
| CDC (hash-based) | `CDCProcessor` (MD5 comparison) | Delta Lake `MERGE` / Change Data Feed |
| AutoSys scheduling | KSH scripts triggered by AutoSys | Databricks Workflows |
| Co>Operating System | Ab Initio runtime engine | Spark cluster |
| `air sandbox run` | Local sandbox execution | `dbx execute` / Jobs API |
| ServiceNow deployment | `DeployManager` + `air_deploy` | Databricks Asset Bundles + CI/CD |
| SLA monitoring | `JobMonitor` + `SLATracker` | Databricks SQL alerts + webhook notifications |
