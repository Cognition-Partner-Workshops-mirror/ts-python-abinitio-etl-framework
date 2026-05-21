# Knowledge Base — Ab Initio ETL Framework

> Generated: 2026-05-21 | Repo: `ts-python-abinitio-etl-framework`

---

## 1. Architecture Overview

### 1.1 Purpose

Enterprise Ab Initio ETL framework managing large-scale data integration between **Oracle** (source) and **Teradata** (data warehouse target). The codebase serves as the **migration source** for a planned transition to **Databricks Lakehouse** (PySpark / Delta Lake / Unity Catalog).

### 1.2 Technology Stack

| Layer | Technology |
|---|---|
| ETL Platform | Ab Initio Co>Operating System |
| Orchestration | AutoSys / Control-M |
| Source Database | Oracle |
| Target Database | Teradata (Data Warehouse) |
| Staging Database | Oracle (Staging) |
| Deployment | ServiceNow (change management) + UrbanCode Deploy |
| Scripting | KornShell (KSH) for orchestration |
| Utilities | Python 3.x (DML parsing, CDC, monitoring, deployment) |
| Schema Language | Ab Initio DML (Data Manipulation Language) |

### 1.3 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION LAYER                         │
│  AutoSys/Control-M  →  KornShell Scripts  →  air sandbox run       │
└───────────┬─────────────────────┬───────────────────────┬───────────┘
            │                     │                       │
            ▼                     ▼                       ▼
┌───────────────────┐  ┌──────────────────┐  ┌────────────────────────┐
│  Daily Orders     │  │  Customer CDC    │  │  Ad-hoc Graphs         │
│  Pipeline (KSH)   │  │  Pipeline (KSH)  │  │  (ParallelLoader.py)   │
│  4 phases:        │  │  4 steps:        │  │  Partition-based        │
│  Extract→CDC→     │  │  Snapshot→CDC→   │  │  parallel execution     │
│  Stage→Prod       │  │  Apply→Audit     │  │                        │
└───────┬───────────┘  └───────┬──────────┘  └────────────┬───────────┘
        │                      │                          │
        ▼                      ▼                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Ab Initio Co>Operating System                     │
│         Graphs (.mp files)  |  PSETs  |  DML Schemas                │
└───────────┬─────────────────────────────────────────────┬───────────┘
            │                                             │
            ▼                                             ▼
    ┌──────────────┐                             ┌──────────────────┐
    │  Oracle      │                             │  Teradata DW     │
    │  (Source)    │                             │  (Target)        │
    └──────────────┘                             └──────────────────┘
```

### 1.4 Module Inventory

| Module | Directory | Language | Lines of Code | Description |
|---|---|---|---|---|
| Parallel Loader | `graphs/parallel_loader.py` | Python | 129 | Partition-based parallel graph execution orchestrator |
| CDC Processor | `graphs/cdc_processor.py` | Python | 80 | Hash-based Change Data Capture (INSERT/UPDATE/DELETE) |
| DML Parser | `utils/dml_parser.py` | Python | 95 | Parses Ab Initio DML files → SQL CREATE TABLE |
| PSET Manager | `psets/pset_manager.py` | Python | 99 | Environment-aware parameter set management |
| Job Monitor | `monitoring/job_monitor.py` | Python | 109 | AutoSys API integration + Slack alerting |
| SLA Tracker | `monitoring/sla_tracker.py` | Python | 97 | 90-day SLA compliance reporting |
| Deploy Manager | `deployment/deploy_manager.py` | Python | 125 | ServiceNow CR validation + UrbanCode deployment |
| Change Validator | `deployment/change_validator.py` | Python | 75 | Pre-deployment checks (naming, dev-path detection) |
| Daily Orders Script | `scripts/run_daily_orders.ksh` | KSH | 63 | 4-phase daily orders batch pipeline |
| Customer CDC Script | `scripts/run_customer_cdc.ksh` | KSH | 48 | 4-step customer CDC pipeline (runs every 4 hours) |
| Environment Setup | `scripts/setenv.ksh` | KSH | 32 | Runtime variable initialization |
| Unit Tests | `tests/test_pset_manager.py` | Python | 112 | Tests for PSET manager, DML parser, CDC processor |

**Total:** ~921 lines Python, ~143 lines KornShell, ~74 lines DML, ~40 lines PSET config.

### 1.5 Communication & Data Flow Patterns

| Pattern | Implementation |
|---|---|
| **Batch ETL** | KSH scripts invoke `air sandbox run` with graph files (.mp) and PSETs |
| **Parallel Execution** | `ParallelLoader` uses `ThreadPoolExecutor` to run graph partitions concurrently via `air_run` CLI |
| **CDC** | `CDCProcessor` compares source vs. target DataFrames using MD5 row hashing to detect INSERTs, UPDATEs, DELETEs |
| **Monitoring** | `JobMonitor` polls AutoSys REST API; alerts via Slack webhook on SLA breach or job failure |
| **Deployment** | `DeployManager` validates ServiceNow CR approval, then deploys via `air_deploy` CLI or UrbanCode |

---

## 2. Data Models

### 2.1 DML Record Layouts

The `dml/` directory contains Ab Initio DML files defining record schemas used by ETL graphs. These are the logical data models for the pipeline.

#### Customer (`dml/customer.dml`)

| Field | DML Type | Delimiter | Notes |
|---|---|---|---|
| `customer_id` | decimal | `,` | Primary key |
| `first_name` | string | `,` | |
| `last_name` | string | `,` | |
| `email` | string | `\n` (newline) | End-of-record delimiter |

#### Account Balance (`dml/account_balance.dml`)

| Field | DML Type | Delimiter / Format | Notes |
|---|---|---|---|
| `account_id` | decimal | `\|` | Primary key |
| `account_holder` | string | `\|` | |
| `balance` | decimal(8.2) | `\|` | Two-decimal precision |
| `opened_date` | date(YYYY-MM-DD) | `;` | Date format specified |
| `branch` | string | `\n` | |

#### Order Items (`dml/order_items.dml`)

| Field | DML Type | Delimiter | Notes |
|---|---|---|---|
| `order_id` | decimal | `,` | Primary key |
| `item_count` | decimal | `,` | Controls variable-length arrays |
| `item_names` | string[item_count] | `,` | Variable-length array |
| `item_quantities` | decimal[item_count] | `,` | Variable-length array |
| `order_status` | string | `\n` | |

#### Transaction Detail (`dml/transaction_detail.dml`) — Complex Nested Record

| Field | DML Type | Notes |
|---|---|---|
| `txn_id` | decimal | Primary key |
| `txn_timestamp` | datetime(YYYY-MM-DD HH24:MI:SS) | |
| `customer_id` | decimal | Foreign key to customer |
| `txn_type` | decimal | 1=purchase, 2=refund |
| **`merchant_info`** (nested record) | | |
| → `merchant_name` | string (nullable) | |
| → `merchant_category` | string | |
| → `amount` | decimal(10.2) | |
| **`line_items`** (array[item_count]) | | Variable-length nested array |
| → `sku` | string | |
| → `quantity` | decimal | |
| → `line_total` | decimal(8.2) | |
| **`refund_details`** (conditional) | | Only present when `txn_type == 2` |
| → `original_txn_id` | decimal | |
| → `refund_reason` | string | |
| `channel` | string (nullable, default "UNKNOWN") | WEB, STORE, APP |

#### Packed Account (`dml/packed_account.dml`) — Mainframe Format

| Field | DML Type | Notes |
|---|---|---|
| `account_num` | packed_decimal(5) | COMP-3 mainframe format |
| `balance` | packed_decimal(7.2) | |
| `status_code` | zoned_decimal(4) | Zoned decimal (EBCDIC) |
| `account_name` | string(20) | Fixed-width |

#### Account Status (`dml/account_status.dml`)

| Field | DML Type | Notes |
|---|---|---|
| `id` | decimal | Primary key |
| `padding1` | void | Filler / skip field |
| `name` | string | |
| `padding2` | void | Filler / skip field |
| `status` | string | |

#### Common Address Type (`dml/common_address.dml`) — Reusable Type

| Field | DML Type |
|---|---|
| `street` | string |
| `city` | string |
| `state` | string |
| `zip` | string |

#### Customer Address (`dml/customer_address.dml`) — Uses Include

| Field | DML Type | Notes |
|---|---|---|
| `customer_id` | decimal | Primary key |
| `name` | string | |
| `address` | address_t | Includes `common_address.dml` type |
| `phone` | string | |

### 2.2 Entity Relationships

```
Customer (customer_id)
  │
  ├──── 1:N ────→ Order (order_id, customer_id)
  │                  └── N:M → Order Items (variable-length array)
  │
  ├──── 1:N ────→ Transaction (txn_id, customer_id)
  │                  ├── 1:1 → Merchant Info (nested)
  │                  ├── 1:N → Line Items (array)
  │                  └── 0:1 → Refund Details (conditional on txn_type=2)
  │
  ├──── 1:1 ────→ Customer Address (includes address_t)
  │
  └──── 1:N ────→ Account Balance (account_id)
                     └── → Account Status
```

### 2.3 Sample Data Formats

| Dataset | File | Delimiter | Records | Format |
|---|---|---|---|---|
| Customers | `data/sample/customers.dat` | Comma | 10 | `id,first,last,email,street,city,state,zip,status` |
| Orders | `data/sample/orders.dat` | Pipe | 10 | `order_id\|customer_id\|date\|status\|items\|total\|currency` |
| Transactions | `data/sample/transactions.dat` | Pipe | 5 | Flat representation of nested transaction_detail |

---

## 3. API Surface Map

This codebase does not expose HTTP APIs. Instead, it provides **Python class APIs** and **CLI entry points** for ETL operations.

### 3.1 Python Class APIs

#### `ParallelLoader` (`graphs/parallel_loader.py`)

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(air_root="/usr/local/abinitio", max_workers=4)` | Initialize loader with Ab Initio root path and parallelism |
| `run_graph` | `(graph_path, pset_path, partition_ranges, timeout=3600, on_complete=None) → Dict` | Execute graph in parallel across partitions; returns summary with status, timing, partition results |

#### `PartitionManager` (`graphs/parallel_loader.py`)

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(partition_count=4)` | Set number of partitions |
| `generate_ranges` | `(total_records: int) → List[Dict]` | Split record count into partition ranges |

#### `CDCProcessor` (`graphs/cdc_processor.py`)

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(key_columns, compare_columns=None)` | Configure key and comparison columns |
| `process` | `(source_df, target_df) → Dict[str, DataFrame]` | Compute CDC delta — returns `{inserts, updates, deletes, stats}` |

#### `DMLParser` (`utils/dml_parser.py`)

| Method | Signature | Description |
|---|---|---|
| `parse_file` | `(dml_path: str) → Dict` | Parse a DML file and return schema metadata |
| `parse_content` | `(content: str, record_name="record") → Dict` | Parse DML content string into schema dict |
| `to_create_table_sql` | `(schema: Dict, dialect="snowflake") → str` | Generate CREATE TABLE SQL from parsed DML |

#### `PSETManager` (`psets/pset_manager.py`)

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(pset_dir="psets/pset_templates")` | Set PSET template directory |
| `load_pset` | `(pset_name, environment="dev") → Dict` | Load PSET file with env fallback |
| `render_pset` | `(template_params, overrides=None) → str` | Render PSET file content from params dict |
| `write_pset` | `(pset_name, params, environment="dev") → Path` | Write rendered PSET to filesystem |
| `diff_psets` | `(pset_a, pset_b) → Dict` | Compare two PSET dicts and return differences |

#### `JobMonitor` (`monitoring/job_monitor.py`)

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(autosys_url, api_token=None, slack_webhook=None)` | Configure AutoSys and Slack connections |
| `get_job_status` | `(job_name) → Dict` | Fetch job status from AutoSys REST API |
| `check_sla` | `(job_name, expected_complete_by) → Dict` | Check if job meets SLA deadline |
| `monitor_jobs` | `(jobs, poll_interval_seconds=60, max_polls=60) → List` | Poll jobs until completion or timeout |

#### `SLATracker` (`monitoring/sla_tracker.py`)

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(state_file="/tmp/abinitio_sla_state.json")` | Initialize with persistent state file |
| `register_job` | `(job_name, sla_window_end, criticality="high")` | Register a job with SLA definition |
| `record_completion` | `(job_name, completed_at, status, duration_minutes) → Dict` | Record run and evaluate SLA compliance |
| `generate_report` | `(days=7) → Dict` | Generate SLA compliance report |

#### `DeployManager` (`deployment/deploy_manager.py`)

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(ucd_url=None, snow_url=None, api_token=None)` | Configure UrbanCode and ServiceNow connections |
| `deploy` | `(graphs, source_env, target_env, change_request_id, deployed_by) → Dict` | Deploy graphs after CR validation |

#### `ChangeValidator` (`deployment/change_validator.py`)

| Method | Signature | Description |
|---|---|---|
| `validate` | `(graphs, target_env, pset_manager=None) → Dict` | Run pre-deployment validation checks |

### 3.2 CLI Entry Points (KornShell)

| Script | Trigger | Schedule | Description |
|---|---|---|---|
| `scripts/run_daily_orders.ksh` | AutoSys `JOB_DAILY_ORDERS_LOAD` | Daily | 4-phase orders pipeline: Extract → CDC → Staging → Prod |
| `scripts/run_customer_cdc.ksh` | AutoSys `JOB_CUSTOMER_CDC` | Every 4 hours | 4-step CDC: Snapshot → Detect → Apply → Audit |
| `scripts/setenv.ksh` | Sourced by all scripts | N/A | Sets `AI_HOME`, paths, DB connections, partition defaults |

### 3.3 External CLI Tools Invoked

| Tool | Used By | Purpose |
|---|---|---|
| `air_run` | `ParallelLoader._run_partition()` | Execute Ab Initio graph partition |
| `air sandbox run` | KSH scripts | Execute graph in sandbox environment |
| `air_deploy` | `DeployManager._deploy_graph()` | Deploy graph between environments |

---

## 4. Business Logic Inventory

### 4.1 Daily Orders Pipeline (`run_daily_orders.ksh`)

Four sequential phases, each invoking a separate Ab Initio graph:

| Phase | Graph | PSET | Description |
|---|---|---|---|
| 1 — Extract | `extract_orders.mp` | `orders_extract.pset` | Pull raw orders from Oracle source |
| 2 — CDC | `cdc_orders.mp` | `orders_cdc.pset` | Detect changes (4 partitions) |
| 3 — Staging | `load_staging_orders.mp` | `orders_staging.pset` | Load deltas to staging tables |
| 4 — Production | `prod_rollover_orders.mp` | `orders_prod.pset` | Roll staging data into production |

**Error handling:** `set -e` causes script to abort on first non-zero return code. Phase 1 has explicit RC check with FATAL log; phases 2–4 rely on `set -e`.

### 4.2 Customer CDC Pipeline (`run_customer_cdc.ksh`)

| Step | Graph | PSET | Description |
|---|---|---|---|
| 1 — Snapshot | `snapshot_customer.mp` | `customer_snapshot.pset` | Capture current state from source |
| 2 — CDC Detect | `cdc_detect_customer.mp` | `customer_cdc.pset` | Hash-based comparison (8 partitions) |
| 3 — Apply | `apply_customer_changes.mp` | `customer_apply.pset` | Apply INSERTs/UPDATEs/DELETEs to target |
| 4 — Audit | `audit_customer_changes.mp` | `customer_audit.pset` | Write change audit trail |

### 4.3 CDC Processing Logic (`CDCProcessor`)

- **Key identification:** Rows identified by configurable `key_columns`
- **Change detection:** MD5 hash of all non-key columns (or specified `compare_columns`)
- **INSERT:** Key in source but not target
- **DELETE:** Key in target but not source
- **UPDATE:** Key in both, but hash differs
- Uses pandas DataFrames for in-memory comparison

### 4.4 Parallel Execution Logic (`ParallelLoader`)

- **Partitioning:** `PartitionManager.generate_ranges()` splits total record count into equal chunks
- **Execution:** `ThreadPoolExecutor` submits one `air_run` subprocess per partition
- **Result aggregation:** Collects per-partition status, counts successes/failures
- **Fallback:** If `air_run` binary not found, simulates execution (for CI/CD testing)

### 4.5 Deployment Workflow (`DeployManager`)

1. Validate ServiceNow change request is in "approved" or "implement" state
2. For each graph: invoke `air_deploy` CLI with source/target env
3. Update CR with deployment outcome (work notes)
4. **Fallback:** If ServiceNow API is unreachable, defaults to "approved" (simulation mode)

### 4.6 Pre-Deployment Validation (`ChangeValidator`)

Three checks per graph:
1. **File existence** — graph file exists on filesystem (blocking)
2. **No dev paths in prod** — blocks if graph path contains `/dev/`, `/test/`, `DEV.`, etc. (blocking, prod only)
3. **Naming convention** — warns if graph doesn't end with `.mp` or `.mf` (non-blocking)

### 4.7 SLA Compliance Tracking (`SLATracker`)

- Jobs registered with SLA window end time (e.g., "06:00" UTC)
- Each completion recorded with timestamp, status, duration
- History retained for 90 days
- Compliance reports show percentage of runs meeting SLA per job
- State persisted to JSON file

---

## 5. Integration Points

| System | Component | Protocol | Purpose |
|---|---|---|---|
| **AutoSys** | `JobMonitor` | REST API (`/api/v1/jobs/{name}/status`) | Poll job status, detect failures |
| **Slack** | `JobMonitor._send_alert()` | Webhook (POST) | Alert on SLA breach or job failure |
| **ServiceNow** | `DeployManager._get_cr_status()` | REST API (`/api/now/table/change_request`) | Validate change request approval |
| **ServiceNow** | `DeployManager._update_cr()` | REST API (PATCH) | Update CR with deployment outcome |
| **UrbanCode Deploy** | `DeployManager` | CLI (`air_deploy`) | Push graphs between environments |
| **Ab Initio Runtime** | `ParallelLoader` | CLI (`air_run`) | Execute graph partitions |
| **Ab Initio Runtime** | KSH scripts | CLI (`air sandbox run`) | Execute graphs in sandbox |
| **Oracle** | KSH scripts (via PSET) | Database (JDBC) | Source data extraction |
| **Teradata** | KSH scripts (via PSET) | Database (JDBC) | Target data warehouse loading |
| **Filesystem** | `SLATracker` | JSON file (`/tmp/abinitio_sla_state.json`) | Persistent SLA state |

### 5.1 Environment Variables (`setenv.ksh`)

| Variable | Value | Purpose |
|---|---|---|
| `AI_HOME` | `/opt/abinitio` | Ab Initio installation root |
| `AI_PROJECT_DIR` | `/data/projects/enterprise_etl` | Project working directory |
| `AI_LOG_DIR` | `/data/logs/abinitio` | Log output directory |
| `AI_SOURCE_DB` | `ORACLE_PROD` | Source database connection name |
| `AI_TARGET_DB` | `TERADATA_DW` | Target database connection name |
| `AI_DEFAULT_PARTITIONS` | `4` | Default parallelism |
| `AI_MAX_ERRORS` | `100` | Error threshold before abort |
| `AI_ERROR_ACTION` | `ABORT` | Error handling policy |
| `AI_CHECKPOINT_ENABLED` | `true` | Checkpoint/restart enabled |

---

## 6. Build & Deployment Summary

### 6.1 Python Dependencies (`requirements.txt`)

| Package | Version | Used By |
|---|---|---|
| `requests` | 2.32.2 | `JobMonitor`, `DeployManager` — HTTP calls to AutoSys/ServiceNow |
| `pydantic` | 2.7.1 | Declared but not used in current codebase |
| `pyyaml` | 6.0.1 | Declared but not used in current codebase |
| `pandas` | 2.2.2 | `CDCProcessor` — DataFrame-based CDC |
| `python-dotenv` | 1.0.1 | Declared but not used in current codebase |
| `croniter` | 2.0.5 | Declared but not used in current codebase |
| `pytest` | 8.2.2 | Test runner |

### 6.2 Test Suite

- **Framework:** pytest
- **Test file:** `tests/test_pset_manager.py` (112 lines)
- **Coverage:** 10 tests across 3 classes:
  - `TestPSETManager` — 4 tests (load, render, write, diff)
  - `TestDMLParser` — 3 tests (parse fields, type mapping, SQL generation)
  - `TestCDCProcessor` — 3 tests (inserts, deletes, identical data)
- **Known failures:** 2 CDC tests (`test_inserts_detected`, `test_deletes_detected`) fail due to pandas indexing bug in `graphs/cdc_processor.py`
- **Pass rate:** 8/10

### 6.3 Build & Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest tests/ -v

# Quick verification
python -c "from utils.dml_parser import DMLParser; print(DMLParser().parse_file('dml/customer.dml'))"
```

### 6.4 Deployment Model

- **No CI/CD pipeline** configured in the repository
- **No Dockerfile** or containerization
- **No Makefile** or build automation
- Deployment relies on external ServiceNow + UrbanCode integration
- Ab Initio graphs deployed via `air_deploy` CLI between environments (dev → uat → prod)

### 6.5 Migration Target

| Current (Ab Initio) | Target (Databricks) |
|---|---|
| Graphs (.mp) | PySpark notebooks + Delta Live Tables |
| PSETs | Databricks job parameters + Unity Catalog |
| DML schemas | PySpark StructType + Delta table DDL |
| Shell orchestration | Databricks Workflows (JSON/YAML) |
| CDC processing | Delta Lake MERGE + Change Data Feed |
| AutoSys monitoring | Databricks SQL alerts + webhooks |
| Deployment | Databricks Asset Bundles + CI/CD |
