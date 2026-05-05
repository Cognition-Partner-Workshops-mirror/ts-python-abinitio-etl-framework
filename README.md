# Ab Initio ETL Framework — Legacy Enterprise Codebase

Enterprise Ab Initio ETL framework representing a typical legacy data integration estate. Contains graph execution patterns, PSET (Parameter Set) management, DML record layouts, CDC processing, job monitoring (AutoSys), and deployment automation (ServiceNow/UrbanCode).

**This repo serves as a migration source** — the legacy Ab Initio codebase to be modernized to Databricks (PySpark/Delta Lake/Unity Catalog).

---

## Stack

- **ETL Platform:** Ab Initio Co>Operating System
- **Orchestration:** AutoSys / Control-M
- **Databases:** Oracle (source), Teradata (DW target)
- **Deployment:** ServiceNow (change management) + UrbanCode Deploy
- **Languages:** Python (utilities), KornShell (orchestration), Ab Initio DML (schemas)

---

## Repository Structure

```
├── graphs/                  # Ab Initio graph execution patterns
│   ├── parallel_loader.py   # Partition-based parallel graph execution
│   └── cdc_processor.py     # CDC detection (INSERT/UPDATE/DELETE via row hashing)
├── psets/                   # Parameter Sets (environment-aware configuration)
│   └── pset_templates/      # PSET definitions per pipeline
│       ├── orders_pipeline.pset
│       ├── orders_staging.pset
│       └── customer_cdc.pset
├── dml/                     # Ab Initio DML record layouts (schemas)
│   ├── customer.dml         # Customer master record
│   ├── account_balance.dml  # Account balance with dates/decimals
│   ├── order_items.dml      # Variable-length order line items
│   ├── transaction_detail.dml  # Complex nested transaction records
│   ├── packed_account.dml   # Packed/zoned decimal (mainframe format)
│   └── ...                  # Additional schema definitions
├── scripts/                 # KornShell job orchestration
│   ├── run_daily_orders.ksh # Daily orders batch pipeline
│   ├── run_customer_cdc.ksh # Customer CDC (runs every 4 hours)
│   └── setenv.ksh           # Environment setup (paths, connections, defaults)
├── monitoring/              # Job monitoring and SLA tracking
│   ├── job_monitor.py       # AutoSys API integration + Slack alerts
│   └── sla_tracker.py       # 90-day SLA compliance reporting
├── deployment/              # Deployment automation
│   ├── deploy_manager.py    # ServiceNow CR validation + UrbanCode
│   └── change_validator.py  # Pre-deployment checks (naming, dev-path detection)
├── utils/                   # Shared utilities
│   └── dml_parser.py        # DML file parser → SQL CREATE TABLE
├── data/sample/             # Sample data files (pipe/comma delimited)
│   ├── customers.dat
│   ├── orders.dat
│   └── transactions.dat
├── tests/                   # Unit tests
└── requirements.txt         # Python dependencies
```

---

## Key Concepts

| Ab Initio Concept | Description | Databricks Equivalent |
|---|---|---|
| **Graph (.mp)** | Visual dataflow program | Databricks Notebook / Spark Job |
| **DML** | Record layout definition | PySpark StructType / Delta schema |
| **PSET** | Runtime parameters per environment | Job parameters / Widgets / Secrets |
| **Partition** | Parallel execution unit | Spark partitions / `repartition()` |
| **CDC** | Change Data Capture via hashing | Delta Lake MERGE / CDF |
| **AutoSys** | Job scheduler | Databricks Workflows |
| **Co>Operating System** | Runtime engine | Spark cluster |
| **air sandbox run** | Execute graph in sandbox | `dbx execute` / Jobs API |

---

## Quick Start

```bash
pip install -r requirements.txt

# Parse a DML file to see schema
python -c "
from utils.dml_parser import DMLParser
parser = DMLParser()
schema = parser.parse_file('dml/customer.dml')
print(schema)
"

# Load a PSET for an environment
python -c "
from psets.pset_manager import PSETManager
mgr = PSETManager('psets/pset_templates')
params = mgr.load_pset('orders_pipeline', environment='dev')
for k, v in params.items():
    print(f'{k} = {v}')
"
```

---

## Migration Target

This codebase is intended to be migrated to **Databricks Lakehouse** architecture:

- **Graphs** → PySpark notebooks + Delta Live Tables
- **PSETs** → Databricks job parameters + Unity Catalog
- **DML schemas** → PySpark StructType definitions + Delta table DDL
- **Shell orchestration** → Databricks Workflows (JSON/YAML)
- **CDC processing** → Delta Lake MERGE statements + Change Data Feed
- **AutoSys monitoring** → Databricks SQL alerts + webhook notifications
- **Deployment** → Databricks Asset Bundles + CI/CD
