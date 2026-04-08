# ab-initio-etl-framework

Enterprise ETL framework patterns and utilities built around Ab Initio — reusable graph templates, PSET management, parallel processing patterns, and deployment automation.

**Stack:** Python · Ab Initio · AutoSys · ServiceNow · UrbanCode Deploy

## Components

| Module | Description |
|--------|-------------|
| `graphs/parallel_loader.py` | Partition-based parallel graph execution with thread pooling |
| `graphs/cdc_processor.py` | CDC INSERT/UPDATE/DELETE detection via row hashing |
| `psets/pset_manager.py` | Environment-aware PSET loading, rendering, and diffing |
| `monitoring/job_monitor.py` | AutoSys API integration, SLA breach detection, Slack alerts |
| `monitoring/sla_tracker.py` | 90-day SLA compliance history and daily reports |
| `deployment/deploy_manager.py` | ServiceNow CR validation + UrbanCode deployment automation |
| `deployment/change_validator.py` | Pre-deployment: file checks, dev-path detection, naming rules |
| `utils/dml_parser.py` | Parses Ab Initio DML files → SQL CREATE TABLE statements |

## Quick Start

```bash
pip install -r requirements.txt

# Run CDC comparison
python -c "
from graphs.cdc_processor import CDCProcessor
import pandas as pd

source = pd.read_parquet('orders_source.parquet')
target = pd.read_parquet('orders_target.parquet')
cdc = CDCProcessor(key_columns=['order_id'])
delta = cdc.process(source, target)
print(delta['stats'])
"

# Load PSET for environment
python -c "
from psets.pset_manager import PSETManager
mgr = PSETManager('psets/pset_templates')
params = mgr.load_pset('orders_pipeline', environment='prod')
print(params)
"
```

## PSET Environment Management

PSETs allow changing job parameters across environments without touching graph code:

```
psets/pset_templates/
├── orders_pipeline.pset        ← default values
├── dev/orders_pipeline.pset    ← dev overrides
├── uat/orders_pipeline.pset    ← UAT overrides
└── prod/orders_pipeline.pset   ← production values
```
