# PSET → Databricks Job Parameter Mapping

This document maps Ab Initio PSET template parameters to their Databricks
Workflow equivalents.

## orders_pipeline.pset → daily_orders_workflow.json

| PSET Parameter      | Databricks Parameter   | Default Value                        | Notes                                    |
|---------------------|------------------------|--------------------------------------|------------------------------------------|
| `SOURCE_PATH`       | `source_path`          | `/mnt/raw/orders`                    | Mount path replaces local filesystem     |
| `TARGET_TABLE`      | (task-level)           | `lakehouse.bronze.orders`            | Unity Catalog three-level namespace      |
| `PARTITION_COUNT`   | `partition_count`      | `4`                                  | Maps to Spark repartition count          |
| `BATCH_SIZE`        | `batch_size`           | `50000`                              | Read batch size                          |
| `LOG_LEVEL`         | `log_level`            | `INFO`                               | Python logging level                     |
| `RECORD_SOURCE`     | (removed)              | —                                    | Lineage tracked by Unity Catalog         |
| `MAX_ERRORS`        | `max_errors`           | `100`                                | Null-key threshold before abort          |
| `CHECKPOINT_DIR`    | `checkpoint_path`      | `/mnt/checkpoints/orders`            | Delta checkpoint location                |

## orders_staging.pset → daily_orders_workflow.json (load_staging task)

| PSET Parameter      | Databricks Parameter   | Default Value                        | Notes                                    |
|---------------------|------------------------|--------------------------------------|------------------------------------------|
| `SOURCE_PATH`       | (task-level)           | `/mnt/cdc/orders/delta`              | CDC output feeds staging                 |
| `TARGET_TABLE`      | (task-level)           | `lakehouse.silver.orders`            | Silver tier in medallion architecture    |
| `TARGET_SCHEMA`     | (removed)              | —                                    | Unity Catalog manages schemas            |
| `LOAD_MODE`         | (task-level)           | Delta MERGE                          | Replaced by Delta MERGE semantics        |
| `REJECT_PATH`       | (removed)              | —                                    | Bad records handled by DQ checks         |
| `DML_FILE`          | (removed)              | —                                    | Schema defined in PySpark StructType     |
| `SLA_MINUTES`       | (alert-level)          | `30`                                 | Moved to Databricks SQL alert            |

## customer_cdc.pset → customer_cdc_workflow.json

| PSET Parameter             | Databricks Parameter   | Default Value                          | Notes                                    |
|----------------------------|------------------------|----------------------------------------|------------------------------------------|
| `SOURCE_PATH`              | `source_path`          | `/mnt/raw/customer`                    | Mount path replaces local filesystem     |
| `TARGET_TABLE`             | (task-level)           | `lakehouse.bronze.customer`            | Unity Catalog three-level namespace      |
| `PREVIOUS_SNAPSHOT_PATH`   | (removed)              | —                                      | Delta MERGE eliminates snapshot files    |
| `CURRENT_SNAPSHOT_PATH`    | (removed)              | —                                      | Delta MERGE eliminates snapshot files    |
| `CDC_OUTPUT_PATH`          | (removed)              | —                                      | CDC results stored in Delta directly     |
| `PARTITION_COUNT`          | `partition_count`      | `8`                                    | Maps to Spark repartition count          |
| `HASH_COLUMNS`             | `hash_columns`         | `customer_id,name,address,phone,...`   | Columns used for MD5 change detection    |
| `KEY_COLUMNS`              | `key_columns`          | `customer_id`                          | MERGE join keys                          |
| `BATCH_SIZE`               | `batch_size`           | `100000`                               | Read batch size                          |
| `MAX_ERRORS`               | `max_errors`           | `50`                                   | Null-key threshold                       |
| `AUDIT_TABLE`              | `audit_table`          | `lakehouse.audit.customer_changes`     | Unity Catalog audit table                |
| `RETENTION_DAYS`           | (table property)       | `90`                                   | Set via Delta `deletedFileRetentionDuration` |

## Environment Override Strategy

Ab Initio used directory-based PSET overrides (`psets/dev/`, `psets/uat/`,
`psets/prod/`). In Databricks, environment-specific values are handled via:

1. **Job parameters** — default values in the workflow JSON can be overridden
   at run time via the Jobs API or UI.
2. **Databricks Secrets** — sensitive values (credentials, connection strings)
   stored in Databricks Secret Scopes instead of PSET files.
3. **Unity Catalog** — catalog/schema names encode the environment
   (e.g. `dev.bronze.orders`, `prod.bronze.orders`).
4. **Terraform / Databricks Asset Bundles** — environment-specific deployment
   configurations replace the PSET directory structure.
