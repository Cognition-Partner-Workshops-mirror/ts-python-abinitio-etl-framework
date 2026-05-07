# Databricks notebook source
# MAGIC %md
# MAGIC # Common Address Schema (Reusable Type)
# MAGIC **Migrated from:** `dml/common_address.dml`
# MAGIC
# MAGIC ## Type Mapping
# MAGIC | DML Field | Ab Initio Type | Spark Type | Notes                                            |
# MAGIC |-----------|----------------|------------|--------------------------------------------------|
# MAGIC | street    | string(",")    | STRING     | Direct mapping                                   |
# MAGIC | city      | string(",")    | STRING     | Direct mapping                                   |
# MAGIC | state     | string(",")    | STRING     | Direct mapping                                   |
# MAGIC | zip       | string(",")    | STRING     | Kept as STRING to preserve leading zeros          |
# MAGIC
# MAGIC ### Migration Decision: Reusable Type
# MAGIC Ab Initio `type address_t = record ... end` defines a reusable named type that
# MAGIC can be included via `include` directives. In Spark this maps to a reusable
# MAGIC `StructType` that is embedded as a nested struct in parent schemas.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

address_schema = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** `common_address` is a reusable type definition (`type address_t`), not a
# MAGIC standalone table. It is embedded as a `STRUCT` column in tables that include it
# MAGIC (e.g., `customer_address`). No standalone Delta table is created for this type.
