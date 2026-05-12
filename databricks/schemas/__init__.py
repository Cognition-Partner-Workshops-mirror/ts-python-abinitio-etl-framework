"""
Delta Lake schema definitions migrated from Ab Initio DML record layouts.

Each module corresponds to one .dml file in the legacy dml/ directory and provides:
  1. A PySpark StructType definition for use in DataFrame creation and validation.
  2. A Delta Lake DDL string for CREATE TABLE execution in Databricks SQL.

Type mapping reference (Ab Initio → Spark):
  decimal (no precision)      → LongType / BIGINT
  decimal("p.s")              → DecimalType(p,s) / DECIMAL(p,s)
  string / string(n)          → StringType / STRING
  date("fmt")                 → DateType / DATE
  datetime("fmt")             → TimestampType / TIMESTAMP
  packed_decimal(n)           → DecimalType(n,0) / DECIMAL(n,0)
  packed_decimal("p.s")       → DecimalType(p,s) / DECIMAL(p,s)
  zoned_decimal(n)            → DecimalType(n,0) / DECIMAL(n,0)
  void                        → SKIPPED (mainframe padding)
  nested record               → StructType / STRUCT<>
  type[count] (var-len array) → ArrayType / ARRAY<>
  conditional record (if ...) → Nullable StructType / Nullable STRUCT<>
"""
