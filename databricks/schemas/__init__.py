"""
Databricks Delta Lake schema definitions — migrated from Ab Initio DML record layouts.

Each module contains:
  - A PySpark StructType definition (for DataFrame operations)
  - A Delta Lake CREATE TABLE DDL statement (for table creation)
  - Type mapping documentation (Ab Initio → Spark type decisions)

Source DML files: dml/*.dml
"""
