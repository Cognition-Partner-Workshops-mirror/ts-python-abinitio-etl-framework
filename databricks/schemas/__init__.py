"""
Databricks Delta Lake schema definitions.

Migrated from Ab Initio DML record layouts in dml/ directory.
Each module contains:
  - A PySpark StructType definition for programmatic use
  - A Delta Lake CREATE TABLE DDL string for SQL-based provisioning

Ab Initio DML Type -> Spark Type Mapping Summary:
  decimal          -> LongType (integer) or DecimalType(p,s) (with precision)
  string           -> StringType
  date             -> DateType
  datetime         -> TimestampType
  packed_decimal   -> DecimalType (COMP-3 mainframe format, decoded on ingest)
  zoned_decimal    -> DecimalType (EBCDIC zoned numeric, decoded on ingest)
  void             -> Dropped (padding/filler bytes with no semantic value)
  record (nested)  -> StructType (embedded struct)
  type[count]      -> ArrayType (variable-length arrays)
"""
