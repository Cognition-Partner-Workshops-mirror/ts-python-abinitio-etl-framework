"""
Delta Lake schema for customer.dml — Customer master record.

Source DML (Ab Initio):
    record
      decimal(",") customer_id;
      string(",") first_name;
      string(",") last_name;
      string("\\n") email;
    end;

Type Mapping Applied:
    Ab Initio decimal (no precision) → Spark LongType
        Rationale: A bare "decimal" with only a delimiter (no precision/scale spec)
        is used as an integer identifier in Ab Initio. LongType (64-bit signed int)
        provides sufficient range for ID columns and avoids unnecessary DECIMAL overhead.
    Ab Initio string → Spark StringType
        Rationale: Ab Initio strings are variable-length by default. Delimiter metadata
        (",", "\\n") controls serialization in flat files and is not relevant to
        Delta Lake's columnar Parquet storage.
"""

from pyspark.sql.types import StructType, StructField, LongType, StringType


# PySpark StructType definition for the customer record
CUSTOMER_SCHEMA = StructType([
    # Ab Initio: decimal(",") customer_id → LongType (bare decimal = integer ID)
    StructField("customer_id", LongType(), nullable=False),
    # Ab Initio: string(",") first_name → StringType
    StructField("first_name", StringType(), nullable=True),
    # Ab Initio: string(",") last_name → StringType
    StructField("last_name", StringType(), nullable=True),
    # Ab Initio: string("\n") email → StringType
    StructField("email", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
CUSTOMER_DDL = """\
-- Delta Lake table migrated from Ab Initio DML: customer.dml
-- Customer master record
CREATE TABLE IF NOT EXISTS customer (
    customer_id   BIGINT       NOT NULL  COMMENT 'Ab Initio decimal (bare) → BIGINT identifier',
    first_name    STRING                 COMMENT 'Ab Initio string → STRING',
    last_name     STRING                 COMMENT 'Ab Initio string → STRING',
    email         STRING                 COMMENT 'Ab Initio string → STRING'
)
USING DELTA
COMMENT 'Customer master record — migrated from Ab Initio DML (customer.dml)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'migration.source'                 = 'abinitio',
    'migration.source_dml'             = 'dml/customer.dml'
);
"""
