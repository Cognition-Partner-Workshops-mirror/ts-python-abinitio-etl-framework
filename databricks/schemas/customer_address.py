"""
Delta Lake schema for customer_address.dml — Customer address with included type.

Source DML (Ab Initio):
    include "common_address.dml";

    record
      decimal(",") customer_id;
      string(",") name;
      address_t address;
      string("\\n") phone;
    end;

Where common_address.dml defines:
    type address_t = record
      string(",") street;
      string(",") city;
      string(",") state;
      string(",") zip;
    end;

Type Mapping Applied:
    Ab Initio decimal (bare) → Spark LongType
    Ab Initio string → Spark StringType
    Ab Initio include + type reference (address_t) → Spark nested StructType
        Rationale: Ab Initio's "include" directive imports type definitions from
        other DML files. The referenced type (address_t) is resolved and embedded
        as a nested STRUCT column. In Delta Lake, this becomes a STRUCT<street,
        city, state, zip> column — preserving the logical grouping without
        requiring a separate table or join.

        The reusable type definition (ADDRESS_T_SCHEMA) is imported from
        common_address.py to maintain DRY principles across schemas.
"""

from pyspark.sql.types import StructType, StructField, LongType, StringType

# Import the reusable address type — mirrors Ab Initio's include "common_address.dml"
from databricks.schemas.common_address import ADDRESS_T_SCHEMA


# PySpark StructType definition for the customer_address record
CUSTOMER_ADDRESS_SCHEMA = StructType([
    # Ab Initio: decimal(",") customer_id → LongType (bare decimal = integer ID)
    StructField("customer_id", LongType(), nullable=False),
    # Ab Initio: string(",") name → StringType
    StructField("name", StringType(), nullable=True),
    # Ab Initio: address_t address → StructType (resolved from common_address.dml)
    StructField("address", ADDRESS_T_SCHEMA, nullable=True),
    # Ab Initio: string("\n") phone → StringType
    StructField("phone", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
CUSTOMER_ADDRESS_DDL = """\
-- Delta Lake table migrated from Ab Initio DML: customer_address.dml
-- Customer address record with embedded address struct (from common_address.dml)
--
-- Migration Notes:
--   - The address column is a STRUCT that resolves Ab Initio's type reference
--     (address_t from common_address.dml). The include/type system in Ab Initio
--     is analogous to Spark's nested StructType.
--   - In Spark SQL, access nested fields via dot notation: address.street, address.city
CREATE TABLE IF NOT EXISTS customer_address (
    customer_id   BIGINT       NOT NULL  COMMENT 'Ab Initio decimal (bare) → BIGINT identifier',
    name          STRING                 COMMENT 'Ab Initio string → STRING',
    address       STRUCT<
                      street: STRING,
                      city: STRING,
                      state: STRING,
                      zip: STRING
                  >                      COMMENT 'Ab Initio address_t (from common_address.dml) → STRUCT',
    phone         STRING                 COMMENT 'Ab Initio string → STRING'
)
USING DELTA
COMMENT 'Customer address with nested address struct — migrated from Ab Initio DML (customer_address.dml)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'migration.source'                 = 'abinitio',
    'migration.source_dml'             = 'dml/customer_address.dml'
);
"""
