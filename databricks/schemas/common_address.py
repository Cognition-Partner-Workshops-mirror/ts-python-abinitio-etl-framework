# -----------------------------------------------------------------------------
# Delta Lake Schema: common_address (reusable type definition)
# Source: dml/common_address.dml
#
# Type Mapping:
#   type address_t = record -> Spark StructType (reusable nested struct)
#   string(",")             -> StringType
#
# In Ab Initio, `type ... = record` defines a reusable named type that can be
# included via `include` in other DML files. In Spark, this maps to a StructType
# that is embedded as a nested column in parent schemas.
# -----------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType

address_type_schema = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
])

# No standalone DDL — this type is embedded in tables that reference it
# (e.g., customer_address). Provided here for documentation and reuse.
COMMON_ADDRESS_DDL = """
-- Reusable type definition (no standalone table).
-- Embedded as STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>
-- in tables that reference address_t.
-- Source: dml/common_address.dml (type address_t = record)
"""
