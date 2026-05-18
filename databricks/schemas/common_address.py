# --------------------------------------------------------------------------
# common_address.py — Databricks Delta Lake schema for Ab Initio common_address.dml
#
# Source DML (dml/common_address.dml):
#   type address_t = record
#     string(",") street;
#     string(",") city;
#     string(",") state;
#     string(",") zip;
#   end;
#
# Type Mapping Decisions:
#   - Ab Initio named type (address_t) → reusable PySpark StructType
#   - This is a shared/included type used by customer_address.dml
#   - string → StringType: direct 1:1 mapping
#   - No standalone Delta table — this is an embedded struct type
# --------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType

# Reusable address struct — corresponds to Ab Initio "type address_t"
# Imported by customer_address.py (mirrors the DML include pattern)
address_schema = StructType([
    StructField("street", StringType(), nullable=True),   # Ab Initio: string(",")
    StructField("city", StringType(), nullable=True),     # Ab Initio: string(",")
    StructField("state", StringType(), nullable=True),    # Ab Initio: string(",")
    StructField("zip", StringType(), nullable=True),      # Ab Initio: string(",")
])

# No standalone DDL — this type is embedded in customer_address and other tables
# See customer_address.py for the Delta Lake DDL that uses this struct
