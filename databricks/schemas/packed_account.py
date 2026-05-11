"""
Delta Lake schema for packed_account.dml — Packed/zoned decimal (mainframe format).

Source DML (Ab Initio):
    record
      packed_decimal(5) account_num;
      packed_decimal("7.2") balance;
      zoned_decimal(4) status_code;
      string(20) account_name;
    end;

This DML represents a fixed-width mainframe-style record using packed and zoned
decimal types — common in COBOL/mainframe data processed through Ab Initio.

Type Mapping Applied:
    Ab Initio packed_decimal(n) → Spark DecimalType(n, 0)
        Rationale: Packed decimal (BCD — Binary Coded Decimal) stores two digits
        per byte plus a sign nibble. In Ab Initio, packed_decimal(5) means 5 digits
        of precision with no fractional part. In Spark, DecimalType(5, 0) preserves
        the exact precision. We use DECIMAL rather than BIGINT to maintain the
        explicit precision constraint from the mainframe layout.
    Ab Initio packed_decimal("p.s") → Spark DecimalType(p, s)
        Rationale: packed_decimal("7.2") specifies precision=7, scale=2 (7 total
        digits, 2 after the decimal point). Maps directly to DecimalType(7, 2).
    Ab Initio zoned_decimal(n) → Spark DecimalType(n, 0)
        Rationale: Zoned decimal stores one digit per byte (EBCDIC zones). Similar
        to packed_decimal but less storage-efficient. The precision is preserved as
        DecimalType(4, 0) for a 4-digit zoned field.
    Ab Initio string(n) [fixed-width] → Spark StringType
        Rationale: Fixed-width strings (e.g., string(20)) define byte-level record
        layout in mainframe files. In Delta Lake, all strings are variable-length
        in Parquet. The original width (20) is documented in comments but not
        enforced at the schema level — use CHECK constraints or data quality rules
        if length enforcement is needed.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
)


# PySpark StructType definition for the packed_account record
PACKED_ACCOUNT_SCHEMA = StructType([
    # Ab Initio: packed_decimal(5) account_num → DecimalType(5, 0)
    # Mainframe BCD: 5 digits, no fractional part
    StructField("account_num", DecimalType(5, 0), nullable=False),
    # Ab Initio: packed_decimal("7.2") balance → DecimalType(7, 2)
    # Mainframe BCD: 7 total digits, 2 after decimal point
    StructField("balance", DecimalType(7, 2), nullable=True),
    # Ab Initio: zoned_decimal(4) status_code → DecimalType(4, 0)
    # Mainframe EBCDIC zoned: 4 digits, no fractional part
    StructField("status_code", DecimalType(4, 0), nullable=True),
    # Ab Initio: string(20) account_name → StringType
    # Fixed-width 20 chars in mainframe layout; variable-length in Delta/Parquet
    StructField("account_name", StringType(), nullable=True),
])

# Delta Lake CREATE TABLE DDL
PACKED_ACCOUNT_DDL = """\
-- Delta Lake table migrated from Ab Initio DML: packed_account.dml
-- Mainframe-format packed/zoned decimal record
--
-- Migration Notes:
--   - packed_decimal and zoned_decimal are mainframe BCD/EBCDIC formats.
--     They map to DECIMAL with explicit precision to preserve the original
--     field constraints from the COBOL copybook / Ab Initio DML.
--   - string(20) becomes STRING (Parquet is variable-length). Add a CHECK
--     constraint if the 20-char limit must be enforced:
--     ALTER TABLE packed_account ADD CONSTRAINT chk_name_len CHECK (length(account_name) <= 20);
CREATE TABLE IF NOT EXISTS packed_account (
    account_num    DECIMAL(5,0)   NOT NULL  COMMENT 'Ab Initio packed_decimal(5) → DECIMAL(5,0) mainframe BCD',
    balance        DECIMAL(7,2)             COMMENT 'Ab Initio packed_decimal("7.2") → DECIMAL(7,2) mainframe BCD',
    status_code    DECIMAL(4,0)             COMMENT 'Ab Initio zoned_decimal(4) → DECIMAL(4,0) EBCDIC zoned',
    account_name   STRING                   COMMENT 'Ab Initio string(20) → STRING (was fixed-width 20 chars)'
)
USING DELTA
COMMENT 'Packed/zoned decimal account record (mainframe format) — migrated from Ab Initio DML (packed_account.dml)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'migration.source'                 = 'abinitio',
    'migration.source_dml'             = 'dml/packed_account.dml'
);
"""
