# Ab Initio DML → Delta Lake / PySpark Type Mapping Reference

This document describes the type mapping decisions applied when converting Ab Initio DML record layouts to PySpark StructType definitions and Delta Lake `CREATE TABLE` DDL.

---

## Type Mapping Table

| Ab Initio DML Type | Example | PySpark Type | Delta Lake SQL Type | Rationale |
|---|---|---|---|---|
| `decimal` (bare, delimiter-only) | `decimal(",") customer_id` | `LongType()` | `BIGINT` | Bare decimal with no precision/scale is used as an integer identifier in Ab Initio. LongType (64-bit signed) provides sufficient range for ID columns and avoids unnecessary DECIMAL overhead. |
| `decimal("p.s")` | `decimal("8.2") balance` | `DecimalType(p, s)` | `DECIMAL(p,s)` | The format string specifies precision and scale. Maps directly to Spark's fixed-point DecimalType for exact numeric representation (critical for monetary values). |
| `string` (delimiter-only) | `string(",") name` | `StringType()` | `STRING` | Ab Initio strings are variable-length. Delimiter metadata controls flat-file serialization and is irrelevant in Delta Lake's columnar Parquet storage. |
| `string(n)` (fixed-width) | `string(20) account_name` | `StringType()` | `STRING` | Fixed-width strings define byte-level layout in mainframe records. Parquet stores all strings as variable-length. Original width is documented in comments; use CHECK constraints if enforcement is needed. |
| `date("fmt")` | `date("YYYY-MM-DD") opened_date` | `DateType()` | `DATE` | Format string is serialization metadata. Delta Lake uses Parquet's native DATE type (days since epoch). Format is used only during data ingestion/parsing, not in the schema. |
| `datetime("fmt")` | `datetime("YYYY-MM-DD HH24:MI:SS") ts` | `TimestampType()` | `TIMESTAMP` | Same as date — format governs flat-file representation. Parquet stores TIMESTAMP natively at microsecond precision. |
| `packed_decimal(n)` | `packed_decimal(5) account_num` | `DecimalType(n, 0)` | `DECIMAL(n,0)` | Mainframe BCD (Binary Coded Decimal) — two digits per byte plus sign nibble. DECIMAL preserves the explicit precision constraint from the COBOL copybook / Ab Initio DML. |
| `packed_decimal("p.s")` | `packed_decimal("7.2") balance` | `DecimalType(p, s)` | `DECIMAL(p,s)` | Packed decimal with fractional digits. Maps directly to DecimalType(7, 2). |
| `zoned_decimal(n)` | `zoned_decimal(4) status_code` | `DecimalType(n, 0)` | `DECIMAL(n,0)` | Mainframe EBCDIC zoned decimal — one digit per byte. Same mapping as packed_decimal for precision preservation. |
| `void` | `void(",") padding1` | **SKIPPED** | **OMITTED** | Padding/filler bytes for flat-file record alignment. No business data. Columnar Parquet storage does not require fixed-width padding. |
| Nested `record ... end name` | `record ... end merchant_info` | `StructType([...])` | `STRUCT<...>` | Inline nested records map to Spark STRUCT columns, preserving logical grouping without requiring separate tables. |
| `record[count] ... end name` | `record[item_count] ... end line_items` | `ArrayType(StructType([...]))` | `ARRAY<STRUCT<...>>` | Variable-length record arrays map to ARRAY of STRUCT. The count field is retained for compatibility but is redundant in Spark (use `size()` function). |
| `if (cond) record ... end name` | `if (txn_type == 2) record ... end refund_details` | `StructType([...])` (nullable) | `STRUCT<...>` (nullable) | Conditional records become nullable STRUCT columns. NULL when condition is false, populated when true. The condition becomes application logic, not schema-level. |
| `type name = record ... end` | `type address_t = record ... end` | `StructType([...])` (reusable) | *(no standalone table)* | Reusable type definitions become shared StructType constants imported by other schemas. No Delta table is created for type-only definitions. |
| `include "file.dml"` | `include "common_address.dml"` | Python `import` | *(resolved at definition time)* | Include directives are resolved by importing the corresponding Python module's StructType. |
| `null("sentinel")` modifier | `string(",", null("")) name` | `nullable=True` | *(column is nullable)* | Sentinel values (empty string, "UNKNOWN", etc.) are replaced with proper SQL NULLs during data migration. The schema marks the column as nullable. |

---

## Delimiter Handling

Ab Initio DML uses delimiter annotations (e.g., `","`, `"|"`, `"\n"`) to describe flat-file serialization format. These delimiters are **not carried into the Delta Lake schema** because:

1. Delta Lake stores data in **columnar Parquet** format — each field is stored independently, not as delimited text.
2. Delimiters are only relevant during **data ingestion** (reading the legacy flat files). Use `spark.read.csv(..., sep="|")` or similar when loading source data.
3. The last field typically uses `"\n"` as its delimiter, indicating end-of-record in the flat file.

---

## Array Length Control Fields

Ab Initio uses a pattern where a `decimal` field (e.g., `item_count`) precedes an array field and defines the array's runtime length (e.g., `string[item_count]`). In the migrated schema:

- The **control field is retained** (e.g., `item_count INT`) for backward compatibility and as documentation.
- In Spark SQL, use `size(array_column)` to get the array length dynamically — the control field becomes redundant.
- Data validation should ensure `item_count == size(item_names)` during migration.

---

## Void / Padding Fields

Ab Initio `void` fields reserve space in fixed-width flat files for record alignment. They are:

- **Omitted** from the Delta Lake schema (no business value).
- **Documented** in code comments and DDL migration notes for audit trail.
- If downstream processes depend on field ordinal positions, use explicit column ordering in `SELECT` statements.

---

## Conditional Records

Ab Initio's `if (condition) record ... end name` pattern creates fields that are conditionally present based on another field's value. In Delta Lake:

- These map to **nullable STRUCT** columns.
- The column is `NULL` when the condition evaluates to false.
- Application logic (Spark SQL `WHEN`/`CASE`, Python `if`) must enforce the condition during writes.
- Example: `refund_details` is `NULL` unless `txn_type == 2`.

---

## Mainframe Numeric Types

The `packed_decimal` and `zoned_decimal` types originate from mainframe (COBOL/EBCDIC) data processed through Ab Initio:

| Mainframe Type | Storage | Ab Initio | Spark |
|---|---|---|---|
| Packed Decimal (COMP-3) | 2 digits/byte + sign nibble | `packed_decimal(p)` or `packed_decimal("p.s")` | `DecimalType(p, s)` |
| Zoned Decimal | 1 digit/byte (EBCDIC zones) | `zoned_decimal(n)` | `DecimalType(n, 0)` |

We use `DecimalType` (not `IntegerType`/`LongType`) to preserve the **explicit precision constraints** from the original mainframe layout. This ensures:
- No silent precision loss during migration.
- Schema documentation matches the source COBOL copybook.
- Downstream consumers can validate data against original field sizes.

---

## DML Files Migrated

| DML File | Delta Table | Key Features |
|---|---|---|
| `customer.dml` | `customer` | Simple flat record |
| `account_balance.dml` | `account_balance` | DATE, DECIMAL(p,s) |
| `order_items.dml` | `order_items` | Variable-length arrays (ARRAY<STRING>, ARRAY<BIGINT>) |
| `transaction_detail.dml` | `transaction_detail` | Nested STRUCT, ARRAY<STRUCT>, conditional nullable STRUCT, null sentinels |
| `packed_account.dml` | `packed_account` | Mainframe packed_decimal, zoned_decimal |
| `account_status.dml` | `account_status` | void fields (omitted as padding) |
| `customer_address.dml` | `customer_address` | Include/type reference (address_t → nested STRUCT) |
| `common_address.dml` | *(reusable type only)* | Type definition (address_t), no standalone table |
