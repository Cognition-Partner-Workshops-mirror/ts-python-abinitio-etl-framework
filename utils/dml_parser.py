"""Ab Initio DML file parser for schema extraction."""
import re
import logging
from typing import Dict, List, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class DMLParser:
    """
    Parses Ab Initio DML (Data Manipulation Language) files to extract schema metadata.
    DML files define record layouts used across Ab Initio components.
    """

    TYPE_MAP = {
        "integer": "INTEGER",
        "decimal": "DECIMAL",
        "string": "VARCHAR",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "double": "DOUBLE",
        "long": "BIGINT",
    }

    def parse_file(self, dml_path: str) -> Dict[str, Any]:
        """Parse a DML file and return schema metadata."""
        path = Path(dml_path)
        if not path.exists():
            raise FileNotFoundError(f"DML file not found: {dml_path}")
        content = path.read_text(encoding="utf-8")
        return self.parse_content(content, record_name=path.stem)

    def parse_content(self, content: str, record_name: str = "record") -> Dict[str, Any]:
        """Parse DML content string into a schema dict."""
        fields = []
        in_record = False
        record_name_found = record_name

        for line in content.split("\n"):
            line = line.strip()
            if not line or line.startswith("//") or line.startswith("#"):
                continue

            # Record definition start
            record_match = re.match(r"^record\s+(\w+)\s*\{?$", line, re.IGNORECASE)
            if record_match:
                in_record = True
                record_name_found = record_match.group(1)
                continue

            if line == "}" and in_record:
                in_record = False
                continue

            if in_record:
                field = self._parse_field_line(line)
                if field:
                    fields.append(field)

        return {
            "record_name": record_name_found,
            "field_count": len(fields),
            "fields": fields,
        }

    def _parse_field_line(self, line: str) -> Dict[str, Any]:
        """Parse a single DML field definition line."""
        # Pattern: type(size) field_name; or type field_name;
        patterns = [
            r"^([\w]+)\s*\((\d+)(?:,(\d+))?\)\s+(\w+)\s*;",  # type(size) name;
            r"^([\w]+)\s+(\w+)\s*;",                             # type name;
        ]
        for pattern in patterns:
            m = re.match(pattern, line)
            if m:
                groups = m.groups()
                if len(groups) == 4:  # type(size,scale) name
                    raw_type, size, scale, field_name = groups
                    sql_type = self.TYPE_MAP.get(raw_type.lower(), raw_type.upper())
                    if size:
                        sql_type += f"({size},{scale})" if scale else f"({size})"
                elif len(groups) == 2:  # type name
                    raw_type, field_name = groups
                    sql_type = self.TYPE_MAP.get(raw_type.lower(), raw_type.upper())
                else:
                    continue
                return {"name": field_name, "dml_type": raw_type, "sql_type": sql_type}
        return None

    def to_create_table_sql(self, schema: Dict[str, Any], dialect: str = "snowflake") -> str:
        """Generate a CREATE TABLE SQL statement from parsed DML schema."""
        table_name = schema["record_name"].upper()
        col_defs = ",\n    ".join([f"{f['name'].upper()} {f['sql_type']}" for f in schema["fields"]])
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {col_defs}\n);"
