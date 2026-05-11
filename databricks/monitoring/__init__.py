"""
Databricks monitoring definitions migrated from Ab Initio AutoSys monitoring.

Converts monitoring/job_monitor.py (AutoSys REST polling + Slack alerts) and
monitoring/sla_tracker.py (90-day SLA compliance reports) into Databricks-native
SQL alert definitions, dashboard queries, and alerting configuration.

Modules:
    alert_definitions  - SQL alert queries for job failure and SLA breach detection
    sla_dashboard      - SQL dashboard queries for SLA compliance reporting

See monitoring/job_monitor.py and monitoring/sla_tracker.py for the legacy
implementations these definitions replace.
"""

from databricks.monitoring.alert_definitions import (
    ALERT_DEFINITIONS,
    get_alert_definition,
    get_all_alert_names,
    validate_alert_definition,
)
from databricks.monitoring.sla_dashboard import (
    DASHBOARD_QUERIES,
    get_dashboard_query,
    get_all_dashboard_query_names,
)

__all__ = [
    "ALERT_DEFINITIONS",
    "get_alert_definition",
    "get_all_alert_names",
    "validate_alert_definition",
    "DASHBOARD_QUERIES",
    "get_dashboard_query",
    "get_all_dashboard_query_names",
]
