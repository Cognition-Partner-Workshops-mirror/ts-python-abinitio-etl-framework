"""
Databricks SQL alert definitions — migrated from Ab Initio AutoSys monitoring.

Replaces monitoring/job_monitor.py which:
  - Polled AutoSys REST API for job status (get_job_status)
  - Checked SLA windows and flagged breaches (check_sla)
  - Sent Slack alerts on failure or breach (_send_alert)
  - Polled job lists until all complete or max_polls reached (monitor_jobs)

In Databricks, this monitoring is replaced by:
  - Databricks SQL Alerts: scheduled SQL queries that evaluate a condition
    and fire notifications (email, Slack, PagerDuty) when triggered
  - system.workflow.job_run_timeline: built-in system table that tracks
    all job run history, durations, and outcomes (replaces AutoSys API)
  - Webhook notifications on workflow JSON (replaces Slack webhook in _send_alert)

Each alert definition below contains:
  - query_sql: the Databricks SQL query to execute on schedule
  - trigger_condition: the column/operator/value that triggers the alert
  - schedule: how often to evaluate (Quartz cron or interval)
  - notifications: placeholder for email/Slack/PagerDuty destinations
  - migration_notes: mapping back to the original Ab Initio monitoring
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Alert definition constants — each maps to one monitoring/job_monitor.py flow
# ---------------------------------------------------------------------------
ALERT_DEFINITIONS: Dict[str, Dict[str, Any]] = {

    # -----------------------------------------------------------------------
    # Alert 1: Job Failure Detection
    # Replaces: JobMonitor.monitor_jobs() → status == "FAILURE" → _send_alert()
    # In Ab Initio, the monitor polled AutoSys API every 60s for up to 60 polls.
    # In Databricks, the system.workflow tables are updated automatically.
    # -----------------------------------------------------------------------
    "job_failure_alert": {
        "display_name": "ETL Job Failure Alert",
        "description": (
            "Detects failed Databricks workflow runs for migrated Ab Initio pipelines. "
            "Replaces monitoring/job_monitor.py AutoSys polling + Slack alert."
        ),

        # Query runs against Databricks system tables (available in Unity Catalog)
        # system.workflow.job_run_timeline tracks every job run with outcome
        "query_sql": """
-- Job Failure Detection Alert
-- Migrated from: monitoring/job_monitor.py → JobMonitor.monitor_jobs()
-- Original: polled AutoSys REST API every 60s, sent Slack on FAILURE status
-- Databricks: queries system.workflow tables for failed runs in the last hour

SELECT
    jr.job_id,
    jr.run_id,
    j.name                                          AS job_name,
    jr.result_state                                 AS status,
    jr.start_time,
    jr.end_time,
    TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time) AS duration_minutes,
    jr.state_message                                AS error_message,
    -- Tag-based filtering for migrated Ab Initio pipelines
    j.settings.tags['migration_source']             AS migration_source,
    j.settings.tags['legacy_job']                   AS legacy_autosys_job
FROM
    system.workflow.job_run_timeline jr
    JOIN system.workflow.jobs j ON jr.job_id = j.job_id
WHERE
    -- Only look at runs that completed in the last hour
    jr.end_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
    -- Only failed runs
    AND jr.result_state IN ('FAILED', 'TIMED_OUT', 'CANCELED')
    -- Only migrated Ab Initio pipelines (tagged in workflow JSON)
    AND j.settings.tags['migration_source'] = 'abinitio'
ORDER BY
    jr.end_time DESC
""",

        # Alert triggers when the query returns one or more rows (any failures)
        "trigger_condition": {
            "op": "GREATER_THAN",
            "operand": {"column": "rows_count"},
            "threshold": 0,
            "_migration_note": (
                "Ab Initio: if status == 'FAILURE' → _send_alert(job_name, status, 'JOB_FAILURE'). "
                "Databricks: alert fires when query returns > 0 rows (any failed run)."
            ),
        },

        # Evaluate every 5 minutes — more responsive than Ab Initio's 60s * 60 polls model
        # because the system table is updated in near real-time
        "schedule": {
            "interval_minutes": 5,
            "quartz_cron_expression": "0 0/5 * * * ?",
            "_migration_note": (
                "Ab Initio: JobMonitor polled every 60s for up to 60 polls (1 hour window). "
                "Databricks SQL Alert runs every 5 min with a 1-hour lookback — simpler, stateless."
            ),
        },

        "notifications": {
            "email": {
                "recipients": [],
                "_migration_note": "Add team distribution list. Replaces AutoSys job notification.",
            },
            "slack_webhook": {
                "url": "",
                "_migration_note": (
                    "Add Slack incoming webhook URL. Replaces job_monitor.py slack_webhook parameter "
                    "and _send_alert() POST to webhook. Emoji mapping: FAILURE → :red_circle:."
                ),
            },
            "pagerduty": {
                "integration_key": "",
                "_migration_note": "Optional PagerDuty integration for critical pipeline failures.",
            },
        },

        "severity": "critical",
        "tags": ["etl", "abinitio-migration", "job-failure"],
    },

    # -----------------------------------------------------------------------
    # Alert 2: SLA Breach Detection
    # Replaces: JobMonitor.check_sla() → sla_breached → _send_alert(SLA_BREACH)
    # and SLATracker.record_completion() → sla_met evaluation
    # In Ab Initio, SLA was checked by comparing completion time against a deadline.
    # In Databricks, we query the system table for runs exceeding their timeout.
    # -----------------------------------------------------------------------
    "sla_breach_alert": {
        "display_name": "ETL SLA Breach Alert",
        "description": (
            "Detects Ab Initio pipeline runs that exceeded their SLA window. "
            "Replaces monitoring/job_monitor.py check_sla() + monitoring/sla_tracker.py record_completion()."
        ),

        "query_sql": """
-- SLA Breach Detection Alert
-- Migrated from: monitoring/job_monitor.py → JobMonitor.check_sla()
-- Original: compared datetime.utcnow() against expected_complete_by deadline
-- Also replaces: sla_tracker.py → SLATracker.record_completion() sla_met check
-- Databricks: compares actual duration against configured timeout_seconds

SELECT
    jr.job_id,
    jr.run_id,
    j.name                                          AS job_name,
    jr.result_state                                 AS status,
    jr.start_time,
    jr.end_time,
    TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time) AS actual_duration_minutes,
    -- SLA is defined by timeout_seconds in workflow JSON
    ROUND(j.settings.timeout_seconds / 60.0, 0)    AS sla_limit_minutes,
    -- How far over the SLA window the run went
    TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time)
        - ROUND(j.settings.timeout_seconds / 60.0, 0) AS minutes_over_sla,
    j.settings.tags['legacy_job']                   AS legacy_autosys_job,
    j.settings.tags['domain']                       AS domain
FROM
    system.workflow.job_run_timeline jr
    JOIN system.workflow.jobs j ON jr.job_id = j.job_id
WHERE
    -- Completed in the last 24 hours (daily check window)
    jr.end_time >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    -- Only runs that actually completed (not still running)
    AND jr.result_state IS NOT NULL
    -- Duration exceeded the configured timeout (SLA proxy)
    AND TIMESTAMPDIFF(SECOND, jr.start_time, jr.end_time) > j.settings.timeout_seconds
    -- Only migrated Ab Initio pipelines
    AND j.settings.tags['migration_source'] = 'abinitio'
ORDER BY
    minutes_over_sla DESC
""",

        "trigger_condition": {
            "op": "GREATER_THAN",
            "operand": {"column": "rows_count"},
            "threshold": 0,
            "_migration_note": (
                "Ab Initio: check_sla() compared utcnow() > expected_complete_by. "
                "Databricks: alert fires when any run's duration > timeout_seconds."
            ),
        },

        # Evaluate hourly — SLA breaches are less time-sensitive than failures
        "schedule": {
            "interval_minutes": 60,
            "quartz_cron_expression": "0 0 * * * ?",
            "_migration_note": (
                "Ab Initio: check_sla() called per-job inline during monitoring. "
                "Databricks: single hourly query catches all SLA breaches across all pipelines."
            ),
        },

        "notifications": {
            "email": {
                "recipients": [],
                "_migration_note": "Add team distribution list. Replaces AutoSys SLA notification.",
            },
            "slack_webhook": {
                "url": "",
                "_migration_note": (
                    "Add Slack incoming webhook URL. Replaces _send_alert() with alert_type='SLA_BREACH'. "
                    "Emoji mapping: SLA_BREACH → :warning:."
                ),
            },
        },

        "severity": "high",
        "tags": ["etl", "abinitio-migration", "sla-breach"],
    },

    # -----------------------------------------------------------------------
    # Alert 3: Long-Running Job Detection
    # Replaces: JobMonitor.monitor_jobs() poll loop that ran up to max_polls * interval
    # In Ab Initio, jobs that ran beyond max_polls * poll_interval were abandoned.
    # In Databricks, we detect currently-running jobs that exceed 80% of their timeout.
    # -----------------------------------------------------------------------
    "long_running_job_alert": {
        "display_name": "Long-Running ETL Job Warning",
        "description": (
            "Early warning for jobs approaching their SLA deadline. "
            "Replaces the implicit timeout in monitoring/job_monitor.py monitor_jobs() "
            "where max_polls * poll_interval_seconds defined the monitoring window."
        ),

        "query_sql": """
-- Long-Running Job Early Warning
-- Migrated from: monitoring/job_monitor.py → JobMonitor.monitor_jobs()
-- Original: max_polls=60, poll_interval=60s → 1 hour monitoring window
-- Databricks: detects running jobs that have exceeded 80% of their timeout

SELECT
    jr.job_id,
    jr.run_id,
    j.name                                          AS job_name,
    jr.start_time,
    TIMESTAMPDIFF(MINUTE, jr.start_time, CURRENT_TIMESTAMP()) AS running_minutes,
    ROUND(j.settings.timeout_seconds / 60.0, 0)    AS timeout_minutes,
    -- Percentage of timeout consumed
    ROUND(
        TIMESTAMPDIFF(SECOND, jr.start_time, CURRENT_TIMESTAMP())
        * 100.0 / j.settings.timeout_seconds, 1
    )                                               AS pct_timeout_used,
    j.settings.tags['legacy_job']                   AS legacy_autosys_job,
    j.settings.tags['domain']                       AS domain
FROM
    system.workflow.job_run_timeline jr
    JOIN system.workflow.jobs j ON jr.job_id = j.job_id
WHERE
    -- Currently running (no end_time yet)
    jr.result_state IS NULL
    AND jr.end_time IS NULL
    -- Has been running for more than 80% of its timeout
    AND TIMESTAMPDIFF(SECOND, jr.start_time, CURRENT_TIMESTAMP())
        > (j.settings.timeout_seconds * 0.8)
    -- Only migrated Ab Initio pipelines
    AND j.settings.tags['migration_source'] = 'abinitio'
ORDER BY
    pct_timeout_used DESC
""",

        "trigger_condition": {
            "op": "GREATER_THAN",
            "operand": {"column": "rows_count"},
            "threshold": 0,
            "_migration_note": (
                "Ab Initio: monitor_jobs() had an implicit 1-hour window (60 polls * 60s). "
                "Databricks: proactive alert when a job exceeds 80% of its configured timeout."
            ),
        },

        "schedule": {
            "interval_minutes": 10,
            "quartz_cron_expression": "0 0/10 * * * ?",
            "_migration_note": (
                "Check every 10 minutes for jobs approaching their timeout limit."
            ),
        },

        "notifications": {
            "email": {
                "recipients": [],
                "_migration_note": "Add on-call engineer email for early warning.",
            },
            "slack_webhook": {
                "url": "",
                "_migration_note": "Add Slack webhook for #data-engineering-alerts channel.",
            },
        },

        "severity": "warning",
        "tags": ["etl", "abinitio-migration", "long-running"],
    },

    # -----------------------------------------------------------------------
    # Alert 4: Data Quality Threshold Breach
    # New alert — extends the max_errors concept from Ab Initio
    # In Ab Initio, AI_MAX_ERRORS=100 + AI_ERROR_ACTION=ABORT controlled error limits.
    # In Databricks, we can query the Delta transaction log for rejected records.
    # -----------------------------------------------------------------------
    "data_quality_alert": {
        "display_name": "ETL Data Quality Threshold Alert",
        "description": (
            "Monitors data quality metrics in migrated pipelines. "
            "Extends Ab Initio AI_MAX_ERRORS threshold concept (setenv.ksh: AI_MAX_ERRORS=100, "
            "AI_ERROR_ACTION=ABORT) with Databricks table-level quality tracking."
        ),

        "query_sql": """
-- Data Quality Threshold Alert
-- Extends: setenv.ksh AI_MAX_ERRORS=100, AI_ERROR_ACTION=ABORT
-- In Ab Initio, errors were counted per partition and ABORT halted the graph.
-- In Databricks, we track quality metrics via table properties and audit tables.

SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    tp.value                                        AS max_errors_threshold,
    -- Count recent quality failures from the audit/quality log table
    COALESCE(q.error_count, 0)                      AS recent_error_count,
    CASE
        WHEN COALESCE(q.error_count, 0) > CAST(tp.value AS INT)
        THEN 'THRESHOLD_EXCEEDED'
        ELSE 'WITHIN_LIMITS'
    END                                             AS quality_status
FROM
    information_schema.tables t
    -- Tables with a configured max_errors threshold
    JOIN information_schema.table_properties tp
        ON t.table_catalog = tp.table_catalog
        AND t.table_schema = tp.table_schema
        AND t.table_name = tp.table_name
        AND tp.key = 'migration.max_errors'
    -- Left join to quality metrics (populated by notebook error handling)
    LEFT JOIN (
        SELECT
            target_table,
            COUNT(*) AS error_count
        FROM
            main.audit.data_quality_log
        WHERE
            logged_at >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
        GROUP BY target_table
    ) q ON q.target_table = CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name)
WHERE
    -- Only tables from migrated pipelines
    tp.key = 'migration.max_errors'
    AND COALESCE(q.error_count, 0) > CAST(tp.value AS INT)
""",

        "trigger_condition": {
            "op": "GREATER_THAN",
            "operand": {"column": "rows_count"},
            "threshold": 0,
            "_migration_note": (
                "Ab Initio: AI_MAX_ERRORS=100 per partition, AI_ERROR_ACTION=ABORT. "
                "Databricks: alert fires when any table's error count exceeds its threshold."
            ),
        },

        "schedule": {
            "interval_minutes": 30,
            "quartz_cron_expression": "0 0/30 * * * ?",
            "_migration_note": "Check quality metrics twice per hour.",
        },

        "notifications": {
            "email": {
                "recipients": [],
                "_migration_note": "Add data steward email for quality notifications.",
            },
            "slack_webhook": {
                "url": "",
                "_migration_note": "Add Slack webhook for #data-quality channel.",
            },
        },

        "severity": "high",
        "tags": ["etl", "abinitio-migration", "data-quality"],
    },
}


# ---------------------------------------------------------------------------
# Lookup and validation helpers
# ---------------------------------------------------------------------------

def get_alert_definition(alert_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve a single alert definition by name.

    Args:
        alert_name: Key from ALERT_DEFINITIONS (e.g. 'job_failure_alert').

    Returns:
        Alert definition dict, or None if not found.
    """
    return ALERT_DEFINITIONS.get(alert_name)


def get_all_alert_names() -> List[str]:
    """Return the names of all defined alerts."""
    return list(ALERT_DEFINITIONS.keys())


def validate_alert_definition(alert_name: str) -> Dict[str, Any]:
    """
    Validate that an alert definition has all required fields.

    Checks for query_sql, trigger_condition, schedule, and notifications.
    Returns a dict with 'valid', 'errors', and 'warnings' keys.

    Args:
        alert_name: Key from ALERT_DEFINITIONS.

    Returns:
        Validation result dict with 'valid' bool and any 'errors'/'warnings'.
    """
    defn = ALERT_DEFINITIONS.get(alert_name)
    if defn is None:
        return {"valid": False, "errors": [f"Alert '{alert_name}' not found"], "warnings": []}

    errors = []
    warnings = []

    # Required fields
    for field in ["display_name", "query_sql", "trigger_condition", "schedule", "notifications"]:
        if field not in defn:
            errors.append(f"Missing required field: {field}")

    # Validate query_sql is non-empty
    if defn.get("query_sql") and not defn["query_sql"].strip():
        errors.append("query_sql is empty")

    # Validate trigger_condition has operator and threshold
    trigger = defn.get("trigger_condition", {})
    if "op" not in trigger:
        errors.append("trigger_condition missing 'op' field")
    if "threshold" not in trigger:
        errors.append("trigger_condition missing 'threshold' field")

    # Validate schedule has either interval or cron
    schedule = defn.get("schedule", {})
    if not schedule.get("interval_minutes") and not schedule.get("quartz_cron_expression"):
        errors.append("schedule must have interval_minutes or quartz_cron_expression")

    # Warn if no notification destinations configured
    notifs = defn.get("notifications", {})
    has_destination = False
    for channel in ["email", "slack_webhook", "pagerduty"]:
        channel_config = notifs.get(channel, {})
        # Check if any actual destination is configured (not just placeholders)
        if channel == "email" and channel_config.get("recipients"):
            has_destination = True
        elif channel in ("slack_webhook", "pagerduty"):
            for key in ("url", "integration_key"):
                if channel_config.get(key):
                    has_destination = True
    if not has_destination:
        warnings.append(
            "No notification destinations configured — add email recipients, "
            "Slack webhook URL, or PagerDuty integration key before activating."
        )

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}
