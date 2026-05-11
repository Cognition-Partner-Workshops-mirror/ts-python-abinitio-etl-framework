"""
Databricks SQL dashboard queries — migrated from Ab Initio SLA tracker.

Replaces monitoring/sla_tracker.py which:
  - Tracked per-job SLA compliance with a JSON state file (90-day retention)
  - Registered jobs with sla_window_end (e.g. "06:00" = complete by 6am UTC)
  - Recorded each completion and evaluated sla_met (completed_at <= sla_deadline)
  - Generated compliance reports: total_runs, sla_met, sla_compliance_pct, avg_duration

In Databricks, this is replaced by SQL dashboard queries against system tables:
  - system.workflow.job_run_timeline: complete run history with start/end times
  - system.workflow.jobs: job configuration including timeout, tags, schedule
  - No JSON state file needed — system tables are the source of truth
  - Retention is unlimited (vs sla_tracker.py's 90-day window)
  - Dashboards can be pinned to Databricks SQL workspace for live monitoring

Each query below maps to a SLATracker method or report section.
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dashboard query definitions — each maps to one sla_tracker.py report section
# ---------------------------------------------------------------------------
DASHBOARD_QUERIES: Dict[str, Dict[str, Any]] = {

    # -----------------------------------------------------------------------
    # Query 1: SLA Compliance Summary (7-day)
    # Replaces: SLATracker.generate_report(days=7)
    # Original returned: total_runs, sla_met, sla_compliance_pct, avg_duration_min
    # -----------------------------------------------------------------------
    "sla_compliance_summary": {
        "display_name": "SLA Compliance Summary — Migrated ETL Pipelines",
        "description": (
            "7-day SLA compliance summary across all migrated Ab Initio pipelines. "
            "Replaces monitoring/sla_tracker.py → SLATracker.generate_report(days=7)."
        ),

        "query_sql": """
-- SLA Compliance Summary (7-day rolling window)
-- Migrated from: monitoring/sla_tracker.py → SLATracker.generate_report(days=7)
-- Original: read JSON state file, filtered to runs with date >= cutoff,
--   computed sla_met_count / total_runs per job
-- Databricks: queries system.workflow tables directly — no state file needed

SELECT
    j.name                                          AS job_name,
    j.settings.tags['domain']                       AS domain,
    j.settings.tags['legacy_job']                   AS legacy_autosys_job,
    -- Total runs in the reporting window
    COUNT(*)                                        AS total_runs,
    -- SLA met: completed within timeout_seconds of start
    SUM(
        CASE WHEN jr.result_state IN ('SUCCESS')
            AND TIMESTAMPDIFF(SECOND, jr.start_time, jr.end_time)
                <= j.settings.timeout_seconds
        THEN 1 ELSE 0 END
    )                                               AS sla_met,
    -- SLA compliance percentage (maps to sla_compliance_pct in sla_tracker.py)
    ROUND(
        SUM(
            CASE WHEN jr.result_state IN ('SUCCESS')
                AND TIMESTAMPDIFF(SECOND, jr.start_time, jr.end_time)
                    <= j.settings.timeout_seconds
            THEN 1 ELSE 0 END
        ) * 100.0 / COUNT(*), 1
    )                                               AS sla_compliance_pct,
    -- Average duration in minutes (maps to avg_duration_min in report)
    ROUND(AVG(TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time)), 1)
                                                    AS avg_duration_minutes,
    -- Additional metrics not available in the legacy tracker
    MIN(TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time))
                                                    AS min_duration_minutes,
    MAX(TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time))
                                                    AS max_duration_minutes,
    -- Failure count (new — sla_tracker.py only tracked completion, not failures)
    SUM(CASE WHEN jr.result_state IN ('FAILED', 'TIMED_OUT') THEN 1 ELSE 0 END)
                                                    AS failure_count
FROM
    system.workflow.job_run_timeline jr
    JOIN system.workflow.jobs j ON jr.job_id = j.job_id
WHERE
    -- 7-day window (matches default generate_report(days=7))
    jr.end_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    -- Only completed runs
    AND jr.result_state IS NOT NULL
    -- Only migrated pipelines
    AND j.settings.tags['migration_source'] = 'abinitio'
GROUP BY
    j.name,
    j.settings.tags['domain'],
    j.settings.tags['legacy_job'],
    j.settings.timeout_seconds
ORDER BY
    sla_compliance_pct ASC,  -- Worst compliance first
    job_name
""",

        "visualization": {
            "type": "table",
            "sort_column": "sla_compliance_pct",
            "sort_order": "ASC",
            "conditional_formatting": [
                {"column": "sla_compliance_pct", "threshold_red": 90.0, "threshold_yellow": 95.0},
            ],
            "_migration_note": (
                "sla_tracker.py used criticality='high'|'medium'|'low' per job. "
                "In the dashboard, color-code compliance: red < 90%, yellow 90-95%, green > 95%."
            ),
        },

        "parameters": {
            "days": {"type": "int", "default": 7, "label": "Lookback Days"},
        },
        "refresh_schedule": "0 0 6 * * ?",
        "_migration_note": (
            "SLATracker.generate_report() was called on demand. Dashboard auto-refreshes "
            "daily at 06:00 UTC (after batch windows close)."
        ),
    },

    # -----------------------------------------------------------------------
    # Query 2: SLA Compliance Trend (90-day)
    # Replaces: SLATracker run_history (kept last 90 records per job)
    # Extends: provides daily trend data for time-series visualization
    # -----------------------------------------------------------------------
    "sla_compliance_trend": {
        "display_name": "SLA Compliance Trend — 90-Day Daily Breakdown",
        "description": (
            "Daily SLA compliance trend for the last 90 days. "
            "Replaces the 90-day run_history window in monitoring/sla_tracker.py "
            "(job_config['run_history'] = job_config['run_history'][-90:])."
        ),

        "query_sql": """
-- SLA Compliance Trend (90-day daily)
-- Migrated from: sla_tracker.py 90-day run_history retention
-- Original: kept last 90 entries in JSON array per job
-- Databricks: system tables retain full history — no 90-day truncation needed

SELECT
    DATE(jr.end_time)                               AS run_date,
    j.name                                          AS job_name,
    j.settings.tags['domain']                       AS domain,
    COUNT(*)                                        AS total_runs,
    SUM(
        CASE WHEN jr.result_state = 'SUCCESS'
            AND TIMESTAMPDIFF(SECOND, jr.start_time, jr.end_time)
                <= j.settings.timeout_seconds
        THEN 1 ELSE 0 END
    )                                               AS sla_met,
    ROUND(
        SUM(
            CASE WHEN jr.result_state = 'SUCCESS'
                AND TIMESTAMPDIFF(SECOND, jr.start_time, jr.end_time)
                    <= j.settings.timeout_seconds
            THEN 1 ELSE 0 END
        ) * 100.0 / COUNT(*), 1
    )                                               AS daily_compliance_pct,
    ROUND(AVG(TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time)), 1)
                                                    AS avg_duration_minutes
FROM
    system.workflow.job_run_timeline jr
    JOIN system.workflow.jobs j ON jr.job_id = j.job_id
WHERE
    jr.end_time >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())
    AND jr.result_state IS NOT NULL
    AND j.settings.tags['migration_source'] = 'abinitio'
GROUP BY
    DATE(jr.end_time),
    j.name,
    j.settings.tags['domain'],
    j.settings.timeout_seconds
ORDER BY
    run_date DESC, job_name
""",

        "visualization": {
            "type": "line_chart",
            "x_axis": "run_date",
            "y_axis": "daily_compliance_pct",
            "series": "job_name",
            "reference_line": {"value": 95.0, "label": "95% SLA Target"},
            "_migration_note": (
                "sla_tracker.py stored run_history as a flat list. "
                "Dashboard provides a time-series view with trend lines per pipeline."
            ),
        },

        "parameters": {
            "days": {"type": "int", "default": 90, "label": "Lookback Days"},
        },
        "refresh_schedule": "0 0 6 * * ?",
        "_migration_note": (
            "sla_tracker.py truncated to 90 days (run_history[-90:]). "
            "System tables retain full history — adjust lookback parameter as needed."
        ),
    },

    # -----------------------------------------------------------------------
    # Query 3: Job Duration Heatmap
    # New — extends sla_tracker.py avg_duration_min with per-hour granularity
    # Useful for identifying batch window pressure and scheduling conflicts
    # -----------------------------------------------------------------------
    "job_duration_heatmap": {
        "display_name": "Job Duration Heatmap — Hour-of-Day Analysis",
        "description": (
            "Shows average job duration by hour of day and day of week. "
            "Extends monitoring/sla_tracker.py avg_duration_min metric with "
            "temporal analysis to identify batch window pressure points."
        ),

        "query_sql": """
-- Job Duration Heatmap (hour-of-day × day-of-week)
-- New capability extending sla_tracker.py avg_duration_min
-- Helps identify optimal scheduling windows and resource contention periods

SELECT
    j.name                                          AS job_name,
    DAYOFWEEK(jr.start_time)                        AS day_of_week,
    CASE DAYOFWEEK(jr.start_time)
        WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue'
        WHEN 4 THEN 'Wed' WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri'
        WHEN 7 THEN 'Sat'
    END                                             AS day_name,
    HOUR(jr.start_time)                             AS hour_of_day,
    COUNT(*)                                        AS run_count,
    ROUND(AVG(TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time)), 1)
                                                    AS avg_duration_minutes,
    ROUND(MAX(TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time)), 1)
                                                    AS max_duration_minutes,
    -- P95 approximation for outlier detection
    ROUND(PERCENTILE_APPROX(
        TIMESTAMPDIFF(MINUTE, jr.start_time, jr.end_time), 0.95
    ), 1)                                           AS p95_duration_minutes
FROM
    system.workflow.job_run_timeline jr
    JOIN system.workflow.jobs j ON jr.job_id = j.job_id
WHERE
    jr.end_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
    AND jr.result_state IS NOT NULL
    AND j.settings.tags['migration_source'] = 'abinitio'
GROUP BY
    j.name,
    DAYOFWEEK(jr.start_time),
    HOUR(jr.start_time)
ORDER BY
    job_name, day_of_week, hour_of_day
""",

        "visualization": {
            "type": "heatmap",
            "x_axis": "hour_of_day",
            "y_axis": "day_name",
            "value": "avg_duration_minutes",
            "color_scale": "RdYlGn_r",
        },

        "parameters": {
            "days": {"type": "int", "default": 30, "label": "Lookback Days"},
        },
        "refresh_schedule": "0 0 7 * * ?",
        "_migration_note": (
            "New capability. Ab Initio had no equivalent — scheduling was static via AutoSys. "
            "This helps tune Quartz cron schedules (e.g. daily_orders at 02:00, customer_cdc at /4h)."
        ),
    },

    # -----------------------------------------------------------------------
    # Query 4: Pipeline Run Detail Log
    # Replaces: SLATracker._state['jobs'][name]['run_history'] JSON entries
    # Each entry had: date, completed_at, status, duration_minutes, sla_met
    # -----------------------------------------------------------------------
    "pipeline_run_detail": {
        "display_name": "Pipeline Run Detail Log",
        "description": (
            "Detailed log of every pipeline run with SLA evaluation. "
            "Replaces the run_history list in monitoring/sla_tracker.py state file "
            "(fields: date, completed_at, status, duration_minutes, sla_met)."
        ),

        "query_sql": """
-- Pipeline Run Detail Log
-- Migrated from: sla_tracker.py → run_history list entries
-- Original fields: date, completed_at, status, duration_minutes, sla_met
-- Databricks: reconstructs the same fields from system tables

SELECT
    jr.run_id,
    j.name                                          AS job_name,
    j.settings.tags['domain']                       AS domain,
    j.settings.tags['legacy_job']                   AS legacy_autosys_job,
    -- Maps to sla_tracker.py run_record['date']
    DATE(jr.end_time)                               AS run_date,
    -- Maps to sla_tracker.py run_record['completed_at']
    jr.end_time                                     AS completed_at,
    -- Maps to sla_tracker.py run_record['status'] (SUCCESS/COMPLETED/FAILURE)
    jr.result_state                                 AS status,
    -- Maps to sla_tracker.py run_record['duration_minutes']
    ROUND(TIMESTAMPDIFF(SECOND, jr.start_time, jr.end_time) / 60.0, 1)
                                                    AS duration_minutes,
    -- Maps to sla_tracker.py run_record['sla_met']
    CASE
        WHEN jr.result_state = 'SUCCESS'
            AND TIMESTAMPDIFF(SECOND, jr.start_time, jr.end_time)
                <= j.settings.timeout_seconds
        THEN TRUE
        ELSE FALSE
    END                                             AS sla_met,
    -- Additional detail not in the original tracker
    jr.start_time,
    jr.state_message                                AS error_message,
    ROUND(j.settings.timeout_seconds / 60.0, 0)    AS sla_limit_minutes
FROM
    system.workflow.job_run_timeline jr
    JOIN system.workflow.jobs j ON jr.job_id = j.job_id
WHERE
    jr.end_time >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())
    AND jr.result_state IS NOT NULL
    AND j.settings.tags['migration_source'] = 'abinitio'
ORDER BY
    jr.end_time DESC
""",

        "visualization": {
            "type": "table",
            "sort_column": "completed_at",
            "sort_order": "DESC",
            "conditional_formatting": [
                {"column": "sla_met", "true_color": "green", "false_color": "red"},
                {"column": "status", "value_colors": {
                    "SUCCESS": "green", "FAILED": "red", "TIMED_OUT": "orange",
                }},
            ],
        },

        "parameters": {
            "days": {"type": "int", "default": 90, "label": "Lookback Days"},
            "job_name": {"type": "string", "default": "", "label": "Filter by Job Name"},
        },
        "refresh_schedule": "0 0/30 * * * ?",
        "_migration_note": (
            "SLATracker.record_completion() wrote entries to JSON state file. "
            "Dashboard reads from system tables — no explicit record_completion() call needed."
        ),
    },

    # -----------------------------------------------------------------------
    # Query 5: CDC Change Volume Tracker
    # New — tracks MERGE operation metrics from CDF-enabled tables
    # Extends the concept from cdc_processor.py stats dict (inserts/updates/deletes)
    # -----------------------------------------------------------------------
    "cdc_change_volume": {
        "display_name": "CDC Change Volume — Insert/Update/Delete Tracking",
        "description": (
            "Tracks the volume of CDC changes (inserts, updates, deletes) across "
            "migrated tables using Delta Change Data Feed. Extends the stats dict "
            "from graphs/cdc_processor.py (inserts/updates/deletes counts)."
        ),

        "query_sql": """
-- CDC Change Volume Tracker
-- Extends: graphs/cdc_processor.py stats = {'inserts': N, 'updates': N, 'deletes': N}
-- In Ab Initio, CDCProcessor.process() returned counts per run.
-- In Databricks, CDF provides a queryable history of all changes.
-- This query aggregates daily change volumes across CDF-enabled tables.

-- NOTE: This query template targets CDF-enabled tables.
-- Replace 'catalog.schema.table_name' with actual target tables.
-- The CDF columns (_change_type, _commit_version, _commit_timestamp) are
-- automatically available on tables with delta.enableChangeDataFeed = true.

SELECT
    'customer_master'                               AS table_name,
    DATE(_commit_timestamp)                         AS change_date,
    -- Maps to cdc_processor.py stats['inserts']
    SUM(CASE WHEN _change_type = 'insert' THEN 1 ELSE 0 END) AS inserts,
    -- Maps to cdc_processor.py stats['updates'] (update = preimage + postimage)
    SUM(CASE WHEN _change_type = 'update_postimage' THEN 1 ELSE 0 END) AS updates,
    -- Maps to cdc_processor.py stats['deletes']
    SUM(CASE WHEN _change_type = 'delete' THEN 1 ELSE 0 END) AS deletes,
    -- Total changes per day
    SUM(CASE WHEN _change_type IN ('insert', 'update_postimage', 'delete')
        THEN 1 ELSE 0 END)                         AS total_changes,
    -- Number of distinct commits (merge operations)
    COUNT(DISTINCT _commit_version)                 AS merge_operations
FROM
    -- CDF read syntax: reads the change feed from the target table
    table_changes('main.production.customer_master', '2024-01-01')
WHERE
    _commit_timestamp >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY
    DATE(_commit_timestamp)
ORDER BY
    change_date DESC
""",

        "visualization": {
            "type": "stacked_bar_chart",
            "x_axis": "change_date",
            "y_axis": ["inserts", "updates", "deletes"],
            "colors": {"inserts": "#2ecc71", "updates": "#3498db", "deletes": "#e74c3c"},
        },

        "parameters": {
            "days": {"type": "int", "default": 30, "label": "Lookback Days"},
            "table_name": {
                "type": "string",
                "default": "main.production.customer_master",
                "label": "CDF Table Name",
            },
        },
        "refresh_schedule": "0 0 7 * * ?",
        "_migration_note": (
            "Ab Initio CDC produced flat files with counts. Databricks CDF provides a "
            "queryable log. Adjust the table_changes() call for each CDF-enabled table."
        ),
    },
}


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

def get_dashboard_query(query_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve a single dashboard query definition by name.

    Args:
        query_name: Key from DASHBOARD_QUERIES.

    Returns:
        Dashboard query definition dict, or None if not found.
    """
    return DASHBOARD_QUERIES.get(query_name)


def get_all_dashboard_query_names() -> List[str]:
    """Return the names of all defined dashboard queries."""
    return list(DASHBOARD_QUERIES.keys())
