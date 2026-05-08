-- =============================================================================
-- Databricks SQL Dashboard — SLA Compliance Tracking
-- Migrated from: monitoring/sla_tracker.py (90-day SLA compliance reporting)
--
-- Ab Initio original:
--   SLATracker class maintained a JSON state file with:
--     - Per-job SLA window end times (e.g., "06:00" = must complete by 6am UTC)
--     - Run history (last 90 days)
--     - Compliance percentage calculations
--
-- Databricks equivalent:
--   SQL queries against system.lakeflow.job_run_timeline for real-time
--   SLA tracking. No state file needed — Delta Lake retains full history.
--
-- Usage: Create a Databricks SQL Dashboard with the queries below as widgets.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Widget 1: SLA Compliance Summary (Last 7 Days)
-- Replaces: SLATracker.generate_report(days=7)
-- Shows per-job compliance percentage, avg duration, and criticality
-- ---------------------------------------------------------------------------

SELECT
    j.name                                              AS job_name,
    -- Map jobs to SLA deadlines (replaces SLATracker.register_job sla_window_end)
    CASE
        WHEN j.name LIKE '%daily_orders%'    THEN '06:00 UTC'
        WHEN j.name LIKE '%customer_cdc%'    THEN '+4h from start'
        ELSE 'Not defined'
    END                                                 AS sla_deadline,
    -- Criticality mapping (replaces SLATracker criticality field)
    CASE
        WHEN j.name LIKE '%daily_orders%'    THEN 'HIGH'
        WHEN j.name LIKE '%customer_cdc%'    THEN 'HIGH'
        ELSE 'MEDIUM'
    END                                                 AS criticality,
    COUNT(*)                                            AS total_runs,
    -- SLA met: completed successfully within deadline
    SUM(
        CASE
            WHEN r.result_state IN ('SUCCESS', 'COMPLETED')
                AND (
                    -- Daily orders: must complete by 06:00 UTC
                    (j.name LIKE '%daily_orders%'
                     AND CAST(r.end_time AS TIME) <= TIME '06:00:00')
                    -- Customer CDC: must complete within 1 hour of start
                    OR (j.name LIKE '%customer_cdc%'
                        AND TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time) <= 60)
                    -- Default: must complete within 2 hours
                    OR (j.name NOT LIKE '%daily_orders%'
                        AND j.name NOT LIKE '%customer_cdc%'
                        AND TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time) <= 120)
                )
            THEN 1 ELSE 0
        END
    )                                                   AS sla_met_count,
    -- SLA compliance percentage (replaces SLATracker sla_compliance_pct)
    ROUND(
        100.0 * SUM(
            CASE
                WHEN r.result_state IN ('SUCCESS', 'COMPLETED')
                    AND (
                        (j.name LIKE '%daily_orders%'
                         AND CAST(r.end_time AS TIME) <= TIME '06:00:00')
                        OR (j.name LIKE '%customer_cdc%'
                            AND TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time) <= 60)
                        OR (j.name NOT LIKE '%daily_orders%'
                            AND j.name NOT LIKE '%customer_cdc%'
                            AND TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time) <= 120)
                    )
                THEN 1 ELSE 0
            END
        ) / COUNT(*), 1
    )                                                   AS sla_compliance_pct,
    -- Average duration (replaces SLATracker avg_duration_min)
    ROUND(
        AVG(TIMESTAMPDIFF(SECOND, r.start_time, r.end_time)) / 60.0, 1
    )                                                   AS avg_duration_minutes,
    -- Failure count for visibility
    SUM(CASE WHEN r.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failure_count
FROM
    system.lakeflow.job_run_timeline r
    JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
WHERE
    r.end_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    AND r.result_state IS NOT NULL
    AND j.tags['source_system'] = 'abinitio'
GROUP BY
    j.name
ORDER BY
    sla_compliance_pct ASC;


-- ---------------------------------------------------------------------------
-- Widget 2: SLA Compliance Trend (Last 90 Days — Weekly Buckets)
-- Replaces: SLATracker.generate_report(days=90) with run_history[-90:]
-- Provides weekly trend data for dashboard time-series charts
-- ---------------------------------------------------------------------------

SELECT
    DATE_TRUNC('WEEK', r.end_time)                      AS week_start,
    j.name                                              AS job_name,
    COUNT(*)                                            AS total_runs,
    SUM(
        CASE WHEN r.result_state IN ('SUCCESS', 'COMPLETED') THEN 1 ELSE 0 END
    )                                                   AS successful_runs,
    ROUND(
        100.0 * SUM(
            CASE WHEN r.result_state IN ('SUCCESS', 'COMPLETED') THEN 1 ELSE 0 END
        ) / COUNT(*), 1
    )                                                   AS success_rate_pct,
    ROUND(
        AVG(TIMESTAMPDIFF(SECOND, r.start_time, r.end_time)) / 60.0, 1
    )                                                   AS avg_duration_minutes,
    MAX(
        TIMESTAMPDIFF(SECOND, r.start_time, r.end_time) / 60.0
    )                                                   AS max_duration_minutes
FROM
    system.lakeflow.job_run_timeline r
    JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
WHERE
    r.end_time >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())
    AND r.result_state IS NOT NULL
    AND j.tags['source_system'] = 'abinitio'
GROUP BY
    DATE_TRUNC('WEEK', r.end_time),
    j.name
ORDER BY
    week_start DESC,
    job_name;


-- ---------------------------------------------------------------------------
-- Widget 3: Recent Job Runs (Last 24 Hours Detail View)
-- Replaces: JobMonitor.monitor_jobs() polling with per-job status tracking
-- Provides real-time view of recent pipeline execution
-- ---------------------------------------------------------------------------

SELECT
    j.name                                              AS job_name,
    r.run_id,
    r.result_state                                      AS status,
    r.start_time,
    r.end_time,
    ROUND(
        TIMESTAMPDIFF(SECOND, r.start_time, r.end_time) / 60.0, 1
    )                                                   AS duration_minutes,
    -- Visual status indicator for dashboard
    CASE
        WHEN r.result_state IN ('SUCCESS', 'COMPLETED') THEN 'OK'
        WHEN r.result_state = 'FAILED'                  THEN 'ALERT'
        WHEN r.result_state = 'RUNNING'                 THEN 'IN PROGRESS'
        WHEN r.result_state = 'TIMED_OUT'               THEN 'ALERT'
        ELSE 'UNKNOWN'
    END                                                 AS health_status,
    r.state_message
FROM
    system.lakeflow.job_run_timeline r
    JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
WHERE
    r.start_time >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    AND j.tags['source_system'] = 'abinitio'
ORDER BY
    r.start_time DESC;


-- ---------------------------------------------------------------------------
-- Widget 4: CDC Statistics Summary (Change Data Feed metrics)
-- Replaces: CDCProcessor stats dict and SLATracker.record_completion()
-- Shows insert/update/delete volumes from the audit tables
-- ---------------------------------------------------------------------------

SELECT
    target_table,
    DATE(recorded_at)                                   AS run_date,
    SUM(inserts)                                        AS total_inserts,
    SUM(updates)                                        AS total_updates,
    SUM(deletes)                                        AS total_deletes,
    SUM(inserts + updates + deletes)                    AS total_changes,
    COUNT(*)                                            AS run_count
FROM
    catalog.audit.customer_cdc_log
WHERE
    recorded_at >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY
    target_table,
    DATE(recorded_at)
ORDER BY
    run_date DESC,
    target_table;
