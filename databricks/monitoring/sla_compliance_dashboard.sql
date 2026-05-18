-- --------------------------------------------------------------------------
-- sla_compliance_dashboard.sql — Databricks SQL Dashboard: SLA Compliance
--
-- Migrated from: monitoring/sla_tracker.py (SLATracker class)
--
-- Ab Initio Original:
--   SLATracker maintains JSON state file with 90-day run history
--   register_job() defines SLA window (e.g., "06:00" = must complete by 6am UTC)
--   record_completion() evaluates SLA compliance per run
--   generate_report() produces 7-day compliance summary with:
--     - total_runs, sla_met count, sla_compliance_pct, avg_duration_min, criticality
--
-- Databricks Equivalent:
--   SQL query against system.lakeflow.job_run_timeline (no external state file)
--   SLA windows defined as CASE expressions (replaces register_job() config)
--   Delta Lake history provides automatic 90-day retention
--   Visualization: Databricks SQL Dashboard with charts and counters
--
-- Dashboard Widgets:
--   1. SLA Compliance Summary table (7-day window)
--   2. SLA Trend line chart (30-day rolling)
--   3. Job Duration heatmap
--   4. Failed runs counter
-- --------------------------------------------------------------------------

-- =============================================
-- Widget 1: SLA Compliance Summary (Last 7 Days)
-- Replaces SLATracker.generate_report(days=7)
-- =============================================
WITH sla_definitions AS (
    -- SLA window definitions — replaces SLATracker.register_job() calls
    -- Format: job_name_pattern → sla_hour (must complete by this hour UTC)
    -- Ab Initio equivalent: job_config["sla_window_end"] in sla_tracker.py
    SELECT 'daily_orders_pipeline'   AS job_name, 6   AS sla_hour_utc, 'high'     AS criticality
    UNION ALL
    SELECT 'customer_cdc_pipeline',                4,                  'high'
),
recent_runs AS (
    -- Fetch completed runs from the last 7 days
    SELECT
        run_name                                           AS job_name,
        state.result_state                                 AS result_state,
        FROM_UNIXTIME(start_time / 1000)                   AS started_at,
        FROM_UNIXTIME(end_time / 1000)                     AS ended_at,
        ROUND((end_time - start_time) / 60000, 1)          AS duration_minutes,
        HOUR(FROM_UNIXTIME(end_time / 1000))               AS completed_hour_utc,
        DATE(FROM_UNIXTIME(start_time / 1000))             AS run_date
    FROM
        system.lakeflow.job_run_timeline
    WHERE
        start_time >= UNIX_TIMESTAMP(DATE_ADD(CURRENT_TIMESTAMP(), -7)) * 1000
        AND state.life_cycle_state = 'TERMINATED'
        AND (
            run_name LIKE '%orders%'
            OR run_name LIKE '%customer_cdc%'
        )
)
SELECT
    r.job_name,
    sd.criticality,
    -- Total runs — equivalent to SLATracker report["total_runs"]
    COUNT(*)                                               AS total_runs,
    -- SLA met count — equivalent to SLATracker sla_met_count
    -- SLA is met when: job completed successfully AND before the SLA hour
    SUM(CASE
        WHEN r.result_state = 'SUCCESS' AND r.completed_hour_utc <= sd.sla_hour_utc
        THEN 1 ELSE 0
    END)                                                   AS sla_met,
    -- SLA compliance percentage — equivalent to SLATracker sla_compliance_pct
    ROUND(
        SUM(CASE
            WHEN r.result_state = 'SUCCESS' AND r.completed_hour_utc <= sd.sla_hour_utc
            THEN 1.0 ELSE 0.0
        END) / COUNT(*) * 100, 1
    )                                                      AS sla_compliance_pct,
    -- Average duration — equivalent to SLATracker avg_duration_min
    ROUND(AVG(r.duration_minutes), 1)                      AS avg_duration_minutes,
    -- Additional metrics not in original Ab Initio tracker
    MAX(r.duration_minutes)                                AS max_duration_minutes,
    SUM(CASE WHEN r.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    CONCAT(sd.sla_hour_utc, ':00 UTC')                     AS sla_deadline
FROM
    recent_runs r
    LEFT JOIN sla_definitions sd ON r.job_name LIKE CONCAT('%', sd.job_name, '%')
GROUP BY
    r.job_name, sd.criticality, sd.sla_hour_utc
ORDER BY
    sla_compliance_pct ASC;

-- =============================================
-- Widget 2: SLA Trend (Last 30 Days, Rolling)
-- Extends SLATracker.generate_report() with time-series view
-- =============================================
-- (Run as a separate query for a line chart visualization)
/*
SELECT
    DATE(FROM_UNIXTIME(start_time / 1000))                 AS run_date,
    run_name                                               AS job_name,
    COUNT(*)                                               AS total_runs,
    SUM(CASE WHEN state.result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    ROUND(
        SUM(CASE WHEN state.result_state = 'SUCCESS' THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 1
    )                                                      AS daily_success_rate_pct
FROM
    system.lakeflow.job_run_timeline
WHERE
    start_time >= UNIX_TIMESTAMP(DATE_ADD(CURRENT_TIMESTAMP(), -30)) * 1000
    AND state.life_cycle_state = 'TERMINATED'
    AND (run_name LIKE '%orders%' OR run_name LIKE '%customer_cdc%')
GROUP BY
    run_date, run_name
ORDER BY
    run_date ASC, run_name;
*/
