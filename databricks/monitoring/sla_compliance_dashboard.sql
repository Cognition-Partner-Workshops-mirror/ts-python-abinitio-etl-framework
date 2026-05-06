-- Databricks SQL Dashboard: SLA Compliance
--
-- Migrated from: monitoring/sla_tracker.py (JSON-file-based SLA state tracking)
--
-- Ab Initio pattern: SLATracker class maintained a JSON state file with 90-day
-- rolling history, registered jobs with SLA windows, and generated compliance reports.
--
-- Databricks equivalent: This query directly queries the system job run timeline
-- table. No separate state file needed — Databricks retains full job history.
--
-- Dashboard Configuration:
--   Name:      ETL Pipeline SLA Compliance
--   Refresh:   Every 15 minutes
--   Filters:   pipeline_group, date range

-- ============================================================================
-- Query 1: Daily SLA Compliance Summary (last 7 days)
-- ============================================================================
WITH job_sla_config AS (
    -- SLA definitions migrated from SLATracker.register_job() calls
    SELECT 'daily_orders_pipeline' AS job_name, '06:00' AS sla_window_end, 'high' AS criticality
    UNION ALL
    SELECT 'customer_cdc_pipeline', '23:59', 'high'
),

job_runs AS (
    SELECT
        jrt.job_name,
        DATE(jrt.run_start_time)                                   AS run_date,
        jrt.run_start_time,
        jrt.run_end_time,
        jrt.state.result_state                                     AS result_state,
        TIMESTAMPDIFF(MINUTE, jrt.run_start_time, jrt.run_end_time) AS duration_minutes,
        cfg.sla_window_end,
        cfg.criticality,
        CASE
            WHEN jrt.state.result_state IN ('SUCCESS', 'COMPLETED')
                 AND DATE_FORMAT(jrt.run_end_time, 'HH:mm') <= cfg.sla_window_end
            THEN TRUE
            ELSE FALSE
        END AS sla_met
    FROM
        system.lakeflow.job_run_timeline jrt
    INNER JOIN
        job_sla_config cfg ON jrt.job_name = cfg.job_name
    WHERE
        jrt.run_start_time >= CURRENT_DATE() - INTERVAL 7 DAY
)

SELECT
    job_name,
    criticality,
    COUNT(*)                                                      AS total_runs,
    SUM(CASE WHEN sla_met THEN 1 ELSE 0 END)                     AS sla_met_count,
    ROUND(SUM(CASE WHEN sla_met THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
                                                                  AS sla_compliance_pct,
    ROUND(AVG(duration_minutes), 1)                               AS avg_duration_min,
    MAX(duration_minutes)                                         AS max_duration_min,
    sla_window_end
FROM
    job_runs
GROUP BY
    job_name, criticality, sla_window_end
ORDER BY
    sla_compliance_pct ASC;


-- ============================================================================
-- Query 2: Per-Day SLA Trend (for line chart visualization)
-- ============================================================================
WITH job_sla_config AS (
    SELECT 'daily_orders_pipeline' AS job_name, '06:00' AS sla_window_end
    UNION ALL
    SELECT 'customer_cdc_pipeline', '23:59'
),

daily_runs AS (
    SELECT
        DATE(jrt.run_start_time)                                   AS run_date,
        jrt.job_name,
        jrt.state.result_state                                     AS result_state,
        TIMESTAMPDIFF(MINUTE, jrt.run_start_time, jrt.run_end_time) AS duration_minutes,
        CASE
            WHEN jrt.state.result_state IN ('SUCCESS', 'COMPLETED')
                 AND DATE_FORMAT(jrt.run_end_time, 'HH:mm') <= cfg.sla_window_end
            THEN 1 ELSE 0
        END AS sla_met
    FROM
        system.lakeflow.job_run_timeline jrt
    INNER JOIN
        job_sla_config cfg ON jrt.job_name = cfg.job_name
    WHERE
        jrt.run_start_time >= CURRENT_DATE() - INTERVAL 90 DAY
)

SELECT
    run_date,
    job_name,
    COUNT(*)                                         AS total_runs,
    SUM(sla_met)                                     AS sla_met_count,
    ROUND(SUM(sla_met) * 100.0 / COUNT(*), 1)       AS sla_compliance_pct,
    ROUND(AVG(duration_minutes), 1)                  AS avg_duration_min
FROM
    daily_runs
GROUP BY
    run_date, job_name
ORDER BY
    run_date DESC, job_name;
