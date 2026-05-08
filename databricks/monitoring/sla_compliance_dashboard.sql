-- Databricks SQL Dashboard Query: SLA Compliance Tracking
--
-- Migrated from: monitoring/sla_tracker.py (SLATracker class)
-- Ab Initio equivalent: 90-day SLA compliance reporting with JSON state file
--
-- In Ab Initio, SLATracker maintained a JSON state file tracking:
--   - Job registration with SLA window end times
--   - Per-run completion recording with sla_met boolean
--   - 90-day rolling compliance reports with percentage calculations
--
-- In Databricks, this is replaced by a SQL query against the system tables
-- that provides the same metrics without maintaining external state files.
--
-- Dashboard Configuration:
--   Name:      ETL Pipeline SLA Compliance Dashboard
--   Refresh:   Every 15 minutes
--   Widgets:   lookback_days (default: 7), criticality filter

-- SLA definitions for migrated Ab Initio pipelines
-- Equivalent to SLATracker.register_job() calls
-- Maps each pipeline to its SLA deadline (must complete by this UTC time)
WITH sla_definitions AS (
    SELECT * FROM VALUES
        -- job_name, sla_window_end (UTC), criticality
        -- Equivalent to SLATracker.register_job("JOB_DAILY_ORDERS_LOAD", "06:00", "high")
        ('daily_orders_pipeline',   '06:00', 'high'),
        -- Equivalent to SLATracker.register_job("JOB_CUSTOMER_CDC", "04:00", "high")
        ('customer_cdc_pipeline',   '04:00', 'high')
    AS t(job_name, sla_window_end, criticality)
),

-- Collect job run history from Databricks system tables
-- Equivalent to SLATracker.record_completion() populating run_history[]
job_runs AS (
    SELECT
        j.name                                      AS job_name,
        r.run_id,
        r.result_state,
        r.start_time,
        r.end_time,
        ROUND(
            TIMESTAMPDIFF(SECOND, r.start_time, r.end_time) / 60.0, 1
        )                                           AS duration_minutes,
        DATE(r.end_time)                            AS run_date,
        DATE_FORMAT(r.end_time, 'HH:mm')           AS completion_time
    FROM
        system.lakeflow.job_run_timeline r
        JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
    WHERE
        j.settings.tags['source'] = 'ab_initio_migration'
        AND r.result_state IS NOT NULL
        -- 90-day lookback (matches SLATracker's run_history[-90:] retention)
        AND r.end_time >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())
),

-- Evaluate SLA compliance per run
-- Equivalent to SLATracker.record_completion() computing sla_met boolean
sla_evaluation AS (
    SELECT
        jr.job_name,
        jr.run_id,
        jr.run_date,
        jr.result_state,
        jr.duration_minutes,
        jr.completion_time,
        sd.sla_window_end,
        sd.criticality,
        -- SLA met if: completed before deadline AND status is successful
        -- Mirrors: sla_met = completed_at <= today_sla and status in ("SUCCESS", "COMPLETED")
        CASE
            WHEN jr.result_state IN ('SUCCESS')
                 AND jr.completion_time <= sd.sla_window_end
            THEN TRUE
            ELSE FALSE
        END AS sla_met
    FROM
        job_runs jr
        JOIN sla_definitions sd ON jr.job_name = sd.job_name
)

-- Generate compliance report
-- Equivalent to SLATracker.generate_report(days=7)
SELECT
    job_name,
    criticality,
    sla_window_end                                  AS sla_deadline_utc,
    COUNT(*)                                        AS total_runs,
    SUM(CASE WHEN sla_met THEN 1 ELSE 0 END)       AS sla_met_count,
    ROUND(
        SUM(CASE WHEN sla_met THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    )                                               AS sla_compliance_pct,
    ROUND(AVG(duration_minutes), 1)                 AS avg_duration_min,
    ROUND(MAX(duration_minutes), 1)                 AS max_duration_min,
    ROUND(MIN(duration_minutes), 1)                 AS min_duration_min,
    SUM(CASE WHEN result_state != 'SUCCESS' THEN 1 ELSE 0 END)
                                                    AS failure_count,
    CURRENT_TIMESTAMP()                             AS report_generated_at
FROM
    sla_evaluation
-- Filter to requested lookback period (default 7 days, matches generate_report default)
WHERE
    run_date >= DATEADD(DAY, -7, CURRENT_DATE())
GROUP BY
    job_name, criticality, sla_window_end
ORDER BY
    sla_compliance_pct ASC,
    criticality DESC;
