-- Databricks SQL Dashboard: SLA Compliance Tracking
--
-- Migrated from: monitoring/sla_tracker.py (SLATracker class)
-- Replaces: SLATracker.generate_report() which produced 7-day/90-day compliance reports
--
-- Original Ab Initio SLA tracking pattern:
--   SLATracker.register_job() — registered jobs with SLA windows (e.g., "06:00" = must finish by 6am)
--   SLATracker.record_completion() — evaluated SLA compliance per run
--   SLATracker.generate_report() — generated compliance % over configurable window
--   State stored in JSON file: /tmp/abinitio_sla_state.json
--
-- Databricks equivalent:
--   This query runs against the system.lakeflow tables (job run history)
--   to compute SLA compliance metrics. No external state file needed.
--
-- Dashboard widget parameters:
--   report_period_days: Number of days for the compliance window (default: 7)
--   Matches SLATracker.generate_report(days=7)

-- SLA definitions for migrated pipelines
-- Maps to SLATracker.register_job(job_name, sla_window_end, criticality)
WITH sla_definitions AS (
    SELECT * FROM (VALUES
        -- (job_name_tag, sla_window_end_utc, criticality, legacy_autosys_job)
        -- These map directly to the jobs registered in sla_tracker.py
        ('daily_orders_pipeline',    '06:00', 'high',   'JOB_DAILY_ORDERS_LOAD'),
        ('customer_cdc_pipeline',    '04:00', 'high',   'JOB_CUSTOMER_CDC')
    ) AS t(job_name, sla_window_end, criticality, legacy_autosys_job)
),

-- Collect recent job runs (replaces SLATracker._state["jobs"][name]["run_history"])
recent_runs AS (
    SELECT
        j.job_name,
        r.run_id,
        r.result_state,
        r.start_time,
        r.end_time,
        ROUND((r.end_time - r.start_time) / 1000 / 60, 1) AS duration_minutes,
        -- Extract completion hour:minute for SLA comparison
        DATE_FORMAT(r.end_time, 'HH:mm') AS completed_time_utc,
        DATE(r.end_time) AS run_date
    FROM
        system.lakeflow.job_run_timeline r
    INNER JOIN
        system.lakeflow.jobs j ON r.job_id = j.job_id
    WHERE
        -- Configurable reporting window — matches SLATracker.generate_report(days=N)
        r.end_time >= date_sub(current_date(), 7)  -- Replace 7 with widget parameter
        AND j.tags['source'] = 'abinitio-migration'
        AND r.result_state IS NOT NULL
),

-- Evaluate SLA compliance per run (replaces SLATracker.record_completion)
sla_evaluation AS (
    SELECT
        rr.*,
        sd.sla_window_end,
        sd.criticality,
        sd.legacy_autosys_job,
        -- SLA met = completed before deadline AND status is success
        -- Matches: SLATracker.record_completion() logic:
        --   sla_met = completed_at <= today_sla and status in ("SUCCESS", "COMPLETED")
        CASE
            WHEN rr.result_state IN ('SUCCESS')
                 AND rr.completed_time_utc <= sd.sla_window_end
            THEN TRUE
            ELSE FALSE
        END AS sla_met
    FROM recent_runs rr
    LEFT JOIN sla_definitions sd
        ON rr.job_name = sd.job_name
)

-- Generate compliance report (replaces SLATracker.generate_report output)
SELECT
    job_name,
    legacy_autosys_job,
    criticality,
    sla_window_end AS sla_deadline_utc,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN sla_met THEN 1 ELSE 0 END) AS sla_met_count,
    -- SLA compliance percentage — matches SLATracker.generate_report()["sla_compliance_pct"]
    ROUND(
        SUM(CASE WHEN sla_met THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        1
    ) AS sla_compliance_pct,
    -- Average duration — matches SLATracker.generate_report()["avg_duration_min"]
    ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
    MAX(duration_minutes) AS max_duration_min,
    MIN(duration_minutes) AS min_duration_min,
    -- Most recent run details
    MAX(run_date) AS last_run_date,
    -- Failure count for the period
    SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failure_count
FROM sla_evaluation
WHERE job_name IS NOT NULL
GROUP BY
    job_name,
    legacy_autosys_job,
    criticality,
    sla_window_end
ORDER BY
    criticality ASC,
    sla_compliance_pct ASC;
