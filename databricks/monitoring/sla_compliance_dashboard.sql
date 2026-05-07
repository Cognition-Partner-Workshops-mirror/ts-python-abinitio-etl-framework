-- Databricks SQL Dashboard: SLA Compliance Tracking
-- Migrated from: monitoring/sla_tracker.py (SLATracker.generate_report + record_completion)
--
-- Concept Mapping:
--   SLATracker.register_job (sla_window_end)  → SLA definitions in CTE below
--   SLATracker.record_completion              → system.workflow.job_run_timeline
--   SLATracker.generate_report (90-day window)→ Dashboard query with configurable lookback
--   JSON state file (/tmp/abinitio_sla_state) → Databricks system tables (no file I/O needed)
--   Criticality levels (high/medium/low)      → Defined in sla_definitions CTE
--
-- This query powers a Databricks SQL dashboard with two panels:
--   1. SLA Compliance Summary (last N days)
--   2. Daily SLA Trend

-- ============================================================
-- Panel 1: SLA Compliance Summary
-- Replaces SLATracker.generate_report() with 90-day lookback
-- ============================================================

WITH sla_definitions AS (
    -- SLA definitions previously registered via SLATracker.register_job()
    -- Maps each job to its completion deadline and criticality
    SELECT 'daily_orders_pipeline'  AS job_name, '06:00' AS sla_window_end, 'high'   AS criticality
    UNION ALL
    SELECT 'customer_cdc_pipeline', '04:00', 'high'
),

job_runs AS (
    SELECT
        j.name                                       AS job_name,
        r.run_id,
        r.result_state                               AS status,
        r.start_time,
        r.end_time,
        ROUND(
            TIMESTAMPDIFF(SECOND, r.start_time, r.end_time) / 60.0, 1
        )                                            AS duration_minutes,
        DATE(r.end_time)                             AS run_date,
        TIME(r.end_time)                             AS completion_time
    FROM
        system.workflow.job_run_timeline r
        JOIN system.workflow.jobs j ON r.job_id = j.job_id
    WHERE
        r.result_state IN ('SUCCESS', 'FAILED')
        AND r.end_time >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())
        AND j.name IN (SELECT job_name FROM sla_definitions)
),

sla_evaluation AS (
    SELECT
        jr.job_name,
        jr.run_id,
        jr.run_date,
        jr.status,
        jr.duration_minutes,
        jr.completion_time,
        sd.sla_window_end,
        sd.criticality,
        CASE
            WHEN jr.status = 'SUCCESS'
                 AND jr.completion_time <= CAST(sd.sla_window_end AS TIME)
            THEN TRUE
            ELSE FALSE
        END                                          AS sla_met
    FROM
        job_runs jr
        JOIN sla_definitions sd ON jr.job_name = sd.job_name
)

-- Summary: one row per job with compliance stats (replaces SLATracker.generate_report output)
SELECT
    job_name,
    criticality,
    sla_window_end                                   AS sla_deadline,
    COUNT(*)                                         AS total_runs,
    SUM(CASE WHEN sla_met THEN 1 ELSE 0 END)        AS sla_met_count,
    ROUND(
        SUM(CASE WHEN sla_met THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    )                                                AS sla_compliance_pct,
    ROUND(AVG(duration_minutes), 1)                  AS avg_duration_min,
    ROUND(MAX(duration_minutes), 1)                  AS max_duration_min,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failure_count
FROM
    sla_evaluation
GROUP BY
    job_name, criticality, sla_window_end
ORDER BY
    criticality DESC, sla_compliance_pct ASC;


-- ============================================================
-- Panel 2: Daily SLA Trend (last 30 days)
-- Provides drill-down view for the dashboard
-- ============================================================

-- WITH sla_definitions and sla_evaluation as defined above (reuse in dashboard)
-- SELECT
--     run_date,
--     job_name,
--     status,
--     duration_minutes,
--     sla_met,
--     completion_time,
--     sla_window_end
-- FROM
--     sla_evaluation
-- WHERE
--     run_date >= DATEADD(DAY, -30, CURRENT_DATE())
-- ORDER BY
--     run_date DESC, job_name;


-- ============================================================
-- SLA Breach Alert (companion to dashboard)
-- Fires when any high-criticality job misses its SLA window
-- ============================================================

-- Alert Query:
-- SELECT
--     job_name,
--     run_date,
--     completion_time,
--     sla_window_end,
--     duration_minutes,
--     status
-- FROM sla_evaluation
-- WHERE
--     NOT sla_met
--     AND criticality = 'high'
--     AND run_date = CURRENT_DATE()
-- ORDER BY completion_time DESC;

-- Alert Configuration:
-- {
--   "name": "SLA Breach Alert - High Criticality",
--   "trigger_condition": "ROWS > 0",
--   "schedule": "0 */1 * * *",
--   "notification_destinations": [
--     {
--       "type": "slack",
--       "channel": "#data-engineering-alerts",
--       "message_template": "⚠️ *SLA Breach*: `{{job_name}}` completed at {{completion_time}} — deadline was {{sla_window_end}}. Duration: {{duration_minutes}} min."
--     }
--   ],
--   "rearm_seconds": 3600
-- }
