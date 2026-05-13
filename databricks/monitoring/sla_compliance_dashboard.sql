-- sla_compliance_dashboard.sql — Databricks SQL Dashboard Queries
-- Converted from: monitoring/sla_tracker.py (SLATracker class with JSON state file)
--
-- Ab Initio → Databricks Mapping:
--   SLATracker._state JSON file            → Databricks system tables (no manual state management)
--   SLATracker.register_job()              → Dashboard configuration (job names and SLA deadlines)
--   SLATracker.record_completion()         → Automatic via system.workflow.job_run_timeline
--   SLATracker.generate_report(days=7)     → Dashboard widget with parameterised lookback period
--   SLATracker._state["run_history"][-90:] → DESCRIBE HISTORY + system tables (90-day retention native)

-- ============================================================================
-- Widget 1: SLA Compliance Summary (last N days)
-- Replaces: SLATracker.generate_report() method
-- Shows per-job compliance percentage, matching the report output format
-- ============================================================================

-- Dashboard parameter: @lookback_days (default: 7, matches generate_report(days=7))
-- Dashboard parameter: @pipeline_filter (default: all)

SELECT
    run_name                                                             AS job_name,
    COUNT(*)                                                             AS total_runs,
    -- SLA met = completed successfully before deadline
    SUM(CASE
        WHEN result_state IN ('SUCCESS', 'COMPLETED')
             AND end_time <= CONCAT(DATE(start_time), ' ',
                 CASE run_name
                     WHEN 'daily_orders_pipeline' THEN '06:00:00'
                     WHEN 'customer_cdc_pipeline' THEN '23:59:00'
                     ELSE '23:59:59'
                 END)
        THEN 1 ELSE 0
    END)                                                                 AS sla_met,
    -- Compliance percentage — matches SLATracker sla_compliance_pct calculation
    ROUND(
        SUM(CASE
            WHEN result_state IN ('SUCCESS', 'COMPLETED')
                 AND end_time <= CONCAT(DATE(start_time), ' ',
                     CASE run_name
                         WHEN 'daily_orders_pipeline' THEN '06:00:00'
                         WHEN 'customer_cdc_pipeline' THEN '23:59:00'
                         ELSE '23:59:59'
                     END)
            THEN 1.0 ELSE 0.0
        END) / COUNT(*) * 100, 1
    )                                                                    AS sla_compliance_pct,
    -- Average duration — matches SLATracker avg_duration_min
    ROUND(AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 1)           AS avg_duration_min,
    -- Failure rate
    ROUND(
        SUM(CASE WHEN result_state IN ('FAILED', 'TIMED_OUT') THEN 1.0 ELSE 0.0 END)
        / COUNT(*) * 100, 1
    )                                                                    AS failure_rate_pct,
    -- Criticality mapping — from SLATracker.register_job(criticality=...)
    CASE run_name
        WHEN 'daily_orders_pipeline' THEN 'high'
        WHEN 'customer_cdc_pipeline' THEN 'high'
        ELSE 'medium'
    END                                                                  AS criticality
FROM system.workflow.job_run_timeline
WHERE start_time >= TIMESTAMPADD(DAY, -7, CURRENT_TIMESTAMP())  -- Replace 7 with :lookback_days parameter
  AND run_name IN ('daily_orders_pipeline', 'customer_cdc_pipeline')
GROUP BY run_name
ORDER BY sla_compliance_pct ASC;  -- Worst compliance first


-- ============================================================================
-- Widget 2: Daily SLA Compliance Trend (last 30 days)
-- Replaces: SLATracker run_history[-90:] with visual trending
-- ============================================================================

SELECT
    DATE(start_time)                                                     AS run_date,
    run_name                                                             AS job_name,
    COUNT(*)                                                             AS runs,
    SUM(CASE
        WHEN result_state IN ('SUCCESS', 'COMPLETED')
             AND end_time <= CONCAT(DATE(start_time), ' ',
                 CASE run_name
                     WHEN 'daily_orders_pipeline' THEN '06:00:00'
                     WHEN 'customer_cdc_pipeline' THEN '23:59:00'
                     ELSE '23:59:59'
                 END)
        THEN 1 ELSE 0
    END)                                                                 AS sla_met,
    ROUND(
        SUM(CASE
            WHEN result_state IN ('SUCCESS', 'COMPLETED')
                 AND end_time <= CONCAT(DATE(start_time), ' ',
                     CASE run_name
                         WHEN 'daily_orders_pipeline' THEN '06:00:00'
                         WHEN 'customer_cdc_pipeline' THEN '23:59:00'
                         ELSE '23:59:59'
                     END)
            THEN 1.0 ELSE 0.0
        END) / COUNT(*) * 100, 1
    )                                                                    AS daily_compliance_pct,
    ROUND(AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 1)           AS avg_duration_min
FROM system.workflow.job_run_timeline
WHERE start_time >= TIMESTAMPADD(DAY, -30, CURRENT_TIMESTAMP())
  AND run_name IN ('daily_orders_pipeline', 'customer_cdc_pipeline')
GROUP BY DATE(start_time), run_name
ORDER BY run_date DESC, run_name;


-- ============================================================================
-- Widget 3: Recent Run Details
-- Replaces: SLATracker.record_completion() individual run records
-- Shows the last 50 runs with SLA status for drill-down
-- ============================================================================

SELECT
    run_id,
    run_name                                                             AS job_name,
    result_state                                                         AS status,
    start_time,
    end_time,
    TIMESTAMPDIFF(MINUTE, start_time, end_time)                          AS duration_minutes,
    -- SLA evaluation — matches SLATracker.record_completion() sla_met logic
    CASE
        WHEN result_state IN ('SUCCESS', 'COMPLETED')
             AND end_time <= CONCAT(DATE(start_time), ' ',
                 CASE run_name
                     WHEN 'daily_orders_pipeline' THEN '06:00:00'
                     WHEN 'customer_cdc_pipeline' THEN '23:59:00'
                     ELSE '23:59:59'
                 END)
        THEN 'MET'
        WHEN result_state IN ('FAILED', 'TIMED_OUT', 'CANCELED')
        THEN 'FAILED'
        ELSE 'BREACHED'
    END                                                                  AS sla_status,
    state_message                                                        AS details
FROM system.workflow.job_run_timeline
WHERE start_time >= TIMESTAMPADD(DAY, -7, CURRENT_TIMESTAMP())
  AND run_name IN ('daily_orders_pipeline', 'customer_cdc_pipeline')
ORDER BY start_time DESC
LIMIT 50;


-- ============================================================================
-- Widget 4: Pipeline Health Scorecard
-- High-level KPIs for executive dashboard
-- ============================================================================

SELECT
    'ETL Pipeline Health' AS metric_group,
    (SELECT COUNT(DISTINCT run_name)
     FROM system.workflow.job_run_timeline
     WHERE start_time >= TIMESTAMPADD(DAY, -1, CURRENT_TIMESTAMP())
       AND run_name IN ('daily_orders_pipeline', 'customer_cdc_pipeline')
    )                                                                    AS active_pipelines,
    (SELECT COUNT(*)
     FROM system.workflow.job_run_timeline
     WHERE start_time >= TIMESTAMPADD(DAY, -1, CURRENT_TIMESTAMP())
       AND result_state IN ('FAILED', 'TIMED_OUT')
       AND run_name IN ('daily_orders_pipeline', 'customer_cdc_pipeline')
    )                                                                    AS failures_last_24h,
    (SELECT ROUND(
        SUM(CASE WHEN result_state IN ('SUCCESS', 'COMPLETED') THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(*), 0) * 100, 1)
     FROM system.workflow.job_run_timeline
     WHERE start_time >= TIMESTAMPADD(DAY, -7, CURRENT_TIMESTAMP())
       AND run_name IN ('daily_orders_pipeline', 'customer_cdc_pipeline')
    )                                                                    AS success_rate_7d_pct;
