-- job_failure_alerts.sql — Databricks SQL Alert Definitions
-- Converted from: monitoring/job_monitor.py (AutoSys REST API polling + Slack alerting)
--
-- Ab Initio → Databricks Mapping:
--   JobMonitor.get_job_status()     → system.workflow.job_run_timeline (built-in)
--   JobMonitor.check_sla()          → SQL alert with deadline comparison
--   JobMonitor._send_alert()        → Databricks SQL alert → webhook/email notification
--   AutoSys REST API polling        → Databricks system tables (real-time, no polling needed)
--   Slack webhook integration       → Databricks alert notification destinations (Slack, PagerDuty, email)

-- ============================================================================
-- Alert 1: Job Failure Detection
-- Replaces: JobMonitor.monitor_jobs() with status == "FAILURE" check
-- Frequency: Every 5 minutes (replaces AutoSys poll_interval_seconds=60)
-- ============================================================================

-- Query for the alert: detect any failed workflow runs in the last 15 minutes
-- This replaces the JobMonitor._send_alert() call with alert_type="JOB_FAILURE"
SELECT
    run_id,
    job_id,
    run_name                                    AS job_name,
    result_state                                AS status,
    start_time,
    end_time,
    TIMESTAMPDIFF(MINUTE, start_time, end_time) AS duration_minutes,
    state_message                               AS error_message
FROM system.workflow.job_run_timeline
WHERE result_state IN ('FAILED', 'TIMED_OUT', 'CANCELED')
  AND start_time >= TIMESTAMPADD(MINUTE, -15, CURRENT_TIMESTAMP())
  AND run_name IN (
      -- Monitored pipelines — equivalent to the jobs list passed to monitor_jobs()
      'daily_orders_pipeline',
      'customer_cdc_pipeline'
  )
ORDER BY start_time DESC;

-- Alert configuration (apply via Databricks SQL UI or API):
--   Name: "ETL Pipeline Failure Alert"
--   Schedule: Every 5 minutes
--   Trigger: When query returns rows (any failure detected)
--   Notification: Slack channel #data-engineering-alerts
--   Rearm: After 30 minutes of no failures


-- ============================================================================
-- Alert 2: Long-Running Job Detection
-- Replaces: Implicit monitoring in JobMonitor.monitor_jobs() max_polls logic
-- Detects jobs running longer than 2x their average duration
-- ============================================================================

SELECT
    jr.run_id,
    jr.run_name                                    AS job_name,
    jr.start_time,
    TIMESTAMPDIFF(MINUTE, jr.start_time, CURRENT_TIMESTAMP()) AS running_minutes,
    avg_stats.avg_duration_minutes,
    ROUND(
        TIMESTAMPDIFF(MINUTE, jr.start_time, CURRENT_TIMESTAMP())
        / NULLIF(avg_stats.avg_duration_minutes, 0),
        1
    )                                              AS duration_ratio
FROM system.workflow.job_run_timeline jr
-- Subquery: average duration over last 30 days for comparison
CROSS JOIN (
    SELECT
        ROUND(AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 1) AS avg_duration_minutes
    FROM system.workflow.job_run_timeline
    WHERE result_state = 'SUCCESS'
      AND start_time >= TIMESTAMPADD(DAY, -30, CURRENT_TIMESTAMP())
      AND run_name IN ('daily_orders_pipeline', 'customer_cdc_pipeline')
) avg_stats
WHERE jr.result_state = 'RUNNING'
  AND TIMESTAMPDIFF(MINUTE, jr.start_time, CURRENT_TIMESTAMP())
      > 2 * avg_stats.avg_duration_minutes
  AND jr.run_name IN ('daily_orders_pipeline', 'customer_cdc_pipeline');

-- Alert configuration:
--   Name: "ETL Long-Running Job Alert"
--   Schedule: Every 10 minutes
--   Trigger: When query returns rows (any job running > 2x average)
--   Notification: Slack channel #data-engineering-alerts


-- ============================================================================
-- Alert 3: SLA Breach Detection
-- Replaces: JobMonitor.check_sla() method and SLATracker.record_completion()
-- Maps the SLA window definitions from sla_tracker.py register_job()
-- ============================================================================

SELECT
    run_name                                    AS job_name,
    result_state                                AS status,
    end_time                                    AS completed_at,
    -- SLA deadline mapping (from SLATracker.register_job sla_window_end values)
    CASE run_name
        WHEN 'daily_orders_pipeline'  THEN '06:00'  -- Must complete by 6:00 AM UTC
        WHEN 'customer_cdc_pipeline'  THEN '23:59'  -- Must complete within 4-hour window
    END                                         AS sla_deadline,
    CASE
        WHEN end_time > CONCAT(CURRENT_DATE(), ' ',
             CASE run_name
                 WHEN 'daily_orders_pipeline' THEN '06:00:00'
                 WHEN 'customer_cdc_pipeline' THEN '23:59:00'
             END)
        THEN 'BREACHED'
        ELSE 'MET'
    END                                         AS sla_status,
    TIMESTAMPDIFF(MINUTE, start_time, end_time) AS duration_minutes
FROM system.workflow.job_run_timeline
WHERE result_state IN ('SUCCESS', 'COMPLETED')
  AND start_time >= TIMESTAMPADD(HOUR, -24, CURRENT_TIMESTAMP())
  AND run_name IN ('daily_orders_pipeline', 'customer_cdc_pipeline')
  -- Only alert on breaches
  AND end_time > CONCAT(CURRENT_DATE(), ' ',
      CASE run_name
          WHEN 'daily_orders_pipeline' THEN '06:00:00'
          WHEN 'customer_cdc_pipeline' THEN '23:59:00'
      END)
ORDER BY end_time DESC;

-- Alert configuration:
--   Name: "ETL SLA Breach Alert"
--   Schedule: Every 15 minutes
--   Trigger: When query returns rows (any SLA breach detected)
--   Notification: PagerDuty (critical) + Slack #data-engineering-alerts
--   Matches: JobMonitor._send_alert() with alert_type="SLA_BREACH"
