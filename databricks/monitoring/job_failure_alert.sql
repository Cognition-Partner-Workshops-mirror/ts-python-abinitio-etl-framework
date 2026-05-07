-- =============================================================================
-- Databricks SQL Alert: Job Failure Detection
-- Migrated from: monitoring/job_monitor.py (JobMonitor._send_alert)
--
-- Ab Initio approach:
--   Python polling loop (monitor_jobs) calls AutoSys REST API every 60s,
--   sends Slack webhook on JOB_FAILURE alert type.
--
-- Databricks approach:
--   Databricks SQL Alert runs this query on a schedule. When the query returns
--   rows (failed jobs), Databricks triggers the configured notification
--   destinations (Slack, PagerDuty, email).
--
-- Setup:
--   1. Create a Databricks SQL Alert using this query
--   2. Set schedule: every 5 minutes
--   3. Trigger condition: "Rows returned" > 0
--   4. Notification destination: Slack webhook (replaces job_monitor.slack_webhook)
-- =============================================================================

SELECT
    run_id,
    job_name,
    overall_status,
    error,
    duration_seconds,
    batch_date,
    run_timestamp,
    CASE
        WHEN overall_status = 'failed' THEN 'CRITICAL'
        WHEN overall_status = 'partial_failure' THEN 'WARNING'
    END AS severity
FROM lakehouse.ops.pipeline_run_log
WHERE
    -- Look at runs from the last 30 minutes (covers 5-minute poll interval with buffer)
    run_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 MINUTES
    AND overall_status IN ('failed', 'partial_failure')
ORDER BY run_timestamp DESC;
