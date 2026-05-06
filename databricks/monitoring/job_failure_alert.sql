-- Databricks SQL Alert: Job Failure Detection
--
-- Migrated from: monitoring/job_monitor.py (AutoSys API polling + Slack alerts)
--
-- Ab Initio pattern: JobMonitor class polled AutoSys REST API every 60s,
-- checked for FAILURE status, and sent Slack notifications.
--
-- Databricks equivalent: Databricks SQL Alert runs this query on a schedule.
-- When rows are returned (i.e. failed jobs exist), the alert fires and sends
-- notifications via configured channels (email, Slack webhook, PagerDuty).
--
-- Alert Configuration:
--   Name:      etl_job_failure_alert
--   Schedule:  Every 5 minutes
--   Trigger:   When query returns rows (row count > 0)
--   Notify:    data-engineering@company.com, #data-alerts Slack channel

SELECT
    job_id,
    run_id,
    job_name,
    run_start_time,
    run_end_time,
    TIMESTAMPDIFF(MINUTE, run_start_time, run_end_time) AS duration_minutes,
    state.result_state                                   AS result_state,
    state.state_message                                  AS error_message,
    trigger_type,
    CASE
        WHEN job_name LIKE '%orders%'   THEN 'orders-pipeline'
        WHEN job_name LIKE '%customer%' THEN 'customer-cdc'
        ELSE 'other'
    END AS pipeline_group
FROM
    system.lakeflow.job_run_timeline
WHERE
    state.result_state IN ('FAILED', 'TIMED_OUT', 'INTERNAL_ERROR')
    AND run_start_time >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
ORDER BY
    run_start_time DESC;
