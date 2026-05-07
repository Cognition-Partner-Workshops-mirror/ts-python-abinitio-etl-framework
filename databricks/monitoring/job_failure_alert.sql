-- Databricks SQL Alert: Job Failure Detection
-- Migrated from: monitoring/job_monitor.py (JobMonitor.monitor_jobs + _send_alert)
--
-- Concept Mapping:
--   AutoSys REST API polling (get_job_status)  → Databricks system.workflow.job_run_timeline
--   Slack webhook alert (_send_alert)          → Databricks SQL Alert notification destination
--   JobMonitor.monitor_jobs poll loop          → Databricks SQL Alert schedule (runs every 5 min)
--
-- This alert fires when any monitored ETL job has failed in the last 30 minutes.
-- Configure the alert to send notifications to Slack, PagerDuty, or email.

-- Alert Query: Detect failed job runs in the last 30 minutes
SELECT
    j.name                                          AS job_name,
    r.run_id                                        AS run_id,
    r.result_state                                  AS status,
    r.start_time                                    AS started_at,
    r.end_time                                      AS ended_at,
    ROUND(
        TIMESTAMPDIFF(MINUTE, r.start_time, r.end_time), 1
    )                                               AS duration_minutes,
    r.state_message                                 AS error_message,
    CONCAT(
        'https://', current_catalog(), '.cloud.databricks.com/#job/',
        j.job_id, '/run/', r.run_id
    )                                               AS run_url
FROM
    system.workflow.job_run_timeline r
    JOIN system.workflow.jobs j ON r.job_id = j.job_id
WHERE
    r.result_state = 'FAILED'
    AND r.end_time >= DATEADD(MINUTE, -30, CURRENT_TIMESTAMP())
    AND j.name IN (
        'daily_orders_pipeline',
        'customer_cdc_pipeline'
    )
ORDER BY
    r.end_time DESC;

-- Alert Configuration:
-- {
--   "name": "ETL Job Failure Alert",
--   "query_id": "<query_id>",
--   "trigger_condition": "ROWS > 0",
--   "schedule": "*/5 * * * *",
--   "notification_destinations": [
--     {
--       "type": "slack",
--       "channel": "#data-engineering-alerts",
--       "message_template": "🔴 *Databricks ETL Failure*: Job `{{job_name}}` failed at {{ended_at}}. Error: {{error_message}}. [View Run]({{run_url}})"
--     },
--     {
--       "type": "email",
--       "recipients": ["data-engineering-alerts@company.com"]
--     }
--   ],
--   "rearm_seconds": 1800
-- }
