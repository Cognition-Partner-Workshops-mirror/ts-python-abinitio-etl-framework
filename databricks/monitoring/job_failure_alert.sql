-- --------------------------------------------------------------------------
-- job_failure_alert.sql — Databricks SQL Alert: Job Failure Detection
--
-- Migrated from: monitoring/job_monitor.py (JobMonitor class)
--
-- Ab Initio Original:
--   JobMonitor polls AutoSys REST API for job status every 60 seconds
--   On FAILURE status → sends Slack alert via _send_alert() webhook
--   Monitors: JOB_DAILY_ORDERS_LOAD, JOB_CUSTOMER_CDC, etc.
--
-- Databricks Equivalent:
--   Databricks SQL Alert runs this query on a schedule
--   Triggers notification (email/Slack/PagerDuty) when rows are returned
--   Replaces both the AutoSys API polling and Slack webhook logic
--
-- Alert Configuration:
--   Schedule: Every 5 minutes
--   Trigger: When query returns 1 or more rows (= failed jobs detected)
--   Notification: Slack channel #data-engineering-alerts + email
-- --------------------------------------------------------------------------

-- Detect failed Databricks job runs in the last 2 hours
-- This replaces the AutoSys API polling loop in JobMonitor.monitor_jobs()
SELECT
    job_id,
    run_id,
    run_name                                              AS job_name,
    state.result_state                                    AS result_state,
    state.life_cycle_state                                AS lifecycle_state,
    -- Duration in minutes — replaces Ab Initio duration tracking
    ROUND((end_time - start_time) / 60000, 1)             AS duration_minutes,
    -- Timestamps for SLA tracking
    FROM_UNIXTIME(start_time / 1000)                      AS started_at,
    FROM_UNIXTIME(end_time / 1000)                        AS ended_at,
    -- Error message for diagnostics — replaces Ab Initio stderr capture
    state.state_message                                   AS error_message,
    -- Tag-based filtering to identify migrated Ab Initio pipelines
    tags                                                  AS job_tags
FROM
    system.lakeflow.job_run_timeline
WHERE
    -- Only look at recent runs (last 2 hours)
    start_time >= UNIX_TIMESTAMP(DATE_ADD(CURRENT_TIMESTAMP(), -2)) * 1000
    -- Failed runs only — equivalent to AutoSys FAILURE status check
    AND state.result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED')
    -- Filter for migrated Ab Initio pipelines by tag
    AND (
        tags LIKE '%migrated_from%abinitio%'
        OR run_name LIKE '%orders%'
        OR run_name LIKE '%customer_cdc%'
    )
ORDER BY
    end_time DESC;
