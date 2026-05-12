-- Databricks SQL Alert: Job Failure Detection
--
-- Migrated from: monitoring/job_monitor.py (JobMonitor class)
-- Replaces: AutoSys REST API polling + Slack alerting for job failures
--
-- Original Ab Initio monitoring pattern:
--   JobMonitor.monitor_jobs() polled AutoSys API every 60s for up to 60 polls
--   JobMonitor._send_alert() sent Slack messages on JOB_FAILURE events
--
-- Databricks equivalent:
--   This SQL alert query runs on a schedule and triggers notifications
--   when pipeline jobs fail. Databricks handles the polling internally.
--
-- Alert configuration:
--   Schedule: Every 5 minutes
--   Trigger: When query returns rows (any failed job in the last hour)
--   Notification: Slack webhook + email (replaces JobMonitor._send_alert)

-- Detect failed jobs in the last hour across all migrated pipelines
-- This replaces the AutoSys API status check (JobMonitor.get_job_status)
SELECT
    job_id,
    run_id,
    job_name,
    -- Map Databricks run states to the Ab Initio status codes used in job_monitor.py
    CASE result_state
        WHEN 'FAILED'    THEN 'FAILURE'     -- Maps to Ab Initio "FAILURE" status
        WHEN 'TIMEDOUT'  THEN 'SLA_BREACH'  -- Maps to JobMonitor.check_sla() breach detection
        WHEN 'CANCELLED' THEN 'CANCELLED'
        ELSE result_state
    END AS alert_type,
    result_state,
    state_message AS error_message,
    start_time,
    end_time,
    ROUND((end_time - start_time) / 1000 / 60, 1) AS duration_minutes,
    -- Tag with the legacy job name for cross-reference during migration
    COALESCE(
        tags['legacy_job'],
        'UNKNOWN'
    ) AS legacy_autosys_job
FROM
    system.lakeflow.job_run_timeline
WHERE
    -- Only check runs from the last hour (replaces AutoSys poll_interval_seconds=60)
    end_time >= date_sub(current_timestamp(), 1)
    AND result_state IN ('FAILED', 'TIMEDOUT')
    -- Filter to migrated Ab Initio pipelines
    AND tags['source'] = 'abinitio-migration'
ORDER BY
    end_time DESC;
