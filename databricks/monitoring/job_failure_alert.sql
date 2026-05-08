-- Databricks SQL Alert: Job Failure Detection
--
-- Migrated from: monitoring/job_monitor.py (JobMonitor class)
-- Ab Initio equivalent: AutoSys REST API polling + Slack webhook alerts
--
-- In Ab Initio, JobMonitor polled the AutoSys API at regular intervals
-- (monitor_jobs with poll_interval_seconds=60) and sent Slack alerts on
-- JOB_FAILURE events via _send_alert().
--
-- In Databricks, this is replaced by a SQL alert query that runs on a
-- schedule against the system.lakeflow.job_run_timeline table.
-- Alert triggers when any pipeline job has failed in the last 60 minutes.
--
-- Alert Configuration:
--   Name:      ETL Pipeline Job Failure Alert
--   Schedule:  Every 5 minutes
--   Trigger:   When query returns rows (failed_job_count > 0)
--   Notify:    Slack channel / PagerDuty / Email (configure in alert destination)

-- Detect failed job runs in the last 60 minutes
-- Equivalent to JobMonitor.get_job_status() returning status="FAILURE"
SELECT
    j.name                                          AS job_name,
    r.run_id                                        AS run_id,
    r.result_state                                  AS result_state,
    r.state_message                                 AS failure_message,
    r.start_time                                    AS start_time,
    r.end_time                                      AS end_time,
    ROUND(
        TIMESTAMPDIFF(SECOND, r.start_time, r.end_time) / 60.0, 1
    )                                               AS duration_minutes,
    -- Tag with original Ab Initio job name for traceability
    COALESCE(j.settings.tags['original_autosys_job'], 'N/A')
                                                    AS original_autosys_job,
    CURRENT_TIMESTAMP()                             AS alert_checked_at
FROM
    system.lakeflow.job_run_timeline r
    JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
WHERE
    -- Filter to migrated Ab Initio pipelines by tag
    j.settings.tags['source'] = 'ab_initio_migration'
    -- Failed runs in the last 60 minutes
    AND r.result_state IN ('FAILED', 'TIMED_OUT', 'CANCELLED')
    AND r.end_time >= DATEADD(MINUTE, -60, CURRENT_TIMESTAMP())
ORDER BY
    r.end_time DESC;
