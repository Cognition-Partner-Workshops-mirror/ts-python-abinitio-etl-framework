-- =============================================================================
-- Databricks SQL Alert Definitions — Job Failure Detection
-- Migrated from: monitoring/job_monitor.py (AutoSys API + Slack alerts)
--
-- Ab Initio original:
--   JobMonitor class polled AutoSys REST API for job status every 60 seconds.
--   On FAILURE status, it sent a Slack webhook alert with job name and status.
--
-- Databricks equivalent:
--   SQL alerts run on a schedule, query system tables for failed jobs,
--   and trigger notifications via configured alert destinations (email, Slack, PagerDuty).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Alert 1: Detect any job failures in the last hour
-- Replaces: JobMonitor.monitor_jobs() polling loop with FAILURE detection
-- Configure as a Databricks SQL Alert with 5-minute refresh interval
-- ---------------------------------------------------------------------------
-- Alert Name: ab_initio_migration_job_failures
-- Schedule: Every 5 minutes
-- Trigger: When query returns rows (any failed job)
-- Notification: Slack webhook + email (replaces job_monitor._send_alert)

SELECT
    job_id,
    run_id,
    job_name,
    result_state                                       AS status,
    start_time,
    end_time,
    ROUND(
        TIMESTAMPDIFF(SECOND, start_time, end_time) / 60.0, 1
    )                                                  AS duration_minutes,
    -- Error context for the alert message (replaces Slack emoji + message formatting)
    CASE
        WHEN result_state = 'FAILED'
            THEN CONCAT('FAILED: ', COALESCE(state_message, 'No error message'))
        WHEN result_state = 'TIMED_OUT'
            THEN CONCAT('TIMED_OUT after ', timeout_seconds, 's')
        WHEN result_state = 'CANCELED'
            THEN 'CANCELED by user or system'
        ELSE result_state
    END                                                AS alert_message
FROM
    system.lakeflow.job_run_timeline
WHERE
    -- Look at runs that completed in the last hour
    end_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
    -- Only alert on failure states (mirrors JobMonitor FAILURE detection)
    AND result_state IN ('FAILED', 'TIMED_OUT', 'CANCELED')
    -- Filter to migrated Ab Initio pipelines by tag
    AND job_id IN (
        SELECT job_id
        FROM system.lakeflow.jobs
        WHERE tags['source_system'] = 'abinitio'
    )
ORDER BY
    end_time DESC;


-- ---------------------------------------------------------------------------
-- Alert 2: Detect jobs that have been running longer than expected
-- Replaces: JobMonitor.check_sla() with expected_complete_by check
-- Configure as a Databricks SQL Alert with 10-minute refresh interval
-- ---------------------------------------------------------------------------
-- Alert Name: ab_initio_migration_long_running_jobs
-- Schedule: Every 10 minutes
-- Trigger: When query returns rows (long-running job detected)

SELECT
    j.job_id,
    r.run_id,
    j.name                                              AS job_name,
    r.start_time,
    ROUND(
        TIMESTAMPDIFF(SECOND, r.start_time, CURRENT_TIMESTAMP()) / 60.0, 1
    )                                                   AS running_minutes,
    -- Thresholds based on original Ab Initio SLA definitions
    CASE
        WHEN j.name LIKE '%daily_orders%'   THEN 60    -- 1 hour SLA
        WHEN j.name LIKE '%customer_cdc%'   THEN 30    -- 30 min SLA
        ELSE 120                                        -- Default 2 hour threshold
    END                                                 AS sla_threshold_minutes,
    CONCAT(
        'LONG RUNNING: ', j.name,
        ' has been running for ',
        ROUND(TIMESTAMPDIFF(SECOND, r.start_time, CURRENT_TIMESTAMP()) / 60.0, 0),
        ' minutes (SLA threshold exceeded)'
    )                                                   AS alert_message
FROM
    system.lakeflow.job_run_timeline r
    JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
WHERE
    r.result_state = 'RUNNING'
    AND j.tags['source_system'] = 'abinitio'
    -- Alert when running time exceeds per-job SLA threshold
    AND TIMESTAMPDIFF(SECOND, r.start_time, CURRENT_TIMESTAMP()) / 60.0 >
        CASE
            WHEN j.name LIKE '%daily_orders%'   THEN 60
            WHEN j.name LIKE '%customer_cdc%'   THEN 30
            ELSE 120
        END
ORDER BY
    running_minutes DESC;


-- ---------------------------------------------------------------------------
-- Alert 3: Detect consecutive failures (reliability degradation)
-- Replaces: manual monitoring patterns in Ab Initio operations
-- Configure as a Databricks SQL Alert with 30-minute refresh interval
-- ---------------------------------------------------------------------------
-- Alert Name: ab_initio_migration_consecutive_failures
-- Schedule: Every 30 minutes
-- Trigger: When query returns rows (3+ consecutive failures)

SELECT
    job_name,
    consecutive_failures,
    last_failure_time,
    CONCAT(
        'ESCALATION: ', job_name,
        ' has failed ', consecutive_failures,
        ' consecutive times. Last failure: ', last_failure_time
    )                                                   AS alert_message
FROM (
    SELECT
        j.name                                          AS job_name,
        COUNT(*)                                        AS consecutive_failures,
        MAX(r.end_time)                                 AS last_failure_time
    FROM
        system.lakeflow.job_run_timeline r
        JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
    WHERE
        r.result_state = 'FAILED'
        AND j.tags['source_system'] = 'abinitio'
        AND r.end_time >= DATEADD(DAY, -1, CURRENT_TIMESTAMP())
        -- Check that no successful run exists after these failures
        AND NOT EXISTS (
            SELECT 1
            FROM system.lakeflow.job_run_timeline r2
            WHERE r2.job_id = r.job_id
              AND r2.result_state = 'SUCCESS'
              AND r2.end_time > r.end_time
        )
    GROUP BY j.name
) consecutive
WHERE
    consecutive_failures >= 3
ORDER BY
    consecutive_failures DESC;
