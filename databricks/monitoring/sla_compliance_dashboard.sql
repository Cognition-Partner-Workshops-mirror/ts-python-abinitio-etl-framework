-- =============================================================================
-- Databricks SQL Dashboard: SLA Compliance
-- Migrated from: monitoring/sla_tracker.py (SLATracker.generate_report)
--
-- Ab Initio approach:
--   Python class tracks SLA state in a JSON file (/tmp/abinitio_sla_state.json),
--   computes compliance percentage over a rolling window (default 7 days,
--   max 90 days history).
--
-- Databricks approach:
--   SQL query over the pipeline_run_log and sla_definitions tables. Results
--   are visualized in a Databricks SQL Dashboard with automatic refresh.
--
-- Prerequisites:
--   1. Table: lakehouse.ops.pipeline_run_log
--      (written by parallel_loader and cdc_processor notebooks)
--   2. Table: lakehouse.ops.sla_definitions
--      (created by setup script below)
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Part 1: SLA Definitions Table Setup
-- Mirrors SLATracker.register_job() — defines SLA windows per pipeline.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS lakehouse.ops.sla_definitions (
    job_name        STRING    NOT NULL  COMMENT 'Pipeline/job name',
    sla_window_end  STRING    NOT NULL  COMMENT 'Daily SLA deadline in HH:MM UTC (e.g., "06:00")',
    criticality     STRING    NOT NULL  COMMENT 'Job criticality: critical, high, medium, low',
    owner_team      STRING              COMMENT 'Owning team for escalation',
    CONSTRAINT pk_sla_def PRIMARY KEY (job_name)
)
USING DELTA
COMMENT 'SLA definitions for monitored pipelines — migrated from SLATracker.register_job()';

-- Seed with known pipelines
MERGE INTO lakehouse.ops.sla_definitions AS target
USING (
    SELECT * FROM VALUES
        ('daily_orders_pipeline',  '06:00', 'critical', 'data-engineering'),
        ('customer_cdc_pipeline',  '23:59', 'high',     'data-engineering')
    AS source(job_name, sla_window_end, criticality, owner_team)
) AS source
ON target.job_name = source.job_name
WHEN NOT MATCHED THEN INSERT *;


-- ---------------------------------------------------------------------------
-- Part 2: SLA Compliance Report (7-day rolling)
-- Mirrors SLATracker.generate_report(days=7)
-- Use this query as a Databricks SQL Dashboard widget.
-- ---------------------------------------------------------------------------

SELECT
    sla.job_name,
    sla.criticality,
    sla.sla_window_end                                        AS sla_deadline_utc,
    COUNT(*)                                                  AS total_runs,
    SUM(CASE
        WHEN log.overall_status IN ('success', 'completed')
             AND CAST(log.run_timestamp AS TIME) <= CAST(sla.sla_window_end AS TIME)
        THEN 1 ELSE 0
    END)                                                      AS sla_met,
    ROUND(
        SUM(CASE
            WHEN log.overall_status IN ('success', 'completed')
                 AND CAST(log.run_timestamp AS TIME) <= CAST(sla.sla_window_end AS TIME)
            THEN 1 ELSE 0
        END) * 100.0 / NULLIF(COUNT(*), 0),
        1
    )                                                         AS sla_compliance_pct,
    ROUND(AVG(log.duration_seconds) / 60.0, 1)               AS avg_duration_min,
    MAX(log.run_timestamp)                                    AS last_run,
    SUM(CASE WHEN log.overall_status = 'failed' THEN 1 ELSE 0 END) AS failure_count
FROM lakehouse.ops.sla_definitions sla
LEFT JOIN lakehouse.ops.pipeline_run_log log
    ON sla.job_name = log.job_name
    AND log.run_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY sla.job_name, sla.criticality, sla.sla_window_end
ORDER BY
    CASE sla.criticality
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        WHEN 'low'      THEN 4
    END,
    sla_compliance_pct ASC;


-- ---------------------------------------------------------------------------
-- Part 3: SLA Breach Alert Query
-- Replaces SLATracker.record_completion() breach detection.
-- Schedule as a Databricks SQL Alert (trigger: rows > 0).
-- ---------------------------------------------------------------------------

SELECT
    sla.job_name,
    sla.criticality,
    sla.sla_window_end AS sla_deadline,
    log.overall_status,
    log.duration_seconds,
    log.run_timestamp,
    sla.owner_team,
    CONCAT(
        'SLA BREACH: ', sla.job_name,
        ' completed at ', CAST(log.run_timestamp AS STRING),
        ' vs deadline ', sla.sla_window_end, ' UTC'
    ) AS alert_message
FROM lakehouse.ops.sla_definitions sla
JOIN lakehouse.ops.pipeline_run_log log
    ON sla.job_name = log.job_name
WHERE
    log.run_timestamp >= CURRENT_DATE()
    AND (
        -- Job failed
        log.overall_status = 'failed'
        -- OR job succeeded but after SLA deadline
        OR (
            log.overall_status IN ('success', 'completed')
            AND CAST(log.run_timestamp AS TIME) > CAST(sla.sla_window_end AS TIME)
        )
    )
ORDER BY
    CASE sla.criticality
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        WHEN 'low'      THEN 4
    END;
