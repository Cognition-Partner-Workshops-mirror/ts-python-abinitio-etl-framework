"""Tests for Databricks monitoring definitions migrated from Ab Initio AutoSys monitoring.

Validates:
  - Alert definitions have required structure and fields
  - SQL queries contain expected clauses for system table access
  - Alert trigger conditions are properly configured
  - Dashboard queries cover all sla_tracker.py report functionality
  - Validation helpers catch missing fields and empty destinations
  - Monitoring maps correctly back to legacy monitoring components
"""

import pytest

from databricks.monitoring.alert_definitions import (
    ALERT_DEFINITIONS,
    get_alert_definition,
    get_all_alert_names,
    validate_alert_definition,
)
from databricks.monitoring.sla_dashboard import (
    DASHBOARD_QUERIES,
    get_dashboard_query,
    get_all_dashboard_query_names,
)


# ---------------------------------------------------------------------------
# Alert definition tests
# ---------------------------------------------------------------------------
class TestAlertDefinitions:
    """Validate alert_definitions.py structure and content."""

    def test_four_alerts_defined(self):
        """Four alert types are defined (failure, SLA breach, long-running, data quality)."""
        assert len(ALERT_DEFINITIONS) == 4

    def test_expected_alert_names(self):
        """Alert names match the expected set."""
        expected = {
            "job_failure_alert",
            "sla_breach_alert",
            "long_running_job_alert",
            "data_quality_alert",
        }
        assert set(ALERT_DEFINITIONS.keys()) == expected

    @pytest.mark.parametrize("alert_name", list(ALERT_DEFINITIONS.keys()))
    def test_alert_has_display_name(self, alert_name):
        """Every alert has a human-readable display_name."""
        assert ALERT_DEFINITIONS[alert_name]["display_name"]

    @pytest.mark.parametrize("alert_name", list(ALERT_DEFINITIONS.keys()))
    def test_alert_has_query_sql(self, alert_name):
        """Every alert has a non-empty SQL query."""
        sql = ALERT_DEFINITIONS[alert_name]["query_sql"]
        assert sql and sql.strip()

    @pytest.mark.parametrize("alert_name", list(ALERT_DEFINITIONS.keys()))
    def test_alert_has_trigger_condition(self, alert_name):
        """Every alert has trigger_condition with op and threshold."""
        trigger = ALERT_DEFINITIONS[alert_name]["trigger_condition"]
        assert "op" in trigger
        assert "threshold" in trigger

    @pytest.mark.parametrize("alert_name", list(ALERT_DEFINITIONS.keys()))
    def test_alert_has_schedule(self, alert_name):
        """Every alert has a schedule with interval or cron expression."""
        schedule = ALERT_DEFINITIONS[alert_name]["schedule"]
        has_interval = "interval_minutes" in schedule
        has_cron = "quartz_cron_expression" in schedule
        assert has_interval or has_cron

    @pytest.mark.parametrize("alert_name", list(ALERT_DEFINITIONS.keys()))
    def test_alert_has_notifications(self, alert_name):
        """Every alert has a notifications section."""
        assert "notifications" in ALERT_DEFINITIONS[alert_name]

    @pytest.mark.parametrize("alert_name", list(ALERT_DEFINITIONS.keys()))
    def test_alert_has_severity(self, alert_name):
        """Every alert has a severity level."""
        severity = ALERT_DEFINITIONS[alert_name]["severity"]
        assert severity in ("critical", "high", "warning", "low")

    @pytest.mark.parametrize("alert_name", list(ALERT_DEFINITIONS.keys()))
    def test_alert_has_tags(self, alert_name):
        """Every alert has at least one tag."""
        tags = ALERT_DEFINITIONS[alert_name]["tags"]
        assert isinstance(tags, list) and len(tags) > 0


class TestAlertSQLQueries:
    """Validate SQL query content in alert definitions."""

    def test_job_failure_queries_system_table(self):
        """Job failure alert queries system.workflow.job_run_timeline."""
        sql = ALERT_DEFINITIONS["job_failure_alert"]["query_sql"]
        assert "system.workflow.job_run_timeline" in sql

    def test_job_failure_filters_failed_states(self):
        """Job failure alert filters for FAILED, TIMED_OUT, CANCELED states."""
        sql = ALERT_DEFINITIONS["job_failure_alert"]["query_sql"]
        assert "FAILED" in sql
        assert "TIMED_OUT" in sql

    def test_job_failure_filters_abinitio_tag(self):
        """Job failure alert filters for migration_source = 'abinitio' tag."""
        sql = ALERT_DEFINITIONS["job_failure_alert"]["query_sql"]
        assert "abinitio" in sql
        assert "migration_source" in sql

    def test_sla_breach_compares_duration_to_timeout(self):
        """SLA breach alert compares actual duration against timeout_seconds."""
        sql = ALERT_DEFINITIONS["sla_breach_alert"]["query_sql"]
        assert "timeout_seconds" in sql
        assert "TIMESTAMPDIFF" in sql

    def test_sla_breach_has_24h_window(self):
        """SLA breach alert uses a 24-hour lookback window."""
        sql = ALERT_DEFINITIONS["sla_breach_alert"]["query_sql"]
        assert "-24" in sql or "24" in sql

    def test_long_running_checks_80_percent_threshold(self):
        """Long-running alert checks for 80% of timeout consumed."""
        sql = ALERT_DEFINITIONS["long_running_job_alert"]["query_sql"]
        assert "0.8" in sql

    def test_long_running_checks_null_result_state(self):
        """Long-running alert targets currently running jobs (result_state IS NULL)."""
        sql = ALERT_DEFINITIONS["long_running_job_alert"]["query_sql"]
        assert "result_state IS NULL" in sql

    def test_data_quality_references_max_errors(self):
        """Data quality alert references the max_errors threshold concept."""
        sql = ALERT_DEFINITIONS["data_quality_alert"]["query_sql"]
        assert "max_errors" in sql


class TestAlertSchedules:
    """Validate alert schedule configuration."""

    def test_failure_alert_runs_every_5_minutes(self):
        """Job failure alert evaluates every 5 minutes for fast detection."""
        schedule = ALERT_DEFINITIONS["job_failure_alert"]["schedule"]
        assert schedule["interval_minutes"] == 5

    def test_sla_breach_runs_hourly(self):
        """SLA breach alert evaluates hourly."""
        schedule = ALERT_DEFINITIONS["sla_breach_alert"]["schedule"]
        assert schedule["interval_minutes"] == 60

    def test_long_running_runs_every_10_minutes(self):
        """Long-running alert evaluates every 10 minutes."""
        schedule = ALERT_DEFINITIONS["long_running_job_alert"]["schedule"]
        assert schedule["interval_minutes"] == 10

    def test_data_quality_runs_every_30_minutes(self):
        """Data quality alert evaluates every 30 minutes."""
        schedule = ALERT_DEFINITIONS["data_quality_alert"]["schedule"]
        assert schedule["interval_minutes"] == 30


class TestAlertNotifications:
    """Validate alert notification structure maps to legacy Slack webhook pattern."""

    def test_failure_alert_has_slack_webhook(self):
        """Job failure alert has Slack webhook placeholder (maps to _send_alert)."""
        notifs = ALERT_DEFINITIONS["job_failure_alert"]["notifications"]
        assert "slack_webhook" in notifs
        # Migration note should reference the original _send_alert method
        assert "_migration_note" in notifs["slack_webhook"]

    def test_failure_alert_has_pagerduty(self):
        """Job failure alert has PagerDuty integration (new capability)."""
        notifs = ALERT_DEFINITIONS["job_failure_alert"]["notifications"]
        assert "pagerduty" in notifs

    def test_all_alerts_have_email(self):
        """All alerts have email notification placeholders."""
        for name, defn in ALERT_DEFINITIONS.items():
            assert "email" in defn["notifications"], f"{name} missing email notification"


# ---------------------------------------------------------------------------
# Alert lookup and validation helper tests
# ---------------------------------------------------------------------------
class TestAlertHelpers:
    """Validate get_alert_definition and validate_alert_definition."""

    def test_get_existing_alert(self):
        """get_alert_definition returns dict for known alert."""
        defn = get_alert_definition("job_failure_alert")
        assert defn is not None
        assert defn["display_name"] == "ETL Job Failure Alert"

    def test_get_nonexistent_alert_returns_none(self):
        """get_alert_definition returns None for unknown alert."""
        assert get_alert_definition("nonexistent") is None

    def test_get_all_alert_names(self):
        """get_all_alert_names returns all 4 alert names."""
        names = get_all_alert_names()
        assert len(names) == 4
        assert "job_failure_alert" in names

    def test_validate_existing_alert_is_valid(self):
        """validate_alert_definition returns valid=True for well-formed alerts."""
        for name in ALERT_DEFINITIONS:
            result = validate_alert_definition(name)
            assert result["valid"], f"{name} validation failed: {result['errors']}"

    def test_validate_nonexistent_alert(self):
        """validate_alert_definition returns valid=False for unknown alert."""
        result = validate_alert_definition("nonexistent")
        assert not result["valid"]
        assert any("not found" in e for e in result["errors"])

    def test_validate_warns_on_empty_destinations(self):
        """Validation warns when no notification destinations are configured."""
        # All alerts have empty placeholder destinations, so all should warn
        for name in ALERT_DEFINITIONS:
            result = validate_alert_definition(name)
            assert len(result["warnings"]) > 0, f"{name} should warn about empty destinations"


# ---------------------------------------------------------------------------
# Dashboard query tests
# ---------------------------------------------------------------------------
class TestDashboardQueries:
    """Validate sla_dashboard.py query definitions."""

    def test_five_dashboard_queries_defined(self):
        """Five dashboard queries are defined."""
        assert len(DASHBOARD_QUERIES) == 5

    def test_expected_query_names(self):
        """Dashboard query names match the expected set."""
        expected = {
            "sla_compliance_summary",
            "sla_compliance_trend",
            "job_duration_heatmap",
            "pipeline_run_detail",
            "cdc_change_volume",
        }
        assert set(DASHBOARD_QUERIES.keys()) == expected

    @pytest.mark.parametrize("query_name", list(DASHBOARD_QUERIES.keys()))
    def test_query_has_display_name(self, query_name):
        """Every dashboard query has a display_name."""
        assert DASHBOARD_QUERIES[query_name]["display_name"]

    @pytest.mark.parametrize("query_name", list(DASHBOARD_QUERIES.keys()))
    def test_query_has_sql(self, query_name):
        """Every dashboard query has non-empty SQL."""
        sql = DASHBOARD_QUERIES[query_name]["query_sql"]
        assert sql and sql.strip()

    @pytest.mark.parametrize("query_name", list(DASHBOARD_QUERIES.keys()))
    def test_query_has_visualization(self, query_name):
        """Every dashboard query has visualization metadata."""
        assert "visualization" in DASHBOARD_QUERIES[query_name]

    @pytest.mark.parametrize("query_name", list(DASHBOARD_QUERIES.keys()))
    def test_query_has_parameters(self, query_name):
        """Every dashboard query has configurable parameters."""
        assert "parameters" in DASHBOARD_QUERIES[query_name]


class TestSLAComplianceSummary:
    """Validate the SLA compliance summary query (replaces generate_report)."""

    @pytest.fixture
    def query(self):
        return DASHBOARD_QUERIES["sla_compliance_summary"]

    def test_queries_system_workflow_tables(self, query):
        """Query accesses system.workflow.job_run_timeline."""
        assert "system.workflow.job_run_timeline" in query["query_sql"]

    def test_computes_sla_compliance_pct(self, query):
        """Query calculates sla_compliance_pct (maps to sla_tracker.py field)."""
        assert "sla_compliance_pct" in query["query_sql"]

    def test_computes_avg_duration(self, query):
        """Query calculates avg_duration_minutes (maps to avg_duration_min)."""
        assert "avg_duration_minutes" in query["query_sql"]

    def test_filters_abinitio_tag(self, query):
        """Query filters for migrated Ab Initio pipelines."""
        assert "abinitio" in query["query_sql"]

    def test_default_lookback_7_days(self, query):
        """Default lookback is 7 days (matches generate_report default)."""
        assert query["parameters"]["days"]["default"] == 7

    def test_visualization_is_table(self, query):
        """Visualization type is table with conditional formatting."""
        assert query["visualization"]["type"] == "table"


class TestSLAComplianceTrend:
    """Validate the 90-day SLA trend query (replaces run_history retention)."""

    @pytest.fixture
    def query(self):
        return DASHBOARD_QUERIES["sla_compliance_trend"]

    def test_default_lookback_90_days(self, query):
        """Default lookback is 90 days (matches sla_tracker.py run_history[-90:])."""
        assert query["parameters"]["days"]["default"] == 90

    def test_groups_by_date(self, query):
        """Query groups by run_date for daily granularity."""
        assert "run_date" in query["query_sql"]
        assert "GROUP BY" in query["query_sql"]

    def test_visualization_is_line_chart(self, query):
        """Visualization type is line chart for trend analysis."""
        assert query["visualization"]["type"] == "line_chart"

    def test_has_95_pct_reference_line(self, query):
        """Visualization includes a 95% SLA target reference line."""
        assert query["visualization"]["reference_line"]["value"] == 95.0


class TestPipelineRunDetail:
    """Validate the pipeline run detail query (replaces run_history entries)."""

    @pytest.fixture
    def query(self):
        return DASHBOARD_QUERIES["pipeline_run_detail"]

    def test_maps_sla_tracker_fields(self, query):
        """Query includes all fields from sla_tracker.py run_record."""
        sql = query["query_sql"]
        # Maps to: date, completed_at, status, duration_minutes, sla_met
        assert "run_date" in sql
        assert "completed_at" in sql
        assert "status" in sql
        assert "duration_minutes" in sql
        assert "sla_met" in sql

    def test_visualization_is_table(self, query):
        """Visualization is a sortable detail table."""
        assert query["visualization"]["type"] == "table"


class TestCDCChangeVolume:
    """Validate the CDC change volume query (extends cdc_processor stats)."""

    @pytest.fixture
    def query(self):
        return DASHBOARD_QUERIES["cdc_change_volume"]

    def test_tracks_insert_update_delete(self, query):
        """Query tracks inserts, updates, and deletes (maps to stats dict)."""
        sql = query["query_sql"]
        assert "inserts" in sql.lower()
        assert "updates" in sql.lower()
        assert "deletes" in sql.lower()

    def test_uses_change_data_feed(self, query):
        """Query reads from Delta Change Data Feed."""
        sql = query["query_sql"]
        assert "table_changes" in sql or "_change_type" in sql

    def test_visualization_is_stacked_bar(self, query):
        """Visualization is a stacked bar chart for I/U/D volumes."""
        assert query["visualization"]["type"] == "stacked_bar_chart"


class TestJobDurationHeatmap:
    """Validate the job duration heatmap query (new capability)."""

    @pytest.fixture
    def query(self):
        return DASHBOARD_QUERIES["job_duration_heatmap"]

    def test_groups_by_hour_and_day(self, query):
        """Query groups by hour_of_day and day_of_week."""
        sql = query["query_sql"]
        assert "hour_of_day" in sql
        assert "day_of_week" in sql

    def test_computes_p95_duration(self, query):
        """Query computes P95 duration for outlier detection."""
        sql = query["query_sql"]
        assert "PERCENTILE_APPROX" in sql or "p95" in sql.lower()

    def test_visualization_is_heatmap(self, query):
        """Visualization type is heatmap."""
        assert query["visualization"]["type"] == "heatmap"


# ---------------------------------------------------------------------------
# Dashboard lookup helper tests
# ---------------------------------------------------------------------------
class TestDashboardHelpers:
    """Validate get_dashboard_query and get_all_dashboard_query_names."""

    def test_get_existing_query(self):
        """get_dashboard_query returns dict for known query."""
        defn = get_dashboard_query("sla_compliance_summary")
        assert defn is not None
        assert "query_sql" in defn

    def test_get_nonexistent_query_returns_none(self):
        """get_dashboard_query returns None for unknown query."""
        assert get_dashboard_query("nonexistent") is None

    def test_get_all_query_names(self):
        """get_all_dashboard_query_names returns all 5 query names."""
        names = get_all_dashboard_query_names()
        assert len(names) == 5
        assert "sla_compliance_summary" in names


# ---------------------------------------------------------------------------
# Cross-validation: monitoring migration completeness
# ---------------------------------------------------------------------------
class TestMonitoringMigrationCompleteness:
    """Verify all monitoring/job_monitor.py and sla_tracker.py features are covered."""

    def test_job_failure_detection_covered(self):
        """JobMonitor.monitor_jobs() failure detection has an alert."""
        assert "job_failure_alert" in ALERT_DEFINITIONS

    def test_sla_breach_detection_covered(self):
        """JobMonitor.check_sla() SLA breach has an alert."""
        assert "sla_breach_alert" in ALERT_DEFINITIONS

    def test_slack_alerting_covered(self):
        """JobMonitor._send_alert() Slack webhook has notification config."""
        for name, defn in ALERT_DEFINITIONS.items():
            assert "slack_webhook" in defn["notifications"], \
                f"{name} missing Slack notification (maps to _send_alert)"

    def test_sla_report_covered(self):
        """SLATracker.generate_report() has a dashboard query."""
        assert "sla_compliance_summary" in DASHBOARD_QUERIES

    def test_run_history_retention_covered(self):
        """SLATracker 90-day run_history has a trend query."""
        assert "sla_compliance_trend" in DASHBOARD_QUERIES

    def test_run_detail_log_covered(self):
        """SLATracker.record_completion() entries have a detail query."""
        assert "pipeline_run_detail" in DASHBOARD_QUERIES

    def test_all_alert_queries_reference_system_tables(self):
        """All alerts that monitor job execution reference system.workflow tables."""
        for name in ["job_failure_alert", "sla_breach_alert", "long_running_job_alert"]:
            sql = ALERT_DEFINITIONS[name]["query_sql"]
            assert "system.workflow" in sql, \
                f"{name} should query system.workflow tables"

    def test_all_dashboard_queries_filter_migrated_pipelines(self):
        """Dashboard queries filter for migration_source='abinitio'."""
        # The CDF query targets specific tables, not system.workflow
        for name in ["sla_compliance_summary", "sla_compliance_trend",
                      "job_duration_heatmap", "pipeline_run_detail"]:
            sql = DASHBOARD_QUERIES[name]["query_sql"]
            assert "abinitio" in sql, \
                f"{name} should filter for migrated Ab Initio pipelines"
