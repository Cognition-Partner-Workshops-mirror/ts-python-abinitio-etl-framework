# Remediation Roadmap — Ab Initio ETL Framework

> Generated: 2026-05-21 | Repo: `ts-python-abinitio-etl-framework`

---

## Phasing Strategy

| Phase | Focus | Criteria | Timeline |
|---|---|---|---|
| **Phase 1 — Quick Wins** | Critical + High severity, Small effort | Fix broken things and security risks fast | 1–2 days |
| **Phase 2 — Important** | High severity + Medium effort, foundational improvements | Build quality infrastructure | 3–5 days |
| **Phase 3 — Polish** | Medium/Low severity, larger efforts | Long-term maintainability and operability | 1–2 weeks |

---

## Phase 1 — Quick Wins

### 1.1 Fix CDC Processor pandas indexing bug (Gap 2.4)

**Severity:** Critical | **Effort:** Small

The `CDCProcessor.process()` method fails when using `set()` on DataFrame index values. The `loc[list(insert_keys)]` call produces incorrect results for single-key integer indices.

**Devin Prompt:**
```
Fix the pandas indexing bug in graphs/cdc_processor.py. The CDCProcessor.process() method
fails test_inserts_detected and test_deletes_detected in tests/test_pset_manager.py.
The issue is in how insert_keys and delete_keys are used with .loc[] after set_index().
When keys are single integers from a set, .loc[list(keys)] can return unexpected results.
Fix the indexing logic so all 10 tests pass. Run: python -m pytest tests/ -v
```

---

### 1.2 Fix ServiceNow fail-open to fail-closed (Gap 2.1)

**Severity:** Critical | **Effort:** Small

`DeployManager._get_cr_status()` returns `"approved"` when the ServiceNow API is unreachable, allowing unapproved deployments.

**Devin Prompt:**
```
In deployment/deploy_manager.py, fix _get_cr_status() to fail closed. Currently when
ServiceNow is unreachable, it defaults to "approved" which allows unapproved deployments.
Change the except block to return "unavailable" instead of "approved". Then update the
deploy() method to treat "unavailable" as a blocked status (same as non-approved).
Add a unit test in a new file tests/test_deploy_manager.py that mocks the requests call
and verifies deployment is blocked when ServiceNow is unreachable.
```

---

### 1.3 Add HTTP retry logic with tenacity (Gap 7.1)

**Severity:** High | **Effort:** Small

No retry logic on any HTTP call. A single transient error causes immediate failure.

**Devin Prompt:**
```
Add retry logic to all HTTP calls in monitoring/job_monitor.py and deployment/deploy_manager.py.
Use the tenacity library (add to requirements.txt). Apply @retry decorator with:
- wait=wait_exponential(multiplier=1, min=2, max=30)
- stop=stop_after_attempt(3)
- retry=retry_if_exception_type(requests.exceptions.ConnectionError)
Apply to: get_job_status(), _get_cr_status(), _update_cr(), _send_alert().
Add tenacity to requirements.txt. Write tests using unittest.mock to verify retry behavior.
```

---

### 1.4 Validate required secrets at startup (Gap 4.1)

**Severity:** High | **Effort:** Small

API tokens accepted without validation; empty strings silently used.

**Devin Prompt:**
```
Add startup validation for required secrets in deployment/deploy_manager.py and
monitoring/job_monitor.py. In DeployManager.__init__(), raise ValueError if api_token
is empty/None when ucd_url or snow_url are non-default (i.e., not simulation mode).
In JobMonitor.__init__(), log a warning if api_token is None (AutoSys may reject
unauthenticated calls). Add a validate_config() classmethod to each class that checks
all required configuration is present.
```

---

### 1.5 Move SLA state out of /tmp/ (Gap 6.4)

**Severity:** High | **Effort:** Small

SLA history is lost on every system restart.

**Devin Prompt:**
```
In monitoring/sla_tracker.py, change the default state_file path from
"/tmp/abinitio_sla_state.json" to a configurable path that defaults to
"${AI_LOG_DIR}/sla_state.json" (reading AI_LOG_DIR from env, falling back to
"./data/sla_state.json"). Update the constructor docstring. Add a test in
tests/test_sla_tracker.py that verifies state persistence across SLATracker instances.
```

---

### 1.6 Remove unused dependencies (Gap 1.5)

**Severity:** Low | **Effort:** Small

`pydantic`, `pyyaml`, `python-dotenv`, and `croniter` are declared but never imported.

**Devin Prompt:**
```
Audit requirements.txt against actual imports in the codebase. Remove any packages
that are not imported by any .py file. Currently pydantic, pyyaml, python-dotenv, and
croniter appear unused. Verify by running: grep -r "import pydantic\|import yaml\|import
dotenv\|import croniter" --include="*.py" . If truly unused, remove them. Keep pandas,
requests, and pytest. Run tests after to confirm nothing breaks.
```

---

### 1.7 Add pyproject.toml for package structure (Gap 1.1)

**Severity:** Medium | **Effort:** Small

No installable package, no version metadata.

**Devin Prompt:**
```
Create a pyproject.toml for this project using setuptools as the build backend.
Package name: abinitio-etl-framework. Version: 0.1.0. Python requires: >=3.10.
Include packages: graphs, psets, monitoring, deployment, utils. Move dependencies
from requirements.txt into [project.dependencies]. Keep requirements.txt as a
reference but have it point to pyproject.toml with "-e .". Add a [project.scripts]
entry point: abinitio-monitor = "monitoring.job_monitor:main" (create a main()
function stub). Verify with: pip install -e . && python -c "from graphs import CDCProcessor"
```

---

### 1.8 Centralize configuration with defaults module (Gap 1.4)

**Severity:** Medium | **Effort:** Small

Magic strings and default values scattered across classes.

**Devin Prompt:**
```
Create a new file config.py at the project root. Define a Settings class (plain dataclass
or Pydantic BaseSettings) with all configurable values currently hardcoded:
- ai_home (default: /opt/abinitio)
- autosys_url (default: http://autosys-api:8080)
- snow_url (default: https://company.service-now.com)
- ucd_url (default: http://ucd-server:8080)
- sla_state_path (default: ./data/sla_state.json)
- default_partitions (default: 4)
- max_errors (default: 100)
All values should be overridable via environment variables with AI_ prefix.
Update DeployManager, JobMonitor, SLATracker, and ParallelLoader to accept a
Settings instance instead of individual URL parameters.
```

---

### 1.9 Fix hardcoded deployment timeout (Gap 7.5)

**Severity:** Medium | **Effort:** Small

`DeployManager._deploy_graph()` has a hardcoded 300s timeout.

**Devin Prompt:**
```
In deployment/deploy_manager.py, make the air_deploy timeout configurable.
Add a deploy_timeout_seconds parameter to DeployManager.__init__() with default 300.
Use self.deploy_timeout in _deploy_graph() instead of the hardcoded value.
Also add it to the Settings class if you've already created config.py.
```

---

### 1.10 Add dependency vulnerability scanning (Gap 4.5)

**Severity:** Medium | **Effort:** Small

No `pip-audit` or safety scanning configured.

**Devin Prompt:**
```
Add pip-audit to dev dependencies. Create a script scripts/security_scan.sh that runs:
pip-audit --strict --desc on -r requirements.txt
Also add a GitHub Actions workflow .github/workflows/security.yml that runs pip-audit
on every PR. If any vulnerability is found, the workflow should fail.
```

---

## Phase 2 — Important

### 2.1 Write comprehensive unit tests for all modules (Gap 3.1)

**Severity:** Critical | **Effort:** Medium

Only 10 tests exist (2 failing). No tests for 4 out of 8 modules.

**Devin Prompt:**
```
Write unit tests for all untested modules. Create these test files:
- tests/test_parallel_loader.py: Test PartitionManager.generate_ranges() with various
  record counts and partition counts. Test ParallelLoader.run_graph() with mocked
  subprocess.run (both success and failure). Test the simulation fallback.
- tests/test_job_monitor.py: Test get_job_status(), check_sla(), monitor_jobs() with
  mocked requests. Test _send_alert() with mocked Slack webhook. Test SLA breach detection.
- tests/test_sla_tracker.py: Test register_job(), record_completion(), generate_report().
  Test SLA met and missed scenarios. Test 90-day retention.
- tests/test_deploy_manager.py: Test deploy() with mocked ServiceNow and air_deploy.
  Test CR status validation. Test the simulation fallback.
- tests/test_change_validator.py: Test all three validation checks (file existence,
  dev paths in prod, naming convention).
Use pytest fixtures and unittest.mock.patch for external dependencies.
Target: all tests passing, >80% coverage. Run: python -m pytest tests/ -v --tb=short
```

---

### 2.2 Add integration tests with mocked external services (Gap 3.2, 3.3)

**Severity:** High | **Effort:** Medium

No cross-module workflow testing.

**Devin Prompt:**
```
Create tests/test_integration.py with end-to-end workflow tests:
1. Test the deployment workflow: ChangeValidator.validate() → DeployManager.deploy()
   with mocked ServiceNow API (use responses library, add to requirements.txt).
2. Test the CDC pipeline: Load sample data from data/sample/customers.dat, parse with
   DMLParser, run through CDCProcessor with modified source data, verify correct
   inserts/updates/deletes.
3. Test PSET-driven parallel execution: PSETManager.load_pset() → PartitionManager →
   ParallelLoader.run_graph() with mocked subprocess.
Use pytest fixtures for common setup. Mock all external HTTP and subprocess calls.
```

---

### 2.3 Add structured JSON logging (Gap 6.1)

**Severity:** High | **Effort:** Medium

Unformatted f-string log messages with no correlation IDs.

**Devin Prompt:**
```
Implement structured JSON logging across the codebase:
1. Create utils/logging_config.py with a configure_logging() function that sets up
   JSON-formatted log output using python-json-logger (add to requirements.txt).
   Include fields: timestamp, level, logger, message, correlation_id.
2. Add a correlation_id parameter to ParallelLoader.run_graph(), DeployManager.deploy(),
   and JobMonitor.monitor_jobs(). Pass it through to all log messages using
   logging.LoggerAdapter or a context variable.
3. Call configure_logging() in setenv.ksh Python equivalent or at module import.
4. Update all logger.info/warning/error calls to use structured extra fields.
```

---

### 2.4 Add PSET input validation (Gap 4.3)

**Severity:** High | **Effort:** Medium

No validation on PSET parameter values; `os.path.expandvars()` used without sanitization.

**Devin Prompt:**
```
Add input validation to psets/pset_manager.py:
1. Create a PSETSchema class (dict of param_name → expected_type/validator) that can be
   registered per pipeline.
2. In load_pset(), after parsing, validate values against the schema if one is registered.
3. Add a sanitize_value() method that strips shell metacharacters before expandvars().
4. Reject PSET values containing dangerous patterns: $(...), backticks, semicolons,
   pipe characters in non-path contexts.
5. Add tests in tests/test_pset_manager.py for validation and sanitization.
```

---

### 2.5 Improve KSH error handling (Gap 2.3)

**Severity:** High | **Effort:** Medium

Inconsistent error handling across pipeline phases in KSH scripts.

**Devin Prompt:**
```
Improve error handling in scripts/run_daily_orders.ksh and scripts/run_customer_cdc.ksh:
1. Create a reusable function run_phase() that takes graph path, pset, phase name,
   and log file. It should: run the air sandbox command, capture RC, log success/failure,
   and exit with the RC if non-zero.
2. Add a trap handler for ERR signal that logs the failing command and line number.
3. Add a cleanup function registered via trap EXIT that logs pipeline end time and
   duration regardless of success/failure.
4. Apply run_phase() to all phases in both scripts.
5. Add explicit RC checks after each air sandbox run call.
```

---

### 2.6 Add custom exception hierarchy (Gap 2.2)

**Severity:** Medium | **Effort:** Small

No domain-specific exceptions.

**Devin Prompt:**
```
Create utils/exceptions.py with a domain exception hierarchy:
- ETLFrameworkError(Exception) — base class
- PSETNotFoundError(ETLFrameworkError)
- PSETValidationError(ETLFrameworkError)
- DMLParseError(ETLFrameworkError)
- CDCProcessingError(ETLFrameworkError)
- DeploymentBlockedError(ETLFrameworkError)
- DeploymentFailedError(ETLFrameworkError)
- SLABreachError(ETLFrameworkError)
- AutoSysConnectionError(ETLFrameworkError)
- GraphExecutionError(ETLFrameworkError)
Update all modules to raise these instead of generic exceptions. Update existing tests.
```

---

### 2.7 Add deployment idempotency (Gap 7.4)

**Severity:** High | **Effort:** Medium

No duplicate deployment protection.

**Devin Prompt:**
```
Add deployment idempotency to deployment/deploy_manager.py:
1. Create a deployment state file (JSON) at a configurable path that records each
   deployment: {cr_id, graphs, target_env, deployed_at, status}.
2. In deploy(), before proceeding, check if the same CR + graph set + target_env
   combination has already been deployed successfully. If so, return the cached result
   with status "already_deployed".
3. Add a force=False parameter to bypass the idempotency check when needed.
4. Write tests verifying: first deploy succeeds, second deploy returns cached,
   force=True re-deploys.
```

---

### 2.8 Add graceful shutdown to ParallelLoader (Gap 7.6)

**Severity:** High | **Effort:** Medium

No cleanup of running subprocesses on interruption.

**Devin Prompt:**
```
Add graceful shutdown handling to graphs/parallel_loader.py:
1. Store running subprocess.Popen handles in a thread-safe list.
2. Register a signal handler for SIGTERM and SIGINT in run_graph().
3. On signal: cancel all pending futures, terminate running subprocesses
   (send SIGTERM, wait 5s, then SIGKILL), log cleanup actions.
4. Return a partial result with status "interrupted".
5. Add a test that verifies cleanup on KeyboardInterrupt.
```

---

### 2.9 Add test coverage reporting (Gap 3.4)

**Severity:** Medium | **Effort:** Small

No visibility into test coverage.

**Devin Prompt:**
```
Add pytest-cov to requirements.txt. Update the test command to:
python -m pytest tests/ -v --cov=graphs --cov=psets --cov=monitoring --cov=deployment
--cov=utils --cov-report=term-missing --cov-report=html --cov-fail-under=70
Create a pytest.ini or pyproject.toml [tool.pytest.ini_options] section with these
defaults. Run tests and report the current coverage baseline.
```

---

### 2.10 Add health check module (Gap 6.2)

**Severity:** Medium | **Effort:** Medium

No way to verify external dependency connectivity.

**Devin Prompt:**
```
Create monitoring/health_check.py with a HealthChecker class:
1. check_autosys(url, token) — GET /api/v1/health with 5s timeout
2. check_servicenow(url, token) — GET /api/now/table/sys_user?sysparm_limit=1
3. check_abinitio() — verify air_run binary exists and is executable
4. check_filesystem(paths) — verify required directories exist and are writable
5. check_all() — run all checks, return {service: {status, latency_ms, error}}
Add a CLI entry point: python -m monitoring.health_check that prints results as JSON.
Write tests with mocked HTTP responses.
```

---

## Phase 3 — Polish

### 3.1 Extend DML parser for all constructs (Gap 5.2)

**Severity:** High | **Effort:** Large

The parser cannot handle the repo's own complex DML files.

**Devin Prompt:**
```
Extend utils/dml_parser.py to handle all DML constructs present in the dml/ directory:
1. Nested records (e.g., merchant_info in transaction_detail.dml)
2. Variable-length arrays (e.g., string[item_count])
3. Conditional fields (e.g., if (txn_type == 2))
4. Type definitions and includes (e.g., type address_t, include "common_address.dml")
5. void filler fields (account_status.dml)
6. packed_decimal and zoned_decimal types (packed_account.dml)
7. Delimiter specifications: string(","), date("YYYY-MM-DD")(";"), null("") handling

Update the schema dict to include nested_records, arrays, and conditionals.
Update to_create_table_sql() to flatten nested records for SQL DDL generation.
Add tests for each DML file in the dml/ directory — parse and verify field counts and types.
```

---

### 3.2 Add typed return models (Gap 5.3)

**Severity:** Medium | **Effort:** Medium

All methods return raw `Dict[str, Any]`.

**Devin Prompt:**
```
Create dataclasses (or Pydantic models) for all return types:
- graphs/models.py: PartitionRange, PartitionResult, GraphExecutionResult, CDCResult
- monitoring/models.py: JobStatus, SLACheckResult, SLAReport, MonitorResult
- deployment/models.py: DeploymentResult, ValidationResult, ValidationCheck
- utils/models.py: DMLSchema, DMLField
Update all methods to return these typed objects instead of Dict[str, Any].
Update tests to assert on model attributes rather than dict keys.
```

---

### 3.3 Add metrics collection with Prometheus (Gap 6.3)

**Severity:** Medium | **Effort:** Large

No performance instrumentation or trending data.

**Devin Prompt:**
```
Add Prometheus metrics instrumentation:
1. Add prometheus-client to requirements.txt.
2. Create monitoring/metrics.py with counters and histograms:
   - etl_pipeline_duration_seconds (histogram, labels: pipeline, phase)
   - etl_records_processed_total (counter, labels: pipeline, operation)
   - etl_cdc_changes_total (counter, labels: change_type: insert/update/delete)
   - etl_errors_total (counter, labels: module, error_type)
   - etl_sla_compliance_ratio (gauge, labels: job_name)
   - etl_deployment_total (counter, labels: target_env, status)
3. Instrument ParallelLoader.run_graph(), CDCProcessor.process(),
   DeployManager.deploy(), and SLATracker.record_completion().
4. Add a /metrics HTTP endpoint using prometheus_client.start_http_server().
5. Add tests verifying metrics are incremented correctly.
```

---

### 3.4 Add circuit breaker for external services (Gap 7.2)

**Severity:** Medium | **Effort:** Medium

No fail-fast mechanism when external APIs are consistently down.

**Devin Prompt:**
```
Add circuit breaker pattern using pybreaker library:
1. Add pybreaker to requirements.txt.
2. Create utils/circuit_breaker.py with pre-configured breakers:
   - autosys_breaker (fail_max=5, reset_timeout=60)
   - servicenow_breaker (fail_max=3, reset_timeout=120)
   - slack_breaker (fail_max=5, reset_timeout=300)
3. Wrap all HTTP calls in JobMonitor and DeployManager with the appropriate breaker.
4. When circuit is open, return a cached/default response instead of attempting the call.
5. Log circuit state transitions (closed→open, open→half-open, half-open→closed).
6. Add tests simulating circuit breaker state transitions.
```

---

### 3.5 Add async polling for job monitor (Gap 7.3)

**Severity:** Medium | **Effort:** Medium

Blocking `time.sleep()` in monitor_jobs() loop.

**Devin Prompt:**
```
Refactor monitoring/job_monitor.py to support async polling:
1. Create an async version: async_monitor_jobs() using asyncio and aiohttp.
2. Keep the synchronous monitor_jobs() for backward compatibility but add a
   callback parameter: on_status_change(job_name, old_status, new_status).
3. Add a timeout parameter (total monitoring timeout) in addition to max_polls.
4. Return intermediate results if monitoring is interrupted.
5. Add tests for both sync and async versions.
```

---

### 3.6 Add deployment audit trail (Gap 6.5)

**Severity:** Medium | **Effort:** Medium

No immutable record of deployment events.

**Devin Prompt:**
```
Add an audit trail to deployment/deploy_manager.py:
1. Create deployment/audit_log.py with an AuditLogger class that writes to an
   append-only JSON Lines file (one JSON object per line).
2. Record: timestamp, cr_id, graphs, source_env, target_env, deployed_by,
   validation_result, deployment_status, duration_seconds.
3. Integrate into DeployManager.deploy() — log before validation, after deployment.
4. Add a query method: get_deployments(since, until, env, status) for filtering.
5. Add tests verifying audit entries are written correctly.
```

---

### 3.7 Add KSH static analysis with shellcheck (Gap 3.5)

**Severity:** Medium | **Effort:** Medium

Shell scripts have no automated quality checks.

**Devin Prompt:**
```
Add shell script quality checks:
1. Create a script scripts/lint_shell.sh that runs shellcheck on all .ksh files
   with appropriate flags (--shell=ksh --severity=warning).
2. Fix any issues found by shellcheck in setenv.ksh, run_daily_orders.ksh, and
   run_customer_cdc.ksh.
3. Add shellcheck to the CI workflow if one exists, or create
   .github/workflows/lint.yml that runs shellcheck on PR.
4. Document any shellcheck exclusions with comments explaining why.
```

---

### 3.8 Add Slack alert fallback (Gap 2.5)

**Severity:** Medium | **Effort:** Small

Alert failures are silently swallowed.

**Devin Prompt:**
```
Improve alert reliability in monitoring/job_monitor.py:
1. Add a file-based fallback: when Slack webhook fails, write the alert to
   ${AI_LOG_DIR}/alerts/pending_alerts.jsonl (one JSON object per line).
2. Add a retry_pending_alerts() method that reads the file and re-attempts delivery.
3. Add a max_retries counter — after 3 failed attempts for the same alert, mark as
   "abandoned" and log a critical error.
4. Add tests for the fallback mechanism.
```

---

### 3.9 Complete docstrings for all public methods (Gap 5.4)

**Severity:** Low | **Effort:** Small

Some methods lack documentation.

**Devin Prompt:**
```
Add Google-style docstrings to all public methods that are missing them:
- ChangeValidator._check_graph()
- ChangeValidator.validate() (improve existing)
- SLATracker (all methods)
- CDCProcessor._row_hash()
Include Args, Returns, and Raises sections. Follow the docstring style already
used in ParallelLoader.run_graph() as the template.
```

---

### 3.10 Split test file into per-module files (Gap 1.3)

**Severity:** Low | **Effort:** Small

All tests in a single file.

**Devin Prompt:**
```
Split tests/test_pset_manager.py into separate files:
- tests/test_pset_manager.py — keep only TestPSETManager class
- tests/test_dml_parser.py — move TestDMLParser class
- tests/test_cdc_processor.py — move TestCDCProcessor class
Update imports in each file. Run python -m pytest tests/ -v to verify all tests
still pass. Add a tests/conftest.py with shared fixtures if needed.
```

---

## Summary

| Phase | Items | Estimated Effort | Critical Fixes |
|---|---|---|---|
| Phase 1 — Quick Wins | 10 items | 1–2 days | CDC bug, ServiceNow fail-open, HTTP retries |
| Phase 2 — Important | 10 items | 3–5 days | Comprehensive tests, structured logging, PSET validation, graceful shutdown |
| Phase 3 — Polish | 10 items | 1–2 weeks | Full DML parser, metrics, circuit breakers, typed models |
| **Total** | **30 items** | **~2–3 weeks** | |

### Priority Order Within Each Phase

**Phase 1** (do in this order):
1. Fix CDC bug (unblocks 2 failing tests)
2. Fix ServiceNow fail-open (critical security)
3. Add HTTP retries (foundational resilience)
4. Validate secrets (security hygiene)
5. Move SLA state from /tmp/ (data loss prevention)
6. Remove unused deps → Add pyproject.toml → Centralize config → Fix timeout → Add vulnerability scanning

**Phase 2** (do in this order):
1. Write unit tests for all modules (biggest quality uplift)
2. Add integration tests (validates cross-module workflows)
3. Add structured logging (operational visibility)
4. Add PSET validation (security)
5. Improve KSH error handling → Custom exceptions → Deployment idempotency → Graceful shutdown → Coverage reporting → Health checks

**Phase 3** (do in this order):
1. Extend DML parser (enables full migration automation)
2. Add typed return models (developer experience)
3. Remaining items in any order
