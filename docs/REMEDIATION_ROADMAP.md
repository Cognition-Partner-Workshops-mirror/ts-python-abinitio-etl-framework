# Ab Initio ETL Framework — Remediation Roadmap

Prioritised remediation plan organised into three phases. Each item includes a Devin prompt that can be executed directly.

---

## Phase 1 — Quick Wins (High Severity / Low Effort)

Items that address critical or high-severity gaps with small implementation effort. Target: 1-2 weeks.

---

### 1.1 Fix Silent Approval on ServiceNow API Failure (GAP-EH-01)

**Severity: Critical | Effort: Small**

The `DeployManager._get_cr_status()` method defaults to `"approved"` when ServiceNow is unreachable, allowing unvalidated production deployments.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, fix the critical bug in `deployment/deploy_manager.py` where `_get_cr_status()` returns `"approved"` as a default when the ServiceNow API call fails. Instead, the method should return `"unknown"` on failure, and `deploy()` should treat `"unknown"` as a blocking status (i.e., do not proceed with deployment). Add a `max_retries` parameter (default 3) with exponential backoff to `_get_cr_status()` before failing. Add unit tests in `tests/test_deploy_manager.py` that mock the ServiceNow API and verify: (a) deployment is blocked when API is unreachable, (b) deployment proceeds when CR is approved, (c) retries are attempted before failing.

---

### 1.2 Add Lock Files to Shell Scripts (GAP-RE-02)

**Severity: High | Effort: Small**

No concurrency control — overlapping AutoSys triggers can corrupt data.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add lock file logic to both `scripts/run_daily_orders.ksh` and `scripts/run_customer_cdc.ksh`. Use a lock file at `${AI_LOG_DIR}/<pipeline_name>.lock` with `flock` (or a PID-based fallback). The script should: (a) acquire an exclusive lock at startup, (b) exit with a clear error message and non-zero return code if another instance is running, (c) release the lock on exit (including on error) via a `trap` handler. Also add `trap` cleanup to both scripts for graceful error reporting.

---

### 1.3 Add Retry Logic to External API Calls (GAP-EH-02)

**Severity: High | Effort: Small**

Single-attempt HTTP calls to AutoSys and ServiceNow fail on transient errors.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add a shared retry utility (e.g., `utils/retry.py`) that implements exponential backoff with jitter (max 3 retries, base delay 1s, max delay 30s). Apply it to: (a) `monitoring/job_monitor.py` — `get_job_status()` and `_send_alert()`, (b) `deployment/deploy_manager.py` — `_get_cr_status()` and `_update_cr()`. Use `requests.Session` with a `urllib3.util.retry.Retry` adapter or a decorator-based approach. Add unit tests with mocked HTTP responses for retry scenarios.

---

### 1.4 Add Return Code Checks to Shell Scripts (GAP-EH-03)

**Severity: High | Effort: Small-Medium**

`run_daily_orders.ksh` only checks Phase 1 return code; phases 2-4 run unchecked. `run_customer_cdc.ksh` relies solely on `set -e`.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, update `scripts/run_daily_orders.ksh` to check the return code after every phase (phases 2, 3, and 4) with the same pattern used for Phase 1 — log a FATAL message and exit with the return code on failure. Update `scripts/run_customer_cdc.ksh` to add explicit return code checks after each of the 4 steps. In both scripts, add a `trap` handler that logs the failure line number and exit code on any unexpected error.

---

### 1.5 Add Custom Exception Hierarchy (GAP-EH-04)

**Severity: Medium | Effort: Small**

No domain-specific exceptions for structured error handling.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a `utils/exceptions.py` module with a base `ETLFrameworkError(Exception)` and specific subclasses: `PSETNotFoundError`, `PSETValidationError`, `DMLParseError`, `DeploymentBlockedError`, `SLABreachError`, `GraphExecutionError`, `CDCProcessingError`. Update all existing modules to raise these domain exceptions instead of generic ones. Update existing tests to assert the correct exception types.

---

### 1.6 Add pyproject.toml Package Structure (GAP-CO-04)

**Severity: Medium | Effort: Small**

No formal Python package definition.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a `pyproject.toml` with: (a) `[project]` metadata (name="abinitio-etl-framework", version="1.0.0", python requires >= 3.10), (b) all dependencies from `requirements.txt` as project dependencies, (c) pytest as a dev dependency, (d) `[tool.pytest.ini_options]` with `testpaths = ["tests"]`. Add a `src/` layout or ensure the current flat layout works with `pip install -e .`. Verify `pytest` runs successfully after the change.

---

### 1.7 Move SLA State Out of /tmp (GAP-RE-04, GAP-SE-04)

**Severity: Medium | Effort: Small**

SLA state stored in volatile, world-readable `/tmp`.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, update `monitoring/sla_tracker.py` to change the default `state_file` path from `/tmp/abinitio_sla_state.json` to `${AI_LOG_DIR}/sla_state.json` (falling back to `~/.abinitio/sla_state.json` if the env var is not set). Ensure the parent directory is created with `mkdir -p` and file permissions are set to `0o600`. Update the constructor to accept the path as a parameter for testability.

---

### 1.8 Upgrade CDC Hash to SHA-256 (GAP-SE-03)

**Severity: Medium | Effort: Small**

MD5 has known collision weaknesses.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, update `graphs/cdc_processor.py` `_row_hash()` to use `hashlib.sha256` instead of `hashlib.md5`. Make the hash algorithm configurable via a constructor parameter `hash_algorithm` (default `"sha256"`, also accept `"md5"` for backward compatibility). Update the existing CDC tests to verify both algorithms produce correct change detection results.

---

### 1.9 Add PSET Schema Validation (GAP-AD-03)

**Severity: Medium | Effort: Small**

Missing PSET parameters are only discovered at graph execution time.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add a `validate_pset()` method to `PSETManager` that accepts a `required_params: List[str]` argument and raises `PSETValidationError` if any required parameter is missing from the loaded PSET. Add a PSET schema definition file (`psets/schemas/`) in YAML format that defines required and optional parameters per pipeline (e.g., `orders_pipeline` requires `SOURCE_PATH`, `TARGET_TABLE`, `PARTITION_COUNT`). Add tests for validation pass and fail cases.

---

### 1.10 Add Centralised Logging Configuration (GAP-OB-01)

**Severity: Medium | Effort: Small**

No root logger setup; log format/level undefined.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a `utils/logging_config.py` module that provides a `setup_logging(level="INFO", json_format=False)` function. When `json_format=True`, output structured JSON logs with fields: `timestamp`, `level`, `module`, `message`, `job_name` (optional). When `False`, output human-readable format with timestamp and module. Update the KSH scripts to set `LOG_LEVEL` environment variable and have the Python modules read it. Add a brief test that verifies log output format.

---

### 1.11 Add Dependency Vulnerability Scanning (GAP-SE-05)

**Severity: Medium | Effort: Small**

No automated dependency security scanning.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add a GitHub Actions workflow (`.github/workflows/security.yml`) that runs `pip-audit` on every push and PR against `requirements.txt`. Add `pip-audit` to dev dependencies in `pyproject.toml`. Also add a `safety` check as a secondary scanner. The workflow should fail the build if any known vulnerabilities are found.

---

## Phase 2 — Important (High Severity / Medium-Large Effort)

Items that address significant gaps requiring more substantial implementation. Target: 3-6 weeks.

---

### 2.1 Rewrite DML Parser for Full Spec Coverage (GAP-CO-01)

**Severity: High | Effort: Large**

The parser must handle the full Ab Initio DML specification used in this codebase.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, rewrite `utils/dml_parser.py` to handle the full DML syntax used across all 8 DML files in the `dml/` directory. The parser must support: (a) delimited fields with delimiter specs like `decimal(",")`, `string("|")`, (b) typed decimals with precision like `decimal("8.2", "|")`, (c) date/datetime with format masks like `date("YYYY-MM-DD")`, (d) nested records (`record ... end name`), (e) variable-length arrays (`type[count_field]`), (f) conditional records (`if (field == value) record ... end`), (g) `void` padding fields, (h) `packed_decimal` and `zoned_decimal` types, (i) `include` directives, (j) named `type` definitions (`type name = record ... end`), (k) `null()` specifications. Use a recursive descent parser or a proper grammar. Add type mappings for Spark (`StructType`), Snowflake, and Teradata dialects in `to_create_table_sql()`. Write comprehensive tests that parse every DML file in the `dml/` directory and verify correct field extraction.

---

### 2.2 Add Unit Tests for All Untested Modules (GAP-TE-01)

**Severity: High | Effort: Large**

Five modules have zero test coverage.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add comprehensive unit tests for the five untested Python modules. Create separate test files mirroring the source structure: (a) `tests/test_parallel_loader.py` — test `PartitionManager.generate_ranges()` with various record counts and partition counts; test `ParallelLoader.run_graph()` with simulated execution (mock `subprocess.run`); test failure handling when a partition fails. (b) `tests/test_job_monitor.py` — mock AutoSys REST API responses; test `get_job_status()`, `check_sla()` with before/after SLA deadline, `monitor_jobs()` polling loop. (c) `tests/test_sla_tracker.py` — test job registration, completion recording, SLA met/missed evaluation, 90-day retention, report generation. (d) `tests/test_deploy_manager.py` — mock ServiceNow and UrbanCode APIs; test approved/blocked CR flow, partial deployment failure, CR update. (e) `tests/test_change_validator.py` — test graph existence check, dev-path detection in prod, naming convention warning. Target: minimum 5 tests per module.

---

### 2.3 Add CDC Update Detection Test (GAP-TE-03)

**Severity: Medium | Effort: Small**

The most complex CDC code path is untested.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add tests to `TestCDCProcessor` in `tests/test_pset_manager.py` (or a new `tests/test_cdc_processor.py`) that cover: (a) update detection — source has same keys as target but different non-key column values, verify `stats["updates"] > 0` and the updates DataFrame contains the correct rows, (b) composite key CDC — use multi-column keys, (c) selective compare_columns — only compare a subset of non-key columns, (d) empty source and empty target edge cases, (e) large DataFrame performance (1000+ rows).

---

### 2.4 Add CI/CD Pipeline (GAP-TE-05)

**Severity: High | Effort: Medium**

No automated testing on push or PR.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a GitHub Actions CI pipeline (`.github/workflows/ci.yml`) that: (a) runs on push to `main` and all PRs, (b) uses Python 3.11, (c) installs dependencies from `requirements.txt`, (d) runs `pytest` with coverage reporting (`pytest --cov=. --cov-report=xml`), (e) runs `ruff` or `flake8` for linting, (f) uploads coverage to Codecov or as an artifact, (g) fails the build if any test fails or coverage drops below 50%.

---

### 2.5 Unify Shell and Python Configuration (GAP-CO-03)

**Severity: Medium | Effort: Medium**

Shell environment variables are disconnected from Python module defaults.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a `utils/config.py` module that: (a) reads all `AI_*` and `AB_*` environment variables (as set by `setenv.ksh`), (b) provides typed access via a Pydantic `Settings` model with defaults matching `setenv.ksh`, (c) validates that critical paths exist on the filesystem, (d) is used by `ParallelLoader` (for `air_root`), `DeployManager` (for URLs/tokens), `SLATracker` (for state file path), and other modules instead of their hardcoded defaults. Add tests that verify the config loads correctly with and without environment variables set.

---

### 2.6 Add Secrets Management Integration (GAP-SE-01)

**Severity: High | Effort: Medium**

Credentials are plain environment variables with no rotation or validation.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a `utils/secrets.py` module that provides a `get_secret(name)` function. It should: (a) first check for a local `.env` file (via `python-dotenv`, already a dependency), (b) then check environment variables, (c) optionally integrate with AWS Secrets Manager or HashiCorp Vault if the `SECRETS_BACKEND` env var is set. Add validation that critical secrets (`DEPLOY_API_TOKEN`, `AUTOSYS_API_TOKEN`, `SLACK_WEBHOOK_URL`) are present at startup. Update `DeployManager` and `JobMonitor` to use the secrets module. Add a `--validate-secrets` CLI flag that checks all required secrets are accessible without executing any pipeline.

---

### 2.7 Implement Checkpoint/Restart in ParallelLoader (GAP-RE-01)

**Severity: High | Effort: Large**

Failed pipelines must be re-run from scratch.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add checkpoint/restart capability to `graphs/parallel_loader.py`. The `ParallelLoader` should: (a) accept a `checkpoint_dir` parameter, (b) before running a partition, check if a checkpoint file exists for that partition (indicating prior successful completion), (c) skip already-completed partitions on restart, (d) write a checkpoint file (JSON with partition number, record range, timestamp, status) after each successful partition, (e) provide a `clear_checkpoints(graph_path)` method to reset for a fresh run, (f) log checkpoint hits/misses. Add corresponding tests with a temp checkpoint directory.

---

### 2.8 Add Structured Response Models (GAP-AD-02)

**Severity: Medium | Effort: Medium**

Return values are untyped dicts with inconsistent structures.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a `utils/models.py` module with Pydantic models for all return types: `GraphExecutionResult`, `PartitionResult`, `CDCResult`, `CDCStats`, `DeploymentResult`, `JobStatus`, `SLACheckResult`, `SLAReport`, `ValidationResult`. Update all modules to return these models instead of raw dicts. Models should have consistent status enums (`SUCCESS`, `FAILED`, `PARTIAL_FAILURE`, `BLOCKED`, `UNKNOWN`). Add `model_dump()` compatibility for JSON serialisation. Update existing tests to work with the new return types.

---

### 2.9 Add Health Checks (GAP-OB-03)

**Severity: Medium | Effort: Small**

No pre-flight validation of external dependencies.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a `utils/healthcheck.py` module with a `HealthChecker` class that can verify: (a) AutoSys API is reachable (HTTP GET to base URL), (b) ServiceNow API is reachable, (c) Ab Initio runtime is available (`air_run --version`), (d) required filesystem paths exist (`AI_PROJECT_DIR`, `AI_LOG_DIR`, `AI_DATA_DIR`), (e) PSET files are loadable for a given pipeline. Provide a `check_all()` method that returns a summary dict of pass/fail per check. Add a `--healthcheck` mode to the KSH scripts that runs this before pipeline execution.

---

## Phase 3 — Polish (Medium-Low Severity)

Items that improve developer experience and operational maturity. Target: 6-10 weeks.

---

### 3.1 Add REST API / CLI Layer (GAP-AD-01)

**Severity: Medium | Effort: Large**

No self-service interface for operations.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add a lightweight FastAPI application (`api/`) that exposes: (a) `GET /api/v1/psets/{name}?env=dev` — load and return PSET parameters, (b) `POST /api/v1/graphs/run` — trigger a graph execution, (c) `GET /api/v1/jobs/{name}/status` — proxy to AutoSys job status, (d) `GET /api/v1/sla/report?days=7` — SLA compliance report, (e) `POST /api/v1/deploy` — trigger deployment with CR validation, (f) `GET /api/v1/health` — health check endpoint. Add OpenAPI docs (auto-generated by FastAPI). Also add a `cli.py` using `click` or `typer` for command-line access to the same operations. Add FastAPI and uvicorn to dependencies.

---

### 3.2 Add Structured JSON Logging (GAP-OB-02)

**Severity: Medium | Effort: Medium**

Logs are unstructured f-strings.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add a `structlog` or `python-json-logger` integration to `utils/logging_config.py`. Define a standard log schema: `{"timestamp", "level", "module", "job_name", "pipeline", "partition", "environment", "correlation_id", "message", "duration_ms"}`. Add a context manager or decorator that automatically injects `job_name`, `pipeline`, and `correlation_id` into all log records for the duration of a pipeline run. Update all modules to use the structured logger. Add `structlog` to dependencies.

---

### 3.3 Add Metrics Collection (GAP-OB-04)

**Severity: Medium | Effort: Medium**

No dashboarding or metrics export.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a `utils/metrics.py` module that collects and exports pipeline metrics. Track: (a) records processed per partition, (b) partition duration, (c) CDC change counts (inserts/updates/deletes), (d) error/reject counts, (e) SLA compliance percentage. Support export via: (a) JSON file (for simple consumption), (b) Prometheus pushgateway (optional, if `PROMETHEUS_PUSHGATEWAY_URL` is set). Instrument `ParallelLoader`, `CDCProcessor`, and `SLATracker` to emit metrics. Add tests with a mock metrics collector.

---

### 3.4 Add Circuit Breaker for External APIs (GAP-RE-03)

**Severity: Medium | Effort: Medium**

No protection against cascading failures from external services.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add a circuit breaker implementation (using `pybreaker` or a lightweight custom implementation) to `utils/circuit_breaker.py`. Apply it to: (a) `JobMonitor` AutoSys API calls, (b) `DeployManager` ServiceNow API calls, (c) Slack webhook calls. Configure: failure threshold = 5, recovery timeout = 60s, half-open max calls = 1. When the circuit is open, return a cached last-known status (for monitoring) or raise `ServiceUnavailableError` (for deployment). Add `pybreaker` to dependencies and tests for circuit state transitions.

---

### 3.5 Add Integration Tests with Sample Data (GAP-TE-02)

**Severity: High | Effort: Medium**

No end-to-end pipeline tests.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create `tests/test_integration.py` with end-to-end tests that exercise the full pipeline path using sample data: (a) Parse `dml/customer.dml` → load `customers.dat` into a DataFrame → run CDC against an empty target → verify all rows are inserts, (b) Load `orders_pipeline.pset` → verify all required parameters are present → simulate graph execution via `ParallelLoader` (with `air_run` not found, using simulated mode) → verify success status, (c) Parse all 8 DML files and verify no parse errors (once parser is upgraded), (d) Load all 3 PSET templates and verify they diff correctly between environments.

---

### 3.6 Improve Shell Script Logging (GAP-OB-05)

**Severity: Low | Effort: Small**

KSH scripts have minimal operational logging.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, enhance both KSH scripts to log: (a) disk space available in `AI_DATA_DIR` and `AI_LOG_DIR` before execution, (b) duration of each phase (using `$SECONDS` or `date` arithmetic), (c) the PSET parameters used (echo key params to log), (d) a summary line at completion with total duration and phase breakdown. Add a `log_phase_result()` function to `setenv.ksh` that standardises the log format across all scripts.

---

### 3.7 Reorganise Test Structure (GAP-CO-05)

**Severity: Low | Effort: Small**

All tests in a single file.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, reorganise the test suite to mirror the source structure: (a) `tests/test_pset_manager.py` — only `TestPSETManager`, (b) `tests/test_dml_parser.py` — only `TestDMLParser`, (c) `tests/test_cdc_processor.py` — only `TestCDCProcessor`. Add a `tests/conftest.py` with shared fixtures (e.g., `tmp_path` wrappers, sample DataFrames). Ensure all tests pass after the reorganisation.

---

### 3.8 Add Shared Constants Module (GAP-CO-02)

**Severity: Medium | Effort: Small**

Configuration values duplicated across shell and Python.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, create a `utils/constants.py` module that defines all shared constants: database aliases, default partition counts, error thresholds, file paths, status enums, and PSET parameter names. Generate a `constants.env` file from this module (via a script) that can be sourced by KSH scripts, ensuring the shell and Python environments share the same values. Update all Python modules to import from `constants.py` instead of using hardcoded values.

---

### 3.9 Add Aggregate ThreadPool Timeout (GAP-RE-05)

**Severity: Low | Effort: Small**

No overall timeout for parallel graph execution.

**Devin Prompt:**
> In `ts-python-abinitio-etl-framework`, add an `overall_timeout` parameter to `ParallelLoader.run_graph()` (default: 2x the per-partition timeout). Use `concurrent.futures.wait()` with a timeout instead of `as_completed()` to enforce the aggregate deadline. If the overall timeout is hit, cancel remaining futures and return a `partial_failure` result with details on which partitions completed and which timed out. Add a test that simulates a slow partition and verifies the aggregate timeout fires.

---

## Summary

| Phase | Items | Estimated Effort | Key Outcomes |
|---|---|---|---|
| **Phase 1** | 11 items | 1-2 weeks | Fix critical deployment bug, add concurrency control, retry logic, error handling, basic security, CI scaffolding |
| **Phase 2** | 9 items | 3-6 weeks | Full DML parser, comprehensive test coverage, CI/CD, unified config, secrets management, checkpoint/restart |
| **Phase 3** | 9 items | 6-10 weeks | REST API/CLI, structured logging, metrics, circuit breakers, integration tests, operational polish |
| **Total** | **29 items** | **~10-18 weeks** | Production-grade ETL framework ready for Databricks migration |
