# Ab Initio ETL Framework — Gap Analysis

Comparison of the current codebase against enterprise engineering best practices, organised by category. Each gap is rated by **Severity** (Critical / High / Medium / Low) and **Effort** to remediate (Small / Medium / Large).

---

## 1. Code Organisation

### GAP-CO-01: DML Parser Does Not Handle Advanced DML Constructs

**Severity: High | Effort: Large**

The `DMLParser` only recognises simple `type(size) field_name;` patterns using two regexes. It cannot parse:
- Nested records (`record ... end merchant_info`)
- Variable-length arrays (`string(",")[item_count] item_names`)
- Conditional records (`if (txn_type == 2) record ... end`)
- `void` padding fields
- `packed_decimal` / `zoned_decimal` mainframe types
- `include` directives and named `type` definitions
- Delimiter specifications inside type declarations (e.g., `decimal(",")`)

Six of the eight DML files in the repository use features the parser cannot handle, making it unreliable for migration tooling.

### GAP-CO-02: No Shared Configuration or Constants Module

**Severity: Medium | Effort: Small**

Database aliases (`ORACLE_PROD`, `TERADATA_DW`), default partition counts, error thresholds, and file paths are duplicated across `setenv.ksh`, PSET templates, and Python modules. There is no single-source-of-truth constants file or shared config module.

### GAP-CO-03: Shell and Python Environments Are Disconnected

**Severity: Medium | Effort: Medium**

`setenv.ksh` defines the canonical environment variables (`AI_HOME`, `AI_PROJECT_DIR`, etc.), but the Python modules (`ParallelLoader`, `DeployManager`) use independent hardcoded defaults (e.g., `/usr/local/abinitio`). There is no mechanism for Python code to read or validate against the shell environment configuration.

### GAP-CO-04: No Python Package Structure (setup.py / pyproject.toml)

**Severity: Medium | Effort: Small**

The project has no `setup.py`, `pyproject.toml`, or `setup.cfg`. Module imports rely on `PYTHONPATH` or running from the repo root. This prevents proper installation, version management, and dependency resolution.

### GAP-CO-05: Flat Test Structure

**Severity: Low | Effort: Small**

All tests reside in a single file (`tests/test_pset_manager.py`) covering three unrelated modules. Test organisation should mirror the source module structure for maintainability.

---

## 2. Error Handling

### GAP-EH-01: Silent Exception Swallowing in DeployManager

**Severity: Critical | Effort: Small**

`DeployManager._get_cr_status()` catches all exceptions and silently returns `"approved"` as a default — meaning a ServiceNow API outage would allow unvalidated deployments to production:

```python
except Exception as e:
    logger.warning(f"Could not fetch CR status: {e}. Defaulting to 'approved' for simulation.")
    return "approved"
```

Similarly, `_update_cr()` silently swallows PATCH failures with only a warning log.

### GAP-EH-02: No Retry Logic for External API Calls

**Severity: High | Effort: Small**

`JobMonitor.get_job_status()`, `DeployManager._get_cr_status()`, and Slack webhook calls have no retry logic. A single transient network failure causes the entire operation to fail or return incorrect defaults.

### GAP-EH-03: Shell Scripts Lack Comprehensive Error Handling

**Severity: High | Effort: Medium**

- `run_daily_orders.ksh` only checks the return code of Phase 1 (extraction). Phases 2-4 run without return code validation — a CDC or staging failure is not detected.
- `run_customer_cdc.ksh` has no return code checking at all beyond `set -e`.
- Neither script implements trap-based cleanup, lock files for concurrent execution prevention, or notification on failure.

### GAP-EH-04: No Custom Exception Hierarchy

**Severity: Medium | Effort: Small**

All Python modules raise generic `Exception`, `FileNotFoundError`, or `requests.RequestException`. There are no domain-specific exceptions (e.g., `PSETNotFoundError`, `DeploymentBlockedError`, `SLABreachError`) for structured error handling.

---

## 3. Testing

### GAP-TE-01: Critical Modules Have No Tests

**Severity: High | Effort: Large**

Five of eight Python modules have zero test coverage:

| Module | Lines | Tests |
|---|---|---|
| `graphs/parallel_loader.py` | 130 | 0 |
| `monitoring/job_monitor.py` | 110 | 0 |
| `monitoring/sla_tracker.py` | 98 | 0 |
| `deployment/deploy_manager.py` | 126 | 0 |
| `deployment/change_validator.py` | 76 | 0 |

### GAP-TE-02: No Integration or End-to-End Tests

**Severity: High | Effort: Large**

There are no tests that exercise the pipeline end-to-end (e.g., DML parsing → PSET loading → graph execution → CDC processing). The existing tests are purely unit-level with mocked/in-memory data.

### GAP-TE-03: CDC Processor Update Detection Not Tested

**Severity: Medium | Effort: Small**

The `TestCDCProcessor` class tests inserts, deletes, and identical-data scenarios, but never tests the **update detection** path (same key, different hash). The update logic in `CDCProcessor.process()` is the most complex code path with several edge cases around `pd.Series` vs `pd.DataFrame` handling.

### GAP-TE-04: No Test for DML Files in Repository

**Severity: Medium | Effort: Small**

The `TestDMLParser` tests use an inline `SAMPLE_DML` string, not the actual DML files in `dml/`. There is no validation that the parser can handle (or gracefully fails on) the real schema files in the repository.

### GAP-TE-05: No CI/CD Pipeline Configuration

**Severity: High | Effort: Medium**

There is no GitHub Actions, Jenkins, or other CI configuration. Tests are not automatically run on push or PR.

---

## 4. Security

### GAP-SE-01: Credentials Handled via Environment Variables Without Validation

**Severity: High | Effort: Medium**

- `DeployManager` reads `DEPLOY_API_TOKEN` from environment with empty-string default — silent failure if unset
- `JobMonitor` accepts `api_token` as a constructor parameter (plain string)
- `setenv.ksh` defines database aliases but no credential management or secret rotation
- No integration with a secrets manager (Vault, AWS Secrets Manager, etc.)

### GAP-SE-02: No Input Validation on PSET Parameters

**Severity: High | Effort: Medium**

`PSETManager._parse_pset()` reads arbitrary key-value pairs from PSET files and expands environment variables via `os.path.expandvars()` without any validation, sanitisation, or allowlist. Malicious PSET content could inject arbitrary environment variable values.

### GAP-SE-03: MD5 Used for CDC Hashing

**Severity: Medium | Effort: Small**

`CDCProcessor._row_hash()` uses MD5, which has known collision vulnerabilities. While not a direct security risk for change detection, it deviates from best practice. SHA-256 would be more appropriate for data integrity.

### GAP-SE-04: SLA State File in /tmp with No Access Control

**Severity: Medium | Effort: Small**

`SLATracker` stores its state in `/tmp/abinitio_sla_state.json` — a world-readable location. Job metadata, SLA compliance history, and run durations are exposed to any user on the system.

### GAP-SE-05: No Dependency Vulnerability Scanning

**Severity: Medium | Effort: Small**

`requirements.txt` pins specific versions but there is no `safety`, `pip-audit`, or Dependabot configuration to detect known vulnerabilities in dependencies.

---

## 5. API Design

### GAP-AD-01: No REST API Layer

**Severity: Medium | Effort: Large**

All functionality is accessible only via Python class instantiation or KSH script execution. There is no REST API, CLI tool, or web interface for:
- Triggering graph execution
- Viewing PSET configurations
- Checking job status / SLA reports
- Initiating deployments

This limits operational self-service and integration with modern orchestration tools.

### GAP-AD-02: Inconsistent Return Value Structures

**Severity: Medium | Effort: Medium**

Each module returns different dict structures with no shared schema:
- `ParallelLoader.run_graph()` returns `overall_status: "success" | "partial_failure"`
- `DeployManager.deploy()` returns `status: "success" | "partial_failure" | "blocked"`
- `JobMonitor.get_job_status()` returns `status: "UNKNOWN"` on error
- `CDCProcessor.process()` returns `stats` nested inside the result

There are no Pydantic models, TypedDicts, or dataclasses enforcing response contracts despite Pydantic being a declared dependency.

### GAP-AD-03: No Versioning or Schema Validation on PSET Files

**Severity: Medium | Effort: Small**

PSET files have no version header, schema definition, or required-parameter validation. A missing critical parameter (e.g., `TARGET_TABLE`) is only discovered at runtime when the Ab Initio graph fails.

---

## 6. Observability

### GAP-OB-01: Logging Configuration Not Centralised

**Severity: Medium | Effort: Small**

Every module calls `logging.getLogger(__name__)` but there is no `logging.conf`, `dictConfig`, or root logger setup. Log format, level, and output destination are undefined — consumers must configure logging externally.

### GAP-OB-02: No Structured Logging

**Severity: Medium | Effort: Medium**

All log messages are f-string formatted (e.g., `f"Starting parallel graph: {graph_path}"`). There is no JSON/structured logging for machine parsing, no correlation IDs for tracing requests across components, and no standard field set (job_name, partition, environment, etc.).

### GAP-OB-03: No Health Checks

**Severity: Medium | Effort: Small**

There is no health check mechanism to validate that the Ab Initio runtime, database connections, AutoSys API, or ServiceNow API are reachable before starting a pipeline run.

### GAP-OB-04: No Metrics Collection

**Severity: Medium | Effort: Medium**

Pipeline execution metrics (records processed, partition durations, error counts, CDC change volumes) are only available in log output and the `SLATracker` JSON file. There is no Prometheus, StatsD, or CloudWatch integration for dashboarding and alerting.

### GAP-OB-05: Shell Script Logging is Minimal

**Severity: Low | Effort: Small**

KSH scripts log start/end timestamps to a run log but do not capture:
- Record counts processed per phase
- Duration per phase
- Error counts or reject record counts
- Disk space validation before execution

---

## 7. Resilience

### GAP-RE-01: No Checkpoint/Restart Implementation in Python Layer

**Severity: High | Effort: Large**

`setenv.ksh` defines `AI_CHECKPOINT_ENABLED=true` and `AI_CHECKPOINT_DIR`, and `orders_pipeline.pset` defines `CHECKPOINT_DIR`. However, neither the `ParallelLoader` nor the KSH scripts implement checkpoint/restart logic. A failure mid-pipeline requires full re-execution from scratch.

### GAP-RE-02: No Concurrency Control or Lock Files

**Severity: High | Effort: Small**

Neither KSH script implements lock files or mutex logic. If AutoSys triggers an overlapping execution (e.g., a delayed daily run overlapping with the next scheduled run), two instances will write to the same output paths simultaneously, causing data corruption.

### GAP-RE-03: No Circuit Breaker or Backoff for External APIs

**Severity: Medium | Effort: Medium**

`JobMonitor` and `DeployManager` make HTTP calls to AutoSys and ServiceNow with single-attempt, fixed timeouts. There is no:
- Circuit breaker pattern (stop calling a failing service)
- Exponential backoff
- Bulkhead isolation
- Fallback behaviour

### GAP-RE-04: SLATracker State Loss on /tmp Wipe

**Severity: Medium | Effort: Small**

`SLATracker` defaults to `/tmp/abinitio_sla_state.json`. Since `/tmp` is volatile (cleared on reboot), all SLA history is lost on machine restart. The 90-day retention window becomes meaningless.

### GAP-RE-05: No Timeout on ThreadPoolExecutor Aggregate

**Severity: Low | Effort: Small**

`ParallelLoader.run_graph()` sets a per-partition timeout on `subprocess.run()`, but the `ThreadPoolExecutor` itself has no aggregate timeout. If all partitions hang just below the per-partition timeout, the overall job can run for `timeout * max_workers` seconds without intervention.

---

## Summary Table

| ID | Category | Gap | Severity | Effort |
|---|---|---|---|---|
| GAP-EH-01 | Error Handling | Silent approval on ServiceNow API failure | **Critical** | Small |
| GAP-CO-01 | Code Organisation | DML parser cannot handle 6/8 DML files | High | Large |
| GAP-EH-02 | Error Handling | No retry logic for external API calls | High | Small |
| GAP-EH-03 | Error Handling | Shell scripts lack return code checks (phases 2-4) | High | Medium |
| GAP-TE-01 | Testing | 5/8 Python modules have zero tests | High | Large |
| GAP-TE-02 | Testing | No integration or E2E tests | High | Large |
| GAP-TE-05 | Testing | No CI/CD pipeline | High | Medium |
| GAP-SE-01 | Security | No secrets management or validation | High | Medium |
| GAP-SE-02 | Security | No input validation on PSET parameters | High | Medium |
| GAP-RE-01 | Resilience | No checkpoint/restart in Python layer | High | Large |
| GAP-RE-02 | Resilience | No lock files for concurrent execution | High | Small |
| GAP-CO-02 | Code Organisation | No shared constants/config module | Medium | Small |
| GAP-CO-03 | Code Organisation | Shell/Python env disconnected | Medium | Medium |
| GAP-CO-04 | Code Organisation | No pyproject.toml / package structure | Medium | Small |
| GAP-EH-04 | Error Handling | No custom exception hierarchy | Medium | Small |
| GAP-TE-03 | Testing | CDC update detection untested | Medium | Small |
| GAP-TE-04 | Testing | DML parser not tested against real DML files | Medium | Small |
| GAP-SE-03 | Security | MD5 used for CDC hashing | Medium | Small |
| GAP-SE-04 | Security | SLA state in world-readable /tmp | Medium | Small |
| GAP-SE-05 | Security | No dependency vulnerability scanning | Medium | Small |
| GAP-AD-01 | API Design | No REST API / CLI interface | Medium | Large |
| GAP-AD-02 | API Design | Inconsistent return value structures | Medium | Medium |
| GAP-AD-03 | API Design | No PSET schema validation | Medium | Small |
| GAP-OB-01 | Observability | No centralised logging config | Medium | Small |
| GAP-OB-02 | Observability | No structured/JSON logging | Medium | Medium |
| GAP-OB-03 | Observability | No health checks | Medium | Small |
| GAP-OB-04 | Observability | No metrics collection | Medium | Medium |
| GAP-RE-03 | Resilience | No circuit breaker / backoff | Medium | Medium |
| GAP-RE-04 | Resilience | SLA state lost on /tmp wipe | Medium | Small |
| GAP-CO-05 | Code Organisation | Flat test structure | Low | Small |
| GAP-OB-05 | Observability | Shell script logging minimal | Low | Small |
| GAP-RE-05 | Resilience | No aggregate ThreadPool timeout | Low | Small |
