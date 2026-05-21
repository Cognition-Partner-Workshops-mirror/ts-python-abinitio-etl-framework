# Gap Analysis — Ab Initio ETL Framework

> Generated: 2026-05-21 | Repo: `ts-python-abinitio-etl-framework`

---

## Methodology

Each gap is rated on two dimensions:

- **Severity:** Critical | High | Medium | Low
- **Effort:** Small (< 1 day) | Medium (1–3 days) | Large (3+ days)

Gaps are assessed against 7 engineering best-practice categories.

---

## 1. Code Organization

### Gap 1.1 — No Python package structure or `setup.py` / `pyproject.toml`

**Current state:** Modules are importable only via path manipulation. No installable package, no entry points, no version metadata.

**Best practice:** Python projects should have a `pyproject.toml` (or `setup.py`) defining the package, its version, entry points, and dependencies.

| Severity | Effort |
|---|---|
| Medium | Small |

---

### Gap 1.2 — All `__init__.py` files are empty

**Current state:** All six `__init__.py` files are empty. No public API is defined at the package level.

**Best practice:** `__init__.py` should export key classes to simplify imports (e.g., `from graphs import CDCProcessor`).

| Severity | Effort |
|---|---|
| Low | Small |

---

### Gap 1.3 — Single test file covers multiple modules

**Current state:** `tests/test_pset_manager.py` (112 lines) contains test classes for PSETManager, DMLParser, and CDCProcessor. There is no per-module test file.

**Best practice:** Each module should have its own test file (e.g., `test_cdc_processor.py`, `test_dml_parser.py`) for maintainability and clarity.

| Severity | Effort |
|---|---|
| Low | Small |

---

### Gap 1.4 — No shared configuration or constants module

**Current state:** Magic strings and default values are scattered across classes (e.g., `"/usr/local/abinitio"`, `"/tmp/abinitio_sla_state.json"`, `"http://autosys-api:8080"`).

**Best practice:** Centralize defaults in a `config.py` or use Pydantic `BaseSettings` for typed, environment-aware configuration.

| Severity | Effort |
|---|---|
| Medium | Small |

---

### Gap 1.5 — Unused dependencies in `requirements.txt`

**Current state:** `pydantic`, `pyyaml`, `python-dotenv`, and `croniter` are declared but never imported in any source file.

**Best practice:** Dependencies should reflect actual usage. Unused deps increase attack surface and install time.

| Severity | Effort |
|---|---|
| Low | Small |

---

## 2. Error Handling

### Gap 2.1 — ServiceNow API failure silently defaults to "approved"

**Current state:** `DeployManager._get_cr_status()` catches all exceptions and returns `"approved"` when the ServiceNow API is unreachable. This means **deployments proceed without change approval** if the network is down.

**Best practice:** Fail-closed: if the approval system is unreachable, the deployment should be blocked, not auto-approved.

| Severity | Effort |
|---|---|
| **Critical** | Small |

---

### Gap 2.2 — No custom exception hierarchy

**Current state:** All errors are either raw `Exception` or library exceptions (`requests.RequestException`, `FileNotFoundError`). No domain-specific exceptions exist.

**Best practice:** Define exceptions like `PSETNotFoundError`, `DeploymentBlockedError`, `SLABreachError` for structured error handling.

| Severity | Effort |
|---|---|
| Medium | Small |

---

### Gap 2.3 — KSH scripts have inconsistent error handling

**Current state:** `run_daily_orders.ksh` has `set -e` and explicit RC check for Phase 1, but phases 2–4 rely solely on `set -e`. `run_customer_cdc.ksh` uses `set -e` but no explicit RC checks at all.

**Best practice:** Each phase/step should log the return code and have explicit error handling with cleanup logic.

| Severity | Effort |
|---|---|
| High | Medium |

---

### Gap 2.4 — CDC Processor has known pandas indexing bug

**Current state:** `CDCProcessor.process()` has a bug in the inserts/deletes logic when using `set()` on DataFrame index values, causing 2 test failures (`test_inserts_detected`, `test_deletes_detected`). The `loc[list(insert_keys)]` call fails when the set contains integer keys.

**Best practice:** Core business logic should be bug-free and fully tested.

| Severity | Effort |
|---|---|
| **Critical** | Small |

---

### Gap 2.5 — `_send_alert()` silently swallows Slack errors

**Current state:** `JobMonitor._send_alert()` catches all exceptions and only logs them. If Slack is down, alerts are lost with no fallback.

**Best practice:** Implement fallback alerting (email, file-based queue) or at minimum raise on repeated failures.

| Severity | Effort |
|---|---|
| Medium | Small |

---

## 3. Testing

### Gap 3.1 — Only 10 tests; 2 are failing

**Current state:** 10 tests in a single file. 8 pass, 2 fail due to the CDC pandas bug. No tests for `JobMonitor`, `SLATracker`, `DeployManager`, `ChangeValidator`, or `ParallelLoader`.

**Best practice:** Every module should have unit tests. Critical business logic (CDC, deployment) needs >80% coverage.

| Severity | Effort |
|---|---|
| **Critical** | Medium |

---

### Gap 3.2 — No integration tests

**Current state:** No tests verify end-to-end pipeline behavior (e.g., PSET loaded → graph invoked → CDC processed).

**Best practice:** Integration tests should validate cross-module workflows, at minimum with mocked external services.

| Severity | Effort |
|---|---|
| High | Medium |

---

### Gap 3.3 — No mocking of external services

**Current state:** `JobMonitor` and `DeployManager` make real HTTP calls in production code. Tests don't mock these (and don't exist for those modules).

**Best practice:** Use `pytest-mock` or `responses` library to mock `requests` calls in unit tests.

| Severity | Effort |
|---|---|
| High | Medium |

---

### Gap 3.4 — No test coverage reporting

**Current state:** No `pytest-cov` configuration or coverage thresholds. No visibility into uncovered code paths.

**Best practice:** Enforce minimum coverage (e.g., 70%) with `pytest-cov` and fail CI on regression.

| Severity | Effort |
|---|---|
| Medium | Small |

---

### Gap 3.5 — No KornShell script tests

**Current state:** KSH scripts have zero automated testing. They can only be validated in a live Ab Initio environment.

**Best practice:** Use `bats` (Bash Automated Testing System) or shellcheck for static analysis of shell scripts.

| Severity | Effort |
|---|---|
| Medium | Medium |

---

## 4. Security

### Gap 4.1 — API tokens stored in environment variables without validation

**Current state:** `DeployManager` reads `DEPLOY_API_TOKEN` from env vars with empty-string fallback. `JobMonitor` accepts `api_token` as a plain constructor parameter.

**Best practice:** Validate that required secrets are present at startup. Use a secrets manager or at minimum `.env` files with `python-dotenv`.

| Severity | Effort |
|---|---|
| High | Small |

---

### Gap 4.2 — Hardcoded service URLs and paths

**Current state:** Default URLs like `http://autosys-api:8080`, `https://company.service-now.com`, `/usr/local/abinitio` are hardcoded in constructor defaults.

**Best practice:** All external URLs should come from configuration (env vars, config file, or PSET), never hardcoded.

| Severity | Effort |
|---|---|
| Medium | Small |

---

### Gap 4.3 — No input validation on PSET parameters

**Current state:** `PSETManager` loads and expands PSET values without any validation. A malicious PSET could inject shell variables via `os.path.expandvars()`.

**Best practice:** Validate PSET values against expected types/ranges. Sanitize before shell expansion.

| Severity | Effort |
|---|---|
| High | Medium |

---

### Gap 4.4 — `subprocess.run()` with unsanitized inputs

**Current state:** `ParallelLoader._run_partition()` passes partition parameters directly into `subprocess.run()` command list. `DeployManager._deploy_graph()` does the same with graph paths. While using list form (not shell=True), paths come from user-controlled PSETs.

**Best practice:** Validate and sanitize all inputs before passing to subprocess. Use allowlists for paths.

| Severity | Effort |
|---|---|
| Medium | Medium |

---

### Gap 4.5 — No dependency vulnerability scanning

**Current state:** No `safety`, `pip-audit`, or Dependabot configured. Dependencies are pinned but never scanned.

**Best practice:** Run `pip-audit` or `safety check` in CI to detect known vulnerabilities.

| Severity | Effort |
|---|---|
| Medium | Small |

---

## 5. API Design

### Gap 5.1 — No data validation on class inputs

**Current state:** None of the Python classes validate their constructor arguments or method inputs. For example, `PartitionManager(partition_count=-1)` will not raise an error until runtime.

**Best practice:** Use Pydantic models or explicit validation for public API inputs.

| Severity | Effort |
|---|---|
| Medium | Medium |

---

### Gap 5.2 — DML parser does not handle all DML constructs

**Current state:** `DMLParser` handles basic `record { type name; }` patterns but does not support:
- Nested records (`transaction_detail.dml` nested `merchant_info`, `line_items`)
- Variable-length arrays (`string[item_count]`)
- Conditional fields (`if (txn_type == 2)`)
- Type includes (`include "common_address.dml"`)
- `void` filler fields
- `packed_decimal` / `zoned_decimal` types

**Best practice:** The parser should handle all DML constructs present in the repository's own DML files.

| Severity | Effort |
|---|---|
| High | Large |

---

### Gap 5.3 — Inconsistent return types

**Current state:** Methods return raw `Dict[str, Any]` everywhere. No typed return models or dataclasses.

**Best practice:** Use dataclasses or Pydantic models for return types (e.g., `CDCResult`, `DeploymentResult`, `SLAReport`).

| Severity | Effort |
|---|---|
| Medium | Medium |

---

### Gap 5.4 — No docstring on `ChangeValidator._check_graph()`

**Current state:** `_check_graph()` is the core validation logic but has no docstring.

**Best practice:** All non-trivial methods should have docstrings documenting parameters, return values, and behavior.

| Severity | Effort |
|---|---|
| Low | Small |

---

## 6. Observability

### Gap 6.1 — No structured logging

**Current state:** All modules use `logging.getLogger(__name__)` with unformatted f-string messages. No JSON logging, no correlation IDs, no log levels configured.

**Best practice:** Use structured logging (JSON format) with correlation IDs for traceability across pipeline stages.

| Severity | Effort |
|---|---|
| High | Medium |

---

### Gap 6.2 — No health checks or readiness probes

**Current state:** No mechanism to verify that the framework, its connections (AutoSys, ServiceNow, databases), or the Ab Initio runtime are healthy.

**Best practice:** Implement health check functions that verify connectivity to all external dependencies.

| Severity | Effort |
|---|---|
| Medium | Medium |

---

### Gap 6.3 — No metrics collection

**Current state:** No Prometheus metrics, StatsD counters, or any performance instrumentation. Pipeline duration is logged but not collected for trending.

**Best practice:** Emit metrics for: pipeline duration, records processed, CDC change counts, error rates, SLA compliance percentage.

| Severity | Effort |
|---|---|
| Medium | Large |

---

### Gap 6.4 — SLA state stored in `/tmp/` (ephemeral)

**Current state:** `SLATracker` persists state to `/tmp/abinitio_sla_state.json`. This file is wiped on system restart, losing all SLA history.

**Best practice:** Store SLA state in a database or persistent file system path.

| Severity | Effort |
|---|---|
| High | Small |

---

### Gap 6.5 — No audit logging for deployments

**Current state:** `DeployManager` logs to Python logger but does not write an immutable audit trail. ServiceNow work notes are best-effort.

**Best practice:** Write deployment events to an append-only audit log or database table.

| Severity | Effort |
|---|---|
| Medium | Medium |

---

## 7. Resilience

### Gap 7.1 — No retry logic on HTTP calls

**Current state:** `JobMonitor.get_job_status()`, `DeployManager._get_cr_status()`, and Slack webhook calls have no retry logic. A single transient network error causes immediate failure.

**Best practice:** Use `urllib3.util.retry.Retry` or `tenacity` for exponential backoff on transient HTTP errors.

| Severity | Effort |
|---|---|
| High | Small |

---

### Gap 7.2 — No circuit breaker pattern

**Current state:** If AutoSys or ServiceNow APIs are down, every call attempts the full request + timeout cycle. No circuit breaker prevents cascading delays.

**Best practice:** Implement circuit breaker (e.g., `pybreaker`) to fail fast after N consecutive failures.

| Severity | Effort |
|---|---|
| Medium | Medium |

---

### Gap 7.3 — `monitor_jobs()` uses blocking `time.sleep()`

**Current state:** `JobMonitor.monitor_jobs()` blocks the calling thread with `time.sleep(poll_interval_seconds)` in a loop. With 60 polls × 60 seconds, this can block for up to 60 minutes.

**Best practice:** Use async polling or provide a non-blocking callback interface.

| Severity | Effort |
|---|---|
| Medium | Medium |

---

### Gap 7.4 — No idempotency in deployment

**Current state:** `DeployManager.deploy()` can be called multiple times with the same CR and graphs. There is no check for duplicate deployments.

**Best practice:** Track deployment state and reject duplicate requests for the same CR + graph combination.

| Severity | Effort |
|---|---|
| High | Medium |

---

### Gap 7.5 — No timeout configuration for `air_run` / `air_deploy`

**Current state:** `ParallelLoader` has a configurable timeout (default 3600s), but `DeployManager._deploy_graph()` has a hardcoded 300s timeout. No way to configure timeouts per-graph.

**Best practice:** Make all timeouts configurable via PSET or environment variables.

| Severity | Effort |
|---|---|
| Medium | Small |

---

### Gap 7.6 — No graceful shutdown in `ParallelLoader`

**Current state:** If the orchestrator is interrupted during parallel execution, there is no cleanup of running `air_run` subprocesses. Zombie processes may remain.

**Best practice:** Register signal handlers (SIGTERM/SIGINT) to cancel running futures and kill child processes.

| Severity | Effort |
|---|---|
| High | Medium |

---

## Summary Table

| # | Gap | Category | Severity | Effort |
|---|---|---|---|---|
| 1.1 | No package structure (`pyproject.toml`) | Code Organization | Medium | Small |
| 1.2 | Empty `__init__.py` files | Code Organization | Low | Small |
| 1.3 | Single test file for all modules | Code Organization | Low | Small |
| 1.4 | No shared config/constants module | Code Organization | Medium | Small |
| 1.5 | Unused dependencies | Code Organization | Low | Small |
| 2.1 | ServiceNow failure defaults to "approved" | Error Handling | **Critical** | Small |
| 2.2 | No custom exception hierarchy | Error Handling | Medium | Small |
| 2.3 | Inconsistent KSH error handling | Error Handling | High | Medium |
| 2.4 | CDC Processor pandas indexing bug | Error Handling | **Critical** | Small |
| 2.5 | Slack alerting silently fails | Error Handling | Medium | Small |
| 3.1 | Only 10 tests, 2 failing | Testing | **Critical** | Medium |
| 3.2 | No integration tests | Testing | High | Medium |
| 3.3 | No mocking of external services | Testing | High | Medium |
| 3.4 | No test coverage reporting | Testing | Medium | Small |
| 3.5 | No KSH script tests | Testing | Medium | Medium |
| 4.1 | API tokens lack validation | Security | High | Small |
| 4.2 | Hardcoded service URLs | Security | Medium | Small |
| 4.3 | No PSET input validation | Security | High | Medium |
| 4.4 | Unsanitized subprocess inputs | Security | Medium | Medium |
| 4.5 | No dependency vulnerability scanning | Security | Medium | Small |
| 5.1 | No input validation on class APIs | API Design | Medium | Medium |
| 5.2 | DML parser incomplete for repo's own DMLs | API Design | High | Large |
| 5.3 | Raw dict return types everywhere | API Design | Medium | Medium |
| 5.4 | Missing docstrings | API Design | Low | Small |
| 6.1 | No structured logging | Observability | High | Medium |
| 6.2 | No health checks | Observability | Medium | Medium |
| 6.3 | No metrics collection | Observability | Medium | Large |
| 6.4 | SLA state in `/tmp/` (ephemeral) | Observability | High | Small |
| 6.5 | No deployment audit trail | Observability | Medium | Medium |
| 7.1 | No HTTP retry logic | Resilience | High | Small |
| 7.2 | No circuit breaker | Resilience | Medium | Medium |
| 7.3 | Blocking `time.sleep()` polling | Resilience | Medium | Medium |
| 7.4 | No deployment idempotency | Resilience | High | Medium |
| 7.5 | Hardcoded deployment timeout | Resilience | Medium | Small |
| 7.6 | No graceful shutdown for parallel loader | Resilience | High | Medium |

### Severity Distribution

| Severity | Count |
|---|---|
| Critical | 3 |
| High | 13 |
| Medium | 16 |
| Low | 4 |
| **Total** | **36** |
