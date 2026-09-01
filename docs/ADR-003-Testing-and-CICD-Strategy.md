# ADR 003: CI/CD and Testing Strategy

## Status
Accepted

## Context
We need automated tests to catch regressions in processing logic, DAG integrity, and schema handling before code reaches production. The environment requires strict separation between code execution (Python/Spark) and infrastructure dependencies (Kafka, Airflow, DBs), particularly for developers on Windows where certain C-based libraries like `confluent-kafka` don't compile locally.

## Decision
We test via a CI/CD pipeline on **GitHub Actions**.

### 1. Test Suite Architecture
The test suite is structured into distinct domains located under `tests/`:
* **Unit Tests (`tests/ingestion/`, `tests/processing/`, `tests/validation/`)**: Tests isolated logic. We use `chispa` to assert PySpark DataFrame equality, bypassing the need for heavy cluster setups for logic verification.
* **DAG Integrity Tests (`tests/test_dags.py`)**: Tests Airflow DAGs for parsing errors, cyclomatic complexity, and import issues without executing the tasks. Heavy dependencies like `dbt` are actively mocked during DAG parsing to prevent environment-specific failures.
* **Integration Tests (`tests/integration/`)**: Tests end-to-end interactions with external systems. We use `testcontainers` (specifically `KafkaContainer`) to spin up ephemeral Docker containers. These tests are marked with `@pytest.mark.integration`.
* **Performance Benchmarks (`tests/performance/`)**: Tracks the execution time of critical path functions using `pytest-benchmark`.

### 2. CI/CD Pipeline Design
We use **GitHub Actions** (`.github/workflows/ci.yml`) as our CI provider, triggering on all pushes and PRs to `main`.
* **Test Isolation**: The CI pipeline runs integration tests (`-m "integration"`) and performance benchmarks (`tests/performance/run_benchmarks.py --suite all`) via Docker Compose. Integration tests add ~2-3 minutes to pipeline time.
* **Cross-Platform Consistency**: CI runs on `ubuntu-latest`, acting as the source of truth for library compatibility (resolving local Windows compilation issues for Kafka drivers).

## Consequences
### Positive
* Tests catch PySpark/Pandas regressions immediately.
* Integration tests run in CI but add ~2-3 minutes to pipeline time.
* Malformed Airflow DAGs are caught in CI before deployment.

### Negative
* **Mocking Overhead**: Maintaining mocks (e.g., for `DbtTaskGroup`) requires vigilance; if the underlying library changes its API, the mock may hide integration issues until runtime.
* **Split Environments**: Developers running full integration tests locally must have Docker installed and running.
