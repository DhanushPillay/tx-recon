# ADR 003: CI/CD and Testing Strategy

## Status
Accepted

## Context
As the transaction reconciliation engine scales, ensuring code quality, DAG integrity, and processing accuracy becomes critical. Financial reconciliation cannot tolerate regressions in logic, performance, or data schema handling. A robust strategy is required to enforce these standards automatically before any code reaches production. 

The environment also dictates a strict separation between code execution (Python/Spark) and infrastructure dependencies (Kafka, Airflow, DBs), particularly for developers on Windows where local compilation of certain C-based libraries (like `confluent-kafka`) can be problematic.

## Decision
We have adopted a comprehensive, multi-layered testing strategy enforced by a fully automated CI/CD pipeline via **GitHub Actions**.

### 1. Test Suite Architecture
The test suite is structured into distinct domains located under `tests/`:
* **Unit Tests (`tests/ingestion/`, `tests/processing/`, `tests/validation/`)**: Tests isolated logic. We use `chispa` to assert PySpark DataFrame equality, bypassing the need for heavy cluster setups for logic verification.
* **DAG Integrity Tests (`tests/test_dags.py`)**: Tests Airflow DAGs for parsing errors, cyclomatic complexity, and import issues without executing the tasks. Heavy dependencies like `dbt` are actively mocked during DAG parsing to prevent environment-specific failures.
* **Integration Tests (`tests/integration/`)**: Tests end-to-end interactions with external systems. We utilize `testcontainers` (specifically `KafkaContainer`) to spin up ephemeral Docker containers. These tests are marked with `@pytest.mark.integration`.
* **Performance Benchmarks (`tests/performance/`)**: Tracks the execution time of critical path functions using `pytest-benchmark`.

### 2. CI/CD Pipeline Design
We use **GitHub Actions** (`.github/workflows/ci.yml`) as our CI provider, triggering on all pushes and PRs to `main`.
* **Test Isolation**: The CI pipeline explicitly skips integration tests (`-m "not integration"`) and performance benchmarks during routine runs to maintain fast build times (< 1 minute) while ensuring core logic is flawless. Integration tests are designed to be run on demand or in pre-production deployment phases.
* **Cross-Platform Consistency**: CI runs on `ubuntu-latest`, acting as the source of truth for library compatibility (resolving local Windows compilation issues for Kafka drivers).

## Consequences
### Positive
* **High Confidence**: Developers can refactor PySpark and Pandas logic with immediate safety nets.
* **Fast Feedback Loop**: Skipping heavy Docker-based tests in standard CI provides sub-minute feedback to developers.
* **DAG Safety**: Malformed Airflow DAGs are caught in CI, preventing deployment of broken orchestration logic.

### Negative
* **Mocking Overhead**: Maintaining mocks (e.g., for `DbtTaskGroup`) requires vigilance; if the underlying library changes its API, the mock may hide integration issues until runtime.
* **Split Environments**: Developers running full integration tests locally must have Docker installed and running.
