# Local Setup

This guide provides instructions for setting up and running the transaction reconciliation pipeline on your local machine.

## Prerequisites

Ensure you have the following installed:

*   **Python:** version >= 3.10 and < 3.13.
*   **Docker & Docker Compose:** For running the infrastructure services.
*   **Java (JDK 17):** Required by PySpark for data processing.

## 1. Install dependencies

Clone the repository and install the project dependencies.

```bash
git clone https://github.com/DhanushPillay/tx-recon.git
cd tx-recon
pip install -e .[dev]
```

## 2. Start infrastructure

The pipeline requires Redpanda (Kafka), MinIO (S3-compatible storage), and Nessie (Iceberg catalog). These are provided via Docker Compose.

```bash
docker compose up -d
```

Verify that the containers are running:
```bash
docker ps
```

### Local Multi-Node Spark Cluster

The project includes a custom local Spark topology to simulate a true distributed environment on a single machine, allowing you to test massive Iceberg `MERGE` benchmarks without a cloud cluster:
* **Windows Host (Driver):** The main execution script runs on the host machine.
* **Docker Linux Containers (Workers):** Two Spark worker containers (`spark-worker-1` and `spark-worker-2`) are spun up via `docker-compose.spark.yml` and connect back to the host.
* **Python Environment Syncing:** PySpark strictly requires the exact same minor version of Python on both the driver and the executors. Because the `apache-airflow` constraint requires Python < 3.13, we standardize on **Python 3.11**. The custom Docker containers use `uv` to install Python 3.11 directly inside the image, and the Windows host runs the benchmarks using `uv run --python 3.11` to match.
* **Networking:** The driver binds to `0.0.0.0` and announces itself to the containers as `host.docker.internal` via Spark config injection.

## 3. Run the pipeline

The primary entry point for local execution is the pipeline script, which simulates a cron-based scheduler.

```bash
python -m src.pipeline --date 2026-09-04
```

This command will:
1.  Generate synthetic webhook events and settlement files.
2.  Validate the settlement data.
3.  Execute the Iceberg `MERGE INTO` operation to reconcile the records.

## 4. Run tests and benchmarks

To verify correctness and measure performance on your hardware:

**Run the unit tests and accuracy gates (does not require Docker):**
```bash
pytest tests/performance/test_recon_accuracy.py tests/processing/test_fee_engine.py -v
```

**Run the quick performance gate:**
```bash
make demo
```

**Run the validation benchmark:**
```bash
python tests/performance/pandas_validation_benchmark.py
```
