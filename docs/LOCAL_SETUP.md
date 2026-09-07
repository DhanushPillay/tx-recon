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
