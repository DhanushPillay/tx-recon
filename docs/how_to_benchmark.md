# How to Run Benchmarks

This guide explains in simple words how to run the performance benchmarks for the Transaction Reconciliation Engine.

## Prerequisites

Before running any benchmarks, make sure you have:
1. **Python Virtual Environment** activated: `.\.venv\Scripts\activate` (Windows) or `source .venv/bin/activate` (Mac/Linux).
2. **Docker Desktop** running.

---

## 1. The Easy Way: Run Everything

If you want to run all the benchmarks at once (Kafka, PySpark, Iceberg, and Pandera), we've set up an orchestrator script that runs them all for you. 

Because PySpark has some limitations when running directly on Windows, the easiest way to run the full suite is inside a Docker container.

First, start your local infrastructure:
```bash
docker-compose up -d
```

Then, run the all-in-one orchestrator via Docker:
```bash
docker run --rm -v ".:/app" -w /app --network tx-recon_default -e PYTHONPATH=/app tx-recon-airflow-scheduler:latest python tests/performance/run_benchmarks.py --suite all
```
*Note: If you run into path errors on Windows, replace `.` with the absolute path to your project folder (e.g., `"C:\path\to\tx-recon:/app"`).*

---

## 2. Running Individual Benchmarks Locally

If you only want to test specific parts of the system, you can run the individual scripts directly from your terminal.

### Kafka Producer
This tests how fast we can push simulated webhook events into Redpanda. You need Docker running for this to work (`docker-compose up -d`).
```bash
python tests/performance/kafka_producer_benchmark.py --count 1000000 --mode both
```

> Want to know how we got from 20K to 130K+ msgs/sec? See [Kafka Benchmark Explained](kafka_benchmark_explained.md).

### PySpark Ingestion
*(Windows Users: You must run this via WSL2 or Docker due to PySpark limitations. See "The Easy Way" above).*
This tests how quickly we can stream rows from Kafka into an Iceberg table.
```bash
python tests/performance/pyspark_ingestion_benchmark.py --partitions 16
```
> [How the ingestion pipeline works](pyspark_ingestion_benchmark_explained.md)

### Iceberg MERGE
Tests how the reconciliation MERGE operation scales from 100K to 2M rows at 10% and 50% update rates.
```bash
python tests/performance/reconciliation_benchmark.py
```
> [How Iceberg MERGE scales](iceberg_merge_benchmark_explained.md)

### Pandera Validation
Compares Pandera, manual pandas, and Pydantic for validating settlement CSVs. No Docker needed.
```bash
python tests/performance/pandas_validation_benchmark.py --rows 1000000
```
> [Pandera vs Manual vs Pydantic](pandera_validation_benchmark_explained.md)

---

## Where Are The Results?

When the benchmarks finish, the results are saved to a file called `tests/performance/results.json`. 

This file includes:
- **Environment Info:** Your operating system, CPU cores, and Python version.
- **Metrics:** Throughput (messages/rows per second), latency percentiles (how long operations took), and execution times.
