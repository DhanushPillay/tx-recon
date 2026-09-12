<div align="center">

<img src="assets/logo.svg" alt="tx-recon logo" width="500"/>

# Transaction Reconciliation

Makes sure the money a payment gateway collected matches what the bank actually settled. Flags anything that doesn't.

[![CI](https://github.com/DhanushPillay/tx-recon/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/DhanushPillay/tx-recon/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)](https://www.python.org)

</div>

## Why this exists

When you pay online, two records are created: the payment gateway says "we collected ₹1,000", and days later the bank says "we settled ₹976.40". The gap is the gateway's fee plus tax. Finance teams match these records by hand to catch overcharging. This project does that matching automatically, on a small local setup.

A concrete example: a ₹1,000 credit card payment carries a 2% gateway fee (₹20) plus 18% tax on that fee (₹3.60), so the bank should settle ₹976.40. If it settles ₹976.40, the payment is marked MATCHED. If it settles ₹970, it is flagged as a fee mismatch for a human to check.

**Honest limits:** this runs on Docker on one machine with made-up data. It is a working proof of concept, not a production system and not a real payments product.

## Run it in 3 steps

```bash
# 1. Configure (compose fails without MINIO_ROOT_PASSWORD set in .env)
cp .env.example .env

# 2. Start the local services
docker compose up -d

# 3. Run the demo check (needs nothing but Python, takes ~10 seconds)
make demo
```

Expected output:

```text
accuracy_min_f1=1.0000
accuracy_max_fp=0
```

That means: on 2,000 test payments with 7 kinds of problems deliberately injected (wrong fees, missing records, duplicates, late corrections), every problem was caught and nothing correct was flagged.

## What it does

- **Reads payment notifications** as they stream in and stores them.
- **Reads the bank's settlement file** (a daily CSV), checks every row is well-formed, and quarantines bad rows into a separate file instead of crashing.
- **Compares the two** using a fee table (`config/fee_rates.yaml`) — UPI is free, credit cards cost 2%, debit 1%, international 3%, each plus 18% tax on the fee.
- **Marks each payment** MATCHED, fee mismatch, or missing, and stores the result.
- **Catches its own mistakes**: a test suite injects known errors and fails if any slip through, and a second check fails a PR if speed drops more than 15%.

## How it fits together

Payment notifications flow through Redpanda into an Iceberg table. The bank file is validated, then a Spark job joins the two and writes the verdict back. A single `pipeline.py` script runs the whole thing on a schedule; the Airflow version in `dags/` shows how it would scale up.

| Local piece | Stands in for |
| :--- | :--- |
| Redpanda | A message queue (Kafka) |
| MinIO | Object storage (S3) |
| Nessie | A data catalog (Glue) |
| PySpark | A batch engine (EMR) |

## Speed, in plain words

Full method and repro commands: [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md). All numbers are from one Windows machine, single runs unless noted.

- **Accuracy:** catches every injected error, zero false alarms (F1 = 1.0).
- **Sending payments in:** ~135,000 messages/second.
- **Checking the bank file:** ~1M rows/second at 10k rows, ~2.8M at 1M rows.
- **Matching 100,000 payments:** ~1.3 seconds.

## Dig deeper

- [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) — full numbers, method, and how to reproduce them
- [`docs/LOCAL_SETUP.md`](docs/LOCAL_SETUP.md) — setup details and every setting explained
- [`config/fee_rates.yaml`](config/fee_rates.yaml) — the fee table (change a rate, rerun, watch verdicts change)

## Contributing

Keep changes small and add a test for new behavior.

```bash
ruff format src/ tests/ dags/ && ruff check src/ tests/ dags/
pytest tests/ -m "not integration"
```
