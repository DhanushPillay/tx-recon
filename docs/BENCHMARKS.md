# Benchmarks

This document details the benchmarking methodology and performance results for the transaction reconciliation pipeline.

## Validation benchmark

We measure the raw validation throughput across different libraries to justify the choice of tooling at the batch boundary.

*   **Script:** `tests/performance/pandas_validation_benchmark.py`
*   **Goal:** Determine the rows-per-second throughput of manual Pandas, Polars, Pandera, and Pydantic on datasets ranging from 10,000 to 10,000,000 rows.
*   **Environment:** Windows 11, 28 cores, Python 3.13.5.

### Results

| Rows | Manual Pandas | Polars | Pandera | Pydantic |
| :--- | :--- | :--- | :--- | :--- |
| 10,000 | 5.8M rows/sec (1.7ms) | 65K rows/sec (153ms)* | 544K rows/sec (18ms) | 271K rows/sec (36ms) |
| 100,000 | 9.0M rows/sec (11ms) | 6.1M rows/sec (16ms) | 2.3M rows/sec (43ms) | 401K rows/sec (249ms) |
| 1,000,000 | 6.8M rows/sec (145ms) | 8.2M rows/sec (120ms) | 2.2M rows/sec (446ms) | 397K rows/sec (2.5s) |
| 10,000,000 | 2.7M rows/sec (3.6s) | 6.8M rows/sec (1.4s) | 1.5M rows/sec (6.6s) | 405K rows/sec (24.6s) |

*\*Polars has a cold-start overhead on the first run.*

Manual pandas is faster below 100,000 rows because it avoids framework overhead. Polars is faster above 1,000,000 rows. Pandera processes 1.5M rows/sec at 10,000,000 rows, which covers the cost of using declarative contracts at the batch boundary.

## Accuracy harness

The accuracy harness verifies that the reconciliation logic correctly classifies transactions. A fast matcher that categorizes records incorrectly corrupts the ledger.

*   **Script:** `tests/performance/recon_accuracy.py`
*   **Methodology:** The script injects known edge cases into synthetic data. The answer key is sealed and never interacts with the matcher.
*   **Break classes tested:** `EXACT`, `ROUNDING`, `FEE_MISMATCH`, `ORPHAN`, `DUPLICATE`, `OUT_OF_ORDER`, `LATE_CORRECTION`.
*   **Gate:** `tests/performance/test_recon_accuracy.py` ensures Precision = Recall = F1 = 1.0, and False Positives = 0. 

## Kafka producer benchmark

Measures the sustained message throughput and latency when publishing to Redpanda.

*   **Configuration:** `acks=all`, `lz4` compression (strongest durability).
*   **Result:** 142,188 msgs/sec. Latency: p50 0.78ms / p95 4.03ms / p99 7.53ms on a 2,000-message run (1KB records).
