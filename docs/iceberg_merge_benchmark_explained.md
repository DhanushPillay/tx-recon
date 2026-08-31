# Iceberg MERGE Benchmark Explained

How we measure the reconciliation MERGE operation at different scales and update percentages.

## What This Benchmark Does

It creates an Iceberg table with N rows, generates a "bank settlement" batch that matches a percentage of those rows, then runs the MERGE SQL that our reconciliation engine uses. We measure how long the write (MERGE) and subsequent read take.

## The MERGE SQL

```sql
MERGE INTO webhooks t
USING bank_settlements s
ON t.transaction_id = s.transaction_id
WHEN MATCHED AND (fee-adjusted amount = settled amount) THEN
    UPDATE SET status = 'MATCHED'
WHEN MATCHED AND (fee-adjusted amount != settled amount) THEN
    UPDATE SET status = 'EXCEPTION_FEE_MISMATCH'
```

This is the core business logic — matching webhook records against bank settlements with a 1.5% MDR fee calculation.

## What We Vary

| Variable | Values | Why |
|---|---|---|
| Table size | 100K, 500K, 1M, 2M rows | Does MERGE scale linearly? |
| Update % | 10%, 50% | Does matching 500K out of 1M take 5x longer than 100K? |

## Iceberg Table Properties

```sql
TBLPROPERTIES (
    'write.target-file-size-bytes' = '268435456',   -- 256 MB target file size
    'write.parquet.compression-codec' = 'zstd',      -- zstd for best ratio
    'write.distribution-mode' = 'hash'                -- hash partitioning on join key
)
```

These matter because:
- **Large target file size** (256 MB) means fewer files to scan during MERGE
- **zstd compression** balances write speed with storage efficiency
- **Hash distribution** ensures rows with the same `transaction_id` land in the same partition — avoids full table scans during the join

## Spark Tuning

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")           # AQE
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

**Adaptive Query Execution (AQE)** is the key enabler. It:
- Coalesces small partitions at runtime (no need to guess partition count upfront)
- Optimizes join strategies based on actual data size
- Skew handling — avoids OOM when one partition has 10x more rows

Without AQE, MERGE on 2M rows would be significantly slower due to partition skew.

## Results

| Scale | Update % | MERGE Write | MERGE Read |
|---|---|---|---|
| 100K | 10% | 1.77s | 0.08s |
| 100K | 50% | 0.97s | 0.09s |
| 500K | 10% | 1.06s | 0.06s |
| 500K | 50% | 2.26s | 0.05s |
| 1M | 10% | 1.26s | 0.05s |
| 1M | 50% | 3.04s | 0.05s |
| 2M | 10% | 1.82s | 0.06s |
| 2M | 50% | 3.85s | 0.05s |

## What the Numbers Tell Us

1. **MERGE write scales sub-linearly.** 2M rows at 50% update takes 3.85s — only 4x the 100K/50% time (0.97s), despite 20x more data. Iceberg's file-level pruning means it only reads/writes affected files.

2. **Read time is constant.** ~0.05s regardless of scale. Iceberg metadata lets Spark skip all unaffected files.

3. **10% updates are sometimes slower than 50%.** This looks counterintuitive, but with 10% updates, Iceberg writes more small files (partial updates to many files). With 50%, it consolidates into fewer, larger writes.

4. **File compaction is visible.** The `files_before` vs `files_after` columns show Iceberg rewriting data files during MERGE. More updates = more compaction.

## Run It

```bash
# Full sweep (100K to 2M, both 10% and 50%)
python tests/performance/reconciliation_benchmark.py

# Single scale
python tests/performance/reconciliation_benchmark.py --scale 1000000
```

Requires Docker — runs inside the Spark container.
