# Pandera Validation Benchmark Explained

> **Environment**: Native Windows Python (28 cores). Results represent single-machine performance only.

How we compare three approaches to validating settlement CSV data and why Pandera won.

## What This Benchmark Does

It generates a CSV with N rows of bank settlement data, then runs three validation methods on the same data:

1. **Pandera** — Declarative schema with typed columns, uniqueness constraints, and range checks
2. **Manual pandas** — Hand-written `is_unique()`, `between()`, `isnull()` checks
3. **Pydantic** — Row-by-row model validation via `BaseModel`

We measure time and compute rows/sec for each.

## The Three Methods

### Pandera (declarative schema)

```python
schema = pa.DataFrameSchema({
    "transaction_id": pa.Column(str, unique=True, nullable=False),
    "settled_amount_paise": pa.Column(int, pa.Check.gt(0), nullable=False),
    "bank_ref_id": pa.Column(str, nullable=False),
}, strict=False)
schema.validate(df, lazy=True)
```

- Validates entire DataFrame in one call
- `lazy=True` collects all errors instead of stopping at first
- Type checking, uniqueness, null checks — all in C-backed pandas operations

### Manual pandas

```python
if not df["transaction_id"].is_unique: ...
invalid = df[~df["settled_amount_paise"].between(1, 999_999_999_999)]
nulls = df[df["bank_ref_id"].isnull()]
``

- Hand-written checks, same operations as Pandera underneath
- Faster because no schema overhead — but you write and maintain every check yourself

### Pydantic

```python
class SettlementRow(BaseModel):
    bank_ref_id: str
    transaction_id: str = Field(..., min_length=3)
    settled_amount_paise: int = Field(..., gt=0)
    settlement_date: str

for r in records:
    pydantic_model(**r)
```

- Row-by-row validation — creates a Python object per row
- Type coercion, custom validators, clear error messages
- But Python loops kill performance at scale

## Results

| Rows | Pandera | Manual Pandas | Pydantic |
|---|---|---|---|
| 10K | 1.2M rows/sec | 8.9M rows/sec | 387K rows/sec |
| 100K | 6.1M rows/sec | 10.8M rows/sec | 397K rows/sec |
| 1M | 5.4M rows/sec | 7.7M rows/sec | 384K rows/sec |
| 10M | 2.5M rows/sec | 3.2M rows/sec | 408K rows/sec |

## What the Numbers Tell Us

1. **Pandera is ~2x slower than manual pandas, but both are fast enough.** At 5M+ rows/sec, validating 1M rows takes 0.18s. Nobody cares about the 70ms difference.

2. **Pydantic is 10-15x slower.** Row-by-row Python object creation is the bottleneck. At 400K rows/sec, 1M rows takes 2.6s. Still fine for a daily batch job, but not for streaming validation.

3. **Manual pandas peaks and drops.** At 10M rows, memory pressure kicks in and pandas slows down. Pandera degrades more gracefully because `lazy=True` avoids materializing intermediate error DataFrames.

4. **All three are "fast enough" for this use case.** We process ~100K settlement rows daily. Even Pydantic finishes in 0.25s.

## Why We Chose Pandera

Performance was a tiebreaker, not the deciding factor. Pandera wins on:

- **Declarative schema** — the validation rules ARE the documentation
- **Schema export** — `schema.to_yaml()` generates a machine-readable YAML spec
- **Lazy validation** — catches all errors in one pass, not just the first
- **DataFrame-level constraints** — uniqueness, null checks, type checking in one call
- **Ecosystem** — integrates with pandas, PySpark (via `pandera.pyspark`), and Great Expectations

Manual pandas is faster but you lose self-documenting schemas. Pydantic is slower but better for API request validation (row-by-row with rich error messages). Pandera is the sweet spot for DataFrame validation.

## Run It

```bash
# Default (10K to 10M rows)
python tests/performance/pandas_validation_benchmark.py

# Single row count
python tests/performance/pandas_validation_benchmark.py --rows 1000000
```

No Docker needed — runs on native Python.
