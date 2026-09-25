import argparse
import json
import os
import statistics
import sys
import time

import pandas as pd
import pandera.pandas as pa

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from hardware import get_hardware_info

REAL_SETTLEMENT_CSV = os.path.join(
    os.path.dirname(__file__), "../../data/real_thiru_full_settlement.csv"
)
SAMPLE_SETTLEMENT_CSV = os.path.join(
    os.path.dirname(__file__), "../../data/samples/real_10k_settlement.csv"
)


def bench_pandera(df, schema):
    schema.validate(df, lazy=True)


def bench_manual(df):
    errors = []
    if not df["transaction_id"].is_unique:
        errors.append("transaction_id not unique")
    invalid = df[~df["settled_amount_paise"].between(1, 999_999_999_999)]
    if not invalid.empty:
        errors.append(f"{len(invalid)} invalid amounts")
    nulls = df[df["bank_ref_id"].isnull()]
    if not nulls.empty:
        errors.append(f"{len(nulls)} null refs")
    return errors


def bench_pydantic(df, pydantic_model):
    records = df.to_dict(orient="records")
    for r in records:
        pydantic_model(**r)


def bench_polars(pf):
    import polars as pl

    errors = []
    if pf.select(pl.col("transaction_id").is_duplicated().any()).item():
        errors.append("transaction_id not unique")
    invalid = pf.filter(~pl.col("settled_amount_paise").is_between(1, 999_999_999_999))
    if invalid.height > 0:
        errors.append("invalid amounts")
    nulls = pf.filter(pl.col("bank_ref_id").is_null())
    if nulls.height > 0:
        errors.append("null refs")
    return errors


def build_pydantic_model():
    from pydantic import BaseModel, Field

    class SettlementRow(BaseModel):
        bank_ref_id: str
        transaction_id: str = Field(..., min_length=3)
        settled_amount_paise: int = Field(..., gt=0)
        settlement_date: str

    return SettlementRow


def run_single(rows, output_dir, warmup_runs=2, iters=7, input_csv=None, dedup=False):
    latest = input_csv or (
        REAL_SETTLEMENT_CSV if os.path.exists(REAL_SETTLEMENT_CSV) else SAMPLE_SETTLEMENT_CSV
    )

    # Warmup (load to memory)
    for _ in range(warmup_runs):
        pd.read_csv(latest)

    df = pd.read_csv(latest)
    import polars as pl

    pf = pl.read_csv(latest)
    rows = len(df)
    # Prod loads CSVs as dtype=str before validation; real numeric ids need
    # the same cast or the bench's str schema rejects them (untimed setup).
    df["transaction_id"] = df["transaction_id"].astype(str)
    df["bank_ref_id"] = df["bank_ref_id"].astype(str)
    pf = pf.with_columns(
        pl.col("transaction_id").cast(pl.String),
        pl.col("bank_ref_id").cast(pl.String),
    )
    if dedup:
        # Untimed setup: prod dedups post-validation (WINDOW latest-wins);
        # the bench schema demands unique ids, so collapse first and report
        # the deduped count. CSV order is oldest-first, keep="last" matches.
        df = df.drop_duplicates("transaction_id", keep="last")
        pf = pf.unique(subset="transaction_id", keep="last")
        rows = len(df)

    settlement_schema = pa.DataFrameSchema(
        {
            "transaction_id": pa.Column(str, unique=True, nullable=False),
            "settled_amount_paise": pa.Column(int, pa.Check.gt(0), nullable=False),
            "bank_ref_id": pa.Column(str, nullable=False),
        },
        strict=False,
    )

    pydantic_model = build_pydantic_model()

    methods = {}

    # Pandera
    times = []
    for _ in range(iters):
        start = time.perf_counter()
        bench_pandera(df, settlement_schema)
        times.append((time.perf_counter() - start) * 1000)
    methods["pandera"] = {
        "mean_ms": round(statistics.mean(times), 2),
        "std_ms": round(statistics.stdev(times), 2) if len(times) > 1 else 0,
    }

    # Manual pandas
    times = []
    for _ in range(iters):
        start = time.perf_counter()
        bench_manual(df)
        times.append((time.perf_counter() - start) * 1000)
    methods["manual_pandas"] = {
        "mean_ms": round(statistics.mean(times), 2),
        "std_ms": round(statistics.stdev(times), 2) if len(times) > 1 else 0,
    }

    # Pydantic
    times = []
    for _ in range(iters):
        start = time.perf_counter()
        bench_pydantic(df, pydantic_model)
        times.append((time.perf_counter() - start) * 1000)
    methods["pydantic"] = {
        "mean_ms": round(statistics.mean(times), 2),
        "std_ms": round(statistics.stdev(times), 2) if len(times) > 1 else 0,
    }

    # Polars (in-memory, same CSV preloaded once — no disk IO in the timed path)
    times = []
    for _ in range(iters):
        start = time.perf_counter()
        bench_polars(pf)
        times.append((time.perf_counter() - start) * 1000)
    methods["polars"] = {
        "mean_ms": round(statistics.mean(times), 2),
        "std_ms": round(statistics.stdev(times), 2) if len(times) > 1 else 0,
    }

    for m in methods.values():
        m["rows_per_sec"] = round(rows / (m["mean_ms"] / 1000)) if m["mean_ms"] > 0 else 0

    return {"rows": rows, "methods": methods}


def main():
    parser = argparse.ArgumentParser(description="Pandera Validation Benchmark (real data only)")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Real CSV to validate (default: data/real_thiru_full_settlement.csv, fallback to data/samples/)",
    )
    parser.add_argument(
        "--dedup",
        action="store_true",
        help="Drop duplicate tx ids pre-timing (real files carry dup rows; bench schema needs unique)",
    )
    args = parser.parse_args()

    hw = get_hardware_info()
    print(f"Hardware: {hw['platform']}, {hw['cpu_count']} cores, Python {hw['python_version']}")

    output_dir = "benchmarks/data"
    os.makedirs(output_dir, exist_ok=True)

    target = args.input or (
        REAL_SETTLEMENT_CSV if os.path.exists(REAL_SETTLEMENT_CSV) else SAMPLE_SETTLEMENT_CSV
    )
    print(f"\n--- Benchmark: real input {target} ---")
    results_list = [run_single(0, output_dir, input_csv=target, dedup=args.dedup)]
    result = results_list[0]
    for method, m in result["methods"].items():
        print(f"  {method:<20} {m['mean_ms']:>10.2f}ms  ({m['rows_per_sec']:>12,} rows/sec)")
    out_name = "results_pandera_real.json"

    output = {"hardware": hw, "benchmarks": results_list}

    out_path = os.path.join(os.path.dirname(__file__), out_name)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults written to {out_path}")

    return output


if __name__ == "__main__":
    main()
