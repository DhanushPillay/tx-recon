import argparse
import json
import os
import platform
import statistics
import sys
import time

import pandas as pd
import pandera.pandas as pa

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.generators.settlement_generator import generate_settlement_file

ROW_COUNTS = [10_000, 100_000, 1_000_000, 10_000_000]


def get_hardware_info():
    return {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "python_version": platform.python_version(),
    }


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


def build_pydantic_model():
    from pydantic import BaseModel, Field

    class SettlementRow(BaseModel):
        bank_ref_id: str
        transaction_id: str = Field(..., min_length=3)
        settled_amount_paise: int = Field(..., gt=0)
        settlement_date: str

    return SettlementRow


def run_single(rows, output_dir, warmup_runs=2):
    generate_settlement_file(rows, output_dir=output_dir)
    import glob

    files = glob.glob(f"{output_dir}/settlement_*.csv")
    latest = max(files, key=os.path.getctime)

    # Warmup (load to memory)
    for _ in range(warmup_runs):
        pd.read_csv(latest)

    df = pd.read_csv(latest)
    df = df.dropna(subset=["transaction_id"])

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
    for _ in range(3):
        start = time.perf_counter()
        bench_pandera(df, settlement_schema)
        times.append((time.perf_counter() - start) * 1000)
    methods["pandera"] = {
        "mean_ms": round(statistics.mean(times), 2),
        "std_ms": round(statistics.stdev(times), 2) if len(times) > 1 else 0,
    }

    # Manual pandas
    times = []
    for _ in range(3):
        start = time.perf_counter()
        bench_manual(df)
        times.append((time.perf_counter() - start) * 1000)
    methods["manual_pandas"] = {
        "mean_ms": round(statistics.mean(times), 2),
        "std_ms": round(statistics.stdev(times), 2) if len(times) > 1 else 0,
    }

    # Pydantic
    times = []
    for _ in range(3):
        start = time.perf_counter()
        bench_pydantic(df, pydantic_model)
        times.append((time.perf_counter() - start) * 1000)
    methods["pydantic"] = {
        "mean_ms": round(statistics.mean(times), 2),
        "std_ms": round(statistics.stdev(times), 2) if len(times) > 1 else 0,
    }

    for m in methods.values():
        m["rows_per_sec"] = (
            round(rows / (m["mean_ms"] / 1000)) if m["mean_ms"] > 0 else 0
        )

    return {"rows": rows, "methods": methods}


def main():
    parser = argparse.ArgumentParser(description="Pandera Validation Benchmark")
    parser.add_argument(
        "--rows", type=int, default=None, help="Custom row count (overrides defaults)"
    )
    args = parser.parse_args()

    hw = get_hardware_info()
    print(
        f"Hardware: {hw['platform']}, {hw['cpu_count']} cores, Python {hw['python_version']}"
    )

    output_dir = "benchmarks/data"
    os.makedirs(output_dir, exist_ok=True)

    row_counts = [args.rows] if args.rows else ROW_COUNTS
    results_list = []

    for rows in row_counts:
        print(f"\n--- Benchmark: {rows:,} rows ---")
        result = run_single(rows, output_dir)
        results_list.append(result)

        for method, m in result["methods"].items():
            print(
                f"  {method:<20} {m['mean_ms']:>10.2f}ms  ({m['rows_per_sec']:>12,} rows/sec)"
            )

    output = {"hardware": hw, "benchmarks": results_list}

    out_path = os.path.join(os.path.dirname(__file__), "results_pandera.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults written to {out_path}")

    return output


if __name__ == "__main__":
    main()
