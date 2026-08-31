import argparse
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


def get_hardware_info():
    import psutil

    mem = psutil.virtual_memory()
    return {
        "os": f"{platform.system()} {platform.release()}",
        "cpu": platform.processor() or "unknown",
        "python": platform.python_version(),
        "cores": os.cpu_count(),
        "ram_gb": round(mem.total / (1024**3), 1),
        "hostname": platform.node(),
    }


def run_kafka_benchmark(count=100000, acks="all"):
    from kafka_producer_benchmark import run

    return run(count, acks, linger_ms=5, batch_size=65536)


def run_ingestion_benchmark():
    from pyspark_ingestion_benchmark import run_benchmark

    # Redirect stdout to capture output
    import io
    from contextlib import redirect_stdout

    f = io.StringIO()
    with redirect_stdout(f):
        run_benchmark()
    output = f.getvalue()

    # Parse key metrics from output
    result = {"raw_output": output}
    for line in output.split("\n"):
        if "Sustained throughput:" in line:
            result["sustained_throughput_rows_sec"] = line.split(":")[-1].strip()
        if "End-to-end latency:" in line:
            result["e2e_latency"] = line.split(":")[-1].strip()
        if "Total rows written:" in line:
            result["total_rows"] = line.split(":")[-1].strip()
    return result


def run_reconciliation_benchmark():
    from reconciliation_benchmark import run_benchmark

    import io
    from contextlib import redirect_stdout

    f = io.StringIO()
    with redirect_stdout(f):
        run_benchmark()
    return {"raw_output": f.getvalue()}


def main():
    parser = argparse.ArgumentParser(description="Benchmark Runner")
    parser.add_argument(
        "--suite",
        choices=["all", "kafka", "ingestion", "reconciliation"],
        default="all",
    )
    parser.add_argument("--kafka-count", type=int, default=100000)
    parser.add_argument("--kafka-acks", choices=["0", "1", "all"], default="all")
    args = parser.parse_args()

    hw = get_hardware_info()
    print(f"Hardware: {json.dumps(hw, indent=2)}")

    results = {"timestamp": datetime.now(timezone.utc).isoformat(), "hardware": hw}

    if args.suite in ("all", "kafka"):
        print("\n=== Kafka Producer Benchmark ===")
        try:
            results["kafka"] = run_kafka_benchmark(args.kafka_count, args.kafka_acks)
        except Exception as e:
            results["kafka"] = {"error": str(e)}
            print(f"Kafka benchmark failed: {e}")

    if args.suite in ("all", "ingestion"):
        print("\n=== PySpark Ingestion Benchmark ===")
        try:
            results["ingestion"] = run_ingestion_benchmark()
        except Exception as e:
            results["ingestion"] = {"error": str(e)}
            print(f"Ingestion benchmark failed: {e}")

    if args.suite in ("all", "reconciliation"):
        print("\n=== Iceberg MERGE Benchmark ===")
        try:
            results["reconciliation"] = run_reconciliation_benchmark()
        except Exception as e:
            results["reconciliation"] = {"error": str(e)}
            print(f"Reconciliation benchmark failed: {e}")

    # Write results
    output_dir = os.path.dirname(__file__)
    output_path = os.path.join(output_dir, "results.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nResults written to {output_path}")


if __name__ == "__main__":
    main()
