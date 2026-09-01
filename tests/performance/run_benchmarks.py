import argparse
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone


def get_hardware_info():
    try:
        import psutil

        mem = psutil.virtual_memory()
        ram_gb = round(mem.total / (1024**3), 1)
    except ImportError:
        # ponytail: psutil missing, degrade gracefully
        ram_gb = None

    return {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "ram_gb": ram_gb,
        "python_version": platform.python_version(),
    }


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def run_module(module_name, extra_args=None):
    cmd = [sys.executable, os.path.join(SCRIPT_DIR, f"{module_name}.py")]
    if extra_args:
        cmd.extend(extra_args)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
            cwd=SCRIPT_DIR,
            check=False,
        )
        if result.returncode != 0:
            return {"error": result.stderr[-500:] if result.stderr else "non-zero exit"}
        return {"status": "completed"}
    except subprocess.TimeoutExpired:
        return {"error": "timeout (600s)"}
    except OSError as e:
        return {"error": str(e)}


def print_summary(results):
    print(f"\n{'='*60}")
    print("  HEADLINE NUMBERS")
    print(f"{'='*60}")
    print(f"{'Benchmark':<30} {'Result':<30}")
    print(f"{'-'*30} {'-'*30}")

    kafka = results.get("kafka", {})
    if "error" not in kafka:
        tp = kafka.get("throughput_msgs_sec", "N/A")
        lat = kafka.get("ack_latency", {})
        print(f"{'Kafka Throughput':<30} {tp} msgs/sec")
        print(f"{'Kafka p99':<30} {lat.get('p99', 'N/A')} ms")
    else:
        print(f"{'Kafka':<30} SKIPPED: {kafka['error'][:40]}")

    pyspark = results.get("pyspark", {})
    if "error" not in pyspark:
        thr = pyspark.get("sustained_throughput_rows_sec", "N/A")
        print(f"{'Ingestion Throughput':<30} {thr} rows/sec")
    else:
        print(f"{'PySpark Ingestion':<30} SKIPPED: {pyspark['error'][:40]}")

    iceberg = results.get("iceberg", {})
    if "error" not in iceberg:
        benchmarks = iceberg.get("benchmarks", {})
        if benchmarks:
            first = next(iter(benchmarks.values()))
            print(f"{'Iceberg MERGE (500k/50%)':<30} {first.get('write_time_sec', 'N/A')}s write")
    else:
        print(f"{'Iceberg MERGE':<30} SKIPPED: {iceberg['error'][:40]}")

    pandera = results.get("pandera", {})
    if "error" not in pandera:
        benchmarks = pandera.get("benchmarks", [])
        if benchmarks:
            last = benchmarks[-1]
            methods = last.get("methods", {})
            for method, m in methods.items():
                print(
                    f"  {method}: {m.get('rows_per_sec', 'N/A')} rows/sec ({m.get('mean_ms', 'N/A')}ms)"
                )
    else:
        print(f"{'Pandera Validation':<30} SKIPPED: {pandera['error'][:40]}")

    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark Orchestrator")
    parser.add_argument(
        "--suite",
        choices=["all", "kafka", "pyspark", "iceberg", "pandera"],
        default="all",
    )
    parser.add_argument("--kafka-count", type=int, default=1000000)
    parser.add_argument("--kafka-acks", choices=["0", "1", "all"], default="all")
    parser.add_argument(
        "--kafka-compression", choices=["lz4", "gzip", "snappy", "none"], default="lz4"
    )
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--scale", type=int, default=None)
    args = parser.parse_args()

    hw = get_hardware_info()
    print(f"Hardware: {json.dumps(hw, indent=2)}")

    results = {"timestamp": datetime.now(timezone.utc).isoformat(), "hardware": hw}

    if args.suite in ("all", "kafka"):
        print("\n=== Kafka Producer Benchmark ===")
        try:
            from kafka_producer_benchmark import run

            results["kafka"] = run(
                "localhost:19092",
                args.kafka_count,
                args.kafka_acks,
                args.kafka_compression,
                1024,
                "both",
            )
        except Exception as e:  # noqa: BLE001 — catch-all for optional import failure
            results["kafka"] = {"error": str(e)}
            print(f"Kafka benchmark failed: {e}")

    if args.suite in ("all", "pyspark"):
        print("\n=== PySpark Ingestion Benchmark ===")
        try:
            from pyspark_ingestion_benchmark import run_benchmark

            results["pyspark"] = run_benchmark(args.partitions)
        except Exception as e:  # noqa: BLE001
            results["pyspark"] = {"error": str(e)}
            print(f"Ingestion benchmark failed: {e}")

    if args.suite in ("all", "iceberg"):
        print("\n=== Iceberg MERGE Benchmark ===")
        try:
            from reconciliation_benchmark import run_benchmark

            results["iceberg"] = run_benchmark(args.scale)
        except Exception as e:  # noqa: BLE001
            results["iceberg"] = {"error": str(e)}
            print(f"Reconciliation benchmark failed: {e}")

    if args.suite in ("all", "pandera"):
        print("\n=== Pandera Validation Benchmark ===")
        try:
            from pandas_validation_benchmark import main as run_pandera

            results["pandera"] = run_pandera()
        except Exception as e:  # noqa: BLE001
            results["pandera"] = {"error": str(e)}
            print(f"Pandera benchmark failed: {e}")

    output_path = os.path.join(SCRIPT_DIR, "results.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nResults written to {output_path}")
    print_summary(results)


if __name__ == "__main__":
    main()
