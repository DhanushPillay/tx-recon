import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.generators.settlement_generator import generate_settlement_file
from src.validation.validate_settlement import settlement_schema


@pytest.fixture(scope="module")
def benchmark_data_file():
    num_records = 100000
    output_dir = "benchmarks/data"
    os.makedirs(output_dir, exist_ok=True)
    generate_settlement_file(num_records, output_dir=output_dir)
    import glob

    files = glob.glob(f"{output_dir}/settlement_*.csv")
    latest_file = max(files, key=os.path.getctime)
    return latest_file


def test_pandas_validation_performance(benchmark, benchmark_data_file):
    def load_and_validate():
        df = pd.read_csv(benchmark_data_file)
        settlement_schema.validate(df, lazy=True)

    benchmark(load_and_validate)


def test_results_json_structure():
    results_path = os.path.join(os.path.dirname(__file__), "results.json")
    if not os.path.exists(results_path):
        pytest.skip("results.json not found -- run benchmarks first")

    with open(results_path) as f:
        results = json.load(f)

    assert "timestamp" in results, "Missing 'timestamp' key"
    assert "hardware" in results, "Missing 'hardware' key"
    hw = results["hardware"]
    assert hw.get("cpu_count", 0) > 0, "cpu_count must be positive"


def test_throughput_threshold():
    results_path = os.path.join(os.path.dirname(__file__), "results.json")
    if not os.path.exists(results_path):
        pytest.skip("results.json not found -- run benchmarks first")

    with open(results_path) as f:
        results = json.load(f)

    kafka = results.get("kafka", {})
    if "error" in kafka:
        pytest.skip("Kafka benchmark not run (no broker)")

    tp = kafka.get("throughput_msgs_sec")
    if tp is None:
        pytest.skip("No throughput data")

    assert tp > 1000, f"Throughput {tp} msgs/sec is below 1000 threshold"

    lat = kafka.get("ack_latency", {})
    p99 = lat.get("p99")
    if p99 is not None:
        assert p99 < 100, f"p99 latency {p99}ms exceeds 100ms threshold"
