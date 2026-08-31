import pytest
import pandas as pd
import pandera as pa
import os
import sys

# Add parent directory to path so we can import src modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.generators.settlement_generator import generate_settlement_file
from src.validation.validate_settlement import settlement_schema


@pytest.fixture(scope="module")
def benchmark_data_file():
    num_records = (
        100000  # scaled down for CI to prevent OOM / timeouts, but still tests perf
    )
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

    # Run the benchmark
    benchmark(load_and_validate)
