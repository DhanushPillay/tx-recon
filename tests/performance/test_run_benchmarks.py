"""Orchestrator routing: every accepted --suite choice must run something."""

import sys

import pytest

pytestmark = pytest.mark.unit


def _load_run_benchmarks():
    import importlib.util
    import os

    path = os.path.join(os.path.dirname(__file__), "run_benchmarks.py")
    spec = importlib.util.spec_from_file_location("run_benchmarks", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["run_benchmarks"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_pyspark_suite_invokes_pyspark_bench(tmp_path, monkeypatch):
    """--suite pyspark must run the pyspark bench, not silently skip."""
    import pyspark_ingestion_benchmark as pib

    rb = _load_run_benchmarks()
    monkeypatch.setattr(rb, "SCRIPT_DIR", str(tmp_path))
    monkeypatch.setattr(
        pib, "run_benchmark", lambda partitions=16: {"sustained_throughput_rows_sec": 1}
    )
    results = {"timestamp": "t", "hardware": {}}
    out = rb.run_real(results, {}, suite="pyspark")
    assert "pyspark" in out, "--suite pyspark ran nothing"
    assert out["pyspark"] == {"sustained_throughput_rows_sec": 1}
