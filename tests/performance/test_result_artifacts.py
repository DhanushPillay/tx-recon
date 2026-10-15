"""A benchmark that measures something should leave the measurement behind.

`pyspark_ingestion_benchmark.py` and `kafka_producer_benchmark.py` both built a
result dict and returned it to a caller that dropped it, so running either
standalone produced no artifact. That is not a cosmetic problem: every number in
docs/BENCHMARKS.md for those two suites had to be transcribed from a console,
and `results*.json` is gitignored, so the prose became the only record and
nothing checked it.

These tests pin the writer and the two call sites. They do not need a broker,
a catalog, or Spark -- the failure being guarded against is a missing file, not
a wrong number in one.
"""

from __future__ import annotations

import ast
import json
import os

import artifacts
import pytest


def _source(name: str) -> str:
    with open(os.path.join(artifacts.HERE, name), encoding="utf-8") as fh:
        return fh.read()


def _load(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _tree(name: str) -> ast.Module:
    return ast.parse(_source(name), filename=name)


def _calls(tree: ast.Module, attr: str) -> list[ast.Call]:
    """Calls to `attr(...)` and to `something.attr(...)`, matched on the name."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if name == attr:
            found.append(node)
    return found


# --- the writer ------------------------------------------------------------


def test_the_writer_stamps_every_artifact_the_same_way(tmp_path):
    path = artifacts.write_result(
        "sample", {"kafka": {"throughput_msgs_sec": 1.0}}, suite_dir=str(tmp_path)
    )
    assert os.path.exists(path), "the whole point is that a file appears"
    record = _load(path)
    assert record["kafka"]["throughput_msgs_sec"] == 1.0
    assert record["timestamp"].endswith("+00:00"), record["timestamp"]
    for field in ("platform", "cpu_count", "ram_gb", "python_version", "fingerprint"):
        assert field in record["hardware"], (
            f"a reader cannot tell which machine produced this without {field}"
        )


def test_a_suite_may_override_the_stamp_but_not_the_payload(tmp_path):
    """A suite that recorded its own hardware knows something the writer does not."""
    path = artifacts.write_result(
        "sample",
        {"hardware": {"platform": "pinned-by-the-suite"}, "value": 7},
        suite_dir=str(tmp_path),
    )
    record = _load(path)
    assert record["value"] == 7
    assert record["hardware"] == {"platform": "pinned-by-the-suite"}, (
        "an explicit hardware block should win, not be silently overwritten"
    )


def test_the_writer_returns_the_path_not_nothing(tmp_path):
    path = artifacts.write_result("sample", {}, suite_dir=str(tmp_path))
    assert path == str(tmp_path / "sample.json")
    assert isinstance(path, str)


# --- the call sites that were missing --------------------------------------


@pytest.mark.parametrize(
    "module",
    ["pyspark_ingestion_benchmark.py", "kafka_producer_benchmark.py"],
)
def test_the_harness_that_used_to_throw_its_result_away_now_writes_one(module):
    tree = _tree(module)
    calls = _calls(tree, "write_result")
    assert calls, f"{module} still measures and discards; a run leaves nothing to cite"


def test_the_kafka_harness_writes_one_artifact_per_broker_not_one_for_both():
    """A comparison run writes two results; overwriting would lose the first."""
    tree = _tree("kafka_producer_benchmark.py")
    loops = [n for n in ast.walk(tree) if isinstance(n, ast.For) and any(_calls(n, "write_result"))]
    assert loops, "the write is not inside a loop, so a --compare run loses a broker's result"
    assert _calls(tree, "write_result")


def test_the_orchestrator_reports_a_suite_that_did_not_run():
    tree = _tree("run_benchmarks.py")
    assert [c for c in _calls(tree, "append") if isinstance(c.func, ast.Attribute)], (
        "no suite failure is recorded anywhere"
    )
    assert _calls(tree, "exit"), (
        "a failed suite must reach the exit code, or `--suite all` still reports success"
    )


def test_the_orchestrator_still_writes_the_partial_result():
    """Knowing which suites worked is worth having even when one did not."""
    src = _source("run_benchmarks.py")
    write_at = src.index("results_real.json")
    check_at = src.index("if failures:")
    assert write_at < check_at, "the partial file has to be written before the failure report"
