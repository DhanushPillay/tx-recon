"""Write a benchmark result where the next person can find it.

Two of the four harnesses measured for minutes and then returned their numbers
to a caller that discarded them. `pyspark_ingestion_benchmark.py` and
`kafka_producer_benchmark.py` both built a result dict and dropped it on the
floor, so running either standalone produced no artifact at all. Every number
in docs/BENCHMARKS.md for those two suites therefore had to be copied out of a
console by hand, and the per-suite files are gitignored, which is why the prose
is the only record.

This module is the single place that knows how to write one: timestamp, full
hardware description, fingerprint, then the payload. Every suite should go
through it so the artifacts carry the same fields and a reader can tell which
machine and which day a number came from.

The fingerprint deliberately hashes `system`, `machine`, `cpu_count` and
`ram_gb` only (see `hardware.py`), so it does not move when an OS build changes.
The full description is written alongside it for exactly that reason.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

HERE = os.path.dirname(os.path.abspath(__file__))


def write_result(stem: str, payload: dict[str, Any], *, suite_dir: str = HERE) -> str:
    """Write `payload` to `<suite_dir>/<stem>.json`, stamped, and return the path.

    `payload` is merged under `timestamp` and `hardware`, so a suite that
    already recorded its own hardware or timestamp keeps it. Returns the path
    either way; the caller prints it, because an artifact nobody can find is
    the same as no artifact.
    """
    from hardware import fingerprint, get_hardware_info

    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "hardware": {**get_hardware_info(), "fingerprint": fingerprint()},
        **payload,
    }
    path = os.path.join(suite_dir, f"{stem}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(record, fh, indent=2, default=str)
    return path
