"""Trino leg: the BI path (Trino -> Nessie/Iceberg) must serve the mart tables.

Uses the Trino HTTP API via httpx (already a dependency) — no new client dep.
Needs the `trino` compose service up (CI starts it in the integration job).
"""

import os
import time

import httpx
import pytest

pytestmark = pytest.mark.integration

TRINO = os.environ.get("TRINO_URL", "http://localhost:8080")
_HEADERS = {"X-Trino-User": "ci", "X-Trino-Catalog": "nessie", "X-Trino-Schema": "db"}


def _run(sql: str, timeout_s: int = 120) -> list:
    """Submit one statement, follow nextUri until data/error, return data rows."""
    with httpx.Client(timeout=30) as c:
        r = c.post(f"{TRINO}/v1/statement", headers=_HEADERS, content=sql)
        r.raise_for_status()
        info = r.json()
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if "error" in info:
                raise AssertionError(f"trino failed {sql!r}: {info['error']}")
            if "data" in info:
                return info["data"]
            # DDL (CREATE/INSERT/DROP) completes with FINISHED and no data rows.
            if info.get("stats", {}).get("state") == "FINISHED" or "nextUri" not in info:
                return []
            time.sleep(1)
            r = c.get(info["nextUri"])
            r.raise_for_status()
            info = r.json()
    raise AssertionError(f"trino timed out on {sql!r}")


def test_trino_serves_iceberg_roundtrip():
    _run("DROP TABLE IF EXISTS trino_probe")
    _run("CREATE TABLE trino_probe (id VARCHAR, amount BIGINT)")
    _run("INSERT INTO trino_probe VALUES ('a', 1), ('b', 2)")
    rows = _run("SELECT count(*), sum(amount) FROM trino_probe")
    assert rows == [[2, 3]]
    _run("DROP TABLE trino_probe")
