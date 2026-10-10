import os
import time

import httpx
import pytest

pytestmark = pytest.mark.integration

TRINO = os.environ.get("TRINO_URL", "http://localhost:8080")
_HEADERS = {"X-Trino-User": "ci", "X-Trino-Catalog": "nessie", "X-Trino-Schema": "db"}


def _run(sql: str, timeout_s: int = 120) -> list:
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


def test_trino_serves_the_mart_columns():
    try:
        cols = _run(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'fact_reconciliation'"
        )
    except AssertionError as e:
        if "does not exist" in str(e):
            pytest.skip("mart not built yet (run a reconciliation batch first)")
        raise
    names = {r[0] for r in cols}
    expected = {
        "transaction_id",
        "amount_paise",
        "merchant_id",
        "instrument_type",
        "gateway_status",
        "status",
        "bank_ref_id",
        "transacted_at",
        "matched_amount_paise",
    }
    assert expected <= names, f"mart missing columns BI questions need: {expected - names}"
