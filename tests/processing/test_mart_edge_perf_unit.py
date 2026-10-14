# ruff: noqa: S608 -- file bodies below are literal test SQL, not input.
"""P6: mart / dashboard contract + render perf (unit-only, no Trino).

Pins the 9 BI columns, mart naming, placeholder rendering, matched-only
amount logic, storage properties, plus migration-loader ordering and
render-time floor.
"""

import os
import time

import pytest

from src.processing.reconcile import _load_mart_sql, _mart_table

pytestmark = pytest.mark.unit

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")

BI_COLUMNS = [
    "transaction_id",
    "amount_paise",
    "merchant_id",
    "instrument_type",
    "gateway_status",
    "status",
    "bank_ref_id",
    "transacted_at",
    "matched_amount_paise",
]


def _rendered(source="nessie.db.webhooks", mart="nessie.db.fact_reconciliation"):
    sql, version = _load_mart_sql(ROOT, source, mart)
    assert version.startswith("V")
    return sql


def test_mart_name_derived():
    assert _mart_table("nessie.db.webhooks") == "nessie.db.fact_reconciliation"
    assert _mart_table("glue.db.webhooks") == "glue.db.fact_reconciliation"


def test_all_bi_columns_present():
    lowered = _rendered().lower()
    for col in BI_COLUMNS:
        assert col in lowered, col


def test_no_placeholders_left():
    sql = _rendered()
    assert "__SOURCE_TABLE__" not in sql and "__MART_TABLE__" not in sql
    assert "nessie.db.webhooks" in sql and "nessie.db.fact_reconciliation" in sql


def test_matched_only_amount():
    assert "WHEN reconciliation_status = 'MATCHED' THEN amount_paise" in _rendered()


def test_storage_properties():
    sql = _rendered().lower()
    assert "bucket(16, transaction_id)" in sql
    assert "zstd" in sql


def test_prefix_pair_renders():
    sql = _rendered("glue.db.webhooks", "glue.db.fact_reconciliation")
    assert "glue.db.webhooks" in sql and "glue.db.fact_reconciliation" in sql
    lowered = sql.lower()
    for col in BI_COLUMNS:
        assert col in lowered, col


def test_newest_migration_wins_numeric_order(tmp_path):
    mig = tmp_path / "sql" / "marts" / "migrations"
    mig.mkdir(parents=True)
    for n, marker in ((1, "one"), (2, "two"), (10, "ten")):
        (mig / f"V{n}__fact_reconciliation.sql").write_text(
            "CREATE OR REPLACE TABLE __MART_TABLE__ USING iceberg AS "
            f"SELECT '{marker}' AS marker FROM __SOURCE_TABLE__"
        )
    sql, version = _load_mart_sql(str(tmp_path), "nessie.db.webhooks", "nessie.db.m")
    assert version == "V10" and "'ten'" in sql


def test_perf_mart_render_200_renders():
    t0 = time.perf_counter()
    for _ in range(200):
        _rendered()
    dt = time.perf_counter() - t0
    assert 200 / dt > 50, f"mart render too slow: {200 / dt:.0f} renders/s"
