"""Dashboard / mart contract (unit-only, no Trino).

The BI layer reads nessie.db.fact_reconciliation; these tests pin the
9 columns dashboards depend on and the migration loader that renders them.
"""

import os

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


def test_mart_table_name_derived_from_source():
    assert _mart_table("nessie.db.webhooks") == "nessie.db.fact_reconciliation"
    assert _mart_table("glue.db.webhooks") == "glue.db.fact_reconciliation"


def test_rendered_mart_sql_has_all_bi_columns():
    sql, version = _load_mart_sql(ROOT, "nessie.db.webhooks", "nessie.db.fact_reconciliation")
    assert version.startswith("V")
    lowered = sql.lower()
    for col in BI_COLUMNS:
        assert col in lowered, f"BI column missing from mart SQL: {col}"


def test_rendered_mart_sql_has_no_placeholders():
    sql, _ = _load_mart_sql(ROOT, "nessie.db.webhooks", "nessie.db.fact_reconciliation")
    assert "__SOURCE_TABLE__" not in sql
    assert "__MART_TABLE__" not in sql
    assert "nessie.db.webhooks" in sql
    assert "nessie.db.fact_reconciliation" in sql


def test_mart_sql_only_marks_matched_amount():
    sql, _ = _load_mart_sql(ROOT, "nessie.db.webhooks", "nessie.db.fact_reconciliation")
    assert "WHEN reconciliation_status = 'MATCHED' THEN amount_paise" in sql


def test_table_prefix_rewrite_keeps_mart_pair():
    sql, _ = _load_mart_sql(ROOT, "glue.db.webhooks", "glue.db.fact_reconciliation")
    assert "glue.db.webhooks" in sql
    assert "glue.db.fact_reconciliation" in sql
    lowered = sql.lower()
    for col in BI_COLUMNS:
        assert col in lowered
