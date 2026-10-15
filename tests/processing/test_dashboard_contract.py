"""Dashboard / mart contract (unit-only, no Trino).

The BI layer reads nessie.db.fact_reconciliation; these tests pin the
9 columns dashboards depend on, the migration loader that renders them, and the
question files the Metabase sync turns into cards.
"""

import importlib.util
import os
import re

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


def _registered_questions() -> tuple[set[str], set[str]]:
    """DISP and ORDER, read out of the sync script without running it."""
    path = os.path.join(ROOT, "scripts", "refresh_dashboard.py")
    spec = importlib.util.spec_from_file_location("refresh_dashboard", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return set(mod.DISP), set(mod.ORDER)


def test_every_question_file_reaches_the_dashboard():
    """A question on disk but unregistered never reaches Metabase.

    The sync builds cards from DISP and places them from ORDER, so a name in one
    and not the other disappears without an error anywhere.
    """
    disp, order = _registered_questions()
    on_disk = {
        name[: -len(".sql")]
        for name in os.listdir(os.path.join(ROOT, "sql", "questions"))
        if name.endswith(".sql")
    }
    assert on_disk == disp == order, (
        "sql/questions, DISP and ORDER must name the same set: "
        f"files without a card {sorted(on_disk - disp)}, "
        f"card without a file {sorted(disp - on_disk)}, "
        f"card without a placement {sorted(disp - order)}, "
        f"placement without a card {sorted(order - disp)}"
    )


def test_question_sql_only_names_catalogs_trino_defines():
    """The cards spell the catalog out, and only the config file name defines it.

    TABLE_PREFIX moves the tables the pipeline writes and nothing else, so a Glue
    deployment has to rename the Trino catalog too, or the cards keep reading the
    old one and the dashboard goes quiet instead of wrong.
    """
    catalogs = {
        name[: -len(".properties")]
        for name in os.listdir(os.path.join(ROOT, "trino", "catalog"))
        if name.endswith(".properties")
    }
    questions = os.path.join(ROOT, "sql", "questions")
    used = set()
    for name in sorted(os.listdir(questions)):
        if name.endswith(".sql"):
            with open(os.path.join(questions, name), encoding="utf-8") as fh:
                used |= set(re.findall(r"\b([A-Za-z_]\w*)\.db\.", fh.read()))
    assert used, "no question names a fully qualified table, so this check is asleep"
    assert used <= catalogs, (
        f"questions read {sorted(used - catalogs)}, which trino/catalog does not define "
        f"(it defines {sorted(catalogs)}): rename the catalog file or the questions, "
        "they are the only two places the name lives"
    )
