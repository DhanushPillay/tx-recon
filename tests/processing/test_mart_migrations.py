# ruff: noqa: S608 -- the fixtures below are literal test SQL, not input.
import glob
import os

from src.processing.reconcile import _load_mart_sql

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")


def _norm(sql: str) -> str:
    return "\n".join(line.rstrip() for line in sql.strip().splitlines())


def test_migrations_chain_loads_newest():
    files = glob.glob(os.path.join(ROOT, "sql", "marts", "migrations", "V*.sql"))
    assert files, "no mart migrations found"
    sql, version = _load_mart_sql(ROOT, "nessie.db.webhooks", "nessie.db.fact_reconciliation")
    assert "__SOURCE_TABLE__" not in sql and "__MART_TABLE__" not in sql
    assert (
        version
        == os.path.basename(
            max(files, key=lambda p: int(os.path.basename(p)[1:].split("__")[0]))
        ).split("__")[0]
    )


def test_migration_order_is_numeric_not_lexicographic(tmp_path):
    mig = tmp_path / "sql" / "marts" / "migrations"
    mig.mkdir(parents=True)
    for n, marker in ((1, "one"), (2, "two"), (10, "ten")):
        (mig / f"V{n}__fact_reconciliation.sql").write_text(
            f"CREATE OR REPLACE TABLE __MART_TABLE__ USING iceberg AS "
            f"SELECT '{marker}' AS marker FROM __SOURCE_TABLE__"
        )
    sql, version = _load_mart_sql(str(tmp_path), "nessie.db.webhooks", "nessie.db.m")
    assert version == "V10"
    assert "'ten'" in sql


def test_checked_in_hand_run_copy_matches_migration():
    sql, _ = _load_mart_sql(ROOT, "nessie.db.webhooks", "nessie.db.fact_reconciliation")
    with open(os.path.join(ROOT, "sql", "marts", "fact_reconciliation.sql")) as f:
        checked_in = f.read()
    body = checked_in.split("CREATE OR REPLACE TABLE", 1)[1]
    rendered_body = sql.split("CREATE OR REPLACE TABLE", 1)[1]
    assert _norm(body) == _norm(rendered_body)
