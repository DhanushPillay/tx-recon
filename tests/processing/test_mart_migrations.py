import glob
import os

from src.processing.reconcile import _load_mart_sql

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")


def _norm(sql: str) -> str:
    return "\n".join(line.rstrip() for line in sql.strip().splitlines())


def test_migrations_chain_loads_newest():
    files = sorted(glob.glob(os.path.join(ROOT, "sql", "marts", "migrations", "V*.sql")))
    assert files, "no mart migrations found"
    sql, version = _load_mart_sql(ROOT, "nessie.db.webhooks", "nessie.db.fact_reconciliation")
    assert "__SOURCE_TABLE__" not in sql and "__MART_TABLE__" not in sql
    assert version == os.path.basename(files[-1]).split("__")[0]


def test_checked_in_hand_run_copy_matches_migration():
    sql, _ = _load_mart_sql(ROOT, "nessie.db.webhooks", "nessie.db.fact_reconciliation")
    with open(os.path.join(ROOT, "sql", "marts", "fact_reconciliation.sql")) as f:
        checked_in = f.read()
    # The hand-run copy carries its own header comment; the SELECT body must match.
    body = checked_in.split("CREATE OR REPLACE TABLE", 1)[1]
    rendered_body = sql.split("CREATE OR REPLACE TABLE", 1)[1]
    assert _norm(body) == _norm(rendered_body)
