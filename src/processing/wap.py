"""Write-audit-publish branches: batches land on a branch, merge to main on gate pass.

Failed validation never reaches consumers: the branch is dropped, main is
untouched. Short-lived branches only (ingest/YYYY-MM-DD, TTL 3 days).

Iceberg-native (ALTER TABLE ... BRANCH + system.fast_forward), verified live
against Nessie 0.107.9: the Nessie Spark-SQL extensions speak API v1 while
the server is v2, so Nessie-level CREATE/MERGE BRANCH statements fail here.
Write isolation requires BOTH the table property and the session conf
(verified: conf alone leaks writes straight to main):
  ALTER TABLE t SET TBLPROPERTIES ('write.wap.enabled' = 'true')
  spark.conf.set("spark.wap.branch", "ingest/2026-09-24")
"""

import logging

logger = logging.getLogger(__name__)

BRANCH_TTL_HINT = "short-lived only: ingest/YYYY-MM-DD, drop after merge or 3 days"


def branch_name(batch_date: str) -> str:
    return f"ingest/{batch_date}"


def create_branch(spark, table: str, branch: str) -> None:
    """CREATE BRANCH on the table. Idempotent."""
    from src.processing.reconcile import _qualified_table

    spark.sql(f"ALTER TABLE {_qualified_table(table)} CREATE BRANCH IF NOT EXISTS {branch}")
    logger.info(f"WAP branch ready: {branch} on {table} ({BRANCH_TTL_HINT})")


def drop_branch(spark, table: str, branch: str) -> None:
    from src.processing.reconcile import _qualified_table

    spark.sql(f"ALTER TABLE {_qualified_table(table)} DROP BRANCH IF EXISTS {branch}")
    logger.info(f"WAP branch dropped: {branch} on {table}")


def enable_wap(spark, table: str) -> None:
    """Allow WAP writes on the table. One-time per table (property persists)."""
    from src.processing.reconcile import _qualified_table

    spark.sql(
        f"ALTER TABLE {_qualified_table(table)} SET TBLPROPERTIES ('write.wap.enabled' = 'true')"
    )
    logger.info(f"WAP enabled on {table}")


def use_branch(spark, branch: str | None) -> None:
    """Route subsequent writes to a branch (None = back to main).

    No-op without enable_wap on the table: the conf alone does NOT isolate
    writes (verified live — rows land on main). Always pair the two.
    """
    if branch is None:
        spark.conf.unset("spark.wap.branch")
    else:
        spark.conf.set("spark.wap.branch", branch)


def merge_branch(spark, table: str, branch: str, target_ref: str = "main") -> None:
    """Fast-forward merge branch into target. The publish gate lives here:
    call only after validation + match-rate checks pass on the branch."""
    from src.processing.reconcile import _qualified_table

    catalog, rest = _qualified_table(table).split(".", 1)
    spark.sql(
        f"CALL {catalog}.system.fast_forward(table => '{rest}', "
        f"branch => '{target_ref}', to => '{branch}')"
    )
    logger.info(f"WAP branch merged: {branch} -> {target_ref} on {table}")


def publish_branch(
    spark, table: str, branch: str, *, validated: bool = False, target_ref: str = "main"
) -> None:
    """Publish gate: merge runs only with validated=True (fail closed).

    The caller passes validation + match-rate results explicitly — an
    unvalidated branch can never reach main by accident.
    """
    if not validated:
        raise RuntimeError(
            f"refusing to publish unvalidated branch {branch} on {table}: "
            "pass validated=True after validation + match-rate checks"
        )
    merge_branch(spark, table, branch, target_ref)
