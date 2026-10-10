import logging

logger = logging.getLogger(__name__)

BRANCH_TTL_HINT = "short-lived only: ingest/YYYY-MM-DD, drop after merge or 3 days"


def branch_name(batch_date: str) -> str:
    return f"ingest/{batch_date}"


def _branch_ident(branch: str) -> str:
    return "`" + branch.replace("`", "``") + "`"


def create_branch(spark, table: str, branch: str) -> None:
    from src.processing.reconcile import _qualified_table

    spark.sql(
        f"ALTER TABLE {_qualified_table(table)} CREATE BRANCH IF NOT EXISTS {_branch_ident(branch)}"
    )
    logger.info(f"WAP branch ready: {branch} on {table} ({BRANCH_TTL_HINT})")


def drop_branch(spark, table: str, branch: str) -> None:
    from src.processing.reconcile import _qualified_table

    spark.sql(
        f"ALTER TABLE {_qualified_table(table)} DROP BRANCH IF EXISTS {_branch_ident(branch)}"
    )
    logger.info(f"WAP branch dropped: {branch} on {table}")


def enable_wap(spark, table: str) -> None:
    from src.processing.reconcile import _qualified_table

    spark.sql(
        f"ALTER TABLE {_qualified_table(table)} SET TBLPROPERTIES ('write.wap.enabled' = 'true')"
    )
    logger.info(f"WAP enabled on {table}")


def use_branch(spark, branch: str | None) -> None:
    # needs enable_wap too, conf alone leaks writes to main
    if branch is None:
        spark.conf.unset("spark.wap.branch")
    else:
        spark.conf.set("spark.wap.branch", branch)


def merge_branch(spark, table: str, branch: str, target_ref: str = "main") -> None:
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
    if not validated:
        raise RuntimeError(
            f"refusing to publish unvalidated branch {branch} on {table}: "
            "pass validated=True after validation + match-rate checks"
        )
    merge_branch(spark, table, branch, target_ref)
