"""A redelivery that disagrees with an already-reconciled transaction.

Before this, the ingestion MERGE had two arms that could match: one gated on
the row still being a placeholder, and one that inserted new rows. A
transaction that had already reached a terminal status -- MATCHED, or
EXCEPTION_MISSING_BANK_STATEMENT -- matched neither. A gateway resending the
same transaction_id with a different amount_paise therefore left no trace: no
row written, no DLQ entry, no counter, no log line. The correction was simply
gone, and the amount the ledger reconciled on stood unchanged.

The sink tests reuse the harness in test_ingest_extra.py: they drive the real
foreachBatch callback and read the MERGE it emits.
"""

import re
from unittest.mock import MagicMock

# Bare module name on purpose: `tests.<subpkg>` is not importable here, see
# tests/test_import_hygiene.py.
from test_ingest_extra import (
    _filter_stubs,
    _merge_sql,
    _mock_stream,
    _patch_ingest,
)

from src.ingestion.ingest_webhooks import ensure_webhook_table
from src.processing.reconcile import fold_status_counts


def _run_sink(mocker):
    """Drive run_ingestion's foreachBatch callback, hand back its MERGE sql."""
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()

    batch = MagicMock(name="batch")
    enriched = MagicMock(name="enriched")
    valid = MagicMock(name="valid")
    valid.dropDuplicates.return_value = valid
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value = enriched
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.select.return_value = invalid
    invalid.count.return_value = 0
    batch.isEmpty.return_value = False
    batch.cache.return_value = batch
    batch.filter.side_effect = _filter_stubs(valid, invalid, 0)
    mocker.patch.object(ing, "lit", return_value=MagicMock())
    mocker.patch.object(ing, "current_timestamp", return_value=MagicMock())
    mocker.patch.object(ing, "F", new=MagicMock())
    captured["fn"](batch, 0)
    return _merge_sql(spark)


def _when_matched_arms(merge_sql: str) -> list[str]:
    """The WHEN MATCHED arms, in the order Spark evaluates them."""
    body = merge_sql.split("WHEN MATCHED", 1)[1].split("WHEN NOT MATCHED", 1)[0]
    return ["WHEN MATCHED" + part for part in re.split(r"WHEN MATCHED", body)]


def _row(scope, st, n):
    return {"scope": scope, "st": st, "n": n}


# --- the table ---------------------------------------------------------------


def test_webhook_table_carries_the_duplicate_mutation_columns():
    spark = MagicMock()
    ensure_webhook_table(spark, "nessie.db.webhooks")
    ddl = "\n".join(c[0][0] for c in spark.sql.call_args_list)
    assert "duplicate_mutation_count bigint" in ddl
    assert "last_duplicate_at timestamp" in ddl


def test_existing_tables_get_the_columns_added():
    # CREATE TABLE IF NOT EXISTS is a no-op for a table that already exists, so
    # a warehouse created before these columns existed keeps the old shape
    # unless something alters it.
    spark = MagicMock()
    ensure_webhook_table(spark, "nessie.db.webhooks")
    alters = [c[0][0] for c in spark.sql.call_args_list if c[0][0].startswith("ALTER TABLE")]
    assert alters == [
        "ALTER TABLE nessie.db.webhooks ADD COLUMN IF NOT EXISTS duplicate_mutation_count bigint",
        "ALTER TABLE nessie.db.webhooks ADD COLUMN IF NOT EXISTS last_duplicate_at timestamp",
        "ALTER TABLE nessie.db.webhooks ADD COLUMN IF NOT EXISTS currency string",
    ]


# --- the merge ---------------------------------------------------------------


def test_the_merge_has_two_when_matched_arms(mocker):
    assert len(_when_matched_arms(_run_sink(mocker))) == 2


def test_the_first_arm_still_updates_a_placeholder(mocker):
    arm = _when_matched_arms(_run_sink(mocker))[0]
    assert "EXCEPTION_MISSING_WEBHOOK" in arm
    assert "PENDING_RECONCILIATION" in arm
    assert "t.amount_paise = s.amount_paise" in arm
    assert "duplicate_mutation_count" not in arm


def test_the_second_arm_catches_whatever_the_first_declined(mocker):
    arm = _when_matched_arms(_run_sink(mocker))[1]
    # No status predicate: this is the terminal-status arm, and the only way it
    # is reachable is by following the gated one.
    assert "reconciliation_status =" not in arm
    assert "COALESCE(t.duplicate_mutation_count, 0) + 1" in arm
    assert "t.last_duplicate_at = current_timestamp()" in arm


def test_the_second_arm_counts_rather_than_overwrites(mocker):
    arm = _when_matched_arms(_run_sink(mocker))[1]
    # The settled amount must not move. A redelivery is evidence, not a
    # correction the pipeline is entitled to apply on its own.
    assert "amount_paise" not in arm
    assert "reconciliation_status" not in arm


def test_the_insert_arm_is_still_last(mocker):
    merge_sql = _run_sink(mocker)
    assert merge_sql.rindex("WHEN MATCHED") < merge_sql.index("WHEN NOT MATCHED THEN INSERT")


# --- the count ---------------------------------------------------------------


def test_table_statuses_land_under_their_own_name():
    counts = fold_status_counts(
        [_row("table", "MATCHED", 3), _row("dupmut", "duplicate_mutations", 0)]
    )
    assert counts["MATCHED"] == 3


def test_batch_scopes_are_prefixed():
    counts = fold_status_counts(
        [_row("batch", "MATCHED", 2), _row("dupmut", "duplicate_mutations", 0)]
    )
    assert counts["batch_MATCHED"] == 2


def test_the_duplicate_total_is_recorded():
    counts = fold_status_counts([_row("dupmut", "duplicate_mutations", 2)])
    assert counts["duplicate_mutations"] == 2


def test_an_empty_warehouse_reads_as_zero_not_missing():
    # SUM over no rows is NULL. An absent key would be indistinguishable from
    # a query that never ran.
    counts = fold_status_counts([_row("dupmut", "duplicate_mutations", None)])
    assert counts["duplicate_mutations"] == 0


def test_the_duplicate_total_is_not_batch_prefixed():
    # check_batch_drift sums the batch_ int keys and compares that total
    # against settlement_rows_evaluated. A duplicate is not a status bucket, so
    # a batch_ prefix would surface as batch drift on any batch that had one.
    counts = fold_status_counts([_row("dupmut", "duplicate_mutations", 5)])
    assert not any(k.startswith("batch_") for k in counts)
