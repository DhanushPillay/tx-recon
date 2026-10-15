"""A refund is an event, not garbage.

`ingest_webhooks` gated every row on `amount_paise > 0`, so a refund, reversal
or chargeback went to the DLQ with the same schema as a corrupt record and the
same log line. `replay_dlq.py` then filtered on the same predicate, so those
rows could not be replayed and `DELETE FROM` never reached them either: a
reversal was write-once, unreadable, and unpurgeable.

These tests pin the split. What they do NOT pin is a fee model for a reversal.
The settlement contract still quarantines a negative settled amount, so a
reversal webhook parks at EXCEPTION_REVERSAL and waits. That wait is visible,
counted and logged; it is not a silent drop, and these tests say so rather than
pretending the end-to-end path exists.
"""

import pandas as pd
import pytest

from src.common.schemas import (
    EXCEPTION_REVERSAL,
    PENDING_RECONCILIATION,
    PLACEHOLDER_STATUSES,
    REVERSAL_GATEWAY_STATUSES,
    TERMINAL_STATUSES,
)

pytestmark = pytest.mark.unit

_SCHEMA = "decodable boolean, transaction_id string, amount_paise bigint, gateway_status string"


def _frame(spark, rows):
    """rows of (decodable, transaction_id, amount_paise, gateway_status)."""
    return spark.createDataFrame(pd.DataFrame(rows, columns=list("dtag")), _SCHEMA)


# --- the vocabulary --------------------------------------------------------------


def test_the_vocabulary_is_uppercase_and_non_empty():
    assert REVERSAL_GATEWAY_STATUSES
    assert all(s == s.upper() for s in REVERSAL_GATEWAY_STATUSES)


def test_a_reversal_status_is_not_a_placeholder_and_not_terminal():
    assert EXCEPTION_REVERSAL not in PLACEHOLDER_STATUSES
    assert EXCEPTION_REVERSAL not in TERMINAL_STATUSES


# --- which rows count as a reversal ---------------------------------------------


@pytest.mark.parametrize("status", sorted(REVERSAL_GATEWAY_STATUSES))
def test_every_vocabulary_word_is_matched_case_insensitively(spark, status):
    from src.ingestion.ingest_webhooks import _reversal_condition

    df = _frame(
        spark,
        [
            (True, "tx_1", -5000, status),
            (True, "tx_1", -5000, status.lower()),
            (True, "tx_1", -5000, f"  {status.title()}  "),
        ],
    )
    assert df.filter(_reversal_condition()).count() == 3, f"{status!r} did not match all spellings"


@pytest.mark.parametrize(
    "status", ["CAPTURED", "AUTHORIZED", "SETTLED", "", "  ", "REFUND_PENDING_X", "REFUNDE"]
)
def test_a_forward_status_is_not_mistaken_for_a_reversal(spark, status):
    from src.ingestion.ingest_webhooks import _reversal_condition

    df = _frame(spark, [(True, "tx_1", -5000, status)])
    assert df.filter(_reversal_condition()).count() == 0


def test_the_amount_is_the_discriminator(spark):
    from src.ingestion.ingest_webhooks import _reversal_condition

    df = _frame(
        spark,
        [
            (True, "tx_1", -5000, "REFUNDED"),  # reversal
            (True, "tx_1", 0, "REFUNDED"),  # zero refund: still a refund
            (True, "tx_1", 5000, "REFUNDED"),  # positive: an ordinary payment
            (True, "tx_1", -5000, "CAPTURED"),  # negative, no refund word: garbage
        ],
    )
    assert df.filter(_reversal_condition()).count() == 2


def test_a_null_gateway_status_is_not_a_reversal(spark):
    """coalesce to empty, not to a default that would match the vocabulary."""
    from src.ingestion.ingest_webhooks import _reversal_condition

    df = _frame(spark, [(True, "tx_1", -5000, None)])
    assert df.filter(_reversal_condition()).count() == 0


def test_a_reversal_without_a_transaction_id_stays_out(spark):
    """An unusable row is still unusable; that is what the DLQ is for."""
    from src.ingestion.ingest_webhooks import _reversal_condition

    df = _frame(spark, [(True, "tx_1", -5000, "REFUNDED"), (True, None, -5000, "REFUNDED")])
    assert df.filter(_reversal_condition()).count() == 1


def test_a_reversal_with_an_undecodable_body_stays_out(spark):
    from src.ingestion.ingest_webhooks import _reversal_condition

    df = _frame(spark, [(True, "tx_1", -5000, "REFUNDED"), (False, "tx_2", -5000, "REFUNDED")])
    assert df.filter(_reversal_condition()).count() == 1


# --- valid, or DLQ ---------------------------------------------------------------


def test_the_dlq_no_longer_takes_the_reversals(spark):
    from src.ingestion.ingest_webhooks import _accepted_condition

    df = _frame(
        spark,
        [
            (True, "tx_1", 5000, "CAPTURED"),  # forward payment: valid, as before
            (True, "tx_2", -5000, "REFUNDED"),  # reversal: valid, newly
            (True, "tx_3", 0, "CHARGEBACK"),  # zero refund: valid, newly
            (True, "tx_4", -5000, "GARBAGE"),  # non-positive, no refund word: DLQ
            (True, "tx_5", 0, "CAPTURED"),  # zero, no refund word: DLQ
            (True, None, 5000, "CAPTURED"),  # no id: DLQ, as before
            (False, "tx_7", 5000, "CAPTURED"),  # undecodable: DLQ, as before
        ],
    )
    got = [r["ok"] for r in df.select(_accepted_condition().alias("ok")).collect()]
    assert got == [True, True, True, False, False, False, False]


def test_the_forward_branch_is_unchanged(spark):
    """The pre-existing predicate must still behave exactly as it did."""
    from src.ingestion.ingest_webhooks import _forward_condition

    df = _frame(
        spark,
        [
            (True, "tx_1", 5000, "CAPTURED"),
            (True, "tx_2", 0, "CAPTURED"),
            (True, "tx_3", -1, "REFUNDED"),
            (True, None, 5000, "CAPTURED"),
            (None, "tx_5", 5000, "CAPTURED"),
        ],
    )
    got = [r["ok"] for r in df.select(_forward_condition().alias("ok")).collect()]
    assert got == [True, False, False, False, False]


# --- where the row lands ---------------------------------------------------------


def test_a_reversal_is_inserted_at_the_reversal_status(spark):
    """Not PENDING: there is no settlement row for it, so PENDING would have the
    late-SLA sweep age it as an ordinary missing settlement."""
    from src.ingestion.ingest_webhooks import _ingest_status_column

    df = _frame(
        spark,
        [
            (True, "tx_1", 1000, "CAPTURED"),
            (True, "tx_2", -1000, "REFUNDED"),
            (True, "tx_3", 0, "REFUNDED"),
        ],
    )
    got = [r["st"] for r in df.select(_ingest_status_column().alias("st")).collect()]
    assert got == [PENDING_RECONCILIATION, EXCEPTION_REVERSAL, EXCEPTION_REVERSAL]


def test_a_null_amount_never_reaches_the_status_column(spark):
    """Neither branch of `_accepted_condition` accepts a NULL amount, so a
    corrupt amount goes to the DLQ instead of picking up a status. Asking
    `_ingest_status_column` directly would answer PENDING (Spark treats
    `NULL <= 0` as not-true, so the ELSE wins) - unreachable in the sink, and
    pinned here so the reasoning is on the record."""
    from src.ingestion.ingest_webhooks import _accepted_condition, _ingest_status_column

    df = _frame(spark, [(True, "tx_1", None, "REFUNDED"), (True, "tx_2", None, "CAPTURED")])
    assert df.filter(_accepted_condition()).count() == 0
    assert df.select(_ingest_status_column().alias("st")).collect()[0]["st"] == (
        PENDING_RECONCILIATION
    )


# --- and the DLQ replay that could never drain them ------------------------------


def test_replay_status_list_is_quoted_and_complete():
    from scripts.replay_dlq import _status_list

    sql_list = _status_list()
    assert sql_list.count("'") == 2 * len(REVERSAL_GATEWAY_STATUSES)
    assert " " not in sql_list, "an unquoted space would break the IN list"
    for word in ("REFUNDED", "CHARGEBACK", "REVERSED"):
        assert f"'{word}'" in sql_list


def test_replay_mirrors_the_split_in_its_own_predicate(spark):
    """Mirrors the SQL main() runs: reversals are reported, malformed negatives
    are still replayable, and both are counted so neither is invisible."""
    from pyspark.sql.functions import trim, upper

    from scripts.replay_dlq import _status_list

    df = spark.createDataFrame(
        pd.DataFrame(
            [
                ("tx_1", 5000, "CAPTURED"),
                ("tx_2", -5000, "REFUNDED"),
                ("tx_3", 0, "CHARGEBACK"),
                ("tx_4", -5000, "GARBAGE"),
                (None, 5000, "CAPTURED"),
            ],
            columns=["transaction_id", "amount_paise", "gateway_status"],
        )
    )
    is_reversal = upper(trim(df["gateway_status"])).isin(list(REVERSAL_GATEWAY_STATUSES))
    assert df.filter(is_reversal).count() == 2, "tx_2 and tx_3 are reversals"
    replayable = df.filter(df["transaction_id"].isNotNull() & ~is_reversal).count()
    assert replayable == 2, "tx_1 and tx_4; the malformed negative is still replayable"
    assert _status_list()
