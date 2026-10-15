from unittest.mock import MagicMock, patch

import pytest

import scripts.replay_dlq as rd

pytestmark = pytest.mark.unit


def _drive(argv, replay_n=3, stranded_n=0):
    mock_spark = MagicMock()
    replayable = MagicMock(name="replayable")
    replayable.count.return_value = replay_n
    replayable.collect.return_value = [{"n": 0}]
    stranded = MagicMock(name="stranded")
    stranded.collect.return_value = [{"n": stranded_n}]

    def _sql(query, *args, **kwargs):
        if "amount_paise <= 0" in query:
            return stranded
        return replayable

    mock_spark.sql.side_effect = _sql
    settings = MagicMock(
        dlq_table="nessie.db.webhooks_dlq",
        webhook_table="nessie.db.webhooks",
        project_root=".",
    )
    with (
        patch("sys.argv", ["replay_dlq.py", *argv]),
        patch.object(rd, "get_spark_session", return_value=mock_spark),
        patch.object(rd, "get_settings", return_value=settings),
        patch.object(rd, "journal_correction") as journal,
        patch("src.validation.pan_guard.count_spark_pan_hits", return_value=0),
    ):
        return rd.main(), mock_spark, journal


def _sqls(mock_spark):
    return [c[0][0] for c in mock_spark.sql.call_args_list]


def test_empty_dlq_noop():
    out, mock_spark, journal = _drive([], replay_n=0)
    assert out == {"replayable": 0, "inserted": 0, "reversal_stranded": 0}
    # The stranded-reversal census runs even when nothing is replayable: that is
    # exactly when an operator needs to be told a reversal is sitting there.
    sqls = _sqls(mock_spark)
    assert len(sqls) == 2
    assert "amount_paise <= 0" in sqls[1]
    journal.assert_not_called()


def test_stranded_reversals_are_reported_not_replayed():
    """A reversal stranded in the DLQ predates the ingestion split. The script
    counts it, says so, and leaves it for an operator to purge deliberately."""
    out, mock_spark, journal = _drive([], replay_n=0, stranded_n=7)
    assert out["reversal_stranded"] == 7
    assert out["replayable"] == 0, "the reversal must not be replayed into the ledger"
    sqls = _sqls(mock_spark)
    assert "'CHARGEBACK'" in sqls[0], "the replay predicate must exclude reversals"
    assert "amount_paise <= 0" in sqls[1]
    journal.assert_not_called()


def test_readonly_default_no_delete_no_journal():
    out, mock_spark, journal = _drive([])
    assert out == {"replayable": 3, "inserted": 3, "deleted": False, "reversal_stranded": 0}
    sqls = _sqls(mock_spark)
    assert any("WHEN NOT MATCHED THEN INSERT" in s for s in sqls)
    merge = next(s for s in sqls if "WHEN NOT MATCHED THEN INSERT" in s)
    assert "PENDING_RECONCILIATION" in merge
    assert "current_timestamp()" in merge
    assert not any("WHEN MATCHED THEN DELETE" in s for s in sqls)
    journal.assert_not_called()


def test_delete_without_reason_fails_before_purge():
    mock_spark = MagicMock()
    replayable = MagicMock()
    replayable.count.return_value = 3
    replayable.collect.return_value = [{"n": 0}]
    mock_spark.sql.return_value = replayable
    settings = MagicMock(
        dlq_table="nessie.db.webhooks_dlq",
        webhook_table="nessie.db.webhooks",
        project_root=".",
    )
    with (
        patch("sys.argv", ["replay_dlq.py", "--delete", "--approved-by", "alice"]),
        patch.object(rd, "get_spark_session", return_value=mock_spark),
        patch.object(rd, "get_settings", return_value=settings),
        patch.object(rd, "journal_correction") as journal,
        patch("src.validation.pan_guard.count_spark_pan_hits", return_value=0),
        pytest.raises(ValueError, match="--reason"),
    ):
        rd.main()
    assert not any("WHEN MATCHED THEN DELETE" in s for s in _sqls(mock_spark))
    journal.assert_not_called()


def test_delete_without_approver_fails_before_purge():
    mock_spark = MagicMock()
    replayable = MagicMock()
    replayable.count.return_value = 3
    replayable.collect.return_value = [{"n": 0}]
    mock_spark.sql.return_value = replayable
    settings = MagicMock(
        dlq_table="nessie.db.webhooks_dlq",
        webhook_table="nessie.db.webhooks",
        project_root=".",
    )
    with (
        patch("sys.argv", ["replay_dlq.py", "--delete", "--reason", "T-1"]),
        patch.object(rd, "get_spark_session", return_value=mock_spark),
        patch.object(rd, "get_settings", return_value=settings),
        patch("src.validation.pan_guard.count_spark_pan_hits", return_value=0),
        pytest.raises(ValueError, match="--approved-by"),
    ):
        rd.main()
    assert not any("WHEN MATCHED THEN DELETE" in s for s in _sqls(mock_spark))


def test_delete_with_maker_checker_purges_and_journals():
    out, mock_spark, journal = _drive(["--delete", "--reason", "T-1", "--approved-by", "alice"])
    assert out == {"replayable": 3, "inserted": 3, "deleted": True, "reversal_stranded": 0}
    assert any("WHEN MATCHED THEN DELETE" in s for s in _sqls(mock_spark))
    journal.assert_called_once()
    _call = journal.call_args
    assert _call[0][1] == "dlq_purge"
    assert _call[0][2] == "T-1"
    assert _call[0][3] == "alice"
