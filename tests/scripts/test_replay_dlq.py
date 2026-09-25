"""DLQ replay gating: read-only default, maker-checker before purge (mocked Spark)."""

from unittest.mock import MagicMock, patch

import pytest

import scripts.replay_dlq as rd

pytestmark = pytest.mark.unit


def _drive(argv, replay_n=3):
    """Run main() with mocked Spark/settings; returns (out, mock_spark, journal)."""
    mock_spark = MagicMock()
    replayable = MagicMock()
    replayable.count.return_value = replay_n
    mock_spark.sql.return_value = replayable
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
    ):
        return rd.main(), mock_spark, journal


def _sqls(mock_spark):
    return [c[0][0] for c in mock_spark.sql.call_args_list]


def test_empty_dlq_noop():
    out, mock_spark, journal = _drive([], replay_n=0)
    assert out == {"replayable": 0}
    assert len(mock_spark.sql.call_args_list) == 1  # count query only
    journal.assert_not_called()


def test_readonly_default_no_delete_no_journal():
    out, mock_spark, journal = _drive([])
    assert out == {"replayable": 3, "deleted": False}
    sqls = _sqls(mock_spark)
    assert any("WHEN NOT MATCHED THEN INSERT" in s for s in sqls)
    assert not any("WHEN MATCHED THEN DELETE" in s for s in sqls)
    journal.assert_not_called()


def test_delete_without_reason_fails_before_purge():
    mock_spark = MagicMock()
    replayable = MagicMock()
    replayable.count.return_value = 3
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
        pytest.raises(ValueError, match="--reason"),
    ):
        rd.main()
    assert not any("WHEN MATCHED THEN DELETE" in s for s in _sqls(mock_spark))
    journal.assert_not_called()


def test_delete_without_approver_fails_before_purge():
    mock_spark = MagicMock()
    replayable = MagicMock()
    replayable.count.return_value = 3
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
        pytest.raises(ValueError, match="--approved-by"),
    ):
        rd.main()
    assert not any("WHEN MATCHED THEN DELETE" in s for s in _sqls(mock_spark))


def test_delete_with_maker_checker_purges_and_journals():
    out, mock_spark, journal = _drive(["--delete", "--reason", "T-1", "--approved-by", "alice"])
    assert out == {"replayable": 3, "deleted": True}
    assert any("WHEN MATCHED THEN DELETE" in s for s in _sqls(mock_spark))
    journal.assert_called_once()
    _call = journal.call_args
    assert _call[0][1] == "dlq_purge"
    assert _call[0][2] == "T-1"
    assert _call[0][3] == "alice"
