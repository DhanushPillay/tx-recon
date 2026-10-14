"""Audit journal + rollback guards (unit-only, mocked Spark).

- journal requires reason + approver, appends without touching prior lines
- DLQ --delete requires --reason + --approved-by BEFORE any purge SQL
- rollback restores the pre-batch state: failed batch drops the WAP branch
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.processing.corrections import journal_correction

pytestmark = pytest.mark.unit


def test_journal_appends_without_touching_history(tmp_path):
    data_dir = str(tmp_path)
    journal_correction(data_dir, "act-1", "T-1", "alice", {"n": 1})
    with open(tmp_path / "corrections.jsonl", encoding="utf-8") as fh:
        first = fh.read()
    journal_correction(data_dir, "act-2", "T-2", "bob", {"n": 2})
    with open(tmp_path / "corrections.jsonl", encoding="utf-8") as fh:
        lines = fh.readlines()
    assert len(lines) == 2
    assert lines[0] == first
    assert json.loads(lines[1])["approved_by"] == "bob"


def test_journal_refuses_without_reason_or_approver(tmp_path):
    with pytest.raises(ValueError, match="reason"):
        journal_correction(str(tmp_path), "x", "", "alice")
    with pytest.raises(ValueError, match="approved_by"):
        journal_correction(str(tmp_path), "x", "T-1", "  ")
    assert not (tmp_path / "corrections.jsonl").exists()


def _replay_main(argv, spark):
    import sys

    from scripts import replay_dlq

    with (
        patch.object(sys, "argv", ["replay_dlq.py", *argv]),
        patch.object(replay_dlq, "get_spark_session", return_value=spark),
        patch.object(replay_dlq, "get_settings") as gs,
        patch("src.validation.pan_guard.count_spark_pan_hits", return_value=0),
    ):
        gs.return_value = MagicMock(
            dlq_table="nessie.db.webhooks_dlq",
            webhook_table="nessie.db.webhooks",
            project_root=".",
        )
        return replay_dlq.main()


def _spark_with_rows(n):
    spark = MagicMock()
    replayable = MagicMock()
    replayable.count.return_value = n
    spark.sql.side_effect = [
        replayable,  # SELECT * replayable
        MagicMock(collect=lambda: [{"n": 0}]),  # already-present probe
        MagicMock(),  # MERGE replay
        MagicMock(),  # MERGE purge (delete only)
    ]
    return spark


def test_delete_without_reason_fails_before_purge():
    from scripts import replay_dlq

    spark = _spark_with_rows(3)
    with (
        patch.object(replay_dlq, "journal_correction") as j,
        pytest.raises(ValueError, match="--reason"),
    ):
        _replay_main(["--delete", "--approved-by", "alice"], spark)
    assert spark.sql.call_count == 3  # no purge MERGE issued
    j.assert_not_called()


def test_delete_without_approver_fails_before_purge():
    from scripts import replay_dlq

    spark = _spark_with_rows(3)
    with (
        patch.object(replay_dlq, "journal_correction") as j,
        pytest.raises(ValueError, match="--approved-by"),
    ):
        _replay_main(["--delete", "--reason", "T-1"], spark)
    assert spark.sql.call_count == 3
    j.assert_not_called()


def test_failed_batch_restores_pre_batch_state(tmp_path, monkeypatch):
    """A failed batch must drop the WAP branch: the table keeps pre-batch data."""
    from src.pipeline import run_daily

    monkeypatch.delenv("WIRED_LEGS", raising=False)
    spark = MagicMock()
    curated = str(tmp_path / "curated_settlement_20250402.csv")
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=curated,
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch(
            "src.processing.reconcile.run_reconciliation",
            side_effect=RuntimeError("boom"),
        ),
        patch("src.processing.reconcile.maintain_tables", return_value={}),
        pytest.raises(RuntimeError, match="boom"),
    ):
        run_daily(date_str="2025-04-02")
    stmts = [c[0][0] for c in spark.sql.call_args_list]
    assert any("DROP BRANCH" in s for s in stmts)
    assert not any("fast_forward" in s for s in stmts)
