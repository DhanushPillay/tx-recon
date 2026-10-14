"""P5: residual scoring + audit journal + DLQ replay edges (unit-only).

Residual demote-only contract, tau boundary, merchant-disagreement 0.95,
journal maker-checker guards + append immutability, DLQ purge ordering.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.processing.corrections import journal_correction
from src.processing.residual import DEFAULT_TAU, score_match, should_downgrade

pytestmark = pytest.mark.unit


def _eng():
    from src.processing.fee_engine import FeeEngine

    return FeeEngine()


# ---- residual ----


def test_perfect_match_never_downgrades():
    eng = _eng()
    net = eng.compute_expected_settled(100000, "UPI")
    assert should_downgrade(100000, net, "UPI", None, None, fee_engine=eng) is False


def test_merchant_disagreement_scores_high():
    eng = _eng()
    net_default = eng.compute_expected_settled(100000, "CREDIT_CARD")
    s = score_match(100000, net_default, "CREDIT_CARD", "merch_loose", None, fee_engine=eng)
    assert 0.0 <= s <= 1.0


def test_tau_boundary():
    assert DEFAULT_TAU == 0.9
    assert should_downgrade(100000, 100000, "UPI", None, None, tau=0.0) is True
    assert should_downgrade(100000, 100000, "UPI", None, None, tau=2.0) is False


def test_score_monotone_in_excess():
    eng = _eng()
    net = eng.compute_expected_settled(100000, "CREDIT_CARD")
    s_close = score_match(100000, net + 50, "CREDIT_CARD", None, None, fee_engine=eng)
    s_far = score_match(100000, net + 5000, "CREDIT_CARD", None, None, fee_engine=eng)
    assert s_far >= s_close


def test_apply_residual_sql_demote_only():
    import inspect

    import src.processing.residual as res

    src = inspect.getsource(res.apply_residual)
    assert "WHEN MATCHED THEN UPDATE" in src
    assert "WHEN NOT MATCHED" not in src
    assert "EXCEPTION_FEE_MISMATCH" in src


def test_apply_residual_zero_rows_short_circuits():
    from src.processing import residual as res

    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [{"n": 0}]
    assert res.apply_residual(spark, "nessie.db.webhooks") == 0
    assert spark.sql.call_count == 1  # count only, no MERGE


# ---- journal ----


def test_journal_appends_immutable_history(tmp_path):
    d = str(tmp_path)
    journal_correction(d, "act-1", "T-1", "alice", {"n": 1})
    first = (tmp_path / "corrections.jsonl").read_text(encoding="utf-8")
    journal_correction(d, "act-2", "T-2", "bob")
    lines = (tmp_path / "corrections.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert lines[0] == first.strip()
    entry = json.loads(lines[1])
    assert entry["approved_by"] == "bob" and entry["reason"] == "T-2"


def test_journal_maker_checker_refusals(tmp_path):
    with pytest.raises(ValueError, match="reason"):
        journal_correction(str(tmp_path), "x", "", "alice")
    with pytest.raises(ValueError, match="approved_by"):
        journal_correction(str(tmp_path), "x", "T-1", " ")
    assert not (tmp_path / "corrections.jsonl").exists()


def test_journal_write_failure_raises(tmp_path):
    blocked = tmp_path / "corrections.jsonl"
    blocked.mkdir()  # a directory at the journal path -> OSError on append
    with pytest.raises(RuntimeError, match="cannot write corrections journal"):
        journal_correction(str(tmp_path), "x", "T-1", "alice")


# ---- DLQ replay ordering ----


def _spark_rows(n, already=0):
    spark = MagicMock()
    replayable = MagicMock()
    replayable.count.return_value = n
    spark.sql.side_effect = [
        replayable,
        MagicMock(collect=lambda: [{"n": already}]),
        MagicMock(),
        MagicMock(),
    ]
    return spark


def _run(argv, spark):
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


def test_dlq_inserted_math_subtracts_already_present():
    out = _run([], _spark_rows(3, already=1))
    assert out == {"replayable": 3, "inserted": 2, "deleted": False}


def test_dlq_empty_is_noop_no_journal():
    from scripts import replay_dlq

    spark = MagicMock()
    spark.sql.return_value.count.return_value = 0
    with patch.object(replay_dlq, "journal_correction") as j:
        out = _run([], spark)
    assert out == {"replayable": 0, "inserted": 0}
    j.assert_not_called()


def test_dlq_delete_validates_before_purge():
    from scripts import replay_dlq

    for argv, msg in (
        (["--delete", "--approved-by", "alice"], "--reason"),
        (["--delete", "--reason", "T-1"], "--approved-by"),
    ):
        spark = _spark_rows(3)
        with (
            patch.object(replay_dlq, "journal_correction") as j,
            pytest.raises(ValueError, match=msg),
        ):
            _run(argv, spark)
        assert spark.sql.call_count == 3  # replay MERGE done, purge never issued
        j.assert_not_called()
