import os
from unittest.mock import MagicMock, call, patch

import pytest

from src.pipeline import _resolve_batch_date, check_batch_drift, main, run_daily

pytestmark = pytest.mark.unit


def test_main_runs_real_file(tmp_path, monkeypatch):
    monkeypatch.setenv("WIRED_LEGS", "bank")
    counts = {"settlement_rows_deduped": 10, "batch_MATCHED": 6, "batch_EXCEPTION_FEE_MISMATCH": 4}
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=str(tmp_path / "curated_settlement_20250402.csv"),
        ),
        patch("src.common.config.get_spark_session", return_value=MagicMock()),
        patch("src.processing.reconcile.run_reconciliation", return_value=counts),
        patch("src.processing.reconcile.refresh_mart"),
        patch("src.processing.reconcile.maintain_tables", return_value={"m": {}}),
    ):
        with patch("sys.argv", ["pipeline", "--date", "2025-04-02"]):
            out = main()
        assert out["settlement_rows_deduped"] == 10
        assert out["maintenance"] == {"m": {}}
        assert (tmp_path / "metrics_20250402.json").exists()


def test_run_daily_drops_the_branch_when_a_gate_fails(tmp_path, monkeypatch):
    monkeypatch.delenv("WIRED_LEGS", raising=False)
    spark = MagicMock()
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=str(tmp_path / "curated_settlement_20250402.csv"),
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch(
            "src.processing.reconcile.run_reconciliation",
            return_value={"settlement_rows_evaluated": 10, "batch_MATCHED": 5},
        ),
        patch(
            "src.processing.reconcile.maintain_tables",
            side_effect=AssertionError("maintenance must not run after a failed gate"),
        ),
        pytest.raises(RuntimeError),
    ):
        run_daily(date_str="2025-04-02")
    stmts = [c[0][0] for c in spark.sql.call_args_list]
    assert any("DROP BRANCH IF EXISTS `ingest/2025-04-02`" in s for s in stmts)
    assert not any("fast_forward" in s for s in stmts)


def test_run_daily_publishes_only_after_the_gates_pass(tmp_path, monkeypatch):
    monkeypatch.delenv("WIRED_LEGS", raising=False)
    spark = MagicMock()
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=str(tmp_path / "curated_settlement_20250402.csv"),
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch(
            "src.processing.residual.apply_residual",
            return_value=0,
        ),
        patch(
            "src.processing.reconcile.run_reconciliation",
            return_value={"settlement_rows_evaluated": 10, "batch_MATCHED": 10},
        ),
        patch("src.processing.reconcile.maintain_tables", return_value={"m": {}}),
    ):
        out = run_daily(date_str="2025-04-02")
    # apply_residual returns a row count, not a report dict.
    assert out["residual"] == 0
    stmts = [c[0][0] for c in spark.sql.call_args_list]
    assert any("DROP BRANCH IF EXISTS `ingest/2025-04-02`" in s for s in stmts)
    assert any("CREATE BRANCH IF NOT EXISTS `ingest/2025-04-02`" in s for s in stmts)
    assert any("fast_forward" in s for s in stmts)
    # drop-before-create + drop-after-publish: DROP must appear both before and after publish
    assert sum("DROP BRANCH" in s for s in stmts) >= 2
    assert spark.conf.set.call_args_list[-1] == call("spark.wap.branch", "ingest/2025-04-02")
    assert spark.conf.unset.call_args_list[-1] == call("spark.wap.branch")


def test_main_drift_raises():
    with pytest.raises(RuntimeError):
        check_batch_drift(
            {"settlement_rows_deduped": 10, "batch_MATCHED": 5, "batch_EXCEPTION_FEE_MISMATCH": 4}
        )


def test_resolve_batch_date_explicit_wins():
    assert _resolve_batch_date("data/curated_settlement_20250402.csv", "2025-05-06") == "2025-05-06"


def test_resolve_batch_date_from_filename():
    assert _resolve_batch_date("data/curated_settlement_20250402.csv", None) == "2025-04-02"


def test_resolve_batch_date_dateless_raises():
    with pytest.raises(ValueError, match="pass --date"):
        _resolve_batch_date("data/curated_settlement_final.csv", None)


def _patched_daily(counts, curated_path, maintain_impl):
    return (
        patch.dict(os.environ, {"WIRED_LEGS": "bank"}),
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=curated_path,
        ),
        patch("src.common.config.get_spark_session", return_value=MagicMock()),
        patch("src.processing.reconcile.run_reconciliation", return_value=counts),
        patch("src.processing.reconcile.refresh_mart"),
        patch("src.processing.reconcile.maintain_tables", maintain_impl),
    )


def test_run_daily_metrics_write_failure_skipped(tmp_path):
    counts = {"settlement_rows_deduped": 2, "batch_MATCHED": 2}
    p1, p2, p3, p4, p5, p6 = _patched_daily(
        counts, str(tmp_path / "curated_settlement_20250402.csv"), lambda: {}
    )
    with p1, p2, p3, p4, p5, p6, patch("os.path.dirname", return_value="/nonexistent-dir-xyz"):
        out = run_daily(date_str="2025-04-02")
    assert out["settlement_rows_deduped"] == 2


def test_run_daily_maintenance_failure_skipped(tmp_path):
    counts = {"settlement_rows_deduped": 2, "batch_MATCHED": 2}

    def _boom():
        raise RuntimeError("expire exploded")

    p1, p2, p3, p4, p5, p6 = _patched_daily(
        counts, str(tmp_path / "curated_settlement_20250402.csv"), _boom
    )
    with p1, p2, p3, p4, p5, p6:
        out = run_daily(date_str="2025-04-02")
    assert out["settlement_rows_deduped"] == 2
    assert "maintenance" not in out


def test_run_daily_no_curated_file_raises():
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=None,
        ),
        pytest.raises(RuntimeError, match="no curated file"),
    ):
        run_daily(date_str="2025-04-02")


def test_run_daily_refuses_publish_on_slo_breach(tmp_path, monkeypatch):
    monkeypatch.delenv("WIRED_LEGS", raising=False)
    spark = MagicMock()
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=str(tmp_path / "curated_settlement_20250402.csv"),
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch(
            "src.processing.reconcile.run_reconciliation",
            return_value={
                "settlement_rows_evaluated": 10,
                "batch_MATCHED": 5,
                "batch_EXCEPTION_FEE_MISMATCH": 5,
                "batch_match_rate": 0.5,
            },
        ),
        pytest.raises(RuntimeError, match="publish gate failed"),
    ):
        run_daily(date_str="2025-04-02")
    stmts = [c[0][0] for c in spark.sql.call_args_list]
    assert not any("fast_forward" in s for s in stmts)
    assert any("DROP BRANCH IF EXISTS `ingest/2025-04-02`" in s for s in stmts)


def test_drift_ignores_duplicate_diagnostic():
    check_batch_drift(
        {
            "settlement_rows_deduped": 2,
            "batch_MATCHED": 1,
            "batch_EXCEPTION_MISSING_WEBHOOK": 1,
            "batch_EXCEPTION_DUPLICATE_SETTLEMENT": 1,
        }
    )


def test_drift_raises_despite_diagnostic():
    with pytest.raises(RuntimeError, match="Batch drift"):
        check_batch_drift(
            {
                "settlement_rows_deduped": 3,
                "batch_MATCHED": 1,
                "batch_EXCEPTION_MISSING_WEBHOOK": 1,
                "batch_EXCEPTION_DUPLICATE_SETTLEMENT": 1,
            }
        )
