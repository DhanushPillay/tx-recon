"""Pipeline pure logic: drift guards (no Spark needed, real-data only)."""

from unittest.mock import patch

import pytest

from src.pipeline import _resolve_batch_date, check_batch_drift, main, run_daily

pytestmark = pytest.mark.unit


def test_main_runs_real_file(tmp_path):
    counts = {"settlement_rows_deduped": 10, "batch_MATCHED": 6, "batch_EXCEPTION_FEE_MISMATCH": 4}
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=str(tmp_path / "curated_settlement_20250402.csv"),
        ),
        patch("src.processing.reconcile.run_reconciliation", return_value=counts),
        patch("src.processing.reconcile.maintain_tables", return_value={"m": {}}),
    ):
        with patch("sys.argv", ["pipeline", "--date", "2025-04-02"]):
            out = main()
        assert out["settlement_rows_deduped"] == 10
        assert out["maintenance"] == {"m": {}}
        assert (tmp_path / "metrics_20250402.json").exists()


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
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=curated_path,
        ),
        patch("src.processing.reconcile.run_reconciliation", return_value=counts),
        patch("src.processing.reconcile.maintain_tables", maintain_impl),
    )


def test_run_daily_metrics_write_failure_skipped(tmp_path):
    """OSError on metrics write warns and continues (fail-open observability)."""
    counts = {"settlement_rows_deduped": 2, "batch_MATCHED": 2}
    p1, p2, p3 = _patched_daily(
        counts, str(tmp_path / "curated_settlement_20250402.csv"), lambda: {}
    )
    with p1, p2, p3, patch("os.path.dirname", return_value="/nonexistent-dir-xyz"):
        out = run_daily(date_str="2025-04-02")
    assert out["settlement_rows_deduped"] == 2


def test_run_daily_maintenance_failure_skipped(tmp_path):
    """maintain_tables raising never fails the batch (warning only)."""
    counts = {"settlement_rows_deduped": 2, "batch_MATCHED": 2}

    def _boom():
        raise RuntimeError("expire exploded")

    p1, p2, p3 = _patched_daily(counts, str(tmp_path / "curated_settlement_20250402.csv"), _boom)
    with p1, p2, p3:
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
