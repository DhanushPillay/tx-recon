"""Pipeline pure logic: drift guards (no Spark needed, real-data only)."""

from unittest.mock import patch

import pytest

from src.pipeline import check_batch_drift, main

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
