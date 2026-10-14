"""Unit-only E2E of run_daily: validate -> MERGE -> residual -> drift/SLO -> publish.

All Spark/IO mocked. No docker. Follows tests/test_pipeline_unit.py patterns.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.pipeline import check_batch_drift, run_daily

pytestmark = pytest.mark.unit


def _green_counts():
    return {"settlement_rows_evaluated": 10, "batch_MATCHED": 10, "batch_match_rate": 1.0}


def test_e2e_green_path_publishes_and_writes_metrics(tmp_path, monkeypatch):
    monkeypatch.delenv("WIRED_LEGS", raising=False)
    monkeypatch.delenv("MATCH_RATE_SLO", raising=False)
    spark = MagicMock()
    curated = str(tmp_path / "curated_settlement_20250402.csv")
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=curated,
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch("src.processing.reconcile.run_reconciliation", return_value=_green_counts()),
        patch("src.processing.residual.apply_residual", return_value=0),
        patch("src.processing.reconcile.maintain_tables", return_value={"status": "ok"}),
    ):
        out = run_daily(date_str="2025-04-02")
    assert out["batch_MATCHED"] == 10
    assert out["maintenance"] == {"status": "ok"}
    assert (tmp_path / "metrics_20250402.json").exists()
    stmts = [c[0][0] for c in spark.sql.call_args_list]
    assert any("CREATE BRANCH IF NOT EXISTS `ingest/2025-04-02`" in s for s in stmts)
    assert any("fast_forward" in s for s in stmts)


def test_e2e_slo_breach_never_publishes(tmp_path, monkeypatch):
    monkeypatch.delenv("WIRED_LEGS", raising=False)
    spark = MagicMock()
    curated = str(tmp_path / "curated_settlement_20250402.csv")
    bad = {
        "settlement_rows_evaluated": 10,
        "batch_MATCHED": 5,
        "batch_EXCEPTION_FEE_MISMATCH": 5,
        "batch_match_rate": 0.5,
    }
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=curated,
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch("src.processing.reconcile.run_reconciliation", return_value=bad),
        patch("src.processing.residual.apply_residual", return_value=0),
        pytest.raises(RuntimeError, match="publish gate failed"),
    ):
        run_daily(date_str="2025-04-02")
    stmts = [c[0][0] for c in spark.sql.call_args_list]
    assert not any("fast_forward" in s for s in stmts)
    assert any("DROP BRANCH IF EXISTS `ingest/2025-04-02`" in s for s in stmts)


def test_e2e_empty_batch_warns_only():
    check_batch_drift({"settlement_rows_evaluated": 0, "batch_MATCHED": 0})
