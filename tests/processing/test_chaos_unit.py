"""Chaos / failure-injection (unit-only, mocked Spark).

Every external failure must fail closed: no publish, WAP branch dropped,
and strict mode raises while non-strict only warns.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.pipeline import _run_residual, run_daily

pytestmark = pytest.mark.unit


def _counts(rate=1.0):
    return {"settlement_rows_evaluated": 4, "batch_MATCHED": 4, "batch_match_rate": rate}


def _patch_green(spark, curated, counts):
    return (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=curated,
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch("src.processing.reconcile.run_reconciliation", return_value=counts),
        patch("src.processing.residual.apply_residual", return_value=0),
        patch("src.processing.reconcile.maintain_tables", return_value={}),
    )


def test_validation_returns_nothing_fails_closed(tmp_path):
    spark = MagicMock()
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=None,
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        pytest.raises(RuntimeError, match="no curated file"),
    ):
        run_daily(date_str="2025-04-02")
    assert not any("fast_forward" in c[0][0] for c in spark.sql.call_args_list)


def test_merge_failure_drops_branch_without_publish(tmp_path, monkeypatch):
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
            side_effect=RuntimeError("Reconciliation MERGE failed: boom"),
        ),
        patch("src.processing.reconcile.maintain_tables", return_value={}),
        pytest.raises(RuntimeError, match="MERGE failed"),
    ):
        run_daily(date_str="2025-04-02")
    stmts = [c[0][0] for c in spark.sql.call_args_list]
    assert not any("fast_forward" in s for s in stmts)
    assert any("DROP BRANCH" in s for s in stmts)


def test_residual_failure_strict_raises(monkeypatch):
    from src.common.settings import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "strict_slo", True)
    monkeypatch.setattr(settings, "wired_legs", "residual")
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("scoring down")
    with (
        patch("src.common.config.get_spark_session", return_value=spark),
        patch("src.processing.reconcile.leg_enabled", return_value=True),
        pytest.raises(RuntimeError, match="Residual pass failed"),
    ):
        _run_residual(dict(_counts()))


def test_residual_failure_nonstrict_warns_only(monkeypatch):
    from src.common.settings import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "strict_slo", False)
    monkeypatch.setattr(settings, "wired_legs", "residual")
    with (
        patch(
            "src.processing.residual.apply_residual",
            side_effect=RuntimeError("scoring down"),
        ),
        patch("src.processing.reconcile.leg_enabled", return_value=True),
    ):
        assert _run_residual(dict(_counts())) is False


def test_residual_leg_disabled_skips():
    with patch("src.processing.reconcile.leg_enabled", return_value=False):
        assert _run_residual(dict(_counts())) is True
