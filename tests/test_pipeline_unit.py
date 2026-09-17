"""Pipeline pure logic: demo plan + drift guards (no Spark needed)."""

from unittest.mock import MagicMock, patch

import pytest

from src.pipeline import _build_demo_plan, _seed_demo_webhooks, main

pytestmark = pytest.mark.unit


def test_build_demo_plan_dedup_and_merchant_split():
    plan = _build_demo_plan(200, seed=42)
    assert len(plan) == 200
    assert len({t[0] for t in plan}) == 200
    assert all(len(t) == 4 for t in plan)
    merch_001 = sum(1 for t in plan if t[3] == "merch_001")
    assert 20 <= merch_001 <= 70  # ~20% with seed tolerance
    assert _build_demo_plan(200, seed=42) == _build_demo_plan(200, seed=42)


def test_seed_demo_webhooks_3_and_4_tuple():
    spark = MagicMock()
    n = _seed_demo_webhooks(
        spark, "nessie.db.webhooks", [("tx_a", 1000, "UPI"), ("tx_b", 2000, "UPI", "m1")]
    )
    assert n == 2
    assert spark.sql.call_count >= 2
    assert spark.createDataFrame.call_count == 1


def test_main_drift_and_demo_warnings():
    counts = {"settlement_rows_deduped": 10, "batch_MATCHED": 6, "batch_EXCEPTION_FEE_MISMATCH": 4}
    with (
        patch("src.pipeline._build_demo_plan", return_value=[]),
        patch("src.generators.settlement_generator.generate_settlement_file", return_value="f"),
        patch("src.validation.validate_settlement.validate_latest_settlement"),
        patch("src.processing.reconcile.run_reconciliation", return_value=counts),
        patch("src.processing.reconcile.maintain_tables", return_value={"m": {}}),
    ):
        with patch("sys.argv", ["pipeline", "--date", "2025-04-02"]):
            out = main()
        assert out["settlement_rows_deduped"] == 10
        assert out["maintenance"] == {"m": {}}


def test_main_drift_raises():
    from src.pipeline import check_batch_drift

    with pytest.raises(RuntimeError):
        check_batch_drift(
            {"settlement_rows_deduped": 10, "batch_MATCHED": 5, "batch_EXCEPTION_FEE_MISMATCH": 4}
        )


def test_main_demo_zero_match_warns_but_returns():
    counts = {"settlement_rows_deduped": 5, "batch_MATCHED": 0, "batch_X": 5}
    with (
        patch("src.pipeline._build_demo_plan", return_value=[("tx_x", 1, "UPI", "m")]),
        patch("src.common.config.get_spark_session"),
        patch("src.pipeline._seed_demo_webhooks", return_value=1),
        patch("src.generators.settlement_generator.generate_settlement_file", return_value="f"),
        patch("src.validation.validate_settlement.validate_latest_settlement"),
        patch("src.processing.reconcile.run_reconciliation", return_value=counts),
        patch("src.processing.reconcile.maintain_tables", return_value={}),
        patch("sys.argv", ["pipeline", "--demo"]),
        pytest.raises(RuntimeError),
    ):
        main()
