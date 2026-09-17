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


def test_assign_mix_tiles_and_generates(tmp_path):
    import csv

    from src.generators.settlement_generator import generate_settlement_file
    from src.pipeline import _assign_mix

    planned = _build_demo_plan(1000, seed=7)
    items, row_opts, orphans = _assign_mix(planned, seed=7, batch_date="2026-09-10")
    assert len(row_opts) == 1000 and set(row_opts) == {t[0] for t in planned}
    assert len(items) == 1000 + sum(len(o) - 1 for o in row_opts.values())
    assert 20 <= len(orphans) <= 80  # ~5% with seed tolerance
    assert _assign_mix(planned, seed=7, batch_date="2026-09-10")[1] == row_opts

    out = generate_settlement_file(
        seed=7, date_str="2026-09-10", planned=items, row_opts=row_opts, output_dir=str(tmp_path)
    )
    with open(out) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == len(items)
    by_tx: dict[str, list] = {}
    for r in rows:
        by_tx.setdefault(r["transaction_id"], []).append(r)
    assert any(len(v) == 2 for v in by_tx.values())  # dup/late extras written
    assert all(r["settlement_date"] >= "2026-09-07" for r in rows)  # ooo backdate bounded


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
