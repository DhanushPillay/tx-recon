"""A rejected batch must not reach the readers of fact_reconciliation.

run_reconciliation used to refresh the mart before returning, so the order was:

    MERGE -> mart refresh -> [pipeline] drift gate -> [pipeline] SLO gate -> publish

WAP branched the webhook table and dropped the branch when a gate failed, which
left the webhook table untouched. The mart was not branched at all, so a batch
that failed its gate had already replaced the table every Trino card and
Metabase panel reads from. The branch was rolled back and the numbers stayed.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.pipeline import run_daily
from src.processing.reconcile import refresh_mart, run_reconciliation

pytestmark = pytest.mark.unit

MART_SQL = "CREATE OR REPLACE TABLE"


def _stmts(spark) -> list[str]:
    return [c[0][0] for c in spark.sql.call_args_list if c[0] and isinstance(c[0][0], str)]


def _patched(tmp_path, counts, demoted=0):
    spark = MagicMock()
    return spark, (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=str(tmp_path / "curated_settlement_20250402.csv"),
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch("src.processing.reconcile.run_reconciliation", return_value=dict(counts)),
        patch("src.processing.residual.apply_residual", return_value=demoted),
        patch("src.processing.reconcile.maintain_tables", return_value={}),
    )


def _green() -> dict:
    return {
        "settlement_rows_evaluated": 10,
        "missing_within_lag": 0,
        "batch_MATCHED": 10,
        "batch_EXCEPTION_FEE_MISMATCH": 0,
        "batch_match_rate": 1.0,
    }


def test_slo_breach_leaves_the_mart_alone(tmp_path):
    # Counts must tile, or check_batch_drift fires before the SLO gate and the
    # test would be asserting about the wrong gate.
    counts = {
        "settlement_rows_evaluated": 10,
        "missing_within_lag": 0,
        "batch_MATCHED": 5,
        "batch_EXCEPTION_FEE_MISMATCH": 5,
        "batch_match_rate": 0.5,
    }
    spark, patches = _patched(tmp_path, counts)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        pytest.raises(RuntimeError, match="publish gate failed"),
    ):
        run_daily(date_str="2025-04-02")
    assert not any(MART_SQL in s for s in _stmts(spark))


def test_drift_failure_leaves_the_mart_alone(tmp_path):
    counts = _green()
    counts["settlement_rows_evaluated"] = 10
    counts["batch_MATCHED"] = 4
    spark, patches = _patched(tmp_path, counts)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        pytest.raises(RuntimeError, match="Batch drift"),
    ):
        run_daily(date_str="2025-04-02")
    assert not any(MART_SQL in s for s in _stmts(spark))


def test_residual_failure_leaves_the_mart_alone(tmp_path, monkeypatch):
    monkeypatch.setenv("STRICT_SLO", "1")
    counts = _green()
    spark, patches = _patched(tmp_path, counts)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[4],
        patch(
            "src.processing.residual.apply_residual", side_effect=RuntimeError("scoring exploded")
        ),
        pytest.raises(RuntimeError, match="Residual pass failed"),
    ):
        run_daily(date_str="2025-04-02")
    assert not any(MART_SQL in s for s in _stmts(spark))


def test_green_batch_does_refresh_the_mart(tmp_path):
    """The control: the mart is refreshed, just not early."""
    spark, patches = _patched(tmp_path, _green())
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        run_daily(date_str="2025-04-02")
    assert any(MART_SQL in s for s in _stmts(spark))


def test_mart_refresh_happens_after_the_gates(tmp_path):
    """Ordering, not just presence: the refresh must follow the gate calls."""
    order: list[str] = []

    def _record(name, ret=None):
        def _fn(*a, **k):
            order.append(name)
            return ret

        return _fn

    spark, patches = _patched(tmp_path, _green())
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch("src.pipeline.check_batch_drift", side_effect=_record("drift")),
        patch("src.pipeline._slo_ok", side_effect=_record("slo", True)),
        patch("src.processing.reconcile.refresh_mart", side_effect=_record("mart")),
    ):
        run_daily(date_str="2025-04-02")
    assert order == ["drift", "slo", "mart"]


def test_run_reconciliation_alone_does_not_touch_the_mart():
    """Direct callers get counts, not a published artifact.

    run_reconciliation has no gate, so it must not decide what the mart says.
    """
    src = __import__("inspect").getsource(run_reconciliation)
    assert "CREATE OR REPLACE TABLE" not in src
    assert "_load_mart_sql" not in src


# --- refresh_mart's own contract ----------------------------------------


def test_refresh_mart_uses_the_qualified_source_and_mart_names():
    settings = MagicMock()
    settings.webhook_table = "nessie.db.webhooks"
    settings.project_root = "."
    settings.strict_slo = False
    spark = MagicMock()
    with patch("src.processing.reconcile._load_mart_sql", return_value=("MARKER", "V1")) as m:
        refresh_mart(spark, settings)
    sql = spark.sql.call_args[0][0]
    assert sql == "MARKER"
    assert m.call_args[0][1] == "nessie.db.webhooks"
    assert m.call_args[0][2] == "nessie.db.fact_reconciliation"


def test_refresh_mart_warns_and_continues_when_not_strict():
    settings = MagicMock()
    settings.webhook_table = "nessie.db.webhooks"
    settings.project_root = "."
    settings.strict_slo = False
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("nessie down")
    with patch("src.processing.reconcile._load_mart_sql", return_value=("MARKER", "V1")):
        refresh_mart(spark, settings)  # must not raise


def test_refresh_mart_raises_when_strict():
    """A stale mart under strict mode still fails the batch.

    The refresh moved later, but it did not get softer.
    """
    settings = MagicMock()
    settings.webhook_table = "nessie.db.webhooks"
    settings.project_root = "."
    settings.strict_slo = True
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("nessie down")
    with (
        patch("src.processing.reconcile._load_mart_sql", return_value=("MARKER", "V1")),
        pytest.raises(RuntimeError, match="Could not refresh fact_reconciliation"),
    ):
        refresh_mart(spark, settings)


def test_refresh_mart_names_the_glue_mart_when_the_prefix_is_glue():
    settings = MagicMock()
    settings.webhook_table = "glue.db.webhooks"
    settings.project_root = "."
    settings.strict_slo = False
    spark = MagicMock()
    with patch("src.processing.reconcile._load_mart_sql", return_value=("MARKER", "V1")) as m:
        refresh_mart(spark, settings)
    assert m.call_args[0][2] == "glue.db.fact_reconciliation"
