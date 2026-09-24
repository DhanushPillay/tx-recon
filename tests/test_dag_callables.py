"""DAG task callables with mocked src."""

from unittest.mock import patch

import pytest

from dags.reconciliation_dag import (
    failure_callback,
    generate_settlement_task,
    run_reconciliation_task,
    validate_settlement_task,
)

pytestmark = pytest.mark.unit


def test_failure_callback_logs(caplog):
    with caplog.at_level("ERROR"):
        failure_callback({"task_id": "t", "dag_id": "d", "exception": "boom"})


def test_task_callables():
    with patch("src.validation.validate_settlement.validate_latest_settlement") as v:
        validate_settlement_task("2025-04-02")
        v.assert_called_once()
    with (
        patch("src.processing.reconcile.run_reconciliation") as r,
        patch("src.processing.reconcile.maintain_tables", return_value={}),
    ):
        run_reconciliation_task("2025-04-02")
        r.assert_called_once()


def test_generate_task_uses_real_file(tmp_path, monkeypatch):
    """Real PG drop present: sensed, generator untouched."""
    d = tmp_path / "data"
    d.mkdir()
    (d / "settlement_20250402.csv").write_text("bank_ref_id\nb1\n")
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    monkeypatch.delenv("GENERATE_DEMO_SETTLEMENT", raising=False)
    with patch("src.generators.settlement_generator.generate_settlement_file") as g:
        out = generate_settlement_task("2025-04-02")
    g.assert_not_called()
    assert out.endswith("settlement_20250402.csv")


def test_generate_task_refuses_to_fabricate(tmp_path, monkeypatch):
    """No PG drop, no opt-in: fail closed instead of merging fiction."""
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    monkeypatch.delenv("GENERATE_DEMO_SETTLEMENT", raising=False)
    with (
        patch("src.generators.settlement_generator.generate_settlement_file") as g,
        pytest.raises(FileNotFoundError, match="refusing to fabricate"),
    ):
        generate_settlement_task("2025-04-02")
    g.assert_not_called()


def test_generate_task_opt_in_synthesis(tmp_path, monkeypatch):
    """Explicit local-dev opt-in still synthesizes."""
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("GENERATE_DEMO_SETTLEMENT", "1")
    with patch(
        "src.generators.settlement_generator.generate_settlement_file", return_value="f"
    ) as g:
        assert generate_settlement_task("2025-04-02") == "f"
    assert g.call_args.kwargs["seed"] == 20250402
