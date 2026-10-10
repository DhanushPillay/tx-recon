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
    with patch("src.pipeline.run_daily") as r:
        r.return_value = {"settlement_rows_evaluated": 0, "settlement_rows_deduped": 0}
        run_reconciliation_task("2025-04-02")
        r.assert_called_once_with(date_str="2025-04-02")


def test_generate_task_uses_real_file(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    (d / "settlement_20250402.csv").write_text("bank_ref_id\nb1\n")
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    out = generate_settlement_task("2025-04-02")
    assert out.endswith("settlement_20250402.csv")


def test_generate_task_refuses_to_fabricate(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    with pytest.raises(FileNotFoundError, match="refusing to fabricate"):
        generate_settlement_task("2025-04-02")
