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
    with patch("src.generators.settlement_generator.generate_settlement_file") as g:
        generate_settlement_task("2025-04-02")
        assert g.call_args.kwargs["seed"] == 20250402
    with patch("src.validation.validate_settlement.validate_latest_settlement") as v:
        validate_settlement_task("2025-04-02")
        v.assert_called_once()
    with patch("src.processing.reconcile.run_reconciliation") as r:
        run_reconciliation_task("2025-04-02")
        r.assert_called_once()
