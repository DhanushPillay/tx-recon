import os
import sys
from unittest.mock import MagicMock, patch

# Pre-mock cosmos so DagBag can load the DAG
cosmos_mock = MagicMock()
sys.modules["cosmos"] = cosmos_mock
sys.modules["astronomer_cosmos"] = cosmos_mock

import pytest
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle


@pytest.fixture(scope="session")
def dagbag():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dag_folder = os.path.join(project_root, "dags")

    if "PROJECT_ROOT" not in os.environ:
        os.environ["PROJECT_ROOT"] = project_root

    with patch("cosmos.DbtTaskGroup") as mock_dtg:
        mock_task_group = MagicMock()
        mock_dtg.return_value = mock_task_group
        return DagBag(dag_folder=dag_folder, include_examples=False)


def test_dagbag_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"DAG import failures: {dagbag.import_errors}"


def test_reconciliation_dag_exists(dagbag):
    dag_id = "daily_tx_reconciliation"
    assert dag_id in dagbag.dags

    dag = dagbag.dags[dag_id]
    check_cycle(dag)
    assert len(dag.tasks) > 0


def test_dag_task_ordering(dagbag):
    dag = dagbag.dags["daily_tx_reconciliation"]

    task_ids = [t.task_id for t in dag.tasks]
    assert "generate_bank_settlement" in task_ids
    assert "validate_settlement" in task_ids
    assert "run_pyspark_reconciliation" in task_ids

    gen = dag.get_task("generate_bank_settlement")
    val = dag.get_task("validate_settlement")
    recon = dag.get_task("run_pyspark_reconciliation")

    assert val.task_id in [t.task_id for t in gen.downstream_list]
    assert recon.task_id in [t.task_id for t in val.downstream_list]


def test_dag_default_args(dagbag):
    dag = dagbag.dags["daily_tx_reconciliation"]
    assert dag.default_args["retries"] == 3
    assert dag.catchup is False
    assert dag.schedule_interval == "@daily"
