import pytest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag
import os


@pytest.fixture(scope="session")
def dagbag():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dag_folder = os.path.join(project_root, "dags")

    if "PROJECT_ROOT" not in os.environ:
        os.environ["PROJECT_ROOT"] = project_root

    # Mock DbtTaskGroup so DagBag can parse the DAG without shelling out to dbt.
    # The DAG integrity test checks Airflow structure (imports, cycles, tasks),
    # not whether dbt itself works. That belongs in dbt's own test suite.
    with patch("cosmos.DbtTaskGroup") as mock_dtg:
        mock_task_group = MagicMock()
        mock_dtg.return_value = mock_task_group
        return DagBag(dag_folder=dag_folder, include_examples=False)


def test_dagbag_no_import_errors(dagbag):
    """Verify that there are no import errors when loading the DAGs."""
    assert (
        len(dagbag.import_errors) == 0
    ), f"DAG import failures: {dagbag.import_errors}"


def test_reconciliation_dag_exists(dagbag):
    """Verify that the daily_tx_reconciliation DAG is successfully loaded."""
    dag_id = "daily_tx_reconciliation"
    assert dag_id in dagbag.dags

    dag = dagbag.dags[dag_id]

    assert not dag.test_cycle()
    assert len(dag.tasks) > 0
