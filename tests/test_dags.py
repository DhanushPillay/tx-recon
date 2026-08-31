import pytest
from airflow.models import DagBag
import os


@pytest.fixture(scope="session")
def dagbag():
    # Use the local dags/ folder
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dag_folder = os.path.join(project_root, "dags")

    # Ensure PROJECT_ROOT is set so that Cosmos can resolve paths during DAG parsing
    if "PROJECT_ROOT" not in os.environ:
        os.environ["PROJECT_ROOT"] = project_root

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

    # Assert there are no cyclic dependencies (Airflow checks this inherently, but this confirms topological sort works)
    assert not dag.test_cycle()

    # Ensure there's a structure (some tasks exist)
    assert len(dag.tasks) > 0
