import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow")
sys.path.insert(0, PROJECT_ROOT)


def failure_callback(context):
    logger.error(
        f"Task {context.get('task_id')} failed in DAG {context.get('dag_id')}: {context.get('exception')}"
    )


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
}


def generate_settlement_task():
    from src.generators.settlement_generator import generate_settlement_file

    generate_settlement_file(500)


def validate_settlement_task():
    from src.validation.validate_settlement import validate_latest_settlement

    validate_latest_settlement()


def run_reconciliation_task():
    from src.processing.reconcile import run_reconciliation

    run_reconciliation()


with DAG(
    "daily_tx_reconciliation",
    default_args=default_args,
    description="Daily reconciliation of payment gateway webhooks against bank settlements",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["finance", "reconciliation"],
    sla_miss_callback=failure_callback,
) as dag:

    generate_settlement = PythonOperator(
        task_id="generate_bank_settlement",
        python_callable=generate_settlement_task,
    )

    validate_settlement_op = PythonOperator(
        task_id="validate_settlement",
        python_callable=validate_settlement_task,
    )

    run_reconciliation_op = PythonOperator(
        task_id="run_pyspark_reconciliation",
        python_callable=run_reconciliation_task,
    )

    (generate_settlement >> validate_settlement_op >> run_reconciliation_op)
