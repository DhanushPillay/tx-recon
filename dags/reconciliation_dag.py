import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow")


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

    generate_settlement = BashOperator(
        task_id="generate_bank_settlement",
        bash_command=f"pip install pyspark==3.5.1 pandas && python {PROJECT_ROOT}/src/generators/settlement_generator.py",
    )

    validate_settlement_task = BashOperator(
        task_id="validate_settlement",
        bash_command=f"python {PROJECT_ROOT}/src/validation/validate_settlement.py",
    )

    run_reconciliation_job = BashOperator(
        task_id="run_pyspark_reconciliation",
        bash_command=f"python {PROJECT_ROOT}/src/processing/reconcile.py",
    )

    dbt_gold_layer = DbtTaskGroup(
        group_id="dbt_gold_layer",
        project_config=ProjectConfig(f"{PROJECT_ROOT}/dbt_recon"),
        profile_config=ProfileConfig(
            profile_name="dbt_recon",
            target_name="dev",
            profiles_yml_filepath=f"{PROJECT_ROOT}/dbt_recon/profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt"),
    )

    (
        generate_settlement
        >> validate_settlement_task
        >> run_reconciliation_job
        >> dbt_gold_layer
    )
