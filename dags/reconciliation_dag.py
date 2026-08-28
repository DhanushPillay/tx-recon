from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_tx_reconciliation',
    default_args=default_args,
    description='Daily reconciliation of payment gateway webhooks against bank settlements',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance', 'reconciliation'],
) as dag:

    # Step 1: Generate Mock Settlement Data (Simulating a file landing in S3/SFTP)
    generate_settlement = BashOperator(
        task_id='generate_bank_settlement',
        bash_command='pip install pyspark==3.5.1 chispa==0.9.2 && python /opt/airflow/src/generators/settlement_generator.py',
    )

    # Step 2: Run the PySpark Batch Reconciliation Job
    run_reconciliation_job = BashOperator(
        task_id='run_pyspark_reconciliation',
        bash_command='python /opt/airflow/src/reconcile.py',
    )

    generate_settlement >> run_reconciliation_job
