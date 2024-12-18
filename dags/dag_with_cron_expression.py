# dag_with_catchup_and_backfill.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'tun',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id = '3_dag_with_catchup_backfill_v01',
    default_args = default_args,
    start_date = datetime(2024,12,11),
    schedule_interval = '0 0 * * *',
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = 'echo This is a simple bash command!'
    )


