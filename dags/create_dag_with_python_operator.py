

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner':'tun',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def greet(name,age):
    print(f"Hello World! My name is {name}, and I am {age} years old!")

def get_name():
    return "Jurry"


with DAG(
    default_args = default_args,
    dag_id = '0_our_dag_with_python_operator_v03',
    description = 'Our first dag using python operator',
    start_date = datetime(2024,12,6),
    schedule_interval = '@daily'
) as dag:
    # task1 = PythonOperator(
    #     task_id = 'greet',
    #     python_callable = greet,
    #     op_kwargs = {'name':'Tom','age':20}
    # )
    # task1

    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable = get_name
    )
    task2