from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello, ini dags awal saya")

with DAG(
    dag_id='dags_awal',
    start_date=datetime(2025, 6, 18),
    schedule_interval='@daily',
    catchup=False
) as dags:

    task_hello = PythonOperator(
        task_id='task_hello',
        python_callable=hello  
    )