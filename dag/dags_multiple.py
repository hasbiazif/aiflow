from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#fungsi setiap task
def start():
    print("🚀 Task nya dimulai")

def diproses():
    print("🔄 Data sedang diproses")

def selesai():
    print("✅ Task selesai")

#definisikan DAG
with DAG(
    dag_id='dags_multiple',
    start_date=datetime(2025, 6, 18),
    schedule_interval='@daily',
    catchup=False
) as dags:

    #definisikan task
    task_start = PythonOperator(
        task_id='task_start',
        python_callable=start
    )

    task_diproses = PythonOperator(
        task_id='task_diproses',
        python_callable=diproses
    )

    task_selesai = PythonOperator(
        task_id='task_selesai',
        python_callable=selesai
    )

#aturan urutan task
    task_start >> task_diproses >> task_selesai