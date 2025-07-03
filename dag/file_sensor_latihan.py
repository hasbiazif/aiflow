from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
import pandas as pd

default_args = {
    'email': ['hasbiazif13@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def read_excel():
    path = '/opt/airflow/include/pegawai_sensor.xlsx'
    df = pd.read_excel(path, engine='openpyxl')
    print("📊 Data dari Excel:\n", df.head())
    print(df)

with DAG(
    dag_id='file_sensor_latihan',
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['file_sensor']
) as dag:
    
    wait_for_file = FileSensor(
        task_id='wait_for_excel',
        filepath='/opt/airflow/include/pegawai_sensor.xlsx',
        poke_interval=30,    # cek tiap 30 detik
        timeout=300,         # maksimal 5 menit (300 detik)
        mode='poke'          # langsung nunggu aktif
    )

    read_task = PythonOperator(
        task_id='read_excel',
        python_callable=read_excel,
        retries=3, # ulangi 3 kali jika gagal
        retry_delay=timedelta(minutes=1),  # jeda 1 menit antar percobaan
        retry_exponential_backoff=True,  # gunakan exponential backoff
        sla=timedelta(minutes=5)  # SLA 10 menit untuk task ini
    )

    wait_for_file >> read_task