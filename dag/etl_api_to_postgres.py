from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
import json

default_args = {
    'start_date': datetime(2025, 6, 26),
    'catchup': False
}

def extract_from_api(**kwargs):
    url = 'https://jsonplaceholder.typicode.com/users'
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    kwargs['ti'].xcom_push(key='api_raw', value=df.to_json())
    print("✅ Extracted from API:\n", df[['id', 'name', 'email']])

def transform_api_data(**kwargs):
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(task_ids='extract_api', key='api_raw')
    df = pd.read_json(raw_json)

    df['name'] = df['name'].str.upper()

    # Ubah kolom dict menjadi string
    df['address'] = df['address'].apply(json.dumps)
    df['company'] = df['company'].apply(json.dumps)

    kwargs['ti'].xcom_push(key='api_transformed', value=df.to_json())
    print("🧼 Transformed API data:\n", df[['id', 'name', 'email']])

def load_api_to_postgres(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='transform_api', key='api_transformed')
    df = pd.read_json(json_data)

    db_url = 'postgresql://postgres:hasbi1995@host.docker.internal:5432/data_enginering'
    engine = create_engine(db_url)

    df.to_sql('users_api', engine, index=False, if_exists='replace')
    print("📥 Loaded API data to table `users_api`")

with DAG('etl_api_to_postgres', default_args=default_args, schedule_interval='@once', tags=['api']) as dag:
    extract = PythonOperator(
        task_id='extract_api',
        python_callable=extract_from_api,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform_api',
        python_callable=transform_api_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_api',
        python_callable=load_api_to_postgres,
        provide_context=True
    )

    extract >> transform >> load
