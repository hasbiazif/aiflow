from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

def extract_data(**kwargs):
    hook = PostgresHook(postgres_conn_id='dblocal')
    df = hook.get_pandas_df("SELECT peg_nip, peg_nama, jabatan_nama FROM v_pegawai_data LIMIT 10;")
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())

def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(task_ids='extract', key='raw_data')
    df = pd.read_json(raw_json)
    df['peg_nama'] = df['peg_nama'].str.upper()
    kwargs['ti'].xcom_push(key='clean_data', value=df.to_json())

def load_data(**kwargs):
    from sqlalchemy import create_engine
    import pandas as pd
    ti = kwargs['ti']
    clean_json = ti.xcom_pull(task_ids='transform', key='clean_data')
    df = pd.read_json(clean_json)
    db_url = 'postgresql://postgres:hasbi1995@host.docker.internal:5432/data_enginering'
    engine = create_engine(db_url)
    df.to_sql('pegawai_cleaned', engine, index=False, if_exists='replace')

default_args = {'start_date': datetime(2025, 6, 22), 'catchup': False}
with DAG('etl_to_postgres', default_args=default_args, schedule_interval='@daily', tags=['etl']) as dag:
    t1 = PythonOperator(task_id='extract', python_callable=extract_data, provide_context=True)
    t2 = PythonOperator(task_id='transform', python_callable=transform_data, provide_context=True)
    t3 = PythonOperator(task_id='load', python_callable=load_data, provide_context=True)

    t1 >> t2 >> t3