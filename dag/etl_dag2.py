from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

default_args = {
    'start_date': datetime(2025, 6, 19),
    'catchup': False
}

def extract_data(**kwargs):
    hook = PostgresHook(postgres_conn_id='dblocal')
    df = hook.get_pandas_df("SELECT peg_nip, peg_nama, jabatan_nama FROM v_pegawai_data LIMIT 10;")
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    print("✅ Data extracted:\n", df)

def transform_data(**kwargs):
    import pandas as pd
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(task_ids='extract', key='raw_data')
    df = pd.read_json(raw_json)
    df['peg_nama'] = df['peg_nama'].str.upper()
    kwargs['ti'].xcom_push(key='clean_data', value=df.to_json())
    print("🧼 Data transformed:\n", df)

def load_data(**kwargs):
    import pandas as pd
    ti = kwargs['ti']
    clean_json = ti.xcom_pull(task_ids='transform', key='clean_data')
    df = pd.read_json(clean_json)
    path = '/opt/airflow/include/cleaned_data.csv'
    df.to_csv(path, index=False)
    print(f"📁 Data saved to: {path}")

with DAG(
    dag_id='simple_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',
    tags=['etl'],
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True
    )

    t1 >> t2 >> t3
