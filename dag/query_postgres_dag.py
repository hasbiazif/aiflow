from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='postgres_query_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['postgres']
) as dag:

    query_postgres = PostgresOperator(
        task_id='cek_versi_postgres',
        postgres_conn_id='dblocal',
        sql='SELECT version();'
    )
