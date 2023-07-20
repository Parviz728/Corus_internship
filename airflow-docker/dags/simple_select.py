import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def run_simple_sql_query():
    request = "SELECT pos from sources.stock"
    pg_hook = PostgresHook(postgres_conn_id="customers_conn", schema="internship_sources")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    return sources

with DAG("simple_select", description="simple_select", schedule_interval="@once",
         start_date=datetime.datetime(2023, 7, 13),
         catchup=False) as dag:
    start_task = DummyOperator(task_id="start_task")
    hook_task = PythonOperator(task_id="hook_task", python_callable=run_simple_sql_query)
    start_task >> hook_task