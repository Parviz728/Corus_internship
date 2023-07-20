import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from valid_data import cleaning_category

with DAG("load_category", description="load_category", schedule_interval="@once",
         start_date=datetime.datetime(2023, 7, 21),
         catchup=False) as dag:
    main_task = PythonOperator(task_id="main_task", python_callable=cleaning_category)
    main_task