import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from valid_data import cleaning_brand

with DAG("load_brand", description="load_brand", schedule_interval="@once",
         start_date=datetime.datetime(2023, 7, 21),
         catchup=False) as dag:
    main_task = PythonOperator(task_id="main_task", python_callable=cleaning_brand)
    main_task