import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from valid_data import cleaning_pos

with DAG("load_pos", description="load_pos", schedule_interval="@once",
         start_date=datetime.datetime(2023, 7, 21),
         catchup=False) as dag:
    main_task = PythonOperator(task_id="main_task", python_callable=cleaning_pos)
    main_task