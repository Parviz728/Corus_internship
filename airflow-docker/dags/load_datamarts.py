import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG("load_marts", description="load_marts", schedule_interval="@once",
         start_date=datetime.datetime(2023, 7, 25),
         catchup=False) as dag:
    task_marts1 = BashOperator(task_id="load_marts1_task",
                             bash_command="python /opt/airflow/dags/scripts_for_datamarts/create_filled_datamarts_marts1.py",
                            )

    task_marts2 = BashOperator(task_id="load_marts2_task",
                              bash_command="python /opt/airflow/dags/scripts_for_datamarts/create_filled_datamarts_marts2.py",
                              )

    task_marts1 >> task_marts2
