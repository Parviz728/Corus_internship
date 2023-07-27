import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG("parent_dag", description="parent_dag", schedule_interval="@once",
         start_date=datetime.datetime(2023, 7, 27),
         catchup=False) as dag:

    task_brand = BashOperator(task_id="clean_brand_task",
                             bash_command="python /opt/airflow/dags/scripts_and_files/cleaning_brand.py",
                            )

    task_category = BashOperator(task_id="clean_category_task",
                              bash_command="python /opt/airflow/dags/scripts_and_files/cleaning_category.py",
                              )

    task_product = BashOperator(task_id="clean_product_task",
                              bash_command="python /opt/airflow/dags/scripts_and_files/cleaning_product.py",
                              )

    task_stores = BashOperator(task_id="clean_stores_task",
                              bash_command="python /opt/airflow/dags/scripts_and_files/cleaning_store.py",
                              )

    task_stock = BashOperator(task_id="clean_stock_task",
                              bash_command="python /opt/airflow/dags/scripts_and_files/cleaning_stock.py",
                              )

    task_pos = BashOperator(task_id="clean_pos_task",
                              bash_command="python /opt/airflow/dags/scripts_and_files/cleaning_pos.py",
                              )

    task_transaction = BashOperator(task_id="clean_transaction_task",
                              bash_command="python /opt/airflow/dags/scripts_and_files/cleaning_transaction.py",
                              )

    trigger_child_dag = TriggerDagRunOperator(
        task_id='trigger_child_dag',
        trigger_dag_id='child_dag',
        reset_dag_run=True,
        wait_for_completion=True
    )

    task_brand >> task_category >> task_product >> task_stores >> task_stock >> task_pos >> task_transaction >> trigger_child_dag