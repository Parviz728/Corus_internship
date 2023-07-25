import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG("load_brand", description="load_brand", schedule_interval="@once",
         start_date=datetime.datetime(2023, 7, 24),
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
    task_brand >> task_category >> task_product >> task_stores >> task_stock >> task_pos >> task_transaction