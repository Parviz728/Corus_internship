import sys
import os
from dotenv import load_dotenv
import pandas as pd
sys.path.append('/opt/airflow/dags/scripts_and_files')

load_dotenv()

if __name__ == "__main__":
    df_stores = pd.read_csv("opt/airflow/dags/scripts_and_files/stores.csv", index_col=["pos", "pos_name"])
    df_stores.to_sql(schema='dds', name='stores', con=os.getenv("CLIENT_DB_URL"), if_exists='append')

