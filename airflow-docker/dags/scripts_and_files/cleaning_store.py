import sys
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
sys.path.append('/opt/airflow/dags/scripts_and_files')

load_dotenv()
client_engine = create_engine(os.getenv("CLIENT_DB_URL"))

if __name__ == "__main__":
    df_stores = pd.read_csv("opt/airflow/dags/scripts_and_files/stores.csv", index_col=["pos", "pos_name"])
    df_stores.to_sql(schema='dds', name='stores', con=client_engine, if_exists='append')

