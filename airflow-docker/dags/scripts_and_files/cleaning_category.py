import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
from dev import category_table, Clean

load_dotenv()
class Clean_category(Clean):

    def __init__(self, table_name):
        self.table_name = table_name

    def clean_and_load(self, df):
        no_duplicate_reader = self.clean_duplicate(df)

        error_batch, no_duplicate_reader = self.clean_pk_duplicates(no_duplicate_reader)
        df_error = pd.DataFrame(error_batch)
        df_error.to_sql(schema="dds", name="error_category", con=create_engine(os.getenv("DB_URL")), if_exists="append")

        df = pd.DataFrame(no_duplicate_reader)
        df.to_sql(schema="dds", name="category", con=create_engine(os.getenv("DB_URL")), if_exists="append")

if __name__ == "__main__":
    clb = Clean_category(category_table)
    df_category = pd.read_sql('SELECT * FROM sources.category', con=os.getenv("CLIENT_DB_URL"))
    clb.clean_and_load(df_category)