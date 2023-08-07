import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
from dev import Clean, Category

load_dotenv()
client_engine = create_engine(os.getenv("CLIENT_DB_URL"))
class Clean_category(Clean):

    def clean_and_load(self, df):
        no_duplicate_reader = self.clean_duplicate(df)

        error_batch, no_duplicate_reader = self.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_category")

        self.fill_dds(cleaned_list_of_values=no_duplicate_reader, table_name="category")

if __name__ == "__main__":
    category_object = Category()
    clb = Clean_category(category_object)
    df_category = pd.read_sql('SELECT * FROM sources.category', con=client_engine)
    clb.clean_and_load(df_category)