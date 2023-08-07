import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from dev import Clean, Brand

load_dotenv()
client_engine = create_engine(os.getenv("CLIENT_DB_URL"))
class Clean_brand(Clean):

# функция очистки
    def clean_and_load(self, df):
        no_duplicate_reader = self.clean_duplicate(df)
        i = 0
        n = len(no_duplicate_reader)
        while i < n:
            # brand_id = no_duplicate_reader[i][0]
            # brand = no_duplicate_reader[i][1]
            try:
                no_duplicate_reader[i][0] = int(no_duplicate_reader[i][0])
            except:
                no_duplicate_reader[i][0], no_duplicate_reader[i][1] = int(no_duplicate_reader[i][1]), no_duplicate_reader[i][0]

            i += 1

        error_batch, no_duplicate_reader = self.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_brand")

        self.fill_dds(cleaned_list_of_values=no_duplicate_reader, table_name="brand")

if __name__ == "__main__":
    brand_object = Brand()
    clb = Clean_brand(brand_object)
    df_brand = pd.read_sql('SELECT * FROM sources.brand', con=client_engine)
    clb.clean_and_load(df_brand)