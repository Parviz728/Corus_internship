import os
from dotenv import load_dotenv
import pandas as pd
from datetime import date
from dev import stock_table, Clean

load_dotenv()
class Clean_stock(Clean):
    def __init__(self, table_name):
        self.table_name = table_name

    def clean_and_load(self, df):
        no_duplicate_reader = self.clean_duplicate(df)
        i = 0
        n = len(no_duplicate_reader)
        while i < n:
            excel_date = int(no_duplicate_reader[i][0])
            dt = date.fromordinal(date(1900, 1, 1).toordinal() + excel_date - 2)
            no_duplicate_reader[i][0] = dt
            i += 1

        error_batch, no_duplicate_reader = self.clean_empties(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_stock")

        error_batch, no_duplicate_reader = self.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_stock")

        for fk_key in self.table_name.FK_indexes:
            error_batch, no_duplicate_reader = self.make_fk_connection(no_duplicate_reader=no_duplicate_reader,
                                                                     fk_key=fk_key,
                                                                     connection_table=self.table_name.FK_indexes[fk_key][0],
                                                                     connection_attribute=self.table_name.FK_indexes[fk_key][1])
            self.send_to_error_table(error_batch=error_batch, table_name="error_stock")

        self.fill_dds(cleaned_list_of_values=no_duplicate_reader, table_name="stock")

if __name__ == "__main__":
    clb = Clean_stock(stock_table)
    df_stock = pd.read_sql('SELECT * FROM sources.stock', con=os.getenv("CLIENT_DB_URL"))
    clb.clean_and_load(df_stock)