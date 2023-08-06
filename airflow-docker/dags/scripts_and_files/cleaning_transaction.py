import os
from dotenv import load_dotenv
import pandas as pd
from dev import transaction_table, Clean

load_dotenv()

class Clean_transaction(Clean):

    def __init__(self, table_name):
        self.table_name = table_name
    def clean_and_load(self, df):
        no_duplicate_reader = self.clean_duplicate(df)
        error_batch, no_duplicate_reader = self.clean_empties(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_transaction")

        error_batch, no_duplicate_reader = self.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_transaction")

        for fk_key in self.table_name.FK_indexes:
            error_batch, no_duplicate_reader = self.make_fk_connection(no_duplicate_reader=no_duplicate_reader,
                                                                     fk_key=fk_key,
                                                                     connection_table=self.table_name.FK_indexes[fk_key][0],
                                                                     connection_attribute=self.table_name.FK_indexes[fk_key][1])
            self.send_to_error_table(error_batch=error_batch, table_name="error_transaction")

        self.fill_dds(cleaned_list_of_values=no_duplicate_reader, table_name="transaction")

if __name__ == "__main__":
    clb = Clean_transaction(transaction_table)
    df_transaction = pd.read_sql('SELECT * FROM sources.transaction', con=os.getenv("CLIENT_DB_URL"))
    clb.clean_and_load(df_transaction)