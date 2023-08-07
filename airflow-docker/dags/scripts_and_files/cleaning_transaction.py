import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
from dev import Clean, Transaction

load_dotenv()
client_engine = create_engine(os.getenv("CLIENT_DB_URL"))

class Clean_transaction(Clean):

    def clean_and_load(self, df):
        no_duplicate_reader = self.clean_duplicate(df)
        error_batch, no_duplicate_reader = self.clean_empties(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_transaction")

        error_batch, no_duplicate_reader = self.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_transaction")

        for fk_key in self.table_name_object.FK_indexes:
            error_batch, no_duplicate_reader = self.make_fk_connection(no_duplicate_reader=no_duplicate_reader,
                                                                     fk_key=fk_key,
                                                                     connection_table=self.table_name_object.FK_indexes[fk_key][0],
                                                                     connection_attribute=self.table_name_object.FK_indexes[fk_key][1])
            self.send_to_error_table(error_batch=error_batch, table_name="error_transaction")

        self.fill_dds(cleaned_list_of_values=no_duplicate_reader, table_name="transaction")

if __name__ == "__main__":
    transaction_object = Transaction()
    clb = Clean_transaction(transaction_object)
    df_transaction = pd.read_sql('SELECT * FROM sources.transaction', con=client_engine)
    clb.clean_and_load(df_transaction)