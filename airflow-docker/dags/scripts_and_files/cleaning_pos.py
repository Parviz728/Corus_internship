import sys
from dotenv import load_dotenv
import pandas as pd
sys.path.append('/opt/airflow/dags/scripts_and_files')
from dev import pos_table, Clean

load_dotenv()
class Clean_pos(Clean):

    def __init__(self, table_name):
        self.table_name = table_name

    def clean_and_load(self, df):
        no_duplicate_reader = self.clean_duplicate(df)
        for i in range(len(no_duplicate_reader)):
            no_duplicate_reader[i] = no_duplicate_reader[i][0].split(';')

        error_batch, no_duplicate_reader = self.clean_empties(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_pos")

        error_batch, no_duplicate_reader = self.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch=error_batch, table_name="error_pos")

        for fk_key in self.table_name.FK_indexes:
            error_batch, no_duplicate_reader = self.make_fk_connection(no_duplicate_reader=no_duplicate_reader,
                                                                     fk_key=fk_key,
                                                                     connection_table=self.table_name.FK_indexes[fk_key][0],
                                                                     connection_attribute=self.table_name.FK_indexes[fk_key][1])
            self.send_to_error_table(error_batch=error_batch, table_name="error_pos")

        self.fill_dds(cleaned_list_of_values=no_duplicate_reader, table_name="pos")

if __name__ == "__main__":
    clb = Clean_pos(pos_table)
    df_pos = pd.read_csv("opt/airflow/dags/scripts_and_files/pos.csv", index_col=["transaction_id", "pos"])
    clb.clean_and_load(df_pos)
