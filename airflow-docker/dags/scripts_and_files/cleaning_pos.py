import psycopg2
import psycopg2.extras as extras
import pandas as pd
from dev import pos_table, conn, Clean

cl = Clean(pos_table, "/opt/airflow/dags/scripts_and_files/pos.csv")

class Clean_pos:

    @staticmethod
    def send_to_error_table(lst):

        cur = conn.cursor()
        extras.execute_values(cur,
                              "INSERT INTO dds.error_pos (transaction_id, pos, detail_info) VALUES %s",
                              lst)
        conn.commit()

    def clean(self):
        no_duplicate_reader = cl.clean_duplicate()
        for i in range(len(no_duplicate_reader)):
            no_duplicate_reader[i] = no_duplicate_reader[i][0].split(';')

        error_batch, no_duplicate_reader = cl.clean_empties(no_duplicate_reader)
        self.send_to_error_table(error_batch)

        error_batch, no_duplicate_reader = cl.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch)

        for fk_key in cl.table_name.FK_indexes:
            error_batch, no_duplicate_reader = cl.make_fk_connection(no_duplicate_reader=no_duplicate_reader,
                                                                     fk_key=fk_key,
                                                                     connection_table=cl.table_name.FK_indexes[fk_key][0],
                                                                     connection_attribute=cl.table_name.FK_indexes[fk_key][1])
            self.send_to_error_table(error_batch)

        df = pd.DataFrame(no_duplicate_reader)
        df.to_csv('cleaned_pos.csv', index=False, header=False)

def execute_pos(conn, df):
    tuples = [tuple(x) for x in df.to_numpy()]
    # SQL query to execute
    query = "INSERT INTO dds.pos (transaction_id, pos) VALUES %s"
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()

clb = Clean_pos()
clb.clean()

# this two commands run ONLY after clb.clean, because class Clean_brand creates a cleaned csv file: cleaned_brand.csv
if __name__ == "__main__":
    df = pd.read_csv('cleaned_pos.csv', encoding='utf-8')
    execute_pos(conn, df)