import psycopg2
import psycopg2.extras as extras
import pandas as pd
from dev import pos_table, conn, Clean

cl = Clean(pos_table, "pos.csv")

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

        i = 0
        n = len(no_duplicate_reader)
        cur = conn.cursor()
        cur.execute(f"SELECT pos from dds.stores")
        poss = set(cur.fetchall())
        error_batch = []
        while i < n:
            pos = no_duplicate_reader[i][1]
            if (pos,) not in poss:
                no_duplicate_reader[i].append("Ошибка ссылочной целостности")
                error_batch.append(no_duplicate_reader[i])
                del no_duplicate_reader[i]
                i -= 1
                n -= 1
            i += 1
        self.send_to_error_table(error_batch)

        df = pd.DataFrame(no_duplicate_reader)
        df.to_csv('cleaned_pos.csv', index=False, header=False)

def execute(conn, df):
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
    execute(conn, df)
