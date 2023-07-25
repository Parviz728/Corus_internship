import psycopg2
import psycopg2.extras as extras
import pandas as pd
from dev import transaction_table, conn, conn_client, Clean

cur_client = conn_client.cursor()
sql = "COPY (SELECT * FROM sources.transaction) TO STDOUT WITH CSV DELIMITER ';'"
with open("transaction.csv", "w", encoding="UTF-8") as file:
    cur_client.copy_expert(sql, file)

cl = Clean(transaction_table, "/opt/airflow/dags/scripts_and_files/transaction.csv")

class Clean_transaction:

    @staticmethod
    def send_to_error_table(lst):
        cur = conn.cursor()
        extras.execute_values(cur, "INSERT INTO dds.error_transaction ("
                                   "transaction_id, "
                                   "product_id, "
                                   "recorded_on, "
                                   "quantity, "
                                   "price, "
                                   "price_full, "
                                   "order_type_id, "
                                   "detail_info) VALUES %s",
                              lst)
        conn.commit()

    def clean(self):
        no_duplicate_reader = cl.clean_duplicate()
        error_batch, no_duplicate_reader = cl.clean_empties(no_duplicate_reader)
        self.send_to_error_table(error_batch)

        error_batch, no_duplicate_reader = cl.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch)

        i = 0
        n = len(no_duplicate_reader)
        cur = conn.cursor()
        cur.execute("SELECT product_id from dds.product")
        product_ids = set(cur.fetchall())
        error_batch = []
        while i < n:
            product_id = no_duplicate_reader[i][1]
            if (int(product_id),) not in product_ids:
                no_duplicate_reader[i].append("Ошибка ссылочной целостности")
                error_batch.append(no_duplicate_reader[i])
                del no_duplicate_reader[i]
                i -= 1
                n -= 1
            i += 1
        self.send_to_error_table(error_batch)

        i = 0
        n = len(no_duplicate_reader)
        cur = conn.cursor()
        cur.execute(f"SELECT transaction_id from dds.pos")
        transaction_ids = set(cur.fetchall())
        error_batch = []
        while i < n:
            transaction_id = no_duplicate_reader[i][0]
            if (transaction_id,) not in transaction_ids:
                no_duplicate_reader[i].append("Ошибка ссылочной целостности")
                error_batch.append(no_duplicate_reader[i])
                del no_duplicate_reader[i]
                i -= 1
                n -= 1
            i += 1
        self.send_to_error_table(error_batch)

        df = pd.DataFrame(no_duplicate_reader)
        df.to_csv('cleaned_transaction.csv', index=False, header=False)

def execute_transaction(conn, df):
    tuples = [tuple(x) for x in df.to_numpy()]
    # SQL query to execute
    query = "INSERT INTO dds.transaction (transaction_id, " \
            "product_id, " \
            "recorded_on, " \
            "quantity, " \
            "price, " \
            "price_full, " \
            "order_type_id ) VALUES %s"
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

clb = Clean_transaction()
clb.clean()

# this two commands run ONLY after clb.clean, because class Clean_brand creates a cleaned csv file: cleaned_brand.csv
if __name__ == "__main__":
    df = pd.read_csv('cleaned_transaction.csv')
    execute_transaction(conn, df)
