import psycopg2
import psycopg2.extras as extras
import pandas as pd
from datetime import date
from dev import stock_table, conn, conn_client, Clean

cur_client = conn_client.cursor()

sql = "COPY (SELECT * FROM sources.stock) TO STDOUT WITH CSV DELIMITER ','"
with open("stock.csv", "w", encoding="UTF-8") as file:
    cur_client.copy_expert(sql, file)

cl = Clean(stock_table, "stock.csv")

class Clean_stock:

    @staticmethod
    def send_to_error_table(lst):
        cur = conn.cursor()
        extras.execute_values(cur,
                              "INSERT INTO dds.error_stock (available_on, product_id, pos, available_quantity, cost_per_item, detail_info) VALUES %s",
                              lst)
        conn.commit()

    def clean(self):
        no_duplicate_reader = cl.clean_duplicate()
        i = 0
        n = len(no_duplicate_reader)
        while i < n:
            excel_date = int(no_duplicate_reader[i][0])
            dt = date.fromordinal(date(1900, 1, 1).toordinal() + excel_date - 2)
            no_duplicate_reader[i][0] = dt
            i += 1

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
            print(f"fk = {fk_key} -> {len(no_duplicate_reader)}")
        df = pd.DataFrame(no_duplicate_reader)
        df.to_csv('cleaned_stock.csv', index=False, header=False)

def execute_stock(conn, df):
    tuples = [tuple(x) for x in df.to_numpy()]
    # SQL query to execute
    query = "INSERT INTO dds.stock (available_on, product_id, pos, available_quantity, cost_per_item) VALUES %s"
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

clb = Clean_stock()
clb.clean()

# this two commands run ONLY after clb.clean, because class Clean_brand creates a cleaned csv file: cleaned_brand.csv
if __name__ == "__main__":
    df = pd.read_csv('cleaned_stock.csv')
    execute_stock(conn, df)
