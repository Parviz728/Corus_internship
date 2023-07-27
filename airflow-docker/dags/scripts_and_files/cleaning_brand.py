import psycopg2
import psycopg2.extras as extras
import pandas as pd
from dev import brand_table, conn, conn_client, Clean

cur_client = conn_client.cursor()

sql = "COPY (SELECT * FROM sources.brand) TO STDOUT WITH CSV DELIMITER ','"
with open("brand.csv", "w", encoding="UTF-8") as file:
    cur_client.copy_expert(sql, file)

cl = Clean(brand_table, "brand.csv")

class Clean_brand:

    @staticmethod
    def send_to_error_table(lst):

        cur = conn.cursor()
        extras.execute_values(cur,
                              "INSERT INTO dds.error_brand (brand_id, brand, detail_info) VALUES %s",
                              lst)
        conn.commit()

    def clean(self):
        no_duplicate_reader = cl.clean_duplicate()
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

        error_batch, no_duplicate_reader = cl.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch)

        df = pd.DataFrame(no_duplicate_reader)
        df.to_csv('cleaned_brand.csv', index=False, header=False)

def execute_brand(conn, df):
    tuples = [tuple(x) for x in df.to_numpy()]
    # SQL query to execute
    query = "INSERT INTO dds.brand (brand_id, brand) VALUES %s"
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

clb = Clean_brand()
clb.clean()

# this two commands run ONLY after clb.clean, because class Clean_brand creates a cleaned csv file: cleaned_brand.csv
if __name__ == "__main__":
    df = pd.read_csv('cleaned_brand.csv')
    execute_brand(conn, df)