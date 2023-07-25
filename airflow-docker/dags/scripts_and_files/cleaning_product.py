import psycopg2
import psycopg2.extras as extras
import pandas as pd
from dev import product_table, conn, conn_client, Clean

cur_client = conn_client.cursor()
sql = "COPY (SELECT * FROM sources.product) TO STDOUT WITH CSV DELIMITER ';'"
with open("product.csv", "w", encoding="UTF-8") as file:
    cur_client.copy_expert(sql, file)

cl = Clean(product_table, "product.csv")

class Clean_product:

    @staticmethod
    def send_to_error_table(lst):
        cur = conn.cursor()
        extras.execute_values(cur, "INSERT INTO dds.error_product (product_id, name_short, category_id, pricing_line_id, brand_id, detail_info) VALUES %s", lst)
        conn.commit()

    def clean(self):
        no_duplicate_reader = cl.clean_duplicate()
        error_batch, no_duplicate_reader = cl.clean_pk_duplicates(no_duplicate_reader)
        self.send_to_error_table(error_batch)

        i = 0
        n = len(no_duplicate_reader)
        cur = conn.cursor()
        cur.execute(f"SELECT category_id from dds.category")
        category_ids = set(cur.fetchall())
        error_batch = []
        while i < n:
            category_id = no_duplicate_reader[i][2]
            if (category_id,) not in category_ids:
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
        cur.execute(f"SELECT brand_id from dds.brand")
        brand_ids = set(cur.fetchall())
        error_batch = []
        while i < n:
            brand_id = no_duplicate_reader[i][4]
            if (brand_id,) not in brand_ids:
                no_duplicate_reader[i].append("Ошибка ссылочной целостности")
                error_batch.append(no_duplicate_reader[i])
                del no_duplicate_reader[i]
                i -= 1
                n -= 1
            i += 1
        self.send_to_error_table(error_batch)

        df = pd.DataFrame(no_duplicate_reader)
        df.to_csv('cleaned_product.csv', index=False, header=False)

def execute_product(conn, df):
    tuples = [tuple(x) for x in df.to_numpy()]
    # SQL query to execute
    query = "INSERT INTO dds.product (product_id, name_short, category_id, pricing_line_id, brand_id) VALUES %s"
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

clb = Clean_product()
clb.clean()

# this two commands run ONLY after clb.clean, because class Clean_brand creates a cleaned csv file: cleaned_brand.csv
if __name__ == "__main__":
    df = pd.read_csv('cleaned_product.csv')
    execute_product(conn, df)