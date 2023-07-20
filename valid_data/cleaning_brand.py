import csv
import psycopg2
import psycopg2.extras as extras
import pandas as pd

class Clean_brand:

    def remove_duplicate(self, lst):
        s = set()
        for i in lst:
            s.add(tuple(i))
        res = []
        for brand_id, brand in s:
            res.append([brand_id, brand])
        return res

    def send_to_error_table(self):
        cur = conn.cursor()
        extras.execute_values(cur, "INSERT INTO dds.error (error_id, tabble, detail) VALUES %s", )

    def clean(self):
        with open('brand.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            reader = list(reader)
            no_duplicate_reader = self.remove_duplicate(reader)
            n = len(no_duplicate_reader)
            PK_brand_id = set()
            for i in range(n):
                # brand_id = no_duplicate_reader[i][0]
                # brand = no_duplicate_reader[i][1]
                try:
                    no_duplicate_reader[i][0] = int(no_duplicate_reader[i][0])
                except:
                    no_duplicate_reader[i][0], no_duplicate_reader[i][1] = int(no_duplicate_reader[i][1]), no_duplicate_reader[i][0]

                if no_duplicate_reader[i][0] not in PK_brand_id:
                    PK_brand_id.add(no_duplicate_reader[i][0])
                else:
                    continue

                brand = no_duplicate_reader[i][1]
                if brand is None:
                    no_duplicate_reader[i][1] = "Не определен"
        df = pd.DataFrame(no_duplicate_reader)
        df.to_csv('cleaned_brand.csv', index=False, header=False)

def execute_values(conn, df):
    tuples = [tuple(x) for x in df.to_numpy()]

    #cols = ','.join(list(df.columns))
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

conn = psycopg2.connect(
    host="10.1.108.29",
    database="internship_10_db",
    user="interns_10",
    password="*3}dgY"
)

clb = Clean_brand()
clb.clean()

# this two commands run ONLY after clb.clean, because class Clean_brand creates a cleaned csv file: cleaned_brand.csv
df = pd.read_csv('cleaned_brand.csv')

execute_values(conn, df)