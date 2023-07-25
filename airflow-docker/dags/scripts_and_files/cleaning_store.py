import psycopg2
import psycopg2.extras as extras
import pandas as pd
from dev import conn

def execute_store(conn, df):
    tuples = [list(x) for x in df.to_numpy()]
    for i in range(len(tuples)):
        tuples[i] = tuples[i][0].split(';')
    # SQL query to execute
    query = "INSERT INTO dds.stores (pos, pos_name) VALUES %s"
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

if __name__ == "__main__":
    df = pd.read_csv('/opt/airflow/dags/scripts_and_files/stores.csv', encoding='cp1251')
    execute_store(conn, df)