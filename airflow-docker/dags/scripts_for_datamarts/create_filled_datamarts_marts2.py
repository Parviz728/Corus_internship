import psycopg2
import psycopg2.extras as extras

PORT = 5432
HOST = "10.1.108.29"
USER = "interns_10"
PASSWORD = "*3}dgY"
DATABASE = "internship_10_db"
conn = psycopg2.connect(user=USER, password=PASSWORD, host=HOST, database=DATABASE)

cur = conn.cursor()
cur.execute('''
select dds.stock.available_on,
dds.stock.product_id,
dds.product.name_short,
dds.stock.pos,
dds.stores.pos_name,
dds.stock.available_quantity, 
dds.stock.cost_per_item,
CURRENT_DATE
from dds.stock
inner join dds.product on stock.product_id = product.product_id
inner join dds.stores on stock.pos = stores.pos;
''')

lst = cur.fetchall()

PK_indexes = [0, 1, 3]

def clean_pk_duplicates(no_duplicate_reader):
    PK_container = set()
    i = 0
    n = len(no_duplicate_reader)

    while i < n:
        keys = tuple(no_duplicate_reader[i][indexes_to_check] for indexes_to_check in PK_indexes)
        if keys not in PK_container:
            PK_container.add(keys)
        else:
            del no_duplicate_reader[i]
            i -= 1
            n -= 1
        i += 1
    return no_duplicate_reader

cleaned_lst = clean_pk_duplicates(lst)

def execute(conn, tuples):
    query = "INSERT INTO datamarts.marts2 (available_on, " \
            "product_id, " \
            "name_short, " \
            "pos, " \
            "pos_name, "  \
            "available_quantity, " \
            "cost_per_item, " \
            "UPDATE_DATE) VALUES %s"
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

execute(conn, cleaned_lst)