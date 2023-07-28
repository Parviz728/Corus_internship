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
DROP schema datamarts CASCADE;

CREATE schema datamarts;

CREATE table datamarts.marts1 (
  brand VARCHAR(255),
  category_name VARCHAR(255),
  pos VARCHAR(30),
  pos_name VARCHAR(255),
  name_short VARCHAR(255),
  transaction_id VARCHAR(50),
  product_id serial,
  quantity FLOAT,
  price FLOAT,
  amount FLOAT,
  recorded_on date,
  UPDATE_DATE date,
  PRIMARY KEY(transaction_id, product_id)
);

CREATE TABLE IF NOT EXISTS datamarts.marts2
(
    available_on date,
    product_id serial,
    name_short VARCHAR(255),
    pos VARCHAR(30),
    pos_name VARCHAR(255),
    available_quantity FLOAT,
    cost_per_item FLOAT,
    update_date date,
    PRIMARY KEY (available_on, product_id, pos)
);
''')

cur.execute('''select dds.brand.brand,
dds.category.category_name,
dds.stores.pos,
dds.stores.pos_name,
dds.product.name_short,
dds.transaction.transaction_id,
dds.product.product_id,
dds.transaction.quantity,
dds.transaction.price,
dds.transaction.quantity * dds.transaction.price as amount,
dds.transaction.recorded_on,
CURRENT_DATE
from dds.brand
inner join dds.product on brand.brand_id = product.brand_id
inner join dds.category on product.category_id = category.category_id
inner join dds.transaction on product.product_id = transaction.product_id
inner join dds.pos on transaction.transaction_id = pos.transaction_id
inner join dds.stores on pos.pos = stores.pos;''')
lst = cur.fetchall()

PK_indexes = [5, 6]

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
    query = "INSERT INTO datamarts.marts1 (brand, " \
            "category_name, " \
            "pos, " \
            "pos_name, " \
            "name_short, " \
            "transaction_id, " \
            "product_id, " \
            "quantity, " \
            "price, " \
            "amount, " \
            "recorded_on, " \
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