import csv
import datetime

import psycopg2
import psycopg2.extras as extras
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, event, MetaData, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

PORT = 5432
HOST = "10.1.108.29"
USER = "interns_10"
PASSWORD = "*3}dgY"
DATABASE = "internship_10_db"

engine = create_engine("postgresql+psycopg2://interns_10:*3}dgY@10.1.108.29:5432/internship_10_db")

@event.listens_for(engine, "connect", insert=True)
def set_search_path(dbapi_connection, connection_record):
    existing_autocommit = dbapi_connection.autocommit
    dbapi_connection.autocommit = True
    cursor = dbapi_connection.cursor()
    cursor.execute("SET SESSION search_path='%s'" % "dds")
    cursor.close()
    dbapi_connection.autocommit = existing_autocommit

connection = engine.connect()
metadata = MetaData()

class Base(DeclarativeBase):
    pass

class Store(Base):
    __tablename__ = "stores"

    pos: Mapped[str] = mapped_column(primary_key=True)
    pos_name: Mapped[str] = mapped_column()

    def __repr__(self):
        return f"Store(pos = {self.pos}, pos_name = {self.pos_name}"

class Category(Base):
    __tablename__ = "category"

    category_id: Mapped[str] = mapped_column(primary_key=True)
    category_name: Mapped[str] = mapped_column()

    def __repr__(self) -> str:
        return f"Category(category_id = {self.category_id}, category_name = {self.category_name}"

class Brand(Base):
    __tablename__ = "brand"

    brand_id: Mapped[int] = mapped_column(primary_key=True)
    brand: Mapped[str] = mapped_column()

    def __repr__(self):
        return f"Brand(brand_id = {self.brand_id}, brand = {self.brand}"

class Product(Base):
    __tablename__ = "product"

    product_id: Mapped[int] = mapped_column(primary_key=True)
    name_short: Mapped[str] = mapped_column()
    category_id: Mapped[str] = mapped_column(ForeignKey("category.category_id"))
    pricing_line_id: Mapped[str] = mapped_column()
    brand_id: Mapped[int] = mapped_column(ForeignKey("brand.brand_id"))

    def __repr__(self) -> str:
        return f"Product(id = {self.product_id}, name = {self.name}, category = {self.category}, brand = {self.brand})"

class Stock(Base):
    __tablename__ = "stock"

    available_on: Mapped[datetime.date] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("product.product_id"), primary_key=True)
    pos: Mapped[str] = mapped_column(ForeignKey("stores.pos"), primary_key=True)
    available_quantity: Mapped[float] = mapped_column()
    cost_per_item: Mapped[float] = mapped_column()

    def __repr__(self):
        return f"Stock(available_on = {self.available_on}, " \
               f"product_id = {self.product_id}, " \
               f"pos = {self.pos}, " \
               f"available_quantity = {self.available_quantity}, " \
               f"cost_per_item = {self.cost_per_item})"

class Pos(Base):
    __tablename__ = "pos"

    pos: Mapped[str] = mapped_column(primary_key=True)
    pos_name: Mapped[str] = mapped_column()

    def __repr__(self):
        return f"Pos(pos = {self.pos}, pos_name = {self.pos_name}"

class Transaction(Base):
    __tablename__ = "transaction"

    transaction_id: Mapped[str] = mapped_column(ForeignKey("pos.transaction_id"), primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("product.product_id"), primary_key=True)
    recorded_on: Mapped[datetime.date] = mapped_column()
    quantity: Mapped[float] = mapped_column()
    price: Mapped[float] = mapped_column()
    price_full: Mapped[float] = mapped_column()
    order_type_id: Mapped[str] = mapped_column()

    def __repr__(self):
        return f"Transaction(transaction_id = {self.transaction_id}, " \
               f"product_id = {self.product_id}, " \
               f"recorded_on = {self.recorded_on}, " \
               f"quantity = {self.quantity}, " \
               f"price = {self.price}, " \
               f"price_full = {self.price_full}, " \
               f"order_type_id = {self.order_type_id})"

stores_table = Store()
class Clean:

    def __init__(self, table):
        self.table_name = table

    def remove_duplicate(self, lst):
        s = set()
        for i in lst:
            s.add(tuple(i))
        res = []
        for available_on, product_id, pos, available_quantity, cost_per_item in s:
            res.append([available_on, product_id, pos, available_quantity, cost_per_item])
        return res

    def send_to_error_table(self, available_on, product_id, pos, available_quantity, cost_per_item, detail_info):
        cur = conn.cursor()
        extras.execute_values(cur, "INSERT INTO dds.error_stock (available_on, product_id, pos, available_quantity, cost_per_item, detail_info) VALUES %s",
                              [(available_on, product_id, pos, available_quantity, cost_per_item, detail_info)])
        conn.commit()

    def clean(self):
        with open('stock.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            reader = list(reader)
            no_duplicate_reader = self.remove_duplicate(reader)
            i = 0
            n = len(no_duplicate_reader)
            while i < n:
                excel_date = int(no_duplicate_reader[i][0])
                dt = date.fromordinal(date(1900, 1, 1).toordinal() + excel_date - 2)
                no_duplicate_reader[i][0] = dt

                product_id = no_duplicate_reader[i][1]
                cost_per_item = no_duplicate_reader[i][4]
                available_quantity = no_duplicate_reader[i][3]
                if product_id == '':
                    no_duplicate_reader[i][1] = 0
                if available_quantity == '':
                    no_duplicate_reader[i][3] = 0.0
                if cost_per_item == '':
                    no_duplicate_reader[i][4] = 0.0
                    self.send_to_error_table(available_on=no_duplicate_reader[i][0],
                                             product_id=no_duplicate_reader[i][1],
                                             pos=no_duplicate_reader[i][2],
                                             available_quantity=no_duplicate_reader[i][3],
                                             cost_per_item=no_duplicate_reader[i][4],
                                             detail_info="cost_per_item пропущен")
                    del no_duplicate_reader[i]
                    i -= 1
                    n -= 1
                i += 1

            i = 0
            n = len(no_duplicate_reader)
            while i < n:
                product_id = no_duplicate_reader[i][1]
                available_quantity = no_duplicate_reader[i][3]
                if product_id == "":
                    if available_quantity == "":
                        no_duplicate_reader[i][3] = 0.0
                    no_duplicate_reader[i][1] = 0
                    self.send_to_error_table(available_on=no_duplicate_reader[i][0],
                                             product_id=no_duplicate_reader[i][1],
                                             pos=no_duplicate_reader[i][2],
                                             available_quantity=no_duplicate_reader[i][3],
                                             cost_per_item=no_duplicate_reader[i][4],
                                             detail_info="product_id пропущен")
                    del no_duplicate_reader[i]
                    i -= 1
                    n -= 1
                i += 1

            i = 0
            n = len(no_duplicate_reader)
            while i < n:
                available_quantity = no_duplicate_reader[i][3]
                if float(available_quantity) < 0.0 or available_quantity == "":
                    no_duplicate_reader[i][3] = 0.0
                    self.send_to_error_table(available_on=no_duplicate_reader[i][0],
                                             product_id=no_duplicate_reader[i][1],
                                             pos=no_duplicate_reader[i][2],
                                             available_quantity=no_duplicate_reader[i][3],
                                             cost_per_item=no_duplicate_reader[i][4],
                                             detail_info="available_quantity ниже нуля или пропущено")
                    del no_duplicate_reader[i]
                    i -= 1
                    n -= 1
                i += 1

            PK_stock = set()
            i = 0
            n = len(no_duplicate_reader)
            while i < n:
                available_on = no_duplicate_reader[i][0]
                # product_id = no_duplicate_reader[i][1]
                # pos = no_duplicate_reader[i][2]
                if (no_duplicate_reader[i][0], no_duplicate_reader[i][1], no_duplicate_reader[i][2]) not in PK_stock:
                    PK_stock.add((no_duplicate_reader[i][0], no_duplicate_reader[i][1], no_duplicate_reader[i][2]))
                else:
                    self.send_to_error_table(available_on=no_duplicate_reader[i][0],
                                             product_id=no_duplicate_reader[i][1],
                                             pos=no_duplicate_reader[i][2],
                                             available_quantity=no_duplicate_reader[i][3],
                                             cost_per_item=no_duplicate_reader[i][4],
                                             detail_info="Ошибка первичного ключа")
                    del no_duplicate_reader[i]
                    i -= 1
                    n -= 1
                i += 1

            i = 0
            n = len(no_duplicate_reader)
            cur = conn.cursor()
            cur.execute(f"SELECT product_id from dds.product")
            product_ids = set(cur.fetchall())
            while i < n:
                product_id = no_duplicate_reader[i][1]
                if (product_id,) not in product_ids:
                    self.send_to_error_table(available_on=no_duplicate_reader[i][0],
                                             product_id=no_duplicate_reader[i][1],
                                             pos=no_duplicate_reader[i][2],
                                             available_quantity=no_duplicate_reader[i][3],
                                             cost_per_item=no_duplicate_reader[i][4],
                                             detail_info="Ошибка ссылочной целостности")
                    del no_duplicate_reader[i]
                    i -= 1
                    n -= 1
                i += 1

        df = pd.DataFrame(no_duplicate_reader)
        df.to_csv('cleaned_stock.csv', index=False, header=False)

def execute_values(conn, df):
    tuples = [tuple(x) for x in df.to_numpy()]
    #cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO dds.stock (available_on, product_id, pos, available_quantity, cost_per_item ) VALUES %s"
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
df = pd.read_csv('cleaned_stock.csv')

execute_values(conn, df)'''
