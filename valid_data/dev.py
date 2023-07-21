import datetime
import csv
import psycopg2
from sqlalchemy import create_engine, event, MetaData, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

PORT = 5432
HOST = "10.1.108.29"
USER = "interns_10"
PASSWORD = "*3}dgY"
DATABASE = "internship_10_db"
conn = psycopg2.connect(user=USER, password=PASSWORD, host=HOST, database=DATABASE)
encodes = {"brand.csv": "utf-8",
           "category.csv": "utf-8",
           "product.csv": "utf-8",
           "stock.csv": "utf-8",
           "transaction.csv": "utf-8",
           "stores.csv": "cp1251",
           "pos.csv": "cp1251"}

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

    index_value = {0: "category_id", 1: "category_name"}
    indexes_to_check_for_empty = None
    PK_indexes = [0]  # индекс Primary key атрибута

    def __repr__(self) -> str:
        return f"Category(category_id = {self.category_id}, category_name = {self.category_name}"

class Brand(Base):
    __tablename__ = "brand"

    brand_id: Mapped[int] = mapped_column(primary_key=True)
    brand: Mapped[str] = mapped_column()

    index_value = {0: "brand_id", 1: "brand"}
    indexes_to_check_for_empty = None
    PK_indexes = [0] # индекс Primary key атрибута

    def __repr__(self):
        return f"Brand(brand_id = {self.brand_id}, brand = {self.brand}"

class Product(Base):
    __tablename__ = "product"

    product_id: Mapped[int] = mapped_column(primary_key=True)
    name_short: Mapped[str] = mapped_column()
    category_id: Mapped[str] = mapped_column(ForeignKey("category.category_id"))
    pricing_line_id: Mapped[str] = mapped_column()
    brand_id: Mapped[int] = mapped_column(ForeignKey("brand.brand_id"))

    index_value = {0: "product_id", 1: "name_short", 2: "category_id", 3: "pricing_line_id", 4: "brand_id"}
    indexes_to_check_for_empty = None
    PK_indexes = [0]  # индекс Primary key атрибута

    def __repr__(self) -> str:
        return f"Product(id = {self.product_id}, name = {self.name}, category = {self.category}, brand = {self.brand})"

class Stock(Base):
    __tablename__ = "stock"

    available_on: Mapped[datetime.date] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("product.product_id"), primary_key=True)
    pos: Mapped[str] = mapped_column(ForeignKey("stores.pos"), primary_key=True)
    available_quantity: Mapped[float] = mapped_column()
    cost_per_item: Mapped[float] = mapped_column()

    index_value = {0: "available_on", 1: "product_id", 2: "pos", 3: "available_quantity", 4: "cost_per_item"}
    indexes_to_check_for_empty = [1, 3, 4]
    PK_indexes = [0, 1, 2]

    def __repr__(self):
        return f"Stock(available_on = {self.available_on}, " \
               f"product_id = {self.product_id}, " \
               f"pos = {self.pos}, " \
               f"available_quantity = {self.available_quantity}, " \
               f"cost_per_item = {self.cost_per_item})"

class Pos(Base):
    __tablename__ = "pos"

    transaction_id: Mapped[str] = mapped_column(primary_key=True)
    pos: Mapped[str] = mapped_column()

    index_value = {0: "transaction_id", 1: "pos"}
    indexes_to_check_for_empty = [1]
    PK_indexes = [0]

    def __repr__(self):
        return f"Pos(transaction_id = {self.transaction_id}, pos = {self.pos}"

class Transaction(Base):
    __tablename__ = "transaction"

    transaction_id: Mapped[str] = mapped_column(ForeignKey("pos.transaction_id"), primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("product.product_id"), primary_key=True)
    recorded_on: Mapped[datetime.date] = mapped_column()
    quantity: Mapped[float] = mapped_column()
    price: Mapped[float] = mapped_column()
    price_full: Mapped[float] = mapped_column()
    order_type_id: Mapped[str] = mapped_column()

    index_value = {0: "transaction_id", 1: "product_id", 2: "recorded_on", 3: "quantity", 4: "price", 5: "price_full", 6: "order_type_id"}
    indexes_to_check_for_empty = [3, 4]
    PK_indexes = [0, 1]

    def __repr__(self):
        return f"Transaction(transaction_id = {self.transaction_id}, " \
               f"product_id = {self.product_id}, " \
               f"recorded_on = {self.recorded_on}, " \
               f"quantity = {self.quantity}, " \
               f"price = {self.price}, " \
               f"price_full = {self.price_full}, " \
               f"order_type_id = {self.order_type_id})"

stores_table = Store()
pos_table = Pos()
transaction_table = Transaction()
stock_table = Stock()
brand_table = Brand()
category_table = Category()
product_table = Product()

class Clean:

    def __init__(self, table_name, file_name: str):
        self.table_name = table_name
        self.file_name = file_name

    @staticmethod
    def remove_duplicate(lst):
        s = set()
        for i in lst:
            s.add(tuple(i))
        res = []
        for val in s:
            res.append(list(val))
        return res

    def clean_duplicate(self):
        with open(self.file_name, 'r', encoding=encodes[self.file_name]) as file:
            reader = csv.reader(file)
            next(reader)
            reader = list(reader)
            no_duplicate_reader = self.remove_duplicate(reader) #устраняем полные дубли
            return no_duplicate_reader


    def clean_empties(self, no_duplicate_reader):
        error_batch = []
        i = 0
        n = len(no_duplicate_reader)
        while i < n:
            if '' in no_duplicate_reader[i]:
                message = "пропущен"
                for index, value in enumerate(no_duplicate_reader[i]):
                    if value == '':
                        no_duplicate_reader[i][index] = "Не определено"
                    try:
                        value = float(value)
                        if isinstance(value, float) or isinstance(value, int):
                            if value < 0:
                                no_duplicate_reader[i][index] = "Некорректно определено"
                                message = "пропущен и отрицателен"
                    except:
                        pass
                no_duplicate_reader[i].append(f"{message} {self.table_name.index_value[index]}")
                error_batch.append(no_duplicate_reader[i])
                del no_duplicate_reader[i]
                i -= 1
                n -= 1
            else:
                for index, value in enumerate(no_duplicate_reader[i]):
                    try:
                        value = float(value)
                        if isinstance(value, float) or isinstance(value, int):
                            if value < 0:
                                no_duplicate_reader[i][index] = "Некорректно определено"
                                no_duplicate_reader[i].append(f"отрицателен {self.table_name.index_value[index]}")
                                error_batch.append(no_duplicate_reader[i])
                                del no_duplicate_reader[i]
                                i -= 1
                                n -= 1
                    except:
                        pass
            i += 1
        return error_batch, no_duplicate_reader

    def clean_pk_duplicates(self, no_duplicate_reader):
        PK_container = set()
        i = 0
        n = len(no_duplicate_reader)
        error_batch = []

        while i < n:
            keys = tuple(no_duplicate_reader[i][indexes_to_check] for indexes_to_check in self.table_name.PK_indexes)
            if keys not in PK_container:
                PK_container.add(keys)
            else:
                no_duplicate_reader[i].append(f"Ошибка первичного ключа")
                error_batch.append(no_duplicate_reader[i])
                del no_duplicate_reader[i]
                i -= 1
                n -= 1
            i += 1
        return error_batch, no_duplicate_reader



