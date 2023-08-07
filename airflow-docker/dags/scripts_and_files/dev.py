import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, event, MetaData, ForeignKey, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

load_dotenv()
# config
Base = declarative_base()
engine = create_engine(os.getenv("DB_URL"))

# ивент для изменения создания сессии и подключения на специализацию схемы
@event.listens_for(engine, "connect", insert=True)
def set_search_path(dbapi_connection, connection_record):
    existing_autocommit = dbapi_connection.autocommit
    dbapi_connection.autocommit = True
    cursor = dbapi_connection.cursor()
    cursor.execute("SET SESSION search_path='%s'" % "dds")
    cursor.close()
    dbapi_connection.autocommit = existing_autocommit

# connection и медаданные
connection = engine.connect()
metadata = MetaData()
# модели таблиц
class Store(Base):
    __tablename__ = "stores"

    pos = Column(String(30), primary_key=True)
    pos_name = Column(String(255))

    index_value = {0: "pos", 1: "pos_name"}
    indexes_to_check_for_empty = []
    PK_indexes = [0]  # индексы Primary key атрибутов
    FK_indexes = {}  # индексы Foreign key атрибутов

    def __repr__(self):
        return f"Store(pos = {self.pos}, pos_name = {self.pos_name}"

class Category(Base):
    __tablename__ = "category"

    category_id = Column(String(50), primary_key=True)
    category_name = Column(String(255))

    index_value = {0: "category_id", 1: "category_name"}
    indexes_to_check_for_empty = []
    PK_indexes = [0]  # индексы Primary key атрибутов
    FK_indexes = {} # индексы Foreign key атрибутов

    def __repr__(self) -> str:
        return f"Category(category_id = {self.category_id}, category_name = {self.category_name}"

class Brand(Base):
    __tablename__ = "brand"

    brand_id = Column(Integer(), primary_key=True)
    brand = Column(String(255))

    index_value = {0: "brand_id", 1: "brand"}
    indexes_to_check_for_empty = []
    PK_indexes = [0] # индексы Primary key атрибутов
    FK_indexes = {}  # индексы Foreign key атрибутов

    def __repr__(self):
        return f"Brand(brand_id = {self.brand_id}, brand = {self.brand}"

class Product(Base):
    __tablename__ = "product"

    product_id = Column(Integer(), primary_key=True)
    name_short = Column(String(255))
    category_id = Column(String(50), ForeignKey("category.category_id"))
    pricing_line_id = Column(String(255))
    brand_id = Column(Integer(), ForeignKey("brand.brand_id"))

    index_value = {0: "product_id", 1: "name_short", 2: "category_id", 3: "pricing_line_id", 4: "brand_id"}
    indexes_to_check_for_empty = []
    PK_indexes = [0]  # индексы Primary key атрибутов
    FK_indexes = {2: ("category", "category_id"), 4: ("brand", "brand_id")}  # индексы Foreign key атрибутов

    def __repr__(self) -> str:
        return f"Product(id = {self.product_id}, name = {self.name}, category = {self.category}, brand = {self.brand})"

class Stock(Base):
    __tablename__ = "stock"

    available_on = Column(Date, primary_key=True)
    product_id = Column(Integer, ForeignKey("product.product_id"), primary_key=True)
    pos = Column(String(30), ForeignKey("stores.pos"), primary_key=True)
    available_quantity = Column(Float)
    cost_per_item = Column(Float)

    index_value = {0: "available_on", 1: "product_id", 2: "pos", 3: "available_quantity", 4: "cost_per_item"}
    indexes_to_check_for_empty = [1, 3, 4]
    PK_indexes = [0, 1, 2]
    FK_indexes = {1: ("product", "product_id"), 2: ("stores", "pos")}  # индексы Foreign key атрибутов

    def __repr__(self):
        return f"Stock(available_on = {self.available_on}, " \
               f"product_id = {self.product_id}, " \
               f"pos = {self.pos}, " \
               f"available_quantity = {self.available_quantity}, " \
               f"cost_per_item = {self.cost_per_item})"

class Pos(Base):
    __tablename__ = "pos"

    transaction_id = Column(String(50), primary_key=True)
    pos = Column(String(30), ForeignKey("stores.pos"))

    index_value = {0: "transaction_id", 1: "pos"}
    indexes_to_check_for_empty = [1]
    PK_indexes = [0]
    FK_indexes = {1: ("stores", "pos")}  # индексы Foreign key атрибутов

    def __repr__(self):
        return f"Pos(transaction_id = {self.transaction_id}, pos = {self.pos}"

class Transaction(Base):
    __tablename__ = "transaction"

    transaction_id = Column(String(50), ForeignKey("pos.transaction_id"), primary_key=True)
    product_id = Column(Integer(), ForeignKey("product.product_id"), primary_key=True)
    recorded_on = Column(Date)
    quantity = Column(Float)
    price = Column(Float)
    price_full = Column(Float)
    order_type_id = Column(String(50))

    index_value = {0: "transaction_id", 1: "product_id", 2: "recorded_on", 3: "quantity", 4: "price", 5: "price_full", 6: "order_type_id"}
    indexes_to_check_for_empty = [3, 4]
    PK_indexes = [0, 1]
    FK_indexes = {0: ("pos", "transaction_id"), 1: ("product", "product_id")}  # индексы Foreign key атрибутов

#универсальный класс очистки таблиц
class Clean:

    def __init__(self, table_name):
        self.table_name = table_name

    def clean_duplicate(self, df):
        reader = list(df)
        s = set()
        for i in reader:
            s.add(tuple(i))
        res = []
        for val in s:
            res.append(list(val))
        return res

    def send_to_error_table(self, error_batch, table_name):
        df_error = pd.DataFrame(error_batch)
        df_error.to_sql(schema="dds", name=table_name, con=engine, if_exists="append")

    def fill_dds(self, cleaned_list_of_values, table_name):
        df = pd.DataFrame(cleaned_list_of_values)
        df.to_sql(schema="dds", name=table_name, con=engine, if_exists="append")

# удаление пропусков и некорректных данных
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
                no_duplicate_reader[i].append(f"{message} {self.table_name.index_value[index]}") # добавление сообщения об ошибке
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

# удаление повторяющихся первичных ключей
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

# корректность внешних ключей
    def make_fk_connection(self, no_duplicate_reader, fk_key, connection_table, connection_attribute):
        i = 0
        n = len(no_duplicate_reader)
        df = pd.read_sql(f"SELECT * FROM sources.{connection_table}", con=create_engine(os.getenv("CLIENT_DB_URL")), index_col=connection_attribute)
        values = set(df)
        error_batch = []
        while i < n:
            val = no_duplicate_reader[i][fk_key]
            try:
                val = int(val)
            except:
                pass
            if (val,) not in values:
                no_duplicate_reader[i].append("Ошибка ссылочной целостности")
                error_batch.append(no_duplicate_reader[i])
                del no_duplicate_reader[i]
                i -= 1
                n -= 1
            i += 1
        return error_batch, no_duplicate_reader

