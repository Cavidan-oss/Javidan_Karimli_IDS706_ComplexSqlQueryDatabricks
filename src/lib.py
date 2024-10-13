import requests
import sqlite3
import csv
from databricks import sql
import os
from dotenv import load_dotenv

load_dotenv()

OLYMPIC_DICTIONARY_PATH = "https://raw.githubusercontent.com/Cavidan-oss/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/refs/heads/main/data/olympic_dictionary.csv"
OLYMPIC_SUMMER_PATH = "https://raw.githubusercontent.com/Cavidan-oss/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/refs/heads/main/data/olympic_summer.csv"


class SQL:
    @classmethod
    def read_sql(cls, query, **params):
        if query.strip().endswith(".sql"):
            with open(query, "r") as file:
                query = file.read().strip()

        return query.format(**params)


class ETLHelper:
    @classmethod
    def load_csv_to_db(cls, csv_file_path, conn, table_name, create_table_sql):
        cursor = None
        # Create a cursor object
        try:
            cursor = conn.cursor()

            with open(csv_file_path, mode="r", newline="", encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)
                next(csv_reader)

                cursor.execute(SQL.read_sql(create_table_sql))

                cursor.execute(f"TRUNCATE TABLE {table_name};")

                sql_insert = f"INSERT INTO {table_name} VALUES "

                rows = []
                for row in csv_reader:
                    processed_row = ["Null" if value == "" else value for value in row]
                    rows.append(processed_row)

                values_str = ", ".join(
                    [f"({', '.join([repr(v) for v in row])})" for row in rows]
                )
                full_sql = sql_insert + values_str.replace("'Null'", "Null")

                # print(full_sql)
                cursor.execute(full_sql)

            # Commit the transaction
            conn.commit()
            return True

        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()
            return False

        finally:
            if cursor:
                cursor.close()

    @classmethod
    def execute_query(cls, connection, query):
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            print(query)
            connection.commit()
            cursor.close()
            return True

        except Exception as e:
            print(e)
            return False

    @classmethod
    def fetchall_result(cls, connection, query, query_params):
        cursor = None
        try:
            cursor = connection.cursor()
            full_exec_query = SQL.read_sql(query, **query_params)
            print(f"Query to be executed - \n{full_exec_query}")
            cursor.execute(full_exec_query)
            return cursor.fetchall()

        except Exception as e:
            print(f"Exception occured - {e}")
            return []

        finally:
            cursor.close()

    @classmethod
    def connect_db(cls, type_of_database, database_conn):

        if type_of_database.lower() == "sqllite":
            conn = sqlite3.connect(f"{database_conn}.db")
            return conn

        if type_of_database.lower() == "databricks":

            conn = sql.connect(**database_conn)

            return conn

        else:
            raise NotImplementedError(
                "Databases other than sqllite, databricks not implemented yet!!!"
            )


def extract_csv(
    url,
    file_path,
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(os.path.join(directory, file_path), "wb") as f:
            f.write(r.content)
    return os.path.join(directory, file_path)
