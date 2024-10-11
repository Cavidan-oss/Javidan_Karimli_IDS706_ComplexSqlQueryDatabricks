import requests
import sqlite3
import csv
from databricks import sql
import os
from dotenv import load_dotenv

load_dotenv()

OLYMPIC_DICTIONARY_PATH = 'https://raw.githubusercontent.com/Cavidan-oss/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/refs/heads/main/data/olympic_dictionary.csv'
OLYMPIC_SUMMER_PATH = ''


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

                # Read the header row to get column names
                header = next(csv_reader)

                # Execute the provided CREATE TABLE SQL statement if any
                cursor.execute(SQL.read_sql(create_table_sql))

                # Prepare placeholders for the insert operation
                placeholders = ", ".join(["?" for _ in header])

                # Truncate the destination table
                cursor.execute(f"TRUNCATE TABLE {table_name}")

                # Insert each row into the table
                sql_insert = f"INSERT INTO {table_name} VALUES ({placeholders})"
                for row in csv_reader:
                    cursor.execute(sql_insert, row)

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
            full_exec_query = SQL.read_sql(query, **query_params )
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
        
        if type_of_database.lower() == 'databricks':

            conn = sql.connect(
                            **database_conn
                            )
            
            return conn

        else:
            raise NotImplementedError(
                "Databases other than sqllite, databricks not implemented yet!!!"
            )


def extract_csv(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/chess-transfers/transfers.csv",
    file_path="chess_transfers.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(os.path.join(directory, file_path), "wb") as f:
            f.write(r.content)
    return file_path






if __name__ == '__main__':
    server_host_name = os.getenv('SERVER_HOSTNAME')
    http_path = os.getenv('HTTP_PATH')
    access_token = os.getenv('ACCESS_TOKEN')
    
    conn = ETLHelper.connect_db(
        type_of_database='databricks',
        database_conn= {
                        "server_hostname" : server_host_name,
                        "http_path" : http_path,
                        "access_token" : access_token
                        }
    )

    extract_csv()


    load_csv_to_db()

    # print(type(conn))

    # res = ETLHelper.fetchall_result(conn, query = '')
    # print(res, type(res))





    conn.close()

