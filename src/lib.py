import requests
import sqlite3
import csv
from databricks import sql
import os
from dotenv import load_dotenv

load_dotenv()

OLYMPIC_DICTIONARY_PATH = 'https://raw.githubusercontent.com/Cavidan-oss/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/refs/heads/main/data/olympic_dictionary.csv'
OLYMPIC_SUMMER_PATH = 'https://raw.githubusercontent.com/Cavidan-oss/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/refs/heads/main/data/olympic_summer.csv'


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
                # placeholders = ", ".join(["?" for _ in header])

                # Truncate the destination table
                cursor.execute(f"TRUNCATE TABLE {table_name}")

                # Insert each row into the table
                sql_insert = f"INSERT INTO {table_name} VALUES "

                rows = []
                for row in csv_reader:
                    processed_row = ['Null' if value == '' else value for value in row]
                    rows.append(processed_row)


                # Prepare the full SQL statement
                # We'll format this later with the actual values
                values_str = ', '.join(
                    [f"({', '.join([repr(v) for v in row])})" for row in rows]
)
                full_sql = sql_insert + values_str.replace("'Null'", 'Null')
                # Complete the SQL query

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

    full_file_path = extract_csv(
        url = 'data/olympic_dictionary',
        file_path='olympic_dictionary.csv',
        directory='data'
    )


    ETLHelper.load_csv_to_db( 
                              'data/olympic_dictionary.csv',
                              conn,
                              table_name='OlympicDictionary_jk645',
                              create_table_sql='src/sql/create_sql_olympic_dictionary.sql'
                            )

    ETLHelper.load_csv_to_db( 
                                'data/olympic_summer.csv',
                                conn,
                                table_name='OlympicSummer_jk645',
                                create_table_sql='src/sql/create_sql_olympic_summer.sql'
                                )


    # res = ETLHelper.fetchall_result(conn, query = 'SELECT')
    # print(type(conn))

    # res = ETLHelper.fetchall_result(conn, query = '')
    # print(res, type(res))





    conn.close()

