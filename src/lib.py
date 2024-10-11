import requests
import sqlite3
import csv
from databricks import sql
import os
from dotenv import load_dotenv

load_dotenv()

class SQL:
    @classmethod
    def read_sql(cls, query, **params):
        if query.strip().endswith(".sql"):
            with open(query, "r") as file:
                query = file.read().strip()

        return query.format(**params)


class ETLHelper:

    @classmethod
    def insert(cls, connection, table_name, values):

        try:
            cursor = connection.cursor()

            _colunms = ", ".join(values.keys())
            _values = ", ".join(values.values())

            base_insert_query = SQL.read_sql(
                "src/sql/insert.sql",
                table_name=table_name,
                columns=_colunms,
                values=_values,
            )

            print(base_insert_query)

            cursor.execute(base_insert_query)
            connection.commit()

            inserted_id = cursor.lastrowid

            return inserted_id

        except Exception as e:
            print(e)
            return None

    @classmethod
    def read_by_id(cls, connection, table_name, id, id_col_name="ID"):

        cursor = connection.cursor()

        # Execute a query to fetch all data from the table
        query = f"SELECT * FROM {table_name} Where {id_col_name} = {id} "
        print(query)
        cursor.execute(query)

        # Fetch all rows from the executed query
        data = cursor.fetchone()

        # Fetch the column names
        column_names = [description[0] for description in cursor.description]

        # Combine column names with the data for better output formatting
        result = {"columns": column_names, "data": data}

        return result

    @classmethod
    def read_all(cls, connection, table_name):
        try:
            # Create a cursor object
            cursor = connection.cursor()

            # Execute a query to fetch all data from the table
            query = f"SELECT * FROM {table_name}"
            print(query)
            cursor.execute(query)

            # Fetch all rows from the executed query
            data = cursor.fetchall()

            # Fetch the column names
            column_names = [description[0] for description in cursor.description]

            # Combine column names with the data for better output formatting
            result = {"columns": column_names, "data": data}

            return result

        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    @classmethod
    def load_csv_to_db(cls, csv_file_path, conn, table_name, create_table_sql=None):
        # Create a cursor object
        try:
            cursor = conn.cursor()

            with open(csv_file_path, mode="r", newline="", encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)

                # Read the header row to get column names
                header = next(csv_reader)

                # Create a column definition for all columns as TEXT
                columns_to_create_table = ", ".join(
                    [
                        f"{col.replace('.', '_').replace(' ', '_')} TEXT"
                        for col in header
                    ]
                )

                # Execute the CREATE TABLE statement
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_to_create_table})"
                )

                placeholders = ", ".join(["?" for _ in header])

                # Truncate the destionation table
                cursor.execute(f"Delete from {table_name}")
                # cursor.execute(f"DELETE FROM SQLITE_SEQUENCE WHERE name='{table_name}' ")

                sql_insert = f"INSERT INTO {table_name}  VALUES ({placeholders})"

                # Insert each row into the table
                for row in csv_reader:
                    cursor.execute(sql_insert, row)

                # INSERT INTO cədvəl(col1 , col2) VALUES ( ? , ? )
            # Commit the transaction
            conn.commit()
            return True

        except Exception as e:
            print(e)
            return False

    @classmethod
    def update(cls, connection, table_name, ids, update_values):
        try:
            cursor = connection.cursor()

            formatted_updates = ", ".join(
                [
                    f"{col_name} = '{update_val}'"
                    for col_name, update_val in update_values.items()
                ]
            )
            formatted_ids = ", ".join(map(str, ids))

            base_update_query = SQL.read_sql(
                "src/sql/update.sql",
                id=formatted_ids,
                table_name=table_name,
                updates=formatted_updates,
            )
            print(base_update_query)

            cursor.execute(base_update_query)
            connection.commit()
            print(f"{formatted_ids} are updated successfully")

            return True

        except Exception as e:
            print(e)
            return None

    @classmethod
    def delete(cls, connection, table_name, ids):
        try:
            cursor = connection.cursor()

            formatted_ids = ", ".join(map(str, ids))

            base_delete_query = SQL.read_sql(
                "src/sql/delete.sql", id=formatted_ids, table_name=table_name
            )
            print(base_delete_query)

            cursor.execute(base_delete_query)
            connection.commit()
            print(f"{formatted_ids} are deleted successfully")

            return True

        except Exception as e:
            print(e)
            return None

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
                "Databases other than sqllite not implemented yet!!!"
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

    print(type(conn))

    conn.close()

