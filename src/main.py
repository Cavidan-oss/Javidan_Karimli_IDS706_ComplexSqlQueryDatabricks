try:
    from lib import ETLHelper, extract_csv
except Exception:
    from src.lib import ETLHelper, extract_csv
import os

OLYMPIC_DICTIONARY_PATH = "https://raw.githubusercontent.com/Cavidan-oss/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/refs/heads/main/data/olympic_dictionary.csv"
OLYMPIC_SUMMER_PATH = "https://raw.githubusercontent.com/Cavidan-oss/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/refs/heads/main/data/olympic_summer.csv"

server_host_name = os.getenv("SERVER_HOSTNAME")
http_path = os.getenv("HTTP_PATH")
access_token = os.getenv("ACCESS_TOKEN")


def extract_to_databricks():
    conn = ETLHelper.connect_db(
        type_of_database="databricks",
        database_conn={
            "server_hostname": server_host_name,
            "http_path": http_path,
            "access_token": access_token,
        },
    )

    olympic_dictionary_path = extract_csv(
        url=OLYMPIC_DICTIONARY_PATH,
        file_path="olympic_dictionary.csv",
        directory="data",
    )

    olympic_summer_path = extract_csv(
        url=OLYMPIC_SUMMER_PATH, file_path="olympic_summer.csv", directory="data"
    )

    # ETLHelper.execute_query('DROP TABLE ids706_data_engineering.default.olympicdictionary_jk645;')

    ETLHelper.load_csv_to_db(
        olympic_dictionary_path,
        conn,
        table_name="OlympicDictionary_jk645",
        create_table_sql="src/sql/create_sql_olympic_dictionary.sql",
    )

    # ETLHelper.execute_query('DROP TABLE ids706_data_engineering.default.olympicsummer_jk645;')


    ETLHelper.load_csv_to_db(
        olympic_summer_path,
        conn,
        table_name="OlympicSummer_jk645",
        create_table_sql="src/sql/create_sql_olympic_summer.sql",
    )

    conn.close()

    return True


def perform_analytics():

    conn = ETLHelper.connect_db(
        type_of_database="databricks",
        database_conn={
            "server_hostname": server_host_name,
            "http_path": http_path,
            "access_token": access_token,
        },
    )

    result = ETLHelper.fetchall_result(conn, "src/sql/analytical_query.sql", {})
    print(result)

    return True


if __name__ == "__main__":
    extract_to_databricks()
    perform_analytics()
