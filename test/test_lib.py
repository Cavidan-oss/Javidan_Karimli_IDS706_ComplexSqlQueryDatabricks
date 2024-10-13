import pytest
from src.lib import ETLHelper
import os


@pytest.fixture
def databricks_connection():
    """Fixture to establish a real Databricks connection and close it after use."""

    server_host_name = os.getenv("SERVER_HOSTNAME")
    http_path = os.getenv("HTTP_PATH")
    access_token = os.getenv("ACCESS_TOKEN")

    conn = ETLHelper.connect_db(
        type_of_database="databricks",
        database_conn={
            "server_hostname": server_host_name,
            "http_path": http_path,
            "access_token": access_token,
        },
    )
    yield conn

    conn.close()


def test_connect_db_databricks(databricks_connection):
    """Test connecting to Databricks using real connection."""
    assert databricks_connection is not None


def test_execute_query_databricks(databricks_connection):
    """Test executing a query on Databricks using real connection."""
    query = "SELECT 1"

    result = ETLHelper.execute_query(databricks_connection, query)

    assert result is True


def test_fetchall_result_databricks(databricks_connection):
    """Test fetching results from a real query on Databricks."""
    query = "select * from ids706_data_engineering.default.olympicdictionary_jk645"
    query_params = {}

    result = ETLHelper.fetchall_result(databricks_connection, query, query_params)

    assert len(result) != 0
