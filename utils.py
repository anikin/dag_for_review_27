import clickhouse_driver
from airflow.hooks.base import *


def get_clickhouse_client():
    conn = BaseHook.get_connection("clickhouse_default")
    return clickhouse_driver.Client(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )
