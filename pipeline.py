from interview.utils import get_clickhouse_client
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import pandas as pd
import clickhouse_driver
import os


CLICKHOUSE_CLIENT = get_clickhouse_client()

default_args = {
    "start_date": datetime(2024, 1, 1)
}


with DAG(
    dag_id="datamarts.daily_revenue_per_country",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    transactions_sensor = S3KeySensor(
        task_id="transactions_sensor",
        bucket_key="data/transactions_{}.csv".format(datetime.now().strftime("%Y-%m-%d")),
        bucket_name="my-bucket",
        aws_conn_id="aws_default",
        timeout=600,
        poke_interval=30,
        mode="poke"
    )

    def extract_from_s3(**kwargs):
        df = pd.read_csv("s3://my-bucket/data/transactions_{}.csv".format(datetime.now().strftime("%Y-%m-%d")))
        kwargs["ti"].xcom_push(key="df", value=df.to_dict())

    def load_to_raw_table(**kwargs):
        df = pd.DataFrame(kwargs["ti"].xcom_pull(task_ids="extract", key="df"))
        rows = [tuple(r) for r in df[["transaction_id", "user_id", "amount", "created_at"]].to_numpy()]
        CLICKHOUSE_CLIENT.execute(
            "INSERT INTO raw.transactions (transaction_id, user_id, amount, created_at) VALUES",
            rows
        )

    def build_aggregate_view():
        query = """
            INSERT INTO datamarts.daily_revenue_per_country
            SELECT
                toDate(r.created_at) as event_date,
                u.country,
                sum(r.amount) as total_revenue
            FROM raw.transactions r
            LEFT JOIN core.userMetadata u ON r.user_id = u.UserId
            WHERE toDate(r.created_at) = '{}'
            GROUP BY event_date, u.country
        """.format(datetime.now().strftime("%Y-%m-%d"))
        CLICKHOUSE_CLIENT.execute(query)

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_from_s3,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_to_raw_table,
        provide_context=True
    )

    aggregate = PythonOperator(
        task_id="aggregate",
        python_callable=build_aggregate_view
    )

    transactions_sensor >> extract >> load >> aggregate
