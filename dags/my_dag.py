from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from database import create_clickhouse_table, insert_data_to_clickhouse, save_to_mongodb
from exchange_rate import get_exchange_rates
from storage import save_to_minio, fetch_from_minio
from processing import process_exchange_rate_data
from exchange_rate import ExchangeRateData
import asyncio

default_args = {
    'owner': 'Rachel',
    'start_date': datetime(2024, 5, 7),
    'retries': 3,
}

dag = DAG(
    'dag',
    default_args=default_args,
    description='A simple DAG to process exchange rates',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

async def fetch_exchange_rates(base_currency="USD"):
    return await get_exchange_rates(base_currency)

def fetch_and_store_exchange_rates(**kwargs):
    exchange_rate_data = asyncio.run(fetch_exchange_rates())
    # Convert exchange_rate_data to dictionary
    exchange_rate_dict = exchange_rate_data.__dict__
    kwargs['ti'].xcom_push(key='exchange_rate_data', value=exchange_rate_dict)
    save_to_minio(exchange_rate_dict)

def load_to_clickhouse(**kwargs):
    csv_data = fetch_from_minio()
    insert_data_to_clickhouse(csv_data)

def process_and_save_to_mongodb(**kwargs):
    exchange_rate_dict = kwargs['ti'].xcom_pull(key='exchange_rate_data')
    # Convert dictionary back to ExchangeRateData object if needed
    exchange_rate_data = ExchangeRateData(**exchange_rate_dict)
    df = process_exchange_rate_data(exchange_rate_data)
    save_to_mongodb(df, 'mydatabase', 'exchange_rates')

with dag:
    fetch_and_store = PythonOperator(
        task_id='fetch_and_store_exchange_rates',
        python_callable=fetch_and_store_exchange_rates,
        provide_context=True,
    )

    load_clickhouse = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_to_clickhouse,
        provide_context=True,
    )

    process_and_save = PythonOperator(
        task_id='process_and_save_to_mongodb',
        python_callable=process_and_save_to_mongodb,
        provide_context=True,
    )

    create_table = PythonOperator(
        task_id='create_clickhouse_table',
        python_callable=create_clickhouse_table,
    )

    fetch_and_store >> load_clickhouse >> create_table >> process_and_save
