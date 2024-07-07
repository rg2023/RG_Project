from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from database import create_clickhouse_table, insert_data_to_clickhouse, save_to_mongodb
from exchange_rate import get_exchange_rates
from storage import save_to_minio, fetch_from_minio
from processing import process_exchange_rate_data

default_args = {
    'owner': 'Rachel',
    'start_date': datetime.now(),
    'retries': 3,
}

dag = DAG(
    'exchange_rate_dag',
    default_args=default_args,
    description='A simple DAG to process exchange rates',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 5, 7),
    catchup=False,
)

def fetch_and_store_exchange_rates():
    base_currency = "USD"
    exchange_rate_data = get_exchange_rates(base_currency)
    save_to_minio(exchange_rate_data)

def load_to_clickhouse():
    csv_data = fetch_from_minio()
    insert_data_to_clickhouse(csv_data)

def process_and_save_to_mongodb():
    exchange_rate_data = get_exchange_rates("USD")
    df = process_exchange_rate_data(exchange_rate_data)
    save_to_mongodb(df, 'mydatabase', 'exchange_rates')

with dag:
    create_table = PythonOperator(
        task_id='create_clickhouse_table',
        python_callable=create_clickhouse_table,
    )

    fetch_and_store = PythonOperator(
        task_id='fetch_and_store_exchange_rates',
        python_callable=fetch_and_store_exchange_rates,
    )

    load_clickhouse = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_to_clickhouse,
    )

    process_and_save = PythonOperator(
        task_id='process_and_save_to_mongodb',
        python_callable=process_and_save_to_mongodb,
    )

    create_table >> fetch_and_store >> load_clickhouse >> process_and_save
