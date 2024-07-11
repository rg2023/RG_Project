from clickhouse_connect import get_client
from pymongo import MongoClient
from datetime import datetime
import csv
import io

def create_clickhouse_table():
    client = get_client(
        host='m7mpjrqan6.europe-west4.gcp.clickhouse.cloud',
        user='default',
        password='0T~Xu.tMtqeui',
        secure=True
    )

    create_table_query = """
    CREATE TABLE IF NOT EXISTS exchange_rates (
        documentation String,
        terms_of_use String,
        time_last_update_unix UInt32,
        time_last_update_utc DateTime,
        time_next_update_unix UInt32,
        time_next_update_utc DateTime,
        base_currency String,
        currency String,
        rate Float64
    ) ENGINE = MergeTree()
    ORDER BY time_last_update_unix
    """

    drop_table_query = "DROP TABLE IF EXISTS exchange_rates"
    client.command(drop_table_query)
    print("Table 'exchange_rates' dropped successfully.")
    client.command(create_table_query)
    print("Table 'exchange_rates' created successfully.")

def insert_data_to_clickhouse(csv_data: str):
    client = get_client(
        host='m7mpjrqan6.europe-west4.gcp.clickhouse.cloud',
        user='default',
        password='0T~Xu.tMtqeui',
        secure=True
    )
    create_clickhouse_table()
    rows = list(csv.DictReader(io.StringIO(csv_data)))
    columns = ['documentation', 'terms_of_use', 'time_last_update_unix', 'time_last_update_utc', 'time_next_update_unix', 'time_next_update_utc', 'base_currency', 'currency', 'rate']
    data_tuples = [(row['documentation'], row['terms_of_use'], int(row['time_last_update_unix']), datetime.strptime(row['time_last_update_utc'], '%Y-%m-%d %H:%M:%S'), int(row['time_next_update_unix']), datetime.strptime(row['time_next_update_utc'], '%Y-%m-%d %H:%M:%S'), row['base_code'], row['currency'], float(row['rate'])) for row in rows]

    client.insert('exchange_rates', data_tuples, column_names=columns)
    print("Data successfully inserted into ClickHouse.")

def save_to_mongodb(df, db_name: str, collection_name: str):
    cluster = MongoClient("mongodb+srv://rachel058326:213319%21Aa@rg-cluster.iwef4ht.mongodb.net/?retryWrites=true&w=majority&appName=RG-cluster")
    db = cluster[db_name]
    collection = db[collection_name]
    collection.delete_many({})
    data = df.reset_index().rename(columns={'index': 'currency'}).to_dict(orient='records')

    formatted_data = []
    for item in data:
        item['_id'] = item['id']
        del item['id']
        formatted_data.append(item)

    collection.insert_many(formatted_data)
    print("Data inserted successfully into MongoDB.")
