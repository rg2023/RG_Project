from clickhouse_connect import get_client
from pymongo import MongoClient
from datetime import datetime
import csv
import io

client = get_client(
    host='m7mpjrqan6.europe-west4.gcp.clickhouse.cloud',
    user='default',
    password='dV1Fkjd3l~1df',
    secure=True
)

def create_clickhouse_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS data (
        base_currency String,
        target_currency String,
        rate Float64
    ) ENGINE = MergeTree()
    ORDER BY target_currency
    """
    drop_table_query = "DROP TABLE IF EXISTS data"
    client.command(drop_table_query)
    print("Table 'exchange_rates' dropped successfully.")
    client.command(create_table_query)
    print("Table 'data' created successfully.")

def insert_data_to_clickhouse(csv_data: str):
    rows = list(csv.DictReader(io.StringIO(csv_data)))
    
    # Define the columns
    columns = ['base_currency', 'target_currency', 'rate']
    
    # Convert rows to tuples
    data_tuples = [
        (
            row['base_currency'], row['target_currency'], float(row['rate'])
        ) for row in rows
    ]
    
    # Insert data into ClickHouse
    client.insert('data', data_tuples, column_names=columns)
    print("Data successfully inserted into ClickHouse.")


def save_to_mongodb(df, db_name: str, collection_name: str):
    cluster = MongoClient("mongodb+srv://rachel058326:213319%21Aa@rg-cluster.iwef4ht.mongodb.net/?retryWrites=true&w=majority&appName=RG-cluster")
    db = cluster[db_name]
    collection = db[collection_name]
    
    # Clear existing data in the collection
    collection.delete_many({})
    
    # Reset index and rename index column to 'currency'
    data = df.reset_index().rename(columns={'index': 'currency'}).to_dict(orient='records')
    formatted_data = [
        {
            'base_currency': item['base_currency'],
            'target_currency': item['target_currency'],
            'rate': item['rate']
        } for item in data
    ]

    collection.insert_many(formatted_data)
    # Insert data into MongoDB collection
    print("Data inserted successfully into MongoDB.")
