from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pydantic import BaseModel
from typing import Dict
import httpx
from pymongo import MongoClient
import pandas as pd
from minio import Minio
from minio.error import S3Error
pip install minio


# Pydantic Models
class ExchangeRateData(BaseModel):
    documentation: str
    terms_of_use: str
    time_last_update_unix: int
    time_last_update_utc: datetime
    time_next_update_unix: int
    time_next_update_utc: datetime
    base_code: str
    conversion_rates: Dict[str, float]

# Define default arguments
base_currency = "USD"

# Fetch exchange rate data
async def get_exchange_rates(base_currency: str) -> ExchangeRateData:
    api_key = "4e61cf72d3ae31c331eeed6a"
    url =  f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{base_currency}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        if response.status_code == 200:
            data = response.json()
            return ExchangeRateData(
                documentation=data["documentation"],
                terms_of_use=data["terms_of_use"],
                time_last_update_unix=data["time_last_update_unix"],
                time_last_update_utc=datetime.strptime(data["time_last_update_utc"], '%a, %d %b %Y %H:%M:%S %z'),
                time_next_update_unix=data["time_next_update_unix"],
                time_next_update_utc=datetime.strptime(data["time_next_update_utc"], '%a, %d %b %Y %H:%M:%S %z'),
                base_code=data["base_code"],
                conversion_rates=data["conversion_rates"]
            )
        else:
            raise Exception("Failed to fetch data from API")

# Process exchange rate data
def process_exchange_rate_data(exchange_rate_data: ExchangeRateData) -> pd.DataFrame:
    # Create DataFrame from filtered rates
    conversion_rates_df = pd.DataFrame.from_dict(exchange_rate_data.conversion_rates, orient='index', columns=['rate'])
    # Add base_currency column
    conversion_rates_df['base_currency'] = exchange_rate_data.base_code
    # Add running ID column
    conversion_rates_df['id'] = range(1, len(conversion_rates_df) + 1)
    return conversion_rates_df

# Save data to MongoDB
def save_to_mongodb(df: pd.DataFrame, db_name: str, collection_name: str):
    cluster = MongoClient("mongodb+srv://rachel058326:213319%21Aa@rg-cluster.iwef4ht.mongodb.net/?retryWrites=true&w=majority&appName=RG-cluster")
    db = cluster[db_name]
    collection = db[collection_name]

    # Clear existing data in the collection
    collection.delete_many({})

    # Convert DataFrame to list of dictionaries
    data = df.reset_index().rename(columns={'index': 'currency'}).to_dict(orient='records')

    # Prepare data with running ID as _id
    formatted_data = []
    for item in data:
        item['_id'] = item['id']  # Set running ID as _id
        del item['id']  # Remove the original id field
        formatted_data.append(item)

    # Insert new data into MongoDB
    collection.insert_many(formatted_data)
    print("Data inserted successfully into MongoDB.")