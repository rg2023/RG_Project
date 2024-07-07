import asyncio
from exchange_rate import get_exchange_rates
from storage import save_to_minio, fetch_from_minio
from database import create_clickhouse_table, insert_data_to_clickhouse, save_to_mongodb
from processing import process_exchange_rate_data

async def main():
    base_currency = "USD"  # ניתן לשנות את המטבע הבסיסי בהתאם לצורך
    exchange_rate_data = await get_exchange_rates(base_currency)  # שליפת הנתונים
    await save_to_minio(exchange_rate_data)  # שמירת הנתונים ב-MinIO
    csv_data = fetch_from_minio()  # שליפת הנתונים מ-MinIO
    create_clickhouse_table()  # יצירת הטבלה ב-ClickHouse
    insert_data_to_clickhouse(csv_data)  # הכנסתם ל-ClickHouse
    df = process_exchange_rate_data(exchange_rate_data)  # עיבוד הנתונים
    save_to_mongodb(df, 'Exchange_Rate', 'data')  # שמירת הנתונים ב-MongoDB

if __name__ == "__main__":
    asyncio.run(main())
