from minio import Minio
from minio.error import S3Error
import io
import csv
from exchange_rate import ExchangeRateData

minio_client = Minio(
    "minio1:9000", 
    access_key="ROOTUSER",
    secret_key="CHANGEME123",
    secure=False
)

bucket_name = "r-bucket"
file_name = "exchange_rate_data.csv"

async def save_to_minio(exchange_rate_data: ExchangeRateData):


    data_dict = exchange_rate_data.to_dict()
    csv_file = io.StringIO()
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["documentation", "terms_of_use", "time_last_update_unix", "time_last_update_utc", "time_next_update_unix", "time_next_update_utc", "base_code", "currency", "rate"])
    for currency, rate in data_dict["conversion_rates"].items():
        csv_writer.writerow([
            data_dict["documentation"],
            data_dict["terms_of_use"],
            data_dict["time_last_update_unix"],
            data_dict["time_last_update_utc"],
            data_dict["time_next_update_unix"],
            data_dict["time_next_update_utc"],
            data_dict["base_code"],
            currency,
            rate
        ])
    csv_bytes = csv_file.getvalue().encode('utf-8')

    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        minio_client.put_object(
            bucket_name,
            file_name,
            data=io.BytesIO(csv_bytes),
            length=len(csv_bytes),
            content_type="text/csv"
        )
        print(f"Data successfully saved to MinIO bucket '{bucket_name}' as '{file_name}'")
    except S3Error as e:
        print(f"MinIO error occurred: {e}")

def fetch_from_minio() -> str:
    try:
        if minio_client.bucket_exists(bucket_name):
            data = minio_client.get_object(bucket_name, file_name)
            return data.read().decode('utf-8')
        else:
            print(f"Bucket '{bucket_name}' does not exist.")
    except S3Error as e:
        print(f"MinIO error occurred: {e}")
