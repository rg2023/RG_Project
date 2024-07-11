from minio import Minio
from minio.error import S3Error
import io
from exchange_rate import ExchangeRateData

minio_client = Minio(
    "host.docker.internal:9000", 
    access_key="ROOTUSER",
    secret_key="CHANGEME123",
    secure=False
)

bucket_name = "rg-bucket"
file_name = "exchange_rate_data.csv"

def save_to_minio(csv_bytes: bytes):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        minio_client.put_object(
            bucket_name,
            file_name,
            io.BytesIO(csv_bytes),
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