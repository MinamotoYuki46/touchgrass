import os
import json
import io
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

def ensure_bucket():
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

def upload_json_to_minio(object_name: str, data: dict):
    ensure_bucket()

    payload = json.dumps(data, indent=2).encode("utf-8")
    buffer = io.BytesIO(payload)

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=buffer,
        length=len(payload),
        content_type="application/json"
    )

    print(f"[MINIO] Uploaded → {MINIO_BUCKET}/{object_name}")

def upload_csv_to_minio(object_name: str, csv_bytes: bytes):
    ensure_bucket()

    buffer = io.BytesIO(csv_bytes)

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=buffer,
        length=len(csv_bytes),
        content_type="text/csv"
    )

    print(f"[MINIO] Uploaded → {MINIO_BUCKET}/{object_name}")
