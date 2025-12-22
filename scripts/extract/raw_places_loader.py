import os
import io
from pathlib import Path
from dotenv import load_dotenv
from minio import Minio

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET", "datalake")

if not MINIO_ENDPOINT:
    raise RuntimeError("MINIO_ENDPOINT not loaded from .env")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

PLACES_CSV_PATH = BASE_DIR / "seeds" / "locations.csv"

if not PLACES_CSV_PATH.exists():
    raise FileNotFoundError(f"Places CSV not found: {PLACES_CSV_PATH}")

def main():
    with open(PLACES_CSV_PATH, "rb") as f:
        csv_bytes = f.read()

    csv_stream = io.BytesIO(csv_bytes)

    client.put_object(
        BUCKET,
        "bronze/places/places_raw.csv",
        data=csv_stream,
        length=len(csv_bytes),
        content_type="text/csv"
    )

    print("places_raw.csv uploaded to bronze layer.")

if __name__ == "__main__":
    main()
