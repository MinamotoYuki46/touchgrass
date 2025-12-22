import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from minio import Minio
from pathlib import Path
from io import BytesIO


BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")

LATITUDE = -3.375
LONGITUDE = 114.625

OPEN_METEO_URL = os.getenv("OPEN_METEO_URL")

def fetch_weather():
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "temperature_2m,uv_index,weathercode",
        "timezone": "Asia/Singapore",
        "forecast_days": 1
    }

    response = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def upload_to_minio(client, data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"bronze/weather/weather_raw_{timestamp}.json"

    payload = json.dumps(data).encode("utf-8")

    client.put_object(
        BUCKET,
        object_name,
        data=BytesIO(payload),
        length=len(payload),
        content_type="application/json"
    )


def main():
    print("Fetching weather from Open-Meteo...")
    weather_data = fetch_weather()

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    upload_to_minio(client, weather_data)
    print("Weather data uploaded to bronze layer.")


if __name__ == "__main__":
    main()
