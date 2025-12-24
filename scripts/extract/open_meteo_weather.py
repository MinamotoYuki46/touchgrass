import os
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path

from scripts.load.write_to_minio import upload_json_to_minio

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

OPEN_METEO_URL = os.getenv("OPEN_METEO_URL")

LATITUDE = -3.375
LONGITUDE = 114.625


def fetch_weather():
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "temperature_2m,uv_index,weathercode",
        "forecast_hours": 3,
        "timezone": "UTC"
    }

    response = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def main():
    print("[EXTRACT] Fetching weather from Open-Meteo...")
    weather_data = fetch_weather()

    payload = {
        "source": "open-meteo",
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "data": weather_data
    }

    object_name = (
        "bronze/weather/"
        f"weather_raw_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    )

    upload_json_to_minio(
        object_name=object_name,
        data=payload
    )

    print("[OK] Weather data uploaded to bronze layer")


if __name__ == "__main__":
    main()
