import os
import json
import io
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
from minio import Minio

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "touchgrass")


client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def get_latest_weather_object():
    objects = list(client.list_objects(
        MINIO_BUCKET,
        prefix="bronze/weather/",
        recursive=True
    ))

    if not objects:
        raise RuntimeError("No bronze weather files found")

    objects.sort(key=lambda o: o.object_name, reverse=True)
    return objects[0].object_name


def read_json(object_name: str) -> dict:
    resp = client.get_object(MINIO_BUCKET, object_name)
    data = json.loads(resp.read().decode("utf-8"))
    resp.close()
    resp.release_conn()
    return data


def weather_category(code: int) -> str:
    if code is None:
        return "unknown"
    if code < 3:
        return "clear"
    if code < 60:
        return "cloudy"
    if code < 80:
        return "rain"
    return "storm"


def main():
    bronze_object = get_latest_weather_object()
    print(f"[INFO] Using bronze weather: {bronze_object}")

    payload = read_json(bronze_object)
    
    data = payload["data"]
    hourly = data["hourly"]

    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    uvs = hourly.get("uv_index", [])
    codes = hourly.get("weathercode", [])

    if not times:
        raise RuntimeError("No hourly weather data found")

    df = pd.DataFrame({
        "timestamp_utc": pd.to_datetime(times, utc=True),
        "temperature_c": temps,
        "uv_index": uvs,
        "weather_code": codes
    })

    now = datetime.now(timezone.utc)
    df["time_diff"] = (df["timestamp_utc"] - now).abs()

    row = df.sort_values("time_diff").iloc[0]

    silver_df = pd.DataFrame([{
        "timestamp_utc": row["timestamp_utc"].isoformat(),
        "temperature_c": float(row["temperature_c"]),
        "uv_index": float(row["uv_index"]),
        "weather_code": int(row["weather_code"]),
        "weather_category": weather_category(row["weather_code"]),
        "horizon_hours": len(df)
    }])

    csv_bytes = silver_df.to_csv(index=False).encode("utf-8")

    client.put_object(
        MINIO_BUCKET,
        "silver/weather.csv",
        data=io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv"
    )

    print("[OK] silver/weather.csv updated")


if __name__ == "__main__":
    main()
