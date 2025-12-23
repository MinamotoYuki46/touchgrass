import os
import io
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

import pandas as pd
from minio import Minio

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

LOCAL_TZ_OFFSET = timedelta(hours=8)

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

def get_latest_bronze_object(prefix: str) -> str:
    objects = list(client.list_objects(
        MINIO_BUCKET,
        prefix=prefix,
        recursive=True
    ))

    if not objects:
        raise RuntimeError("No bronze user activity files found")

    objects.sort(key=lambda o: o.object_name, reverse=True)
    return objects[0].object_name


def read_json_from_minio(object_name: str) -> dict:
    resp = client.get_object(MINIO_BUCKET, object_name)
    data = json.loads(resp.read().decode("utf-8"))
    resp.close()
    resp.release_conn()
    return data


def upload_csv(object_name: str, df: pd.DataFrame):
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv"
    )
    print(f"[MINIO] Uploaded {object_name}")


def split_user_activity():
    bronze_prefix = "bronze/user_activity/"
    bronze_object = get_latest_bronze_object(bronze_prefix)

    print(f"[INFO] Using bronze file: {bronze_object}")
    payload = read_json_from_minio(bronze_object)

    records = payload.get("records", [])
    if not records:
        raise RuntimeError("No records in bronze payload")

    df = pd.DataFrame(records)

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    df = df.sort_values("timestamp_utc", ascending=False)

    valid_location = df[
        df["latitude"].notna() & df["longitude"].notna()
    ]

    if not valid_location.empty:
        last_lat = valid_location.iloc[0]["latitude"]
        last_lon = valid_location.iloc[0]["longitude"]
        location_source = "last_known"
    else:
        last_lat = None
        last_lon = None
        location_source = "unknown"

    df["timestamp_local"] = df["timestamp_utc"] + LOCAL_TZ_OFFSET
    df["local_date"] = df["timestamp_local"].dt.date

    screen_time_df = df[[
        "device",
        "minutes_spent",
        "timestamp_utc",
        "timestamp_local",
        "local_date"
    ]].copy()

    screen_time_df = screen_time_df.head(1)
    
    user_location_df = pd.DataFrame([{
        "device": df.iloc[0]["device"],
        "latitude": last_lat,
        "longitude": last_lon,
        "location_source": location_source,
        "resolved_at_utc": datetime.now(timezone.utc).isoformat()
    }])


    upload_csv("silver/screen_time.csv", screen_time_df)
    upload_csv("silver/user_location.csv", user_location_df)

    print("[OK] Silver user activity tables updated")


if __name__ == "__main__":
    split_user_activity()
