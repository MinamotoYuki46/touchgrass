from pathlib import Path
import io
import pandas as pd
from minio import Minio
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")


def _minio_client():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def _read_csv(object_name: str) -> pd.DataFrame:
    client = _minio_client()
    resp = client.get_object(MINIO_BUCKET, object_name)
    raw = resp.read()
    resp.close()
    resp.release_conn()
    return pd.read_csv(io.BytesIO(raw), keep_default_na=False)


def get_latest_screen_time():
    obj = "silver/screen_time.csv"
    df = _read_csv(obj)
    if df.empty:
        return None
    # pick the latest by timestamp_utc
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    row = df.sort_values("timestamp_utc", ascending=False).iloc[0]
    return row.to_dict()


def get_latest_user_location():
    obj = "silver/user_location.csv"
    df = _read_csv(obj)
    if df.empty:
        return None
    # location uses resolved_at_utc
    df["resolved_at_utc"] = pd.to_datetime(df["resolved_at_utc"])
    row = df.sort_values("resolved_at_utc", ascending=False).iloc[0]
    return row.to_dict()


def get_latest_weather():
    obj = "silver/weather.csv"
    df = _read_csv(obj)
    if df.empty:
        return None
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    row = df.sort_values("timestamp_utc", ascending=False).iloc[0]
    return row.to_dict()


def get_places():
    obj = "silver/places.csv"
    df = _read_csv(obj)
    return df


if __name__ == "__main__":
    print(get_latest_screen_time())
    print(get_latest_user_location())
    print(get_latest_weather())