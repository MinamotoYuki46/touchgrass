from pathlib import Path
import io
import pandas as pd
from minio import Minio
import os
import logging

LOG = logging.getLogger("analytics")

MINIO_OBJECT_KEY = "silver/screen_time_history.csv" 


def _get_minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT")
    access = os.getenv("MINIO_ACCESS_KEY")
    secret = os.getenv("MINIO_SECRET_KEY")
    
    if not access or not secret:
        LOG.error("MinIO Credentials not found in environment variables inside analytics script.")
    
    return Minio(endpoint, access_key=access, secret_key=secret, secure=False)

def _read_csv_from_minio(bucket_name: str, object_name: str) -> pd.DataFrame:
    client = _get_minio_client()
    try:
        LOG.info(f"Attempting to fetch Bucket: {bucket_name}, Key: {object_name}")
        
        resp = client.get_object(bucket_name, object_name)
        raw = resp.read()
        resp.close()
        resp.release_conn()
        
        return pd.read_csv(
            io.BytesIO(raw), 
            parse_dates=["timestamp_local"], 
            keep_default_na=False
        )
    except Exception as exc:
        LOG.error(f"Error reading from MinIO: {exc}")
        return pd.DataFrame()

def compute_daily_trend(last_n_days: int = None):
    bucket = os.getenv("MINIO_BUCKET", "touchgrass")

    df = _read_csv_from_minio(bucket, MINIO_OBJECT_KEY)

    if df.empty:
        LOG.warning("DataFrame is empty after MinIO read.")
        return []

    if "local_date" not in df.columns or df["local_date"].isnull().all():
        if "timestamp_local" in df.columns:
            df["local_date"] = pd.to_datetime(df["timestamp_local"]).dt.date.astype(str)
        elif "timestamp_utc" in df.columns:
            df["local_date"] = pd.to_datetime(df["timestamp_utc"]).dt.date.astype(str)
        else:
            LOG.error("No timestamp column found in CSV")
            return []

    grouped = df.groupby("local_date")["minutes_spent"].max().reset_index()
    grouped = grouped.sort_values("local_date", ascending=False)

    if last_n_days:
        grouped = grouped.head(last_n_days)

    grouped = grouped.sort_values("local_date", ascending=True)

    return grouped.to_dict(orient="records")

if __name__ == "__main__":
    from dotenv import load_dotenv
    BASE_DIR = Path(__file__).resolve().parents[3]
    load_dotenv(BASE_DIR / ".env")

    print(compute_daily_trend(7))