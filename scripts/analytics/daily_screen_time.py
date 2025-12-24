from pathlib import Path
import io
import pandas as pd
from minio import Minio
import os
import logging

# Setup logger agar kita bisa melihat error di log container
LOG = logging.getLogger("analytics")

# Definisikan key object MinIO sebagai string (relative path), bukan absolute file path.
# Sesuaikan string ini dengan lokasi file SEBENARNYA di dalam bucket MinIO Anda.
# Jika di MinIO file ada di root bucket folder silver, gunakan "silver/screen_time.csv"
MINIO_OBJECT_KEY = "touchgrass/silver/screen_time.csv" 
# Alternatif jika Anda benar-benar menyimpan full path /app/... sebagai key di MinIO:
# MINIO_OBJECT_KEY = "/app/minio_data/touchgrass/silver/screen_time.csv"

def _get_minio_client():
    # Pindahkan pengambilan ENV ke dalam fungsi (Lazy Loading)
    # Ini menjamin env var sudah ter-load oleh main.py sebelum fungsi ini jalan
    endpoint = os.getenv("MINIO_ENDPOINT")
    access = os.getenv("MINIO_ACCESS_KEY")
    secret = os.getenv("MINIO_SECRET_KEY")
    
    if not access or not secret:
        LOG.error("MinIO Credentials not found in environment variables inside analytics script.")
    
    return Minio(endpoint, access_key=access, secret_key=secret, secure=False)

def _read_csv_from_minio(bucket_name: str, object_name: str) -> pd.DataFrame:
    client = _get_minio_client()
    try:
        # Debugging: Print apa yang sedang dicoba diakses
        LOG.info(f"Attempting to fetch Bucket: {bucket_name}, Key: {object_name}")
        
        resp = client.get_object(bucket_name, object_name)
        raw = resp.read()
        resp.close()
        resp.release_conn()
        
        return pd.read_csv(
            io.BytesIO(raw), 
            parse_dates=["timestamp_utc", "timestamp_local"], 
            keep_default_na=False
        )
    except Exception as exc:
        # Log error spesifik agar muncul di docker logs
        LOG.error(f"Error reading from MinIO: {exc}")
        # Kembalikan DataFrame kosong agar tidak crash, tapi tercatat di log
        return pd.DataFrame()

def compute_daily_minutes(last_n_days: int = None):
    # Ambil bucket name saat fungsi dipanggil
    bucket = os.getenv("MINIO_BUCKET", "touchgrass")
    
    # Gunakan key yang sudah dibersihkan
    df = _read_csv_from_minio(bucket, MINIO_OBJECT_KEY)

    if df.empty:
        LOG.warning("DataFrame is empty after MinIO read.")
        return []

    # Normalisasi kolom tanggal
    if "local_date" not in df.columns or df["local_date"].isnull().all():
        if "timestamp_local" in df.columns:
            df["local_date"] = pd.to_datetime(df["timestamp_local"]).dt.date.astype(str)
        elif "timestamp_utc" in df.columns:
            df["local_date"] = pd.to_datetime(df["timestamp_utc"]).dt.date.astype(str)
        else:
            LOG.error("No timestamp column found in CSV")
            return []

    grouped = df.groupby("local_date")["minutes_spent"].sum().reset_index()
    grouped = grouped.sort_values("local_date", ascending=False)

    if last_n_days:
        grouped = grouped.head(last_n_days)

    return grouped.to_dict(orient="records")

if __name__ == "__main__":
    # Untuk testing lokal/CLI, kita perlu load dotenv manual di sini
    from dotenv import load_dotenv
    BASE_DIR = Path(__file__).resolve().parents[3]
    load_dotenv(BASE_DIR / ".env")
    
    print(compute_daily_minutes(7))