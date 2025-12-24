import os
import io
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
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

def get_latest_places_object():
    objects = list(client.list_objects(
        MINIO_BUCKET,
        prefix="bronze/places/",
        recursive=True
    ))

    if not objects:
        raise RuntimeError("No bronze places files found")

    objects.sort(key=lambda o: o.object_name, reverse=True)
    return objects[0].object_name


def read_csv(object_name: str) -> pd.DataFrame:
    resp = client.get_object(MINIO_BUCKET, object_name)
    df = pd.read_csv(resp)
    resp.close()
    resp.release_conn()
    return df


def main():
    bronze_object = get_latest_places_object()
    print(f"[INFO] Using bronze places: {bronze_object}")

    bronze_df = read_csv(bronze_object)

    required_cols = {
        "location_id",
        "location_name",
        "address",
        "location_category",
        "latitude",
        "longitude",
        "google_maps_link"
    }

    if not required_cols.issubset(bronze_df.columns):
        raise RuntimeError(f"Bronze places missing columns: {required_cols}")

    now = datetime.now(timezone.utc).isoformat()

    silver_df = bronze_df.rename(columns={
        "location_category": "category"
    })

    silver_df["is_active"] = True
    silver_df["updated_at_utc"] = now

    silver_df = (
        silver_df
        .sort_values("updated_at_utc")
        .drop_duplicates(subset=["location_id"], keep="last")
        .reset_index(drop=True)
    )

    csv_bytes = silver_df.to_csv(index=False).encode("utf-8")

    client.put_object(
        MINIO_BUCKET,
        "silver/places.csv",
        data=io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv"
    )

    print("[OK] silver/places.csv upserted")


if __name__ == "__main__":
    main()
