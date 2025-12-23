import io
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone

from scripts.load.write_to_minio import upload_csv_to_minio

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

PLACES_CSV_PATH = BASE_DIR / "seeds" / "locations.csv"

if not PLACES_CSV_PATH.exists():
    raise FileNotFoundError(f"Places CSV not found: {PLACES_CSV_PATH}")


def main():
    with open(PLACES_CSV_PATH, "rb") as f:
        csv_bytes = f.read()

    object_name = (
        "bronze/places/"
        f"places_raw_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    )

    upload_csv_to_minio(
        object_name=object_name,
        csv_bytes=csv_bytes
    )

    print("[OK] Places raw CSV uploaded to bronze layer")


if __name__ == "__main__":
    main()
