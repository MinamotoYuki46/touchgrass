from datetime import datetime, timezone, timedelta
import os
import sys
from dotenv import load_dotenv
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore
from scripts.load.write_to_minio import upload_json_to_minio

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

FIREBASE_KEY = os.getenv("FIREBASE_SERVICE_ACCOUNT")
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID")
COLLECTION_NAME = "screen_time_logs"

if not FIREBASE_KEY or not FIREBASE_PROJECT_ID:
    print("[ERROR] Firebase env not set", file=sys.stderr)
    sys.exit(1)

if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_KEY)
    firebase_admin.initialize_app(cred, {"projectId": FIREBASE_PROJECT_ID})

db = firestore.client()

def normalize_ts(ts):
    if ts is None:
        return None
    return ts.astimezone(timezone.utc).isoformat()

def extract_history_7_days():
    print("[INFO] Starting 7-day history extraction...")
    now = datetime.now(timezone.utc)
    seven_days_ago = now - timedelta(days=7)


    docs = db.collection(COLLECTION_NAME) \
             .where("timestamp", ">=", seven_days_ago) \
             .order_by("timestamp", direction=firestore.Query.DESCENDING) \
             .stream()

    records = []
    for doc in docs:
        data = doc.to_dict()
        records.append({
            "device": data.get("device"),
            "minutes_spent": data.get("minutes_spent"),
            "timestamp_utc": normalize_ts(data.get("timestamp")),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude")
        })

    payload = {
        "source": "firebase.history",
        "collection": COLLECTION_NAME,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "strategy": "last_7_days",
        "total_records": len(records),
        "records": records
    }


    object_name = f"bronze/screen_time_history/history_{now.strftime('%Y%m%d_%H%M%S')}.json"
    upload_json_to_minio(object_name=object_name, data=payload)

    print(f"[OK] Extracted {len(records)} records to {object_name}")

if __name__ == "__main__":
    extract_history_7_days()