from datetime import datetime, timezone
import os
import sys
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore

from scripts.load.write_to_minio import upload_json_to_minio

load_dotenv()

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


def extract_latest_screen_time():
    query = (
        db.collection(COLLECTION_NAME)
        .order_by("timestamp", direction=firestore.Query.DESCENDING)
        .limit(5)
    )

    records = []
    for doc in query.stream():
        d = doc.to_dict() or {}

        records.append({
            "document_id": doc.id,
            "device": d.get("device"),
            "latitude": d.get("latitude"),
            "longitude": d.get("longitude"),
            "minutes_spent": d.get("minutes_spent"),
            "timestamp_utc": normalize_ts(d.get("timestamp"))
        })

    payload = {
        "source": "firebase.firestore",
        "collection": COLLECTION_NAME,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "strategy": f"latest_{5}_records",
        "record_count": len(records),
        "records": records
    }

    object_name = (
        "bronze/user_activity/"
        f"user_activity_latest_5_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    )

    upload_json_to_minio(object_name=object_name, data=payload)

    print(f"[OK] Extracted {len(records)} latest records → {object_name}")


if __name__ == "__main__":
    extract_latest_screen_time()
