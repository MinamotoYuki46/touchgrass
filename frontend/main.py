#!/usr/bin/env python3
"""
app.py — Flask microservice to serve recommendations from Gold (MinIO).

- Reads MinIO credentials from environment / .env
- Finds latest object under gold/recommendations/
- Returns JSON payload for frontend consumption at /api/recommendations
- Renders templates/index.html for root if available (fallback simple message)
"""

from pathlib import Path
import io
import json
import logging
import os
from typing import Optional, Any, Dict, List

from flask import Flask, jsonify, render_template, abort
from dotenv import load_dotenv
from minio import Minio

# ----------------------------
# Config & init
# ----------------------------
BASE_DIR = Path(__file__).resolve().parents[1] if (Path(__file__).resolve().parents and len(Path(__file__).resolve().parents) > 0) else Path(".")
# load local .env if present
load_dotenv(BASE_DIR / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "touchgrass")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError("MINIO_ACCESS_KEY / MINIO_SECRET_KEY must be set in environment or .env")

# Use secure=False for local MinIO without TLS
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

LOG = logging.getLogger("flask_app")
logging.basicConfig(level=logging.INFO)

app = Flask(__name__, template_folder=str(BASE_DIR / "frontend" / "templates"))

# ----------------------------
# Helpers
# ----------------------------
def _list_minio_objects(prefix: str) -> List[Any]:
    """
    Return list of objects from MinIO under `prefix`.
    Defensively handle None or exceptions.
    """
    try:
        it = minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
        if it is None:
            return []
        return list(it)
    except Exception as exc:
        LOG.warning("Failed to list objects in MinIO (%s): %s", prefix, exc)
        return []


def _safe_latest_object_name(objects: List[Any]) -> Optional[str]:
    """
    Given list from MinIO list_objects, return latest object name by lexical sort.
    Returns None if nothing valid found.
    """
    if not objects:
        return None

    valid: List[str] = []
    for o in objects:
        name = getattr(o, "object_name", None)
        if name:
            valid.append(name)

    if not valid:
        return None

    # objects are saved timestamped (recommendations_YYYYMMDD_HHMMSS.json)
    valid.sort(reverse=True)
    return valid[0]


def get_latest_gold() -> Optional[Dict]:
    """
    Fetch latest gold JSON payload from MinIO.
    Returns parsed dict or None.
    """
    prefix = "gold/recommendations/"
    objects = _list_minio_objects(prefix)
    latest_name = _safe_latest_object_name(objects)
    if not latest_name:
        LOG.info("No gold objects found under %s", prefix)
        return None

    try:
        resp = minio_client.get_object(MINIO_BUCKET, latest_name)
    except Exception as exc:
        LOG.error("Failed to get object %s: %s", latest_name, exc)
        return None

    try:
        raw = resp.read()
        parsed = json.loads(raw)
        return parsed
    except Exception as exc:
        LOG.error("Failed to parse JSON from %s: %s", latest_name, exc)
        return None
    finally:
        try:
            resp.close()
            resp.release_conn()
        except Exception:
            pass

# ----------------------------
# Routes
# ----------------------------
@app.route("/")
def home():
    # serve template if exists, else a simple HTML fallback
    try:
        return render_template("index.html")
    except Exception:
        return (
            "<html><head><title>Recommendations API</title></head>"
            "<body><h2>Recommendations API</h2>"
            "<p>Use <a href='/api/recommendations'>/api/recommendations</a> to get latest recommendations JSON.</p>"
            "</body></html>"
        )


@app.route("/api/recommendations")
def api_recommendations():
    """
    Return latest recommendations produced by the pipeline.
    Standardized response:
    {
      "timestamp": <generated_at>,
      "context": { ... },
      "decision": { ... },
      "recommendations": [ ... ]
    }
    If no gold present, returns 204 No Content with a small JSON.
    """
    gold = get_latest_gold()
    if not gold:
        return jsonify({"status": "NO_DATA", "message": "No recommendation available yet"}), 204

    # Normalize fields for frontend convenience
    timestamp = gold.get("generated_at")
    context = gold.get("context", {})
    decision = gold.get("decision", {})
    recs = gold.get("recommendations", [])

    # Optionally enrich/normalize each recommendation (safe access)
    normalized_recs = []
    for r in recs:
        # keep fields that are useful for UI; don't assume full schema
        normalized_recs.append({
            "name": r.get("location_name") or r.get("place_name") or r.get("location") or r.get("name"),
            "address": r.get("address"),
            "category": r.get("location_category") or r.get("category"),
            "latitude": r.get("latitude"),
            "longitude": r.get("longitude"),
            "distance_km": r.get("distance_km"),
            "score": r.get("priority_score") or r.get("score"),
            "raw": r  # include raw for debugging or extra fields
        })

    payload = {
        "timestamp": timestamp,
        "context": {
            "screen_time_minutes": context.get("screen_time_minutes"),
            "screen_time_level": context.get("screen_time_level"),
            "weather_category": context.get("weather_category"),
            "weather_ok": context.get("weather_ok"),
            "user_location": {
                "lat": context.get("user_lat"),
                "lng": context.get("user_lon")
            }
        },
        "decision": decision,
        "recommendations": normalized_recs
    }

    return jsonify(payload)


# ----------------------------
# Health check (useful for docker-compose healthcheck)
# ----------------------------
@app.route("/health")
def health():
    # lightweight check: MinIO reachable and bucket exists
    try:
        # don't raise if bucket missing, just report false
        exists = minio_client.bucket_exists(MINIO_BUCKET)
        status = {"minio_ok": bool(exists)}
        http_code = 200 if exists else 503
        return jsonify(status), http_code
    except Exception as exc:
        LOG.error("Health check failed: %s", exc)
        return jsonify({"minio_ok": False, "error": str(exc)}), 503


# ----------------------------
# Run
# ----------------------------
if __name__ == "__main__":
    # In container, bind 0.0.0.0 so host can reach it
    HOST = os.getenv("FLASK_HOST", "0.0.0.0")
    PORT = int(os.getenv("FLASK_PORT", "5000"))
    DEBUG = os.getenv("FLASK_DEBUG", "0") not in ("0", "false", "False")
    LOG.info("Starting Flask on %s:%s (debug=%s), MinIO=%s", HOST, PORT, DEBUG, MINIO_ENDPOINT)
    app.run(host=HOST, port=PORT, debug=DEBUG)
