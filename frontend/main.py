#!/usr/bin/env python3
from pathlib import Path
import io
import json
import logging
import os
from typing import Optional, Any, Dict, List

from flask import Flask, jsonify, render_template
from dotenv import load_dotenv
from minio import Minio

# optional analytics helper to supply daily history
from scripts.analytics.daily_screen_time import compute_daily_minutes


# ----------------------------
# Config & init
# ----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")  # .env lives at project root

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError("MINIO_ACCESS_KEY / MINIO_SECRET_KEY must be set in environment or .env")

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

LOG = logging.getLogger("flask_app")
logging.basicConfig(level=logging.INFO)

# ensure we serve templates and static from frontend folder
app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "frontend" / "templates"),
    static_folder=str(BASE_DIR / "frontend" / "static")
)

# ----------------------------
# Helpers: MinIO listing / read
# ----------------------------
def _list_minio_objects(prefix: str) -> List[Any]:
    try:
        it = minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
        if it is None:
            return []
        return list(it)
    except Exception as exc:
        LOG.warning("Failed to list objects in MinIO (%s): %s", prefix, exc)
        return []

def _safe_latest_object_name(objects: List[Any]) -> Optional[str]:
    if not objects:
        return None
    valid: List[str] = []
    for o in objects:
        name = getattr(o, "object_name", None)
        if name:
            valid.append(name)
    if not valid:
        return None
    valid.sort(reverse=True)
    return valid[0]

def _read_json_object(object_name: str) -> Optional[Dict]:
    try:
        resp = minio_client.get_object(MINIO_BUCKET, object_name)
    except Exception as exc:
        LOG.error("Failed to get object %s: %s", object_name, exc)
        return None
    try:
        raw = resp.read()
        return json.loads(raw)
    except Exception as exc:
        LOG.error("Failed to parse JSON from %s: %s", object_name, exc)
        return None
    finally:
        try:
            resp.close(); resp.release_conn()
        except Exception:
            pass

def _map_weather_category_to_label(cat: Optional[str]) -> str:
    if not cat:
        return "Unknown"
    c = str(cat).lower()
    if c in ("clear", "cerah", "sunny"):
        return "Cerah"
    if c in ("cloudy", "clouds", "berawan"):
        return "Berawan"
    if c in ("rain", "hujan"):
        return "Hujan"
    return cat.capitalize()


# ----------------------------
# Routes
# ----------------------------
@app.route("/")
def home():
    try:
        return render_template("index.html")
    except Exception as e:
        LOG.exception("Failed to render template: %s", e)
        return "<h3>Recommendations API</h3><p>Use /api/recommendations</p>"

@app.route("/api/recommendations")
def api_recommendations():
    prefix = "gold/recommendations/"
    objects = _list_minio_objects(prefix)
    latest_name = _safe_latest_object_name(objects)

    if not latest_name:
        return jsonify({"status": "NO_DATA"}), 204

    gold = _read_json_object(latest_name)
    if not gold:
        return jsonify({"status": "INVALID_GOLD"}), 204

    ctx = gold.get("context", {})
    decision = gold.get("decision", {})
    recs = gold.get("recommendations", []) or []

    # -------- core context --------
    screen_time = int(ctx.get("screen_time_minutes", 0))

    weather = {
        "condition": _map_weather_category_to_label(ctx.get("weather_category")),
        "temperature": f"{ctx.get('temperature_c')}°C" if ctx.get("temperature_c") else None,
        "humidity": None
    }

    user_location = {
        "lat": ctx.get("user_lat"),
        "lng": ctx.get("user_lon")
    }

    # -------- history (optional) --------
    daily_spend_time = compute_daily_minutes(7)

    history = [
        {
            "time": r["local_date"],
            "value": int(r["minutes_spent"])
        }
        for r in daily_spend_time
    ]
    

    # -------- recommendation mapping --------
    # sort DESC by priority_score
    recs = sorted(
        recs,
        key=lambda r: float(r.get("priority_score", 0)),
        reverse=True
    )

    TOP_N = 5
    mapped = []

    for i, r in enumerate(recs):
        score = float(r.get("priority_score", 0))

        final_decision = "RECOMMENDED" if i < TOP_N else "WAIT"

        reason = (
            "Jarak dekat dan kondisi lingkungan mendukung"
            if final_decision == "RECOMMENDED"
            else "Skor prioritas lebih rendah dibanding opsi lain"
        )

        mapped.append({
            "place_name": r.get("location_name"),
            "category": r.get("category"),
            "address": r.get("address"),
            "latitude": r.get("latitude"),
            "longitude": r.get("longitude"),
            "distance_km": r.get("distance_km"),
            "score": round(score, 1),
            "final_decision": final_decision,
            "decision_reason": reason,
            "google_maps_link": r.get("google_maps_link")
        })

    return jsonify({
        "screen_time": screen_time,
        "weather": weather,
        "user_location": user_location,
        "screen_time_history": history,
        "recommendations": mapped,
        "decision": decision,
        "generated_at": gold.get("generated_at")
    })

@app.route("/health")
def health():
    try:
        exists = minio_client.bucket_exists(MINIO_BUCKET)
        http_code = 200 if exists else 503
        return jsonify({"minio_ok": bool(exists)}), http_code
    except Exception as exc:
        LOG.error("Health check failed: %s", exc)
        return jsonify({"minio_ok": False, "error": str(exc)}), 503

# ----------------------------
if __name__ == "__main__":
    HOST = os.getenv("FLASK_HOST", "0.0.0.0")
    PORT = int(os.getenv("FLASK_PORT", "5000"))
    DEBUG = os.getenv("FLASK_DEBUG", "0") not in ("0", "false", "False")
    LOG.info("Starting Flask on %s:%s (debug=%s)", HOST, PORT, DEBUG)
    app.run(host=HOST, port=PORT, debug=DEBUG)
