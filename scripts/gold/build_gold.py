import json
from pathlib import Path
from datetime import datetime, timezone

from scripts.prescriptive.read_silver import get_latest_screen_time, get_latest_user_location, get_latest_weather, get_places
from scripts.prescriptive.distance import haversine_km
from scripts.prescriptive.priority import compute_priority_score
from scripts.prescriptive.cooldown import is_in_cooldown
from scripts.prescriptive.decide import decide
from minio import Minio
from dotenv import load_dotenv
import os
import io

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

GOLD_PREFIX = "gold/recommendations/"
LATEST_NAME = GOLD_PREFIX + "latest.json"


def _minio_client():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

def classify_screen_time(minutes: int, rules: dict) -> str:
    thresholds = rules.get("screen_time", {}).get("thresholds_minutes", {})
    if not thresholds:
        return "unknown"

    if minutes >= thresholds.get("critical", 480):
        return "critical"
    if minutes >= thresholds.get("high", 300):
        return "high"
    if minutes >= thresholds.get("medium", 180):
        return "medium"
    return "low"



def build_and_write_gold(rules: dict = None, top_n: int = 10):
    screen = get_latest_screen_time() or {}
    screen_minutes = int(screen.get("minutes_spent", 0))
    screen_level = classify_screen_time(screen_minutes, rules or {})

    loc = get_latest_user_location() or {}
    weather = get_latest_weather() or {}
    places_df = get_places()

    user_lat = loc.get("latitude")
    user_lon = loc.get("longitude")

    candidates = []
    for _, r in places_df.iterrows():
        try:
            place_lat = float(r.get("latitude"))
            place_lon = float(r.get("longitude"))
        except Exception:
            continue

        dist_km = haversine_km(user_lat, user_lon, place_lat, place_lon) if (user_lat and user_lon) else None
        score = compute_priority_score(dist_km or 9999.0, r.get("category"), weather.get("weather_category"), rules)

        candidates.append({
            "location_id": r.get("location_id"),
            "location_name": r.get("location_name"),
            "category": r.get("category"),
            "address": r.get("address"),
            "latitude": place_lat,
            "longitude": place_lon,
            "distance_km": round(dist_km, 3) if dist_km is not None else None,
            "priority_score": score,
            "google_maps_link": r.get("google_maps_link"),
            "is_active": r.get("is_active")
        })

    # filter active
    candidates = [c for c in candidates if str(c.get("is_active")).lower() not in ("false", "0", "none", "")]

    # sort
    candidates = sorted(candidates, key=lambda x: x.get("priority_score", 0), reverse=True)

    # cooldown check (use last location == current location heuristic)
    cooldown_active = False
    # naive: use last two location points? For now we check last known vs itself
    # You can improve by storing last decision state
    cooldown_active = is_in_cooldown(
        loc.get("latitude"), loc.get("longitude"), loc.get("resolved_at_utc"),
        loc.get("latitude"), loc.get("longitude"), None
    )

    decision = decide(screen_time_minutes=int(screen.get("minutes_spent", 0)), ranked_recommendations=candidates, cooldown_active=cooldown_active)

    context = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "screen_time_minutes": int(screen.get("minutes_spent", 0)),
        "screen_time_level": screen_level,
        "user_lat": user_lat,
        "user_lon": user_lon,
        "weather_category": weather.get("weather_category"),
        "temperature_c": weather.get("temperature_c")
    }

    gold_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "context": context,
        "decision": decision,
        "recommendations": candidates[:top_n]
    }

    # write to minio
    client = _minio_client()
    payload = json.dumps(gold_payload).encode("utf-8")
    client.put_object(MINIO_BUCKET, LATEST_NAME, data=io.BytesIO(payload), length=len(payload), content_type="application/json")
    # also write timestamped copy for optional history
    timestamped = GOLD_PREFIX + f"recommendations_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    client.put_object(MINIO_BUCKET, timestamped, data=io.BytesIO(payload), length=len(payload), content_type="application/json")

    return gold_payload


if __name__ == "__main__":
    print(build_and_write_gold())
