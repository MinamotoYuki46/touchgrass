from pathlib import Path
import io
import json
import os
from datetime import datetime, timezone
import logging

import duckdb
import pandas as pd
import yaml
from dotenv import load_dotenv
from minio import Minio
from typing import Optional, Any

# project utils
from scripts.utils.geo import is_cooldown_active
from scripts.utils.ors import route_distance_km

# -----------------------
# Setup & config
# -----------------------
BASE_DIR = Path(__file__).resolve().parents[1]  # analytics/..
load_dotenv(BASE_DIR / ".env")

# Paths
QUERY_DIR = BASE_DIR / "analytics" / "queries"
RULES_PATH = BASE_DIR / "config" / "rules.yaml"
TMP_DIR = BASE_DIR / ".tmp_duckdb"
TMP_DIR.mkdir(exist_ok=True)

# MinIO config (required)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "touchgrass")

if not MINIO_ENDPOINT:
    raise RuntimeError("MINIO_ENDPOINT not configured in .env")

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Silver object paths in MinIO
SILVER_OBJECTS = {
    "screen_time": "silver/screen_time.csv",
    "user_location": "silver/user_location.csv",
    "weather": "silver/weather.csv",
    "places": "silver/places.csv",
}

# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("run_duckdb")

# -----------------------
# Helpers
# -----------------------
def read_csv_from_minio(object_name: str, dst_path: Path) -> None:
    """
    Download object from MinIO and write to dst_path.
    Raises RuntimeError if object not found or unreadable.
    """
    try:
        resp = minio_client.get_object(MINIO_BUCKET, object_name)
    except Exception as exc:
        raise RuntimeError(f"Failed to get {object_name} from MinIO: {exc}") from exc

    try:
        data = resp.read()
    finally:
        try:
            resp.close()
            resp.release_conn()
        except Exception:
            pass

    dst_path.write_bytes(data)


def safe_max_object_name(objects: list) -> Optional[str]:
    """
    Given list of MinIO objects, return the object_name with max sort
    or None if not found. Defensively handle None object_name.
    """
    if not objects:
        return None

    # convert to list of tuples (object_name_str, obj)
    valid = []
    for o in objects:
        name = getattr(o, "object_name", None)
        if name is None:
            # skip malformed entry
            continue
        valid.append((name, o))

    if not valid:
        return None

    # sort by name lexical desc and return top name
    valid.sort(key=lambda t: t[0], reverse=True)
    return valid[0][0]


# -----------------------
# Load rules
# -----------------------
rules = yaml.safe_load(open(RULES_PATH))

SCREEN_TIME_HIGH = int(rules["screen_time"]["thresholds_minutes"]["high"])
ACTIVE_START = rules["user_activity"]["active_hours"]["start"]
ACTIVE_END = rules["user_activity"]["active_hours"]["end"]

DIST_MIN = float(rules["distance"]["min_km"])
DIST_MAX = float(rules["distance"]["max_km"])
RESET_DIST_KM = float(rules["distance"]["reset_cooldown_if_move_km"])

COOLDOWN_MINUTES = int(rules["cooldown"]["minutes"])
MAX_RESULTS = int(rules["recommendation"]["max_results"])

IDEAL_MIN = float(rules["distance"].get("ideal_range_km", {}).get("min", DIST_MIN))
IDEAL_MAX = float(rules["distance"].get("ideal_range_km", {}).get("max", min(DIST_MAX, 3.0)))

# -----------------------
# Step 0: fetch latest silver CSVs from MinIO to temporary files
# -----------------------
local_paths = {}
for key, obj_key in SILVER_OBJECTS.items():
    dst = TMP_DIR / f"{key}.csv"
    try:
        read_csv_from_minio(obj_key, dst)
        local_paths[key] = dst
        log.info("Downloaded %s → %s", obj_key, dst)
    except Exception as e:
        # If required file missing, we should fail early (DuckDB queries expect files)
        log.error("Failed to download %s: %s", obj_key, e)
        raise

# -----------------------
# Step 1: Initialize DuckDB and register views
# -----------------------
con = duckdb.connect(database=":memory:")

for name, path in local_paths.items():
    # read_csv_auto expects path as POSIX
    con.execute(f"""
        CREATE VIEW {name} AS
        SELECT * FROM read_csv_auto('{path.as_posix()}');
    """)

# -----------------------
# Step 2: Evaluate context (screen time, weather, latest user location)
# -----------------------
ctx_sql = (QUERY_DIR / "evaluate_context.sql").read_text()
ctx_sql = ctx_sql.replace("{{SCREEN_TIME_HIGH}}", str(SCREEN_TIME_HIGH))

ctx_row = con.execute(ctx_sql).fetchone()

# ctx_row expected fields from evaluate_context.sql:
# total_minutes, screen_time_level, weather_category, weather_ok, user_latitude, user_longitude, location_available
# Be defensive: ensure tuple length and default values.
(total_minutes,
 screen_time_level,
 weather_category,
 weather_ok,
 user_latitude,
 user_longitude,
 location_available) = (
    ctx_row + (None,) * (7 - len(ctx_row))
) if ctx_row is not None else (0, "normal", None, False, None, None, False)

# Normalize numeric/boolean types
total_minutes = int(total_minutes or 0)
screen_time_level = str(screen_time_level or "normal")
weather_ok = bool(weather_ok)
user_lat = float(user_latitude) if user_latitude not in (None, "") else None
user_lon = float(user_longitude) if user_longitude not in (None, "") else None
location_available = bool(location_available)

now_utc = datetime.now(timezone.utc)
local_time_str = now_utc.astimezone().strftime("%H:%M")
active_hours_ok = (ACTIVE_START <= local_time_str <= ACTIVE_END)

# -----------------------
# Step 3: Load latest Gold to determine cooldown baseline (type-safe)
# -----------------------
last_gold_payload: Optional[dict] = None
last_generated_at: Optional[datetime] = None
last_user_lat: Optional[float] = None
last_user_lon: Optional[float] = None

objects_iter = minio_client.list_objects(
    MINIO_BUCKET,
    prefix="gold/recommendations/",
    recursive=True
)
objects = list(objects_iter) if objects_iter is not None else []

# pick latest object_name safely
latest_name = safe_max_object_name(objects)

if latest_name:
    try:
        resp = minio_client.get_object(MINIO_BUCKET, latest_name)
        try:
            raw = resp.read()
        finally:
            try:
                resp.close()
                resp.release_conn()
            except Exception:
                pass
        last_gold_payload = json.loads(raw)
    except Exception as exc:
        log.warning("Failed reading latest gold %s: %s", latest_name, exc)
        last_gold_payload = None

if last_gold_payload:
    gen_at = last_gold_payload.get("generated_at")
    try:
        if gen_at:
            last_generated_at = datetime.fromisoformat(gen_at)
    except Exception:
        # fallback: ignore malformed timestamp
        last_generated_at = None

    ctx = last_gold_payload.get("context", {})
    last_user_lat = ctx.get("user_lat")
    last_user_lon = ctx.get("user_lon")
    # normalize to floats if present
    try:
        last_user_lat = float(last_user_lat) if last_user_lat not in (None, "") else None
        last_user_lon = float(last_user_lon) if last_user_lon not in (None, "") else None
    except Exception:
        last_user_lat = last_user_lon = None

# -----------------------
# Step 4: Cooldown check (context-aware)
# -----------------------
cooldown_active = is_cooldown_active(
    last_ts=last_generated_at,
    last_lat=last_user_lat,
    last_lon=last_user_lon,
    current_lat=user_lat,
    current_lon=user_lon,
    cooldown_minutes=COOLDOWN_MINUTES,
    reset_distance_km=RESET_DIST_KM
)

# -----------------------
# Step 5: Decision gate (single reason)
# -----------------------
decision = {"should_go_out": False, "reason": None}

if not active_hours_ok:
    decision["reason"] = "outside_active_hours"
elif cooldown_active:
    decision["reason"] = "cooldown_active"
elif screen_time_level != "high":
    decision["reason"] = "screen_time_not_high"
elif not weather_ok:
    decision["reason"] = "weather_not_ok"
elif not location_available:
    decision["reason"] = "location_unavailable"
else:
    decision["should_go_out"] = True
    decision["reason"] = "eligible"

# -----------------------
# Step 6: If eligible -> prepare places_enriched via ORS & run recommendation SQL
# -----------------------
recommendations: list = []

if decision["should_go_out"]:
    # get places base table into dataframe
    places_df = con.execute("SELECT * FROM places").fetchdf()

    enriched_rows = []
    for _, row in places_df.iterrows():
        lat = row.get("latitude")
        lon = row.get("longitude")
        # skip if coordinates missing
        if lat in (None, "") or lon in (None, ""):
            continue
        try:
            # ORS may fail or API key missing; handle per-row defensively
            dist_km = route_distance_km(
                float(user_lat), float(user_lon),
                float(lat), float(lon)
            )
        except Exception as exc:
            log.warning("ORS failed for place %s: %s", row.get("location_name"), exc)
            # skip this place if ORS fails for it
            continue

        row_data = row.to_dict()
        row_data["distance_km"] = float(dist_km)
        enriched_rows.append(row_data)

    if enriched_rows:
        places_enriched_df = pd.DataFrame(enriched_rows)
    else:
        places_enriched_df = pd.DataFrame(columns=list(places_df.columns) + ["distance_km"])

    # register enriched table in DuckDB
    con.register("places_enriched", places_enriched_df)

    # load recommendation SQL, replace placeholders (parameters come from rules)
    rec_sql = (QUERY_DIR / "generate_recommendations.sql").read_text()
    rec_sql = (
        rec_sql
        .replace("{{MIN_DISTANCE_KM}}", str(DIST_MIN))
        .replace("{{MAX_DISTANCE_KM}}", str(DIST_MAX))
        .replace("{{IDEAL_MIN_KM}}", str(IDEAL_MIN))
        .replace("{{IDEAL_MAX_KM}}", str(IDEAL_MAX))
        .replace("{{MAX_RESULTS}}", str(MAX_RESULTS))
    )

    try:
        rec_df = con.execute(rec_sql).fetchdf()
        # ensure numeric types and round distance if present
        if "distance_km" in rec_df.columns:
            rec_df["distance_km"] = rec_df["distance_km"].astype(float).round(2)
        recommendations = rec_df.to_dict(orient="records")
    except Exception as exc:
        log.error("Recommendation SQL failed: %s", exc)
        recommendations = []

# -----------------------
# Step 7: Build Gold payload
# -----------------------
gold_payload = {
    "generated_at": now_utc.isoformat(),
    "context": {
        "screen_time_minutes": total_minutes,
        "screen_time_level": screen_time_level,
        "weather_category": weather_category,
        "weather_ok": bool(weather_ok),
        "user_lat": user_lat,
        "user_lon": user_lon,
        "active_hours_ok": active_hours_ok,
        "cooldown_active": cooldown_active
    },
    "decision": decision,
    "recommendations": recommendations
}

# -----------------------
# Step 8: Write Gold to MinIO (timestamped, append)
# -----------------------
obj_name = f"gold/recommendations/recommendations_{now_utc.strftime('%Y%m%d_%H%M%S')}.json"
payload_bytes = json.dumps(gold_payload, indent=2).encode("utf-8")

try:
    minio_client.put_object(
        MINIO_BUCKET,
        obj_name,
        data=io.BytesIO(payload_bytes),
        length=len(payload_bytes),
        content_type="application/json"
    )
    log.info("Gold written to MinIO: %s", obj_name)
except Exception as exc:
    log.error("Failed to write gold to MinIO: %s", exc)
    raise

# -----------------------
# End
# -----------------------
