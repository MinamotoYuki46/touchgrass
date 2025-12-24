from math import radians, sin, cos, acos
from datetime import datetime, timedelta, timezone

def haversine_km(lat1, lon1, lat2, lon2):
    return 6371 * acos(
        cos(radians(lat1)) * cos(radians(lat2)) *
        cos(radians(lon2) - radians(lon1)) +
        sin(radians(lat1)) * sin(radians(lat2))
    )

def is_cooldown_active(
    last_ts,
    last_lat,
    last_lon,
    current_lat,
    current_lon,
    cooldown_minutes,
    reset_distance_km
):
    if last_ts is None:
        return False

    now = datetime.now(timezone.utc)
    if now - last_ts >= timedelta(minutes=cooldown_minutes):
        return False

    if (
        last_lat is not None
        and current_lat is not None
        and haversine_km(last_lat, last_lon, current_lat, current_lon)
        >= reset_distance_km
    ):
        return False

    return True
