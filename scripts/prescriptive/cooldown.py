from datetime import datetime, time, timezone
from .distance import haversine_km

# default cooldown config
DEFAULT_COOLDOWN_METERS = 100.0
DEFAULT_COOLDOWN_MINUTES = 120
DEFAULT_ACTIVE_HOURS = (7, 22)  # 07:00 - 22:00 local time considered "active"


def is_in_cooldown(last_lat, last_lon, last_ts, current_lat, current_lon, now_ts=None,
                   radius_m=DEFAULT_COOLDOWN_METERS, cooldown_minutes=DEFAULT_COOLDOWN_MINUTES,
                   active_hours=DEFAULT_ACTIVE_HOURS):
    """Return (bool, reason, seconds_remaining)

    last_ts and now_ts are aware datetimes (preferred). If now_ts is None, use UTC now.
    """
    if now_ts is None:
        now_ts = datetime.now(timezone.utc)

    # 1) distance-based cooldown
    if last_lat is not None and last_lon is not None and current_lat is not None and current_lon is not None:
        dist_km = haversine_km(last_lat, last_lon, current_lat, current_lon)
        dist_m = dist_km * 1000.0
        if dist_m <= radius_m:
            # still near previous position
            return True, "user_not_moved", cooldown_minutes * 60

    # 2) active hours
    local_hour = now_ts.astimezone().hour
    start_h, end_h = active_hours
    if not (start_h <= local_hour < end_h):
        return True, "outside_active_hours", None

    return False, None, 0