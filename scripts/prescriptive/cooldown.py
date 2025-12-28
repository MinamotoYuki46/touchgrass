from datetime import datetime, timezone
from .distance import route_distance_km
from .rules_loader import load_rules


def is_in_cooldown(
    last_location,
    current_location,
    now_ts=None,
):
    rules = load_rules()

    if now_ts is None:
        now_ts = datetime.now(timezone.utc)

    active = rules["user_activity"]["active_hours"]
    cooldown_cfg = rules["cooldown"]
    distance_cfg = rules["distance"]

    local_hour = now_ts.astimezone().hour
    start_h = int(active["start"].split(":")[0])
    end_h = int(active["end"].split(":")[0])

    if not (start_h <= local_hour < end_h):
        return True, "outside_active_hours", None

    if last_location is None or current_location is None:
        return False, None, 0

    dist_km = route_distance_km(last_location, current_location)

    if dist_km <= distance_cfg["reset_cooldown_if_move_km"]:
        return True, "user_not_moved", cooldown_cfg["minutes"] * 60

    return False, None, 0
