from .rules_loader import load_rules

def classify_screen_time(minutes: int) -> str:
    rules = load_rules()
    thresholds = rules["screen_time"]["thresholds_minutes"]

    if minutes >= thresholds["critical"]:
        return "critical"
    if minutes >= thresholds["high"]:
        return "high"
    if minutes >= thresholds["medium"]:
        return "medium"
    return "low"
