from .rules_loader import load_rules


def compute_priority_score(candidate: dict) -> float:
    rules = load_rules()

    s = rules["scoring"]
    w = s["weights"]

    dist_cfg = s["distance_score"]
    d = candidate["distance_km"]

    if d <= dist_cfg["near_km"]:
        distance_score = 1.0
    elif d >= dist_cfg["far_km"]:
        distance_score = 0.0
    else:
        distance_score = 1.0 - ((d - dist_cfg["near_km"]) / (dist_cfg["far_km"] - dist_cfg["near_km"]))

    category_score = s["category_score"].get(candidate["category"], 0.0)
    crowd_score = s["crowd_score"].get(candidate["crowd_level"], 0.0)
    weather_score = s["weather_score"].get(candidate["weather"], 0.0)

    score = (
        w["distance"] * distance_score +
        w["category"] * category_score +
        w["crowd"] * crowd_score +
        w["weather"] * weather_score
    )

    return round(score, 3)
