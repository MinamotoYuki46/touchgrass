from typing import Dict

# Default rule tables; these can be overridden by config/rules.yaml
DEFAULT_DISTANCE_SCORE = [
    (0.3, 4.0),
    (0.7, 3.0),
    (1.5, 2.0),
    (3.0, 1.0)
]

DEFAULT_CATEGORY_SCORE = {
    "park": 3.0,
    "outdoor": 3.0,
    "recreation": 2.5,
    "sport": 2.5,
    "cafe": 1.5,
    "mall": 1.0
}

DEFAULT_WEATHER_MULTIPLIER = {
    "clear": 1.0,
    "cloudy": 0.8,
    "rain": 0.0,
    "storm": 0.0,
    "unknown": 0.5
}


def _distance_score(distance_km: float, table=DEFAULT_DISTANCE_SCORE) -> float:
    for thr, score in table:
        if distance_km <= thr:
            return score
    return 0.0


def _category_score(category: str, table=DEFAULT_CATEGORY_SCORE) -> float:
    if not category:
        return 0.5
    return float(table.get(category.lower(), 0.5))


def _weather_multiplier(weather_category: str, table=DEFAULT_WEATHER_MULTIPLIER) -> float:
    return float(table.get((weather_category or "").lower(), 0.5))


def compute_priority_score(distance_km: float, category: str, weather_category: str, rules: Dict = None) -> float:
    """Return priority score (0..10 approx)"""
    if rules is None:
        dist_table = DEFAULT_DISTANCE_SCORE
        cat_table = DEFAULT_CATEGORY_SCORE
        weather_table = DEFAULT_WEATHER_MULTIPLIER
    else:
        dist_table = rules.get("distance_score_table", DEFAULT_DISTANCE_SCORE)
        cat_table = rules.get("category_score", DEFAULT_CATEGORY_SCORE)
        weather_table = rules.get("weather_multiplier", DEFAULT_WEATHER_MULTIPLIER)

    raw = _distance_score(distance_km, dist_table) + _category_score(category, cat_table)
    mult = _weather_multiplier(weather_category, weather_table)
    score = raw * mult
    # optionally scale to 0..10; current raw max ~7 -> scale factor 1.4
    score = min(round(score, 2), 10.0)
    return score