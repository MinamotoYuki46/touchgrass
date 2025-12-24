from typing import List, Dict

RECOMMEND_THRESHOLD = 3.5


def decide(screen_time_minutes: int, ranked_recommendations: List[Dict], cooldown_active: bool) -> Dict:
    """Return decision dict

    - should_go_out: True if cooldown not active and top rec >= threshold
    """
    decision = {
        "should_go_out": False,
        "reason": None,
        "cooldown": cooldown_active
    }

    if cooldown_active:
        decision["reason"] = "cooldown_active"
        return decision

    if not ranked_recommendations:
        decision["reason"] = "no_candidates"
        return decision

    top = ranked_recommendations[0]
    if top.get("priority_score", 0) >= RECOMMEND_THRESHOLD:
        decision["should_go_out"] = True
        decision["reason"] = "viable_location"
    else:
        decision["should_go_out"] = False
        decision["reason"] = "no_high_score"

    return decision