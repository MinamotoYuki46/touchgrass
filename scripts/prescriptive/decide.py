from .rules_loader import load_rules


def decide(screen_time_minutes, ranked_candidates, cooldown_active):
    rules = load_rules()
    thresholds = rules["screen_time"]["thresholds_minutes"]

    if cooldown_active:
        return {"should_go_out": False, "reason": "cooldown_active", "cooldown": True}

    if screen_time_minutes < thresholds["medium"]:
        return {"should_go_out": False, "reason": "screen_time_too_low", "cooldown": False}

    if not ranked_candidates:
        return {"should_go_out": False, "reason": "no_candidates", "cooldown": False}

    return {
        "should_go_out": True,
        "reason": "viable_location",
        "score": ranked_candidates[0]["priority_score"],
        "cooldown": False
    }
