import json
from datetime import datetime, timezone
from scripts.load.write_to_minio import upload_json_to_minio


def write_gold_json(
    *,
    context: dict,
    decision: dict,
    recommendations: list
):
    """
    Write GOLD recommendation for UI consumption (JSON).
    """

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "context": context,
        "decision": decision,
        "recommendations": recommendations
    }

    object_name = (
        "gold/recommendations/"
        f"recommendations_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    )

    upload_json_to_minio(
        object_name=object_name,
        data=payload
    )

    print(f"[GOLD] JSON written → {object_name}")
