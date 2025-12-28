import os
import requests
from pathlib import Path
from typing import Tuple
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

ORS_BASE_URL = "https://api.openrouteservice.org"
ORS_API_KEY = os.getenv("ORS_API_KEY")

if not ORS_API_KEY:
    raise RuntimeError("ORS_API_KEY not found")


def route_distance_km(origin, destination):
    url = "https://api.openrouteservice.org/v2/directions/driving-car"
    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json",
    }

    payload = {
        "coordinates": [
            [origin[1], origin[0]],
            [destination[1], destination[0]],
        ]
    }

    response = requests.post(url, json=payload, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json()

    route = data["routes"][0]

    if "summary" in route and "distance" in route["summary"]:
        dist_m = route["summary"]["distance"]
    else:
        dist_m = sum(seg["distance"] for seg in route["segments"])

    return dist_m / 1000.0

