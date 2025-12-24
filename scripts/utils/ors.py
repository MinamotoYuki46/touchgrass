import requests
import os

ORS_API_KEY = os.getenv("ORS_API_KEY")
ORS_URL = "https://api.openrouteservice.org/v2/directions/driving-car"

def route_distance_km(start_lat, start_lon, end_lat, end_lon):
    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "coordinates": [
            [start_lon, start_lat],
            [end_lon, end_lat]
        ]
    }

    r = requests.post(ORS_URL, json=payload, headers=headers, timeout=10)
    r.raise_for_status()
    meters = r.json()["routes"][0]["summary"]["distance"]
    return round(meters / 1000, 2)
