import os
import requests
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

ORS_API_KEY = os.getenv("ORS_API_KEY")
if not ORS_API_KEY:
    raise RuntimeError("ORS_API_KEY not found")

# Dummy coordinates
start_lat, start_lon = -3.4415, 114.8326
end_lat, end_lon = -3.4429, 114.8353

url = "https://api.openrouteservice.org/v2/directions/driving-car"
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

response = requests.post(url, json=payload, headers=headers, timeout=10)
response.raise_for_status()

data = response.json()

distance_km = data["routes"][0]["summary"]["distance"] / 1000

print("=== ROUTE DISTANCE TEST ===")
print(f"Distance : {distance_km:.2f} km")
