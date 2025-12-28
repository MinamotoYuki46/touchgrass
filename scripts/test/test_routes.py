import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

ORS_API_KEY = os.getenv("ORS_API_KEY")
if not ORS_API_KEY:
    raise RuntimeError("ORS_API_KEY not found")

# Dummy coordinates
start_lat, start_lon = -3.3008856, 114.5908285
end_lat, end_lon = -3.2979914, 114.5901445

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

# PRINT FULL JSON RESPONSE
print(json.dumps(data, indent=2))
