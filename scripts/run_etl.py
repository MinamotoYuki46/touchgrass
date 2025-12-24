import subprocess, sys

STEPS = [
    "scripts.extract.firebase_data",
    "scripts.extract.open_meteo_weather",
    "scripts.extract.raw_places_loader",
    "scripts.transform.split_user_activity",
    "scripts.transform.weather_to_silver",
    "scripts.transform.places_upsert",
    "analytics.run_duckdb",
    "scripts.gold.write_gold_json",
]

for step in STEPS:
    print(f"[RUN] {step}")
    if subprocess.run([sys.executable, "-m", step]).returncode != 0:
        raise RuntimeError(step)
