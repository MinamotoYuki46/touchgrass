import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPORT_DIR = PROJECT_ROOT / "analytics_data" / "silver"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    print("[EXPORT] Exporting Silver layer to analytics filesystem")

    cmd = [
        "mc", "mirror",
        "localminio/touchgrass/silver",
        str(EXPORT_DIR)
    ]

    subprocess.run(cmd, check=True)

    print(f"[OK] Silver exported to {EXPORT_DIR}")

if __name__ == "__main__":
    main()
