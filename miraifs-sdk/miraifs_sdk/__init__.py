from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_ID = "0xf1393d749620699a3cf54429b9dd9ae1448cdba8d6b1ccb6c2d6eec83092faed"

DOWNLOADS_DIR = PROJECT_DIR / "downloads"

DOWNLOADS_DIR.mkdir(
    parents=True,
    exist_ok=True,
)
