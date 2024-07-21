from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_ID = "0x8bc863f221c3142c314ee871c811dbf48a67b84a451c4f78473117b1cb583c75"

DOWNLOADS_DIR = PROJECT_DIR / "downloads"

DOWNLOADS_DIR.mkdir(
    parents=True,
    exist_ok=True,
)
