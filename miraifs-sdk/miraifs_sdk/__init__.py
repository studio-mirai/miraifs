from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_ID = "0xcb6f734a53302ddf547f835457877880f3732b8c94c03d630b9ce558237f27f5"

DOWNLOADS_DIR = PROJECT_DIR / "downloads"

DOWNLOADS_DIR.mkdir(
    parents=True,
    exist_ok=True,
)
