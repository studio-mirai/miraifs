from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_ID = "0x4d69fd2cd66445fcd9a9d4b6d3b611f833408ae2b83a4e4a6936dfefaf677f70"

DOWNLOADS_DIR = PROJECT_DIR / "downloads"

Path().mkdir(
    parents=True,
    exist_ok=True,
)
