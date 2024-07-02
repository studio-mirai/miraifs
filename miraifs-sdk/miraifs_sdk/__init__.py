from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_ID = "0xf65f7289a723796ec4af99197d56b9514899778aeebe9660f30a5b29b2a1dfe3"

DOWNLOADS_DIR = PROJECT_DIR / "downloads"

DOWNLOADS_DIR.mkdir(
    parents=True,
    exist_ok=True,
)
