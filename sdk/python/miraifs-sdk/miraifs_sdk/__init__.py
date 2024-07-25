from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
PACKAGE_ID = "0x44611a0cb1e01550395aa25356abe55a0e0dc4c527543c420f058742c1538c93"
MAX_CHUNK_SIZE_BYTES = 128_000

DOWNLOADS_DIR = PROJECT_DIR / "downloads"

DOWNLOADS_DIR.mkdir(
    parents=True,
    exist_ok=True,
)
