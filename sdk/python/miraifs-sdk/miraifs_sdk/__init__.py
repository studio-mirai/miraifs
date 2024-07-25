import os

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

MIRAIFS_PACKAGE_ID = os.environ["MIRAIFS_PACKAGE_ID"]

PROJECT_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = PROJECT_DIR / "downloads"

DOWNLOADS_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

MAX_CHUNK_SIZE_BYTES = 128_000
