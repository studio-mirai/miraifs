import base64
import hashlib
import subprocess
from hashlib import blake2b
import zstandard as zstd
from typing import Any


def get_zstd_version():
    result = subprocess.run(
        ["zstd", "--version"], capture_output=True, text=True, check=False
    )
    output = result.stdout.strip()
    return output


def encode_file(
    data: bytes,
    encoding: str,
) -> str:
    """Encode a file into a Base64 or Base85 string.

    Args:
    ----
        data (bytes): The file data to encode.
        encoding (str): The encoding type to use (base64 or base85).

    """
    if encoding == "base64":
        file_bytes = base64.b64encode(data)
    elif encoding == "base85":
        file_bytes = base64.b85encode(data)

    return file_bytes.decode("utf-8")


def calculate_hash_u256(
    data: bytes,
) -> int:
    hash = hashlib.blake2b(data, digest_size=32)
    hash_u256 = int.from_bytes(hash.digest(), "big")
    return hash_u256


def calculate_hash_for_bytes(
    data: bytes,
) -> blake2b:
    hash = hashlib.blake2b(data, digest_size=32)
    return hash


def chunk_bytes(
    data: bytes,
    chunk_size: int,
) -> list[bytes]:
    """Split a bytes object into chunks of a specified size."""
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def compress_data(
    data: bytes,
    level: int,
):
    cctx = zstd.ZstdCompressor(level=level)
    compressed_data = cctx.compress(data)
    return compressed_data


def decompress_data(
    data: bytes,
):
    dctx = zstd.ZstdDecompressor()
    decompressed_data = dctx.decompress(data)
    return decompressed_data


def chunk_file_data(
    data: str,
    chunk_size: int = 220,
) -> list[bytes]:
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def split_lists_into_sublists(
    list: list,
    sublist_size: int = 256,
) -> list[list[Any]]:
    return [list[i : i + sublist_size] for i in range(0, len(list), sublist_size)]


def to_mist(
    value: float,
) -> int:
    return int(value * 10**9)


def to_sui(
    value: float,
) -> float:
    return value / 10**9
