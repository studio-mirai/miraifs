import base64
import hashlib
import subprocess

import zstandard as zstd


def get_zstd_version():
    result = subprocess.run(["zstd", "--version"], capture_output=True, text=True, check=False)
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


def calculate_hash(
    data: str,
) -> str:
    hash = hashlib.blake2b(data.encode("utf-8"), digest_size=32).hexdigest()
    return hash


def calculate_hash_for_bytes(
    data: bytes,
) -> str:
    hash = hashlib.blake2b(data, digest_size=32).hexdigest()
    return hash


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
    sublist_size: int = 511,
) -> list[list[str]]:
    return [list[i : i + sublist_size] for i in range(0, len(list), sublist_size)]
