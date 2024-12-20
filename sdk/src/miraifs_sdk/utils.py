import base64
import hashlib
import json
import logging
import subprocess
from hashlib import blake2b
from pathlib import Path
from typing import Any

import magic
import zstandard as zstd
from miraifs_sdk.models import Chunk, ChunkRaw, ParsedEvent
from pysui.sui.sui_txresults.complex_tx import Event


def get_mime_type_for_file(
    path: Path,
) -> str:
    mime = magic.Magic(mime=True)
    mime_type = str(mime.from_file(path))
    return mime_type


def calculate_unique_chunk_hash(
    chunk_hash: bytes,
    chunk_index: int,
) -> blake2b:
    chunk_index_bytes = b"\x00" + int_to_bytes(chunk_index)
    logging.debug(f"Identifier Hash Input: {list(chunk_index_bytes + chunk_hash)}")
    return calculate_hash(chunk_index_bytes + chunk_hash)


def load_chunks(
    path: Path,
    chunk_size: int,
) -> list[ChunkRaw]:
    with open(path, "rb") as f:
        data = f.read()
    chunked_data = chunk_data(data, chunk_size)
    chunks: list[ChunkRaw] = []
    for i, data_chunk in enumerate(chunked_data):
        chunk_data_hash = calculate_hash(data_chunk).digest()
        chunk_identifier_hash = calculate_unique_chunk_hash(chunk_data_hash, i).digest()
        chunk = ChunkRaw(
            data=list(data_chunk),
            hash=list(chunk_identifier_hash),
            index=i,
        )
        chunks.append(chunk)
    return chunks


def calculate_chunks_manifest_hash(
    chunks: list[Chunk],
) -> blake2b:
    chunk_hashes = [chunk.hash for chunk in chunks]
    return calculate_hash(b"".join([bytes(hash) for hash in chunk_hashes]))


def split_list(
    input_list: list[int],
) -> list[list[int]]:
    main_sublists = []
    for i in range(0, len(input_list), 10000):
        chunk = input_list[i : i + 10000]
        sublists = [chunk[j : j + 500] for j in range(0, len(chunk), 500)]
        main_sublists.append(sublists)
    return main_sublists


def get_zstd_version():
    result = subprocess.run(
        ["zstd", "--version"], capture_output=True, text=True, check=False
    )
    output = result.stdout.strip()
    return output


def bytes_to_u256(
    data: bytes,
):
    return int.from_bytes(data, "big")


def int_to_bytes(
    number: int,
):
    """
    Convert an integer to its byte representation covering u8 to u256.

    Args:
    number (int): The integer to be converted to bytes.

    Returns:
    bytes: The byte representation of the integer.
    """
    if number == 0:
        return b"\x00"
    num_bytes = (number.bit_length() + 7) // 8
    return number.to_bytes(num_bytes, byteorder="big")


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


def calculate_hash_str(
    data: bytes,
) -> str:
    hash = hashlib.blake2b(data, digest_size=32).hexdigest()
    return hash


def calculate_hash_u256(
    data: bytes,
) -> int:
    hash = hashlib.blake2b(data, digest_size=32)
    return bytes_to_u256(hash.digest())


def calculate_hash(
    data: bytes,
) -> blake2b:
    return hashlib.blake2b(data, digest_size=32)


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


def chunk_data(
    data: Any,
    chunk_size: int,
):
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def split_bytes(
    data: bytes,
    chunk_size: int,
):
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


def estimate_upload_cost_in_mist(
    byte_count: int,
) -> int:
    return byte_count * 21785


def parse_events(
    events: list[Event],
) -> list[ParsedEvent]:
    parsed_events: list[ParsedEvent] = []
    for event in events:
        parsed_events.append(
            ParsedEvent(
                package=event.package_id,
                event_data=json.loads(event.parsed_json.replace("'", '"')),
                event_type=event.event_type,
            )
        )
    return parsed_events
