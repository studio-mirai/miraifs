import base64
import json
import mimetypes
import magic
import logging
import itertools
from hashlib import blake2b, md5
from pathlib import Path
from enum import Enum
from pydantic import BaseModel
import typer
from pysui import SyncClient, SuiConfig, handle_result
import time
from pysui.sui.sui_txresults.complex_tx import TxResponse
from rich import print
from rich.progress import Progress, SpinnerColumn, TextColumn
from concurrent.futures import ThreadPoolExecutor, as_completed
from miraifs_sdk import PACKAGE_ID
from miraifs_sdk.miraifs import CreateChunkCap, FileUploadData, MiraiFs, Chunk
from miraifs_sdk.utils import (
    calculate_hash_u256,
    calculate_hash,
    chunk_file_data,
    int_to_bytes,
    chunk_bytes,
    compress_data,
    chunk_data,
    decompress_data,
    bytes_to_u256,
    encode_file,
    split_lists_into_sublists,
)
from miraifs_sdk import DOWNLOADS_DIR

from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_types.bcs import VariableArrayU8
from pysui.sui.sui_types import (
    SuiU8,
    SuiBoolean,
    SuiString,
    SuiU256,
    SuiU64,
    ObjectID,
    SuiArray,
)

logging.basicConfig(level=logging.WARN)

app = typer.Typer()


TEST_PACKAGE_ID = "0xe44f087aecb0baa358934c0d7e3c26ccf8b82b10538cb0cf0447ce7fb6e70a36"
MAX_CHUNK_SIZE_BYTES = 250_000

config = SuiConfig.default_config()
client = SyncClient(config)


class ChunkMetadata(BaseModel):
    hash: int
    length: int


def calculate_unique_chunk_hash(
    chunk_hash: bytes,
    chunk_index: int,
) -> bytes:
    chunk_index_bytes = b"\x00" + int_to_bytes(chunk_index)
    print(f"Identifier Hash Input: {list(chunk_index_bytes + chunk_hash)}")
    chunk_identifier_hash = calculate_hash(chunk_index_bytes + chunk_hash)  # fmt: skip
    return chunk_identifier_hash


@app.command()
def sui():
    print(list(int_to_bytes(12345)))
    return


class Chunk(BaseModel):
    index: int
    data: bytes
    hash: bytes
    size: int


@app.command()
def verify(
    partition_id: str = typer.Argument(...),
):
    txer = SuiTransaction(
        client=client,
        compress_inputs=True,
        merge_gas_budget=True,
    )
    txer.move_call(
        target=f"{TEST_PACKAGE_ID}::test::verify",
        arguments=[
            ObjectID(partition_id),
        ],
    )
    result = handle_result(
        txer.execute(gas_budget=10_000_000_000),
    )
    print(result.effects.status)
    print(result.effects.created)
    print(result.events)
    print(f"Computation Cost: {int(result.effects.gas_used.computation_cost) / 10**9} SUI")  # fmt: skip
    print(f"Storage Cost: {int(result.effects.gas_used.storage_cost) / 10**9} SUI")  # fmt: skip
    print(f"Storage Rebate: {int(result.effects.gas_used.storage_rebate) / 10**9} SUI")  # fmt: skip
    return


# 14496 MIST/byte
# 0.000014496 SUI/byte
# 0.014496 SUI/kilobyte
# 14.496 SUI/megabyte


@app.command()
def u256():
    txer = SuiTransaction(
        client=client,
        compress_inputs=True,
        merge_gas_budget=True,
    )
    chunk = txer.move_call(
        target=f"{TEST_PACKAGE_ID}::chunk::new_u256",
        arguments=[],
    )
    for _ in range(25):
        vec = [[SuiU8(255) for _ in range(500)] for _ in range(20)]
        txer.move_call(
            target=f"{TEST_PACKAGE_ID}::chunk::add_vec_data",
            arguments=[chunk, vec],
        )
    txer.transfer_objects(
        transfers=[chunk],
        recipient=config.active_address,
    )
    result = handle_result(
        txer.execute(gas_budget=10_000_000_000),
    )
    print(result.effects.status)
    print(result.effects.created)
    print(result.events)
    computation_cost = int(result.effects.gas_used.computation_cost)
    storage_cost = int(result.effects.gas_used.storage_cost)
    total_cost = computation_cost + storage_cost
    mist_per_byte = total_cost / 250_000
    sui_per_kilobyte = mist_per_byte * 1024 / 10**9
    sui_per_megabyte = mist_per_byte * 1024 * 1024 / 10**9
    print(f"Computation Cost: {computation_cost / 10**9} SUI ({computation_cost})")  # fmt: skip
    print(f"Storage Cost: {storage_cost / 10**9} SUI ({storage_cost})")  # fmt: skip
    print(f"Storage Rebate: {int(result.effects.gas_used.storage_rebate) / 10**9} SUI")  # fmt: skip
    print(f"Total Cost: {total_cost} MIST ({total_cost / 10**9} SUI)")
    print(f"Cost per byte: {mist_per_byte} MIST/byte")
    print(f"Cost per kilobyte: {sui_per_kilobyte} SUI/kilobyte")
    print(f"Cost per megabyte: {sui_per_megabyte} SUI/megabyte")
    return


@app.command()
def create(
    path: Path = typer.Argument(...),
):
    with open(path, "rb") as f:
        data = f.read()

    mime = magic.Magic(mime=True)
    mime_type = str(mime.from_buffer(data))

    chunked_data = chunk_data(data, MAX_CHUNK_SIZE_BYTES)

    chunks: list[Chunk] = []
    for i, data_chunk in enumerate(chunked_data):
        chunk = Chunk(
            index=i,
            data=data_chunk,
            hash=calculate_hash(data_chunk).digest(),
            size=len(data_chunk),
        )
        chunks.append(chunk)
    # [0, 0, 115, 157, 226, 25, 45, 5, 184, 29, 66, 168, 78, 21, 82, 7, 47, 21, 27, 178, 191, 118, 241, 115, 10, 13, 206, 200, 14, 80, 204, 138, 3, 197]
    # [18, 63, 199, 52, 209, 74, 221, 58, 218, 121, 113, 222, 69, 57, 183, 223, 198, 11, 173, 149, 143, 246, 223, 254, 150, 149, 62, 105, 159, 205, 213, 100]
    # [0, 1, 195, 146, 115, 237, 89, 107, 162, 155, 243, 222, 241, 206, 217, 42, 87, 181, 100, 165, 24, 18, 221, 198, 45, 245, 15, 28, 67, 221, 148, 255, 135, 255]
    # [212, 228, 249, 149, 237, 206, 251, 0, 172, 5, 63, 255, 51, 79, 199, 216, 179, 195, 151, 244, 36, 12, 15, 7, 80, 177, 69, 32, 108, 103, 121, 62]
    chunk_identifier_hashes: list[bytes] = []
    for chunk in chunks:
        chunk_identifier_hash = calculate_unique_chunk_hash(chunk.hash, chunk.index)
        chunk_identifier_hashes.append(chunk_identifier_hash)
        print(f"Index: {chunk.index}")
        print(f"Data Hash: {chunk.hash}")
        print(f"Identifier Hash: {list(chunk_identifier_hash.digest())}")

    verification_hash = calculate_hash(b"".join([hash.digest() for hash in chunk_identifier_hashes]))  # fmt: skip

    txer = SuiTransaction(
        client=client,
        merge_gas_budget=True,
    )
    file, verify_file_cap = txer.move_call(
        target=f"{TEST_PACKAGE_ID}::file::new",
        arguments=[
            SuiString(mime_type),
            [SuiU8(e) for e in list(verification_hash.digest())],
            ObjectID("0x6"),
        ],
    )
    create_chunk_caps = []
    for chunk in chunks:
        create_chunk_cap = txer.move_call(
            target=f"{TEST_PACKAGE_ID}::file::add_chunk_hash",
            arguments=[
                verify_file_cap,
                file,
                [SuiU8(e) for e in list(chunk.hash)],
            ],
        )
        create_chunk_caps.append(create_chunk_cap)
    txer.transfer_objects(
        transfers=create_chunk_caps,
        recipient=config.active_address,
    )
    txer.move_call(
        target=f"{TEST_PACKAGE_ID}::file::verify",
        arguments=[
            verify_file_cap,
            file,
        ],
    )
    txer.transfer_objects(
        transfers=[file],
        recipient=config.active_address,
    )
    result = handle_result(
        txer.execute(gas_budget=50_000_000_000),
    )
    print(result.effects.status)
    print(result.effects.created)
    print(result.events)
    print(f"Computation Cost: {int(result.effects.gas_used.computation_cost) / 10**9} SUI")  # fmt: skip
    print(f"Storage Cost: {int(result.effects.gas_used.storage_cost) / 10**9} SUI")  # fmt: skip
    print(f"Storage Rebate: {int(result.effects.gas_used.storage_rebate) / 10**9} SUI")  # fmt: skip
    return


@app.command()
def merge_gas():
    mfs = MiraiFs()
    gas_coins = mfs.get_all_gas_coins()
    if len(gas_coins) > 2:
        mfs.merge_coins(gas_coins)
        gas_coins = mfs.get_all_gas_coins()
    else:
        print("Minimum gas coin count has already been reached.")

    print(gas_coins)
    return


@app.command()
def split_gas(
    quantity: int = typer.Argument(...),
    value: int = typer.Argument(...),
):
    mfs = MiraiFs()
    gas_coins = mfs.get_all_gas_coins()
    split_gas_coins = mfs.split_coin(
        gas_coins[-1],
        quantity,
        value,
    )
    print(split_gas_coins)
    return
