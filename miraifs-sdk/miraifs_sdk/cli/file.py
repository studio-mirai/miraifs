import base64
import json
import mimetypes
import magic
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
    calculate_hash_for_bytes,
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
from pysui.sui.sui_types import SuiU8, SuiBoolean, SuiString, SuiU256, SuiU64
from pysui.sui.sui_types.bcs import VariableArrayU8


app = typer.Typer()


class FileEncodingSchemes(str, Enum):
    base64 = "base64"
    base85 = "base85"


TEST_PACKAGE_ID = "0x034bdc11d1b627a942d4960b9fde6fac14e634591d68b85d86e8192d16853f16"
MAX_CHUNK_SIZE_BYTES = 32768

# computation_cost=1000000, non_refundable_storage_fee=9880, storage_cost=2766400, storage_rebate=978120
# computation_cost=1000000, non_refundable_storage_fee=9880, storage_cost=2546000, storage_rebate=978120)

config = SuiConfig.default_config()
client = SyncClient(config)


class ChunkMetadata(BaseModel):
    hash: int
    length: int


def calculate_unique_chunk_hash(
    chunk_hash: int,
    chunk_index: int,
) -> int:
    chunk_identifier_hash = calculate_hash_for_bytes(int_to_bytes(chunk_hash) + int_to_bytes(chunk_index))  # fmt: skip
    return bytes_to_u256(chunk_identifier_hash.digest())


@app.command()
def hash():
    with open(
        "/Users/brianli/Documents/GitHub/mirai-labs/miraifs/miraifs-sdk/test_files/text_64kb.txt",
        "rb",
    ) as f:
        data = f.read()

    hash = calculate_hash_for_bytes(data).digest()

    number = 65536
    num_bytes = (number.bit_length() + 7) // 8
    number_bytes = number.to_bytes(num_bytes, byteorder="big")
    print(list(number_bytes))

    return


@app.command()
def create(
    path: Path = typer.Argument(...),
):
    with open(path, "rb") as f:
        data = f.read()

    mime = magic.Magic(mime=True)
    mime_type = str(mime.from_buffer(data))

    chunks = chunk_data(data, MAX_CHUNK_SIZE_BYTES)
    chunk_buckets: list[list[bytes]] = chunk_data(chunks, 256)

    txer = SuiTransaction(
        client=client,
        compress_inputs=True,
        merge_gas_budget=True,
    )
    file = txer.move_call(
        target=f"{TEST_PACKAGE_ID}::file::new",
        arguments=[
            SuiString(mime_type),
        ],
    )
    for bucket in chunk_buckets:
        chunk_hashes_vec = txer.make_move_vector(
            items=[SuiU256(calculate_hash_u256(chunk)) for chunk in bucket],
            item_type="u256",
        )
        txer.move_call(
            target=f"{TEST_PACKAGE_ID}::file::add_chunks",
            arguments=[
                file,
                chunk_hashes_vec,
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
def vec_test(verify: bool = typer.Option(True)):
    with open("/Users/brianli/Documents/GitHub/mirai-labs/miraifs/miraifs-sdk/test_files/text_64kb.txt", "rb") as f:  # fmt: skip
        data = f.read()

    byte_chunks: list[bytes] = chunk_data(data, 32768)
    for i, byte_chunk in enumerate(byte_chunks):
        hash = calculate_hash_u256(byte_chunk)
        byte_chunk_partitions: list[bytes] = chunk_data(byte_chunk, 511)

        txer = SuiTransaction(
            client=client,
            compress_inputs=True,
        )
        chunk, verify_chunk_cap = txer.move_call(
            target=f"{TEST_PACKAGE_ID}::test::new",
            arguments=[
                SuiU256(hash),
                SuiU64(i),
                SuiBoolean(verify),
            ],
        )
        for partition in byte_chunk_partitions:
            partition_vec = txer.make_move_vector(
                items=[SuiU8(e) for e in list(partition)],
                item_type="u8",
            )
            txer.move_call(
                target=f"{TEST_PACKAGE_ID}::test::add_data",
                arguments=[chunk, partition_vec],
            )
        txer.move_call(
            target=f"{TEST_PACKAGE_ID}::test::verify",
            arguments=[verify_chunk_cap, chunk],
        )
        txer.transfer_objects(
            transfers=[chunk],
            recipient=config.active_address,
        )
        result = handle_result(txer.execute(gas_budget=5_000_000_000))
        print(result.effects.status)
        print(f"Computation Cost: {int(result.effects.gas_used.computation_cost) / 10**9} SUI")  # fmt: skip
        print(f"Storage Cost: {int(result.effects.gas_used.storage_cost) / 10**9} SUI")  # fmt: skip
        print(f"Storage Rebate: {int(result.effects.gas_used.storage_rebate) / 10**9} SUI")  # fmt: skip
        print(result.effects.created)
        print(result.events)
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
