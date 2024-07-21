import magic
import logging
from hashlib import blake2b
from pathlib import Path
import typer
from pysui import SyncClient, SuiConfig, handle_result
from rich import print
from miraifs_sdk import PACKAGE_ID
from pysui.sui.sui_txresults.single_tx import ObjectRead
from pysui.sui.sui_txresults.complex_tx import TxResponse
from miraifs_sdk.miraifs import MiraiFs, Chunk
from miraifs_sdk.utils import (
    calculate_hash,
    int_to_bytes,
    chunk_data,
)
import json

from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_types import (
    SuiU8,
    SuiU32,
    SuiString,
    ObjectID,
)


app = typer.Typer()

MAX_CHUNK_SIZE_BYTES = 128_000

config = SuiConfig.default_config()
client = SyncClient(config)


def print_tx_result(result):
    print(result.effects.status)
    print(result.effects.created)
    print(result.events)
    computation_cost = int(result.effects.gas_used.computation_cost)
    storage_cost = int(result.effects.gas_used.storage_cost)
    total_cost = computation_cost + storage_cost
    print(f"Computation Cost: {computation_cost / 10**9} SUI ({computation_cost})")  # fmt: skip
    print(f"Storage Cost: {storage_cost / 10**9} SUI ({storage_cost})")  # fmt: skip
    print(f"Storage Rebate: {int(result.effects.gas_used.storage_rebate) / 10**9} SUI")  # fmt: skip
    print(f"Total Cost: {total_cost} MIST ({total_cost / 10**9} SUI)")


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
) -> list[Chunk]:
    with open(path, "rb") as f:
        data = f.read()
    chunked_data = chunk_data(data, chunk_size)
    chunks: list[Chunk] = []
    for i, data_chunk in enumerate(chunked_data):
        chunk_data_hash = calculate_hash(data_chunk).digest()
        chunk_identifier_hash = calculate_unique_chunk_hash(chunk_data_hash, i).digest()
        chunk = Chunk(
            data=list(data_chunk),
            hash=list(chunk_identifier_hash),
            index=i,
        )
        chunks.append(chunk)
    return chunks


def calculate_file_verification_hash(
    chunks: list[Chunk],
) -> blake2b:
    chunk_hashes = [chunk.hash for chunk in chunks]
    return calculate_hash(b"".join([bytes(hash) for hash in chunk_hashes]))


@app.command()
def create_chunks(
    file_id: str = typer.Argument(),
    path: Path = typer.Argument(),
):
    mfs = MiraiFs()
    file = mfs.get_file(file_id)

    chunks = load_chunks(path, file.chunk_size)
    chunks_by_hash = {bytes(chunk.hash): chunk for chunk in chunks}

    total_cost = 0
    for create_chunk_cap in file.create_chunk_caps:
        result = mfs.create_chunk(
            create_chunk_cap,
            chunks_by_hash[bytes(create_chunk_cap.hash)],
        )
        if isinstance(result, TxResponse):
            print(f"Uploaded Chunk #{create_chunk_cap.index}: {result.effects.transaction_digest}")  # fmt: skip
            computation_cost = int(result.effects.gas_used.computation_cost)
            storage_cost = int(result.effects.gas_used.storage_cost)
            total_cost = total_cost + computation_cost + storage_cost
        else:
            raise Exception(f"Unable to upload Chunk #{create_chunk_cap.index}")

    print(f"Total Cost: {total_cost} MIST ({total_cost / 10**9} SUI)")


# Cost: 21725 MIST/byte


def estimate_upload_cost_in_mist(
    byte_count: int,
) -> int:
    return byte_count * 21725


@app.command()
def create(
    path: Path = typer.Argument(...),
    chunk_size: int = typer.Option(MAX_CHUNK_SIZE_BYTES),
):
    chunks = load_chunks(path, chunk_size)
    verification_hash = calculate_file_verification_hash(chunks)
    mime = magic.Magic(mime=True)
    mime_type = str(mime.from_file(path))

    print(f"Chunk Count: {len(chunks)}")
    print(f"File Size: {sum([len(chunk.data) for chunk in chunks])} bytes")
    print(f"MIME Type: {mime_type}")
    print(f"Verification Hash: {list(verification_hash.digest())}")
    print(f"Estimated Cost: {estimate_upload_cost_in_mist(sum([len(chunk.data) for chunk in chunks])) / 10**9} SUI")  # fmt: skip
    typer.confirm("Do you want to upload this file?", abort=True)

    txer = SuiTransaction(
        client=client,
        merge_gas_budget=True,
    )
    file, verify_file_cap = txer.move_call(
        target=f"{PACKAGE_ID}::file::new",
        arguments=[
            SuiU32(chunk_size),
            SuiString(mime_type),
            [SuiU8(e) for e in list(verification_hash.digest())],
            ObjectID("0x6"),
        ],
    )
    create_chunk_caps = []
    for chunk in chunks:
        create_chunk_cap = txer.move_call(
            target=f"{PACKAGE_ID}::file::add_chunk_hash",
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
        target=f"{PACKAGE_ID}::file::verify",
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

    if isinstance(result, TxResponse):
        print(f"Computation Cost: {int(result.effects.gas_used.computation_cost) / 10**9} SUI")  # fmt: skip
        print(f"Storage Cost: {int(result.effects.gas_used.storage_cost) / 10**9} SUI")  # fmt: skip
        print(f"Storage Rebate: {int(result.effects.gas_used.storage_rebate) / 10**9} SUI")  # fmt: skip
        for event in result.events:
            if event.event_type == f"{PACKAGE_ID}::file::FileCreatedEvent":
                file_id = json.loads(event.parsed_json.replace("'", '"'))["file_id"]
                print("\nRun the command below to upload file chunks to MiraiFS.")
                print(f"mfs file create-chunks {file_id} {path}")
    return


@app.command()
def view(
    file_id: str = typer.Argument(),
):
    mfs = MiraiFs()
    file = mfs.get_file(file_id)
    print(file)
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
