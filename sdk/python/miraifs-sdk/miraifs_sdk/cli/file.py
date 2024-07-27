import json
import logging
from hashlib import blake2b
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import magic
import typer
from miraifs_sdk import DOWNLOADS_DIR, MAX_CHUNK_SIZE_BYTES, MIRAIFS_PACKAGE_ID
from miraifs_sdk.miraifs import Chunk, MiraiFs
from miraifs_sdk.models import ChunkRaw
from miraifs_sdk.utils import (
    calculate_hash,
    chunk_data,
    int_to_bytes,
    calculate_file_verification_hash,
    estimate_upload_cost_in_mist,
    load_chunks,
)
from pysui import SuiConfig, SyncClient, handle_result
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_types import ObjectID, SuiString, SuiU8, SuiU32
from rich import print
from miraifs_sdk.utils import to_mist

app = typer.Typer()

config = SuiConfig.default_config()
client = SyncClient(config)


@app.command()
def upload(
    file_id: str = typer.Argument(),
    path: Path = typer.Argument(),
    concurrency: int = typer.Option(16),
    gas_budget_per_chunk: int = typer.Option(3, help="Gas budget per chunk in SUI."),
):
    mfs = MiraiFs()
    file = mfs.get_file(file_id)

    chunks = load_chunks(path, file.chunks.size)
    chunks_by_hash = {bytes(chunk.hash): chunk for chunk in chunks}

    print(f"Loaded {len(chunks)} chunks for File {file_id}")

    total_cost = 0
    create_chunk_caps = mfs.get_create_chunk_caps(file.id)

    gas_coins = mfs.get_all_gas_coins(mfs.config.active_address)
    if len(gas_coins) > 1:
        merged_gas_coin = mfs.merge_coins(gas_coins)
    else:
        merged_gas_coin = gas_coins[0]
    split_gas_coins = mfs.split_coin(
        merged_gas_coin,
        len(create_chunk_caps),
        to_mist(gas_budget_per_chunk),
    )

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for create_chunk_cap, gas_coin in zip(create_chunk_caps, split_gas_coins):
            future = executor.submit(
                mfs.create_chunk,
                create_chunk_cap,
                chunks_by_hash[bytes(create_chunk_cap.hash)],
                gas_coin,
            )
            futures.append(future)

        for future in as_completed(futures):
            result = future.result()
            if isinstance(result, TxResponse):
                computation_cost = int(result.effects.gas_used.computation_cost)
                storage_cost = int(result.effects.gas_used.storage_cost)
                total_cost = total_cost + computation_cost + storage_cost

    print(f"Total Cost: {total_cost} MIST ({total_cost / 10**9} SUI)")
    print("\nRun the command below to register file chunks.")
    print(f"mfs file register {file_id}")


@app.command()
def freeze(
    file_id: str = typer.Argument(),
):
    mfs = MiraiFs()
    file = mfs.get_file(file_id)
    result = mfs.freeze_file(file)
    print(result)
    return


@app.command()
def register(
    file_id: str = typer.Argument(),
):
    mfs = MiraiFs()
    file = mfs.get_file(file_id)
    register_chunk_caps = mfs.get_register_chunk_caps(file)
    result = mfs.register_chunks(file, register_chunk_caps)
    print(result)
    print("\nRun the command below to download the file.")
    print(f"mfs file download {file_id}")
    return


@app.command()
def create(
    path: Path = typer.Argument(...),
    chunk_size: int = typer.Option(MAX_CHUNK_SIZE_BYTES),
):
    chunks = load_chunks(path, chunk_size)
    verification_hash = calculate_file_verification_hash(chunks)
    extension = str(path.suffix.replace(".", ""))
    mime = magic.Magic(mime=True)
    mime_type = str(mime.from_file(path))

    print(f"Chunk Count: {len(chunks)}")
    print(f"File Size: {sum([len(chunk.data) for chunk in chunks])} bytes")
    print(f"MIME Type: {mime_type}")
    print(f"File Extension: {extension}")
    print(f"Verification Hash: {list(verification_hash.digest())}")
    print(f"Estimated Cost: {estimate_upload_cost_in_mist(sum([len(chunk.data) for chunk in chunks])) / 10**9} SUI")  # fmt: skip
    typer.confirm("Do you want to upload this file?", abort=True)

    txer = SuiTransaction(
        client=client,
        merge_gas_budget=True,
    )
    file, verify_file_cap = txer.move_call(
        target=f"{MIRAIFS_PACKAGE_ID}::file::new",
        arguments=[
            SuiU32(chunk_size),
            SuiString(extension),
            SuiString(mime_type),
            [SuiU8(e) for e in list(verification_hash.digest())],
            ObjectID("0x6"),
        ],
    )
    create_chunk_caps = []
    for chunk in chunks:
        create_chunk_cap = txer.move_call(
            target=f"{MIRAIFS_PACKAGE_ID}::file::add_chunk_hash",
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
        target=f"{MIRAIFS_PACKAGE_ID}::file::verify",
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
        txer.execute(gas_budget=5_000_000_000),
    )
    if isinstance(result, TxResponse):
        if result.effects.status.status == "success":
            print(f"Computation Cost: {int(result.effects.gas_used.computation_cost) / 10**9} SUI")  # fmt: skip
            print(f"Storage Cost: {int(result.effects.gas_used.storage_cost) / 10**9} SUI")  # fmt: skip
            print(f"Storage Rebate: {int(result.effects.gas_used.storage_rebate) / 10**9} SUI")  # fmt: skip
            for event in result.events:
                if event.event_type == f"{MIRAIFS_PACKAGE_ID}::file::FileCreatedEvent":
                    file_id = json.loads(event.parsed_json.replace("'", '"'))["file_id"]
                    print("\nRun the command below to upload file chunks to MiraiFS.")
                    print(f"mfs file upload {file_id} {path}")
        else:
            print(result.effects.status.error)
    return


@app.command()
def delete(
    file_id: str = typer.Argument(),
):
    mfs = MiraiFs()
    file = mfs.get_file(file_id)
    result = mfs.delete_file(file)
    print(result)
    return


@app.command()
def view(
    file_id: str = typer.Argument(),
    convert_hashes: bool = typer.Option(True),
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


@app.command()
def download(
    file_id: str = typer.Argument(),
    file_name: str = typer.Option(None),
):
    mfs = MiraiFs()
    file = mfs.get_file(file_id)
    chunks = mfs.get_chunks_for_file(file)
    file_bytes = b"".join(bytes(chunk.data) for chunk in chunks)
    if not file_name:
        file_name = file.id
    with open(DOWNLOADS_DIR / f"{file_name}.{file.extension}", "wb") as f:
        f.write(file_bytes)
    print(f"File downloaded to {DOWNLOADS_DIR / f'{file_name}.{file.extension}'}")
