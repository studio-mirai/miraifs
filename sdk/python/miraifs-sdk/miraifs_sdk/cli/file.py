from pathlib import Path

import typer
from pysui import SuiConfig, SyncClient
from pysui.sui.sui_types import SuiAddress
from rich import print

from miraifs_sdk import DOWNLOADS_DIR, MAX_CHUNK_SIZE_BYTES
from miraifs_sdk.miraifs import MiraiFs
from miraifs_sdk.utils import load_chunks

app = typer.Typer()

config = SuiConfig.default_config()
client = SyncClient(config)


@app.command()
def upload(
    path: Path = typer.Argument(...),
    chunk_size: int = typer.Option(MAX_CHUNK_SIZE_BYTES),
    recipient: str = typer.Option(None),
    concurrency: int = typer.Option(16),
    gas_budget_per_chunk: int = typer.Option(3_000_000_000, help="Gas budget per chunk in MIST"),
):  # fmt: skip
    mfs = MiraiFs()

    chunks = load_chunks(path, chunk_size)

    gas_coins = mfs.allocate_gas_coins(
        # Add two more gas coins, one for create_file, one fore register_chunks.
        len(chunks) + 2,
        gas_budget_per_chunk,
    )

    if len(gas_coins) != len(chunks) + 2:
        raise typer.Exit(f"Unable to allocate {len(chunks) + 2} gas coins.")

    if recipient:
        recipient = SuiAddress(recipient)
    if not recipient:
        recipient = mfs.config.active_address

    print(f"File Path: {path}")
    print(f"Chunk Size: {chunk_size}")
    print(f"File Recipient: {recipient}")
    print(f"Upload Concurrency: {concurrency}")
    print(f"Gas Budget Per Chunk: {gas_budget_per_chunk / 10**9} SUI")
    typer.confirm("Please confirm the upload settings:", abort=True)

    print("Creating file...")
    file, path = mfs.create_file(
        path,
        chunks,
        chunk_size,
        recipient=recipient,
        gas_coin=gas_coins.pop(0),
    )

    print(f"Uploading chunks for file {file.id}")
    mfs.upload_chunks(
        file,
        path,
        concurrency,
        [gas_coins.pop(0) for _ in range(len(chunks))],
    )
    print(f"Registering chunks for file {file.id}")
    mfs.register_chunks(
        file,
        gas_coin=gas_coins.pop(0),
    )

    file = mfs.get_file(file.id)
    print(file)

    gas_coins = mfs.get_all_gas_coins(mfs.config.active_address)
    print(gas_coins)
    mfs.merge_coins(gas_coins)

    print("File was uploaded successfully!")
    print(f"Download Link: https://miraifs.sm.xyz/{file.id}/")
    return


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
):
    mfs = MiraiFs()
    file = mfs.get_file(file_id)
    print(file)
    return


@app.command()
def list():
    mfs = MiraiFs()
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
