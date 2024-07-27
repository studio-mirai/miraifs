from pathlib import Path

import typer
from miraifs_sdk import DOWNLOADS_DIR, MAX_CHUNK_SIZE_BYTES
from miraifs_sdk.miraifs import MiraiFs
from pysui import SuiConfig, SyncClient
from pysui.sui.sui_types import SuiAddress
from rich import print

app = typer.Typer()

config = SuiConfig.default_config()
client = SyncClient(config)


@app.command()
def upload(
    path: Path = typer.Argument(...),
    chunk_size: int = typer.Option(MAX_CHUNK_SIZE_BYTES),
    recipient: str = typer.Option(None),
):
    print(f"File Path: {path}")
    print(f"Chunk Size: {chunk_size}")
    if recipient:
        print(f"File Recipient: {recipient}")
    typer.confirm("Please confirm the upload settings:", abort=True)

    mfs = MiraiFs()

    if recipient:
        recipient = SuiAddress(recipient)
    if not recipient:
        recipient = mfs.config.active_address

    print("Creating file...")
    file, path = mfs.create_file(
        path,
        chunk_size,
        recipient=recipient,
    )

    print(f"Uploading chunks for file {file.id}")
    mfs.upload_chunks(file, path)
    print(f"Registering chunks for file {file.id}")
    mfs.register_chunks(file)

    file = mfs.get_file(file.id)
    print(file)

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
