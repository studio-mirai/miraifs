import base64
import json
import mimetypes
from hashlib import blake2b, md5
from pathlib import Path
from enum import Enum
import typer
from pysui.sui.sui_txresults.complex_tx import TxResponse
from rich import print
from rich.progress import Progress, SpinnerColumn, TextColumn
from concurrent.futures import ThreadPoolExecutor
from miraifs_sdk import PACKAGE_ID
from miraifs_sdk.miraifs import CreateFileChunkCap, FileUploadData, MiraiFs, FileChunk
from miraifs_sdk.utils import (
    calculate_hash,
    calculate_hash_for_bytes,
    chunk_file_data,
    compress_data,
    decompress_data,
    encode_file,
    split_lists_into_sublists,
)
from miraifs_sdk import DOWNLOADS_DIR

app = typer.Typer()


class FileEncodingSchemes(str, Enum):
    base64 = "base64"
    base85 = "base85"


@app.command()
def create_image_chunk_caps(
    file_id: str = typer.Argument(...),
):
    mfs = MiraiFs()
    create_image_chunk_cap_ids = mfs.get_create_image_chunk_cap_ids_for_file(file_id)
    print(create_image_chunk_cap_ids)


@app.command()
def receive(
    file_id: str = typer.Argument(...),
):
    mfs = MiraiFs()
    create_file_chunk_cap_ids = mfs.get_create_image_chunk_cap_ids_for_file(
        file_id,
    )
    result = mfs.receive_create_file_chunk_caps(
        file_id,
        create_file_chunk_cap_ids,
    )
    print(result)
    return


@app.command()
def upload(
    path: Path = typer.Argument(
        ...,
        help="The path to the file to initialize.",
    ),
    file_id: str = typer.Argument(),
    verify_hash_onchain: bool = typer.Option(True),
):
    mfs = MiraiFs()

    print(f"Fetching details for {file_id}")

    with open(path, "rb") as f:
        data = f.read()

    data_to_upload = data

    onchain_file = mfs.get_file(file_id)
    local_file_hash = calculate_hash_for_bytes(data)

    if onchain_file.config.compression_algorithm == "zstd":
        data_to_upload = compress_data(data, onchain_file.config.compression_level)

    if onchain_file.hash != local_file_hash:
        raise typer.Exit("Onchain file hash does not match local file hash.")

    # Create an array of the expected chunk hashes stored in the onchain file.
    onchain_chunk_hashes = [chunk.key for chunk in onchain_file.chunks]

    file_data = encode_file(data_to_upload, onchain_file.encoding)
    file_chunks = chunk_file_data(file_data, onchain_file.config.chunk_size)
    file_chunks_sublists = split_lists_into_sublists(
        file_chunks,
        onchain_file.config.sublist_size,
    )
    local_chunk_hashes = [
        blake2b("".join(sublist).encode("utf-8"), digest_size=32).hexdigest()
        for sublist in file_chunks_sublists
    ]

    # Ensure the onchain chunk hashes match the local chunk hashes.
    local_chunks_hash = md5(json.dumps(sorted(onchain_chunk_hashes)).encode()).hexdigest()  # fmt: skip
    onchain_chunks_hash = md5(json.dumps(sorted(local_chunk_hashes)).encode()).hexdigest()  # fmt: skip

    if local_chunks_hash != onchain_chunks_hash:
        raise typer.Exit("Onchain chunk hashes do not match local chunk hashes.")

    create_file_chunk_cap_ids = mfs.get_create_image_chunk_cap_ids_for_file(
        onchain_file.id
    )

    create_file_chunk_caps = [
        mfs.get_create_image_chunk_cap(cap_id) for cap_id in create_file_chunk_cap_ids
    ]

    for create_file_chunk_cap in create_file_chunk_caps:
        owner = mfs.get_owner_address(create_file_chunk_cap.id)
        if owner == onchain_file.id:
            typer.confirm(
                text="CreateFileChunkCap objects are owned by the parent file. Would you like to receive them now?",
                abort=True,
            )
            result = mfs.receive_create_file_chunk_caps(
                file_id,
                create_file_chunk_caps,
            )
            if len(result.errors) == 0:
                print(f"Successfully received CreateFileChunkCap objects for file {onchain_file.id}!")  # fmt: skip
                break
            else:
                raise typer.Exit(f"Failed to receive CreateFileChunkCap objects for file {onchain_file.id}!")  # fmt: skip
        elif owner != mfs.config.active_address.address:
            raise typer.Exit(f"CreateFileChunkCap {create_file_chunk_cap.id} is owned by another address.")  # fmt: skip

    file_chunks_by_hash = {
        blake2b("".join(sublist).encode("utf-8"), digest_size=32).hexdigest(): sublist
        for sublist in file_chunks_sublists
    }

    gas_coins = mfs.request_gas_coins(len(create_file_chunk_caps), 5)
    print("\nFinding gas coins...")
    print(gas_coins)
    with ThreadPoolExecutor(
        max_workers=min(len(create_file_chunk_caps), 16)
    ) as executor:
        futures = []
        for i, create_file_chunk_cap in enumerate(create_file_chunk_caps):
            future = executor.submit(
                mfs.create_file_chunk,
                create_file_chunk_cap,
                file_chunks_by_hash[create_file_chunk_cap.hash],
                verify_hash_onchain,
                gas_coin=gas_coins[i],
            )
            futures.append(future)

    file_chunk_ids: list[FileChunk] = []

    for future in futures:
        result = future.result()  # Wait for the task to complete and get the result
        # print(result.effects.status, result.effects.transaction_digest)
        if isinstance(result, TxResponse):
            if len(result.errors) == 0:
                print(f"SUCCESS: {result.effects.transaction_digest}!")  # fmt: skip
                for event in result.events:
                    if event.event_type == f"{PACKAGE_ID}::file::FileChunkCreatedEvent":  # fmt: skip
                        event_json = json.loads(event.parsed_json.replace("'", '"'))
                        file_chunk_ids.append(event_json["id"])
            else:
                print(f"FAILED: {result.effects.transaction_digest}!")

    print("\nFile chunks created successfully!")
    print(file_chunk_ids)


@app.command()
def register(
    file_id: str = typer.Argument(...),
):
    mfs = MiraiFs()

    file = mfs.get_file(file_id)

    register_file_chunk_caps = mfs.get_register_file_chunk_caps_for_file(file.id)

    if len(register_file_chunk_caps) == 0:
        raise typer.Exit("No RegisterFileChunkCap objects found for this file.")

    result = mfs.receive_and_register_file_chunks(
        file,
        register_file_chunk_caps,
    )

    if len(result.errors) == 0:
        print(f"Successfully registered file chunks for {file.id}!")
        file = mfs.get_file(file_id)
        print(file)


@app.command()
def create(
    path: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="The path to the file to initialize.",
    ),
    encoding: FileEncodingSchemes = typer.Option(
        FileEncodingSchemes.base85,
        help="The encoding type to use for the file (b64 or base85).",
    ),
    confirm: bool = typer.Option(
        False,
        help="Confirm file upload details are correct.",
    ),
    compression: int = typer.Option(
        9,
        min=1,
        max=22,
        help="The zstd compression level to use.",
    ),
    chunk_size: int = typer.Option(
        230,
        min=1,
        max=230,
        help="The size of each file chunk.",
    ),
    sublist_size: int = typer.Option(
        511,
        min=1,
        max=511,
        help="The size of each sublist.",
    ),
):
    """
    Initialize a file upload by creating a MiraiFS `File` object
    that contains file metadata and the expected file chunk hashes.
    """
    mfs = MiraiFs()

    with open(path, "rb") as f:
        data = f.read()

    # Calculate the hash of the original uncompressed file.
    original_file_hash = calculate_hash_for_bytes(data)
    data_to_upload = data
    compressed_data = compress_data(data, compression)

    potential_savings = len(data) - len(compressed_data)
    if potential_savings > 0:
        comp_conf = typer.confirm(f"Would you like to compress this file to reduce filesize by {potential_savings / 1024}KB")  # fmt: skip
        if comp_conf:
            # Overwrite data_to_upload with compressed data.
            data_to_upload = compressed_data

    file_data = encode_file(data_to_upload, encoding.value)
    file_size_bytes = len(file_data)
    file_chunks = chunk_file_data(file_data, chunk_size)
    file_chunks_sublists = split_lists_into_sublists(file_chunks, sublist_size)

    file_chunk_hashes = [
        calculate_hash("".join(sublist)) for sublist in file_chunks_sublists
    ]

    mime_type, _ = mimetypes.guess_type(path)
    extension = path.suffix.lower().replace(".", "")

    file_upload_data = FileUploadData(
        encoding=encoding.value,
        mime_type=mime_type,
        extension=extension,
        size=file_size_bytes,
        hash=original_file_hash,
        chunk_size=chunk_size,
        sublist_size=sublist_size,
        chunk_hashes=file_chunk_hashes,
        compression_algorithm="zstd",
        compression_level=compression,
    )

    print(f"File Size: {file_size_bytes}B ({round(file_size_bytes / 1024)}KB)")
    print(f"File Chunks: {len(file_chunk_hashes)}")
    print(file_upload_data.model_dump_json(indent=4))

    if not confirm:
        typer.confirm(
            "Please confirm file upload details are correct.",
            abort=True,
        )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(description="Broadcasting transaction...", total=2)
        result = mfs.create_file(file_upload_data)

    if isinstance(result, TxResponse):
        file_created_event_data = None
        create_file_chunk_cap_created_event_data = []

        for event in result.events:
            event_data = json.loads(event.parsed_json.replace("'", '"'))
            if event.event_type == f"{PACKAGE_ID}::file::FileCreatedEvent":  # fmt: skip
                file_created_event_data = event_data
            if event.event_type == f"{PACKAGE_ID}::file::CreateFileChunkCapCreatedEvent":  # fmt: skip
                create_file_chunk_cap_created_event_data.append(event_data)

        file = mfs.get_file(file_created_event_data["id"])
        print(file)

        create_file_chunk_caps: list[CreateFileChunkCap] = []
        for event_data in create_file_chunk_cap_created_event_data:
            create_file_chunk_caps.append(
                CreateFileChunkCap(
                    id=event_data["id"],
                    hash=event_data["hash"],
                    file_id=file.id,
                ),
            )

        print("\nThe file below has been initialized successfully!")
        print(f"\n{file.model_dump_json(indent=4)}")
        print("\nUse the CreateFileChunkCap objects below to upload the file chunks.")
        print(f"\n{json.dumps([c.model_dump() for c in create_file_chunk_caps], indent=4)}")  # fmt: skip

        print("\nUpload file chunks with the command below!")
        print(f"\nmfs upload {path} {file.id}")
    else:
        print("CRAP!")


@app.command()
def view(
    file_id: str = typer.Argument(...),
):
    mfs = MiraiFs()
    file = mfs.get_file(file_id)
    print(file)


@app.command()
def download(
    file_id: str = typer.Argument(
        ...,
        help="The object ID of the file to download.",
    ),
    file_name: str = typer.Option(
        None,
        help="The filename to save the file as.",
    ),
    output_dir: Path = typer.Option(
        DOWNLOADS_DIR,
        help="The output dir to download the file to. Defaults to ./downlodas.",
    ),
):
    """
    Download a MiraiFS file.

    Args:
        file_id (str): The file ID of the file to download.
        file_name (str): The name to save the file as.
        output_dir (Path): The output directory to save the file to.
    """
    mfs = MiraiFs()

    print(f"Fetching file {file_id}...")
    file = mfs.get_file(file_id)
    print(file)

    file_chunk_ids = [chunk.value for chunk in file.chunks]
    print(f"Downloading {len(file_chunk_ids)} file chunks...")

    chunk_strings: list[str] = []
    for file_chunk_id in file_chunk_ids:
        result = mfs.get_file_chunk(file_chunk_id)
        chunk_strings.append("".join(result.data))

    print("Reconstructing file from chunks...")
    joined_data = "".join(chunk_strings)

    print(f"Decoding file data from {file.encoding} to binary data...")
    if file.encoding == "base64":
        data = base64.b64decode(joined_data)
    elif file.encoding == "base85":
        data = base64.b85decode(joined_data)

    if file.config.compression_algorithm == "zstd":
        print(f"Decompressing file data with {file.config.compression_algorithm}...")
        data = decompress_data(data)

    print("Verifying data integrity...")
    downloaded_file_hash = calculate_hash_for_bytes(data)
    if downloaded_file_hash == file.hash:
        print("OK!")
    else:
        raise typer.Exit("File hash of downloaded file does not match expected hash.")

    if file_name:
        output_file_name = file_name
    elif file.name:
        output_file_name = f"{file.name}.{file.extension}"
    else:
        output_file_name = f"{file.id}.{file.extension}"

    with open(output_dir / output_file_name, "wb") as f:
        print(f"Saving file to ./downloads/{file.id}.{file.extension}")
        f.write(data)


@app.command()
def split_gas(
    quantity: int = typer.Argument(...),
    value: int = typer.Argument(...),
):
    mfs = MiraiFs()

    result = mfs.request_gas_coins(quantity, value)
    print(result)
    return
