import base64
import json
import mimetypes
import itertools
from hashlib import blake2b, md5
from pathlib import Path
from enum import Enum
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
    chunk_bytes,
    compress_data,
    decompress_data,
    encode_file,
    split_lists_into_sublists,
)
from miraifs_sdk import DOWNLOADS_DIR

from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_types import SuiU8, SuiBoolean, SuiString, SuiU256


app = typer.Typer()


class FileEncodingSchemes(str, Enum):
    base64 = "base64"
    base85 = "base85"


TEST_PACKAGE_ID = "0xf0d110a18df07a23b36365d41a4641cddbdf2a998b0cad0cd5a35750f5c5ed3a"

# computation_cost=1000000, non_refundable_storage_fee=9880, storage_cost=2766400, storage_rebate=978120
# computation_cost=1000000, non_refundable_storage_fee=9880, storage_cost=2546000, storage_rebate=978120)

config = SuiConfig.default_config()
client = SyncClient(config)


@app.command()
def vec_test(verify: bool = typer.Option(False)):
    with open("/Users/brianli/Desktop/machin.jpg", "rb") as f:
        data = f.read()

    hash = calculate_hash_for_bytes(data)
    hash_u256 = int.from_bytes(hash.digest(), "big")

    # Split bytes into 256 byte chunks.
    byte_vectors: list[list[int]] = split_lists_into_sublists(list(data), 256)
    chunked_byte_vectors = split_lists_into_sublists(byte_vectors, 128)

    for chunked_byte_vector in chunked_byte_vectors:
        txer = SuiTransaction(
            client=client,
            compress_inputs=True,
        )

        chunk, cap = txer.move_call(
            target=f"{TEST_PACKAGE_ID}::chunk::new",
            arguments=[SuiU256(hash_u256), SuiBoolean(verify)],
        )

        for v in chunked_byte_vector:
            data = txer.make_move_vector(
                items=[SuiU8(i) for i in v],
                item_type="u8",
            )

            txer.move_call(
                target=f"{TEST_PACKAGE_ID}::chunk::insert_data",
                arguments=[chunk, data],
            )

        txer.move_call(
            target=f"{TEST_PACKAGE_ID}::chunk::verify",
            arguments=[cap, chunk],
        )

        txer.transfer_objects(
            transfers=[chunk],
            recipient=config.active_address,
        )

        result = handle_result(txer.execute(gas_budget=10_000_000_000))
        print(result.effects.status, result.effects.gas_used)
        print(result.effects.d)

    return


@app.command()
def receive(
    file_id: str = typer.Argument(...),
):
    mfs = MiraiFs()
    create_chunk_cap_ids = mfs.get_create_image_chunk_cap_ids_for_file(
        file_id,
    )
    result = mfs.receive_create_chunk_caps(
        file_id,
        create_chunk_cap_ids,
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
    verify_hash_onchain: bool = typer.Option(False),
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

    create_chunk_cap_ids = mfs.get_create_image_chunk_cap_ids_for_file(onchain_file.id)

    create_chunk_caps = [
        mfs.get_create_image_chunk_cap(cap_id) for cap_id in create_chunk_cap_ids
    ]

    for create_chunk_cap in create_chunk_caps:
        owner = mfs.get_owner_address(create_chunk_cap.id)
        if owner == onchain_file.id:
            typer.confirm(
                text="CreateChunkCap objects are owned by the parent file. Would you like to receive them now?",
                abort=True,
            )
            result = mfs.receive_create_chunk_caps(
                file_id,
                create_chunk_caps,
            )
            if len(result.errors) == 0:
                print(f"Successfully received CreateChunkCap objects for file {onchain_file.id}!")  # fmt: skip
                break
            else:
                raise typer.Exit(f"Failed to receive CreateChunkCap objects for file {onchain_file.id}!")  # fmt: skip
        elif owner != mfs.config.active_address.address:
            raise typer.Exit(f"CreateChunkCap {create_chunk_cap.id} is owned by another address.")  # fmt: skip

    file_chunks_by_hash = {
        blake2b("".join(sublist).encode("utf-8"), digest_size=32).hexdigest(): sublist
        for sublist in file_chunks_sublists
    }

    print(f"\nPreparing to upload {len(file_chunks_sublists)} file chunks...")

    if verify_hash_onchain:
        gas_coin_value = 5
    else:
        gas_coin_value = 1

    print(f"\nFinding {len(file_chunks_sublists)} {gas_coin_value} SUI gas coins...")

    all_gas_coins = mfs.get_all_gas_coins()
    time.sleep(1)
    mfs.merge_coins(all_gas_coins)
    time.sleep(1)
    all_gas_coins = mfs.get_all_gas_coins()
    time.sleep(1)
    gas_coins = mfs.split_coin(
        mfs.find_largest_gas_coin(all_gas_coins),
        len(create_chunk_caps),
        gas_coin_value,
    )

    print(f"Found {len(gas_coins)} for file chunk uploads!")

    if len(gas_coins) != len(create_chunk_caps):
        raise typer.Exit("Not enough gas coins to upload file chunks!")

    with ThreadPoolExecutor(max_workers=min(len(create_chunk_caps), 16)) as executor:
        futures = []
        for i, create_chunk_cap in enumerate(create_chunk_caps):
            future = executor.submit(
                mfs.create_chunk,
                create_chunk_cap,
                file_chunks_by_hash[create_chunk_cap.hash],
                verify_hash_onchain,
                gas_coin=gas_coins[i],
            )
            futures.append(future)

    file_chunk_ids: list[Chunk] = []

    for future in futures:
        result = future.result()  # Wait for the task to complete and get the result
        # print(result.effects.status, result.effects.transaction_digest)
        if isinstance(result, TxResponse):
            if len(result.errors) == 0:
                print(f"SUCCESS: {result.effects.transaction_digest}!")  # fmt: skip
                for event in result.events:
                    if event.event_type == f"{PACKAGE_ID}::file::ChunkCreatedEvent":  # fmt: skip
                        event_json = json.loads(event.parsed_json.replace("'", '"'))
                        file_chunk_ids.append(event_json["id"])
            else:
                print(f"FAILED: {result.effects.transaction_digest}!")

    print(file_chunk_ids)
    print(f"\nSUCCESS: Uploaded {len(file_chunk_ids)} file chunks for file {file_id}!")  # fmt: skip


@app.command()
def register(
    file_id: str = typer.Argument(...),
):
    mfs = MiraiFs()

    file = mfs.get_file(file_id)

    register_file_chunk_caps = mfs.get_register_file_chunk_caps_for_file(file.id)

    if len(register_file_chunk_caps) == 0:
        raise typer.Exit("No RegisterChunkCap objects found for this file.")

    result = mfs.receive_and_register_file_chunks(
        file,
        register_file_chunk_caps,
    )

    if len(result.errors) == 0:
        file = mfs.get_file(file_id)
        print(file)
        print(f"\nSUCCESS: Registered {len(register_file_chunk_caps)} file chunks for {file.id}!")  # fmt: skip


@app.command()
def create(
    path: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="The path to the file to initialize.",
    ),
    confirm: bool = typer.Option(
        False,
        help="Confirm file upload details are correct.",
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
    original_file_hash = calculate_hash_u256(data)
    print(f"File Hash: {original_file_hash}")

    data_to_upload = data

    chunks = chunk_bytes(data_to_upload, 32768)
    chunk_hashes = [calculate_hash_u256(b) for b in chunks]

    file_size_bytes = len(data)
    mime_type, _ = mimetypes.guess_type(path)

    # print(file_upload_data.model_dump(exclude=["chunk_hashes"]))
    print(f"\nFile Size: {file_size_bytes}B ({round(file_size_bytes / 1024)}KB)")
    print(f"File Chunks: {len(chunks)}")

    if not confirm:
        typer.confirm(
            "\nPlease confirm file upload details are correct.",
            abort=True,
        )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(description="Broadcasting transaction...", total=2)
        result = mfs.create_file(chunk_hashes, mime_type)

    if isinstance(result, TxResponse):
        file_created_event_data = None
        create_chunk_cap_created_event_data = []

        for event in result.events:
            event_data = json.loads(event.parsed_json.replace("'", '"'))
            if event.event_type == f"{PACKAGE_ID}::file::FileCreatedEvent":  # fmt: skip
                file_created_event_data = event_data
            if event.event_type == f"{PACKAGE_ID}::file::CreateChunkCapCreatedEvent":  # fmt: skip
                create_chunk_cap_created_event_data.append(event_data)

        file = mfs.get_file(file_created_event_data["id"])
        create_chunk_caps: list[CreateChunkCap] = []
        for event_data in create_chunk_cap_created_event_data:
            create_chunk_caps.append(
                CreateChunkCap(
                    id=event_data["id"],
                    index=int(event_data["index"]),
                    hash=event_data["hash"],
                    file_id=file.id,
                ),
            )

        print("\nThe file below has been initialized successfully!")
        print(f"\n{file.model_dump_json(indent=4)}")
        print("\nUse the CreateChunkCap objects below to upload the file chunks.")
        print(f"\n{json.dumps([c.model_dump() for c in create_chunk_caps], indent=4)}")  # fmt: skip

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
    concurrency: int = typer.Option(
        8,
        min=1,
        max=32,
        help="The number of concurrent file chunk downloads to perform.",
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

    file_chunks: list[Chunk] = []
    with ThreadPoolExecutor(
        max_workers=min(len(file_chunk_ids), concurrency)
    ) as executor:
        futures = []
        for file_chunk_id in file_chunk_ids:
            future = executor.submit(mfs.get_file_chunk, file_chunk_id)
            futures.append(future)

        for future in as_completed(futures):
            result = future.result()
            file_chunks.append(result)
            # chunk_strings.append("".join(result.data))

    file_chunks.sort(key=lambda x: x.index)

    print("Reconstructing file from chunks...")
    joined_data = "".join(["".join(chunk.data) for chunk in file_chunks])

    if file.config.compression_algorithm == "zstd":
        print(f"Decompressing file data with {file.config.compression_algorithm}...")
        data = decompress_data(joined_data)

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
