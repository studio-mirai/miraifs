from hashlib import blake2b
from miraifs_sdk import MIRAIFS_PACKAGE_ID
from miraifs_sdk.models import Chunk, File
from pysui import SyncClient, handle_result
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_types import ObjectID, SuiAddress, SuiString, SuiU8, SuiU32


def create_file_txb(
    chunk_size: int,
    chunks: list[Chunk],
    chunks_manifest_hash: blake2b,
    mime_type: str,
    recipient: SuiAddress,
    client: SyncClient,
    gas_budget: int = 2_000_000_000,
) -> TxResponse:
    txer = SuiTransaction(
        client=client,
        merge_gas_budget=True,
    )
    file, verify_file_cap = txer.move_call(
        target=f"{MIRAIFS_PACKAGE_ID}::file::new",
        arguments=[
            SuiU32(chunk_size),
            SuiString(mime_type),
            [SuiU8(e) for e in list(chunks_manifest_hash.digest())],
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
        recipient=recipient,
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
        recipient=recipient,
    )
    result = handle_result(
        txer.execute(gas_budget=gas_budget),
    )
    return result


def delete_file_txb(
    file: File,
    client: SyncClient,
    gas_budget: int = 2_000_000_000,
) -> TxResponse:
    txer = SuiTransaction(
        client=client,
        merge_gas_budget=True,
    )
    for item in file.chunks.manifest:
        txer.move_call(
            target=f"{MIRAIFS_PACKAGE_ID}::file::receive_and_drop_chunk",
            arguments=[
                ObjectID(file.id),
                ObjectID(item.id),
            ],
        )
    txer.move_call(
        target=f"{MIRAIFS_PACKAGE_ID}::file::destroy_empty",
        arguments=[
            ObjectID(file.id),
        ],
    )
    result = handle_result(txer.execute(gas_budget=gas_budget))
    return result


def freeze_file_txb(
    file: File,
    client: SyncClient,
    gas_budget: int = 1_000_000_000,
):
    txer = SuiTransaction(
        client=client,
        merge_gas_budget=True,
    )
    txer.move_call(
        target="0x2::transfer::public_freeze_object",
        arguments=[ObjectID(file.id)],
        type_arguments=[f"{MIRAIFS_PACKAGE_ID}::file::File"],
    )
    result = handle_result(txer.execute(gas_budget=gas_budget))
    return result
