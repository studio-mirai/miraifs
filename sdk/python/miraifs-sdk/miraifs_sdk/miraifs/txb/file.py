from miraifs_sdk import MIRAIFS_PACKAGE_ID
from pysui import SyncClient, handle_result
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_types import (
    ObjectID,
    SuiString,
    SuiU64,
    SuiU256,
    SuiAddress,
)
from miraifs_sdk.models import File


def create_file_txb(
    chunk_hashes: list[int],
    chunk_lengths: list[int],
    mime_type: str,
    recipient: SuiAddress,
    client: SyncClient,
    gas_budget: int = 2_000_000_000,
) -> TxResponse:
    txer = SuiTransaction(
        client=client,
        merge_gas_budget=True,
    )
    chunk_hashes_vector = txer.make_move_vector(
        items=[SuiU256(hash) for hash in chunk_hashes],
        item_type="u256",
    )
    chunk_lengths_vector = txer.make_move_vector(
        items=[SuiU64(length) for length in chunk_lengths],
        item_type="u64",
    )
    file = txer.move_call(
        target=f"{MIRAIFS_PACKAGE_ID}::file::new",
        arguments=[
            SuiString(mime_type),
            SuiU64(len(chunk_hashes)),
        ],
    )
    txer.move_call(
        target=f"{MIRAIFS_PACKAGE_ID}::file::add_chunk_hashes",
        arguments=[
            file,
            chunk_hashes_vector,
            chunk_lengths_vector,
        ],
    )
    txer.transfer_objects(
        transfers=[file],
        recipient=recipient,
    )
    result = handle_result(txer.execute(gas_budget=gas_budget))
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
